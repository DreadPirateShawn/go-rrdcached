// +build integration

package rrdcached

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/lestrrat/go-tcptest"
)

var (
	testDir  = flag.String("tmpDir", "/tmp", "temp directory for test rrds (default /tmp)")
	logLevel = flag.Int("logLevel", 0, "logging threshold")
)

var (
	testSocket      = *testDir + "/go-rrdcached-test.sock"
	testRrdFile     = "foo-subdir/go-rrdcached-test.rrd"
	testRrdFileFull = *testDir + "/" + testRrdFile
)

var (
	defineDS   = []string{"DS:test1:GAUGE:600:0:100", "DS:test2:GAUGE:600:0:100", "DS:test3:GAUGE:600:0:100", "DS:test4:GAUGE:600:0:100"}
	defineRRA  = []string{"RRA:MIN:0.5:12:1440", "RRA:MAX:0.5:12:1440", "RRA:AVERAGE:0.5:1:1440"}
	rrdUpdates = []string{"10:20:30:40", "90:80:70:60", "25:35:45:55", "55:65:75:85"}
)

var (
	cmd         *exec.Cmd
	driver_port int64
	driver      *Rrdcached
	daemon      *tcptest.TCPTest
)

// ------------------------------------------
// Logging

func log(level int, msg string, values ...interface{}) {
	if level <= *logLevel {
		fmt.Printf("%v\n", fmt.Sprintf(msg, values...))
	}
}

// ------------------------------------------
// Setup & Teardown

func testSetup(t *testing.T) {
	testCleanup()
	daemonStart()     // Start rrdcached daemon
	daemonConnect()   // Connect to rrdcached
	createFreshRRD(t) // Create fresh RRD
}

func testCleanup() {
	os.Remove(testRrdFileFull) // Remove existing RRD file
}

func testTeardown() {
	driver.Quit() // Not strictly necessary, but it feels nice to call this.
	daemonStop()  // Stop rrdcached daemon
}

func daemonStart() {
	// Note: '-g' flag is crucial ("run in foreground"), otherwise rrdcached will run as a forked child process,
	// and the parent (cmd) will exit and orphan the rrdcached process, leaving no (sane) way to tear it down.
	cmd_wrapper := func(port int) {
		driver_port = int64(port)

		var cmd_args = []string{"-g",
			"-R",
			"-p", fmt.Sprintf("%v/go-rrdached-test-%d.pid", *testDir, port),
			"-B", "-b", *testDir,
			"-l", testSocket,
			"-l", fmt.Sprintf("0.0.0.0:%d", port)}

		log(0, "Daemon: rrdcached %v", strings.Join(cmd_args, " "))

		cmd = exec.Command("rrdcached", cmd_args...)
		cmd.SysProcAttr = &syscall.SysProcAttr{
			Setpgid: true,
		}
		cmd.Run()
	}

	// daemon is a global, but err is new. Using "daemon, err :=" creates a local shadowed variable "daemon". Thus the tmp var.
	daemon_tmp, err := tcptest.Start(cmd_wrapper, 30*time.Second)
	if err != nil {
		log(0, "Daemon: Failed to start:", err)
		panic(err)
	}
	daemon = daemon_tmp
}

func daemonStop() {
	if cmd != nil && cmd.Process != nil {
		if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
			log(0, "SIGTERM failed:", err)
		}
		daemon.Wait()
	}
}

func daemonConnect() {
	driver = ConnectToSocket(testSocket)
}

func createFreshRRD(t *testing.T) {
	// Note: The '-O' flag is only recently supported, manually remove RRD file otherwise.
	resp, err := driver.Create(testRrdFile, -1, -1, false, defineDS, defineRRA)
	if err != nil {
		if cmderr, ok := err.(*UnrecognizedArgumentError); ok {
			if cmderr.BadArgument() == "-O" {
				log(3, "Warning: CREATE with '-O' is unsupported on this system.")
				os.Remove(testRrdFile)
				resp, _ = driver.Create(testRrdFile, -1, -1, true, defineDS, defineRRA)
			}
		}
	}
	verifySuccessResponse(t, resp)
}

// ------------------------------------------
// Helper Fixtures

func generateTimestamps(values []string) []string {
	for i := 0; i < len(values); i++ {
		values[i] = NowString() + ":" + values[i]
		time.Sleep(1 * time.Second)
	}
	return values
}

// ------------------------------------------
// Helper Validations

func verifySuccessResponse(t *testing.T, resp *Response) {
	if resp.Status != 0 {
		t.Errorf("Status %v != success (0), message: \"%v\"", resp.Status, resp.Message)
	}
}

func verifyUpdateResponseForN(t *testing.T, resp *Response, update_values []string) {
	if resp.Status != 0 {
		t.Errorf("Status %v != success (0), message: \"%v\"", resp.Status, resp.Message)
	} else {
		update_count := int64(len(update_values))
		expected := "0 errors, enqueued " + strconv.FormatInt(update_count, 10)
		if !strings.Contains(resp.Raw, expected) {
			t.Errorf("Raw message response does not contain \"%v\" - actual: \"%v\"", expected, resp.Raw)
		}
	}
}

func verifyPendingResponseForN(t *testing.T, resp *Response, update_values []string) {
	if resp.Status < 0 {
		t.Errorf("Status error %v, message: \"%v\"", resp.Status, resp.Message)
	} else {
		expected := strconv.FormatInt(int64(len(update_values)), 10) + " updates pending"
		if !strings.Contains(resp.Raw, expected) {
			t.Errorf("Raw message response does not contain \"%v\"", expected)
		}
		for i := 0; i < len(update_values); i++ {
			if !strings.Contains(resp.Raw, update_values[i]) {
				t.Errorf("Update value \"%v\" not found in pending updates", update_values[i])
			}
		}
		if t.Failed() {
			t.Logf("Raw pending updates:\n%v", resp.Raw)
		}
	}
}

func verifyStatsFresh(t *testing.T, stats_diff map[string]uint64) {
	stats := driver.GetStats()
	log(5, ">> STATS << %+v", stats)
	verifyStatsDiff(t, &Stats{}, stats, stats_diff)
}

func verifyStatsChange(t *testing.T, stats_pre *Stats, stats_post *Stats, stats_diff map[string]uint64) {
	log(5, ">> PRE << %+v", stats_pre)
	log(5, ">> POST << %+v", stats_post)
	verifyStatsDiff(t, stats_pre, stats_post, stats_diff)
}

func verifyStatsDiff(t *testing.T, stats_pre *Stats, stats_post *Stats, stats_diff map[string]uint64) {
	stats_pre_struct := reflect.Indirect(reflect.ValueOf(stats_pre))
	stats_post_struct := reflect.Indirect(reflect.ValueOf(stats_post))

	for key, expected_value := range stats_diff {
		value_pre := stats_pre_struct.FieldByName(key).Uint()
		value_post := stats_post_struct.FieldByName(key).Uint()

		actual_value := uint64(value_post - value_pre)

		log(5, "%v: Expected %v, got %v", key, expected_value, actual_value)
		if expected_value != actual_value {
			t.Errorf("%v: Expected %v, got %v", key, expected_value, actual_value)
		}
	}
}

// ------------------------------------------
// Tests

func TestIntegrationConnect(t *testing.T) {
	testSetup(t)
	defer testTeardown()

	test_driver1 := ConnectToSocket(testSocket)
	test_driver1.Quit()

	test_driver2 := ConnectToIP("localhost", driver_port)
	test_driver2.Quit()
}

func TestIntegrationCreate(t *testing.T) {
	testSetup(t)
	defer testTeardown()

	initialStats := map[string]uint64{
		"QueueLength":     0,
		"UpdatesReceived": 0,
		"FlushesReceived": 0,
		"UpdatesWritten":  0,
		"DataSetsWritten": 0,
		"TreeNodesNumber": 0,
		"TreeDepth":       0,
		"JournalBytes":    0,
		"JournalRotate":   0,
	}

	now := int64(float64(time.Now().UnixNano()) / float64(time.Second))

	testCleanup()
	resp1, _ := driver.Create(testRrdFile, -1, -1, true, defineDS, defineRRA)
	verifySuccessResponse(t, resp1)
	verifyStatsFresh(t, initialStats)

	testCleanup()
	resp2, _ := driver.Create(testRrdFile, now, -1, true, defineDS, defineRRA)
	verifySuccessResponse(t, resp2)
	verifyStatsFresh(t, initialStats)

	testCleanup()
	resp3, _ := driver.Create(testRrdFile, now, 10, true, defineDS, defineRRA)
	verifySuccessResponse(t, resp3)
	verifyStatsFresh(t, initialStats)
}

func TestIntegrationUpdate(t *testing.T) {
	testSetup(t)
	defer testTeardown()

	update_values1 := generateTimestamps(rrdUpdates)
	resp1, _ := driver.Update(testRrdFile, update_values1...)
	verifyUpdateResponseForN(t, resp1, update_values1)

	update_values2 := generateTimestamps(rrdUpdates)
	resp2, _ := driver.Update(testRrdFile, update_values2...)
	verifyUpdateResponseForN(t, resp2, update_values2)

	verifyStatsFresh(t, map[string]uint64{
		"UpdatesReceived": 2,
	})
}

func TestIntegrationUpdateWithoutExistingRRD(t *testing.T) {
	testSetup(t)
	testCleanup()
	defer testTeardown()

	update_values1 := generateTimestamps(rrdUpdates)
	_, err := driver.Update(testRrdFile, update_values1...)
	if err == nil {
		t.Error("Error expected from Update of a non-existing RRD file, but no error received.")
	}
	if _, ok := err.(*FileDoesNotExistError); !ok {
		t.Errorf("FileDoesNotExistError expected from Update of a non-existing RRD file, but %T received.", err)
	}
}

func TestIntegrationPending(t *testing.T) {
	testSetup(t)
	defer testTeardown()

	update_values := generateTimestamps(rrdUpdates)

	resp, _ := driver.Update(testRrdFile, update_values...)
	verifyUpdateResponseForN(t, resp, update_values)

	resp, _ = driver.Pending(testRrdFile)
	verifyPendingResponseForN(t, resp, update_values)

	verifyStatsFresh(t, map[string]uint64{
		"UpdatesReceived": 1,
		"UpdatesWritten":  0,
		"DataSetsWritten": 0,
	})
}

func TestIntegrationFlush(t *testing.T) {
	testSetup(t)
	defer testTeardown()

	update_values := generateTimestamps(rrdUpdates)

	resp, _ := driver.Update(testRrdFile, update_values...)
	verifyUpdateResponseForN(t, resp, update_values)

	resp, _ = driver.Flush(testRrdFile)
	verifySuccessResponse(t, resp)

	verifyStatsFresh(t, map[string]uint64{
		"FlushesReceived": 1,
		"UpdatesReceived": 1,
	})
}

func TestIntegrationFlushAll(t *testing.T) {
	testSetup(t)
	defer testTeardown()

	update_values := generateTimestamps(rrdUpdates)

	resp, _ := driver.Update(testRrdFile, update_values...)
	verifyUpdateResponseForN(t, resp, update_values)

	resp, _ = driver.FlushAll()
	verifySuccessResponse(t, resp)

	verifyStatsFresh(t, map[string]uint64{
		"FlushesReceived": 0,
		"UpdatesReceived": 1,
	})
}

func TestIntegrationForget(t *testing.T) {
	testSetup(t)
	defer testTeardown()

	update_values := generateTimestamps(rrdUpdates)

	resp, _ := driver.Update(testRrdFile, update_values...)
	verifyUpdateResponseForN(t, resp, update_values)

	resp, _ = driver.Forget(testRrdFile)
	verifySuccessResponse(t, resp)

	verifyStatsFresh(t, map[string]uint64{
		"FlushesReceived": 0,
		"UpdatesReceived": 1,
		"UpdatesWritten":  0,
		"DataSetsWritten": 0,
	})
}

func TestIntegrationFirst(t *testing.T) {
	testSetup(t)
	defer testTeardown()

	// Note: The FIRST command is only recently supported.
	// Tests included for completeness, but lack of support shouldn't cause test failure.
	resp1, cmderr := driver.First(testRrdFile, 0)
	if cmderr != nil {
		if _, ok := cmderr.(*UnknownCommandError); ok {
			log(3, "Warning: FIRST is unsupported on this system.")
			return
		}
	}
	verifySuccessResponse(t, resp1)

	timestamp1, err1 := strconv.ParseUint(resp1.Message, 10, 64)
	if err1 != nil {
		t.Errorf("FIRST timestamp %v is not parseable: %v", resp1.Message, err1)
	}

	resp2, _ := driver.First(testRrdFile, 1)
	verifySuccessResponse(t, resp2)

	timestamp2, err2 := strconv.ParseUint(resp2.Message, 10, 64)
	if err2 != nil {
		t.Errorf("FIRST timestamp %v is not parseable: %v", resp2.Message, err2)
	}

	if timestamp1 != timestamp2 {
		t.Errorf("FIRST timestamps are not consistent: %d != %d", timestamp1, timestamp2)
	}
}

func TestIntegrationLast(t *testing.T) {
	testSetup(t)
	defer testTeardown()

	// Note: The LAST command is only recently supported.
	// Tests included for completeness, but lack of support shouldn't cause test failure.
	resp, cmderr := driver.Last(testRrdFile)
	if cmderr != nil {
		if _, ok := cmderr.(*UnknownCommandError); ok {
			log(3, "Warning: LAST is unsupported on this system.")
			return
		}
	}
	verifySuccessResponse(t, resp)

	_, err := strconv.ParseUint(resp.Message, 10, 64)
	if err != nil {
		t.Errorf("LAST timestamp %v is not parseable: %v", resp.Message, err)
	}
}
