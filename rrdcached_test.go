package rrdcached

import (
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

const TEST_DIR = "/tmp"
const SOCKET = TEST_DIR + "/go-rrdcached-test.sock"
const RRD_FILE = TEST_DIR + "/go-rrdcached-test.rrd"

var defineDS = []string{"DS:test1:GAUGE:600:0:100", "DS:test2:GAUGE:600:0:100", "DS:test3:GAUGE:600:0:100", "DS:test4:GAUGE:600:0:100"}
var defineRRA = []string{"RRA:MIN:0.5:12:1440", "RRA:MAX:0.5:12:1440", "RRA:AVERAGE:0.5:1:1440"}
var rrdUpdates = []string{"10:20:30:40", "90:80:70:60", "25:35:45:55", "55:65:75:85"}

var (
	cmd         *exec.Cmd
	driver_port int64
	driver      *Rrdcached
	daemon      *tcptest.TCPTest
)

// ------------------------------------------
// Setup & Teardown

func testSetup(t *testing.T) {
	os.Remove(RRD_FILE) // Remove existing RRD file
	daemonStart()       // Start rrdcached daemon
	daemonConnect()     // Connect to rrdcached
	createFreshRRD(t)   // Create fresh RRD
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
			"-p", fmt.Sprintf("%v/go-rrdached-test-%d.pid", TEST_DIR, port),
			"-B", "-b", TEST_DIR,
			"-l", SOCKET,
			"-l", fmt.Sprintf("0.0.0.0:%d", port)}

		fmt.Printf("======\nDaemon: rrdcached %v\n-------\n", strings.Join(cmd_args, " "))

		cmd = exec.Command("rrdcached", cmd_args...)
		cmd.SysProcAttr = &syscall.SysProcAttr{
			Setpgid: true,
		}
		cmd.Run()
	}

	// daemon is a global, but err is new. Using "daemon, err :=" creates a local shadowed variable "daemon". Thus the tmp var.
	daemon_tmp, err := tcptest.Start(cmd_wrapper, 30*time.Second)
	if err != nil {
		fmt.Println("Failed to start rrdcached:", err)
		panic(err)
	}
	daemon = daemon_tmp
	fmt.Printf("rrdcached started on port %d\n", daemon.Port())
}

func daemonStop() {
	if cmd != nil && cmd.Process != nil {
		if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
			fmt.Println("SIGTERM failed:", err)
		}
		daemon.Wait()
	}
}

func daemonConnect() {
	driver = ConnectToSocket(SOCKET)
}

func createFreshRRD(t *testing.T) {
	resp := driver.Create(RRD_FILE, -1, -1, false, defineDS, defineRRA)
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
	fmt.Printf(">> STATS << %+v\n", stats)
	verifyStatsDiff(t, &Stats{}, stats, stats_diff)
}

func verifyStatsChange(t *testing.T, stats_pre *Stats, stats_post *Stats, stats_diff map[string]uint64) {
	fmt.Printf(">> PRE << %+v\n", stats_pre)
	fmt.Printf(">> POST << %+v\n", stats_post)
	verifyStatsDiff(t, stats_pre, stats_post, stats_diff)
}

func verifyStatsDiff(t *testing.T, stats_pre *Stats, stats_post *Stats, stats_diff map[string]uint64) {
	stats_pre_struct := reflect.Indirect(reflect.ValueOf(stats_pre))
	stats_post_struct := reflect.Indirect(reflect.ValueOf(stats_post))

	for key, expected_value := range stats_diff {
		value_pre := stats_pre_struct.FieldByName(key).Uint()
		value_post := stats_post_struct.FieldByName(key).Uint()

		actual_value := uint64(value_post - value_pre)

		fmt.Printf("%v: Expected %v, got %v\n", key, expected_value, actual_value)
		if expected_value != actual_value {
			t.Errorf("%v: Expected %v, got %v", key, expected_value, actual_value)
		}
	}
}

// ------------------------------------------
// Tests

func TestConnect(t *testing.T) {
	testSetup(t)
	defer testTeardown()

	test_driver1 := ConnectToSocket(SOCKET)
	test_driver1.Quit()

	test_driver2 := ConnectToIP("localhost", driver_port)
	test_driver2.Quit()
}

func TestCreate(t *testing.T) {
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

	resp1 := driver.Create(RRD_FILE, -1, -1, true, defineDS, defineRRA)
	verifySuccessResponse(t, resp1)
	verifyStatsFresh(t, initialStats)

	resp2 := driver.Create(RRD_FILE, now, -1, true, defineDS, defineRRA)
	verifySuccessResponse(t, resp2)
	verifyStatsFresh(t, initialStats)

	resp3 := driver.Create(RRD_FILE, now, 10, true, defineDS, defineRRA)
	verifySuccessResponse(t, resp3)
	verifyStatsFresh(t, initialStats)
}

func TestUpdate(t *testing.T) {
	testSetup(t)
	defer testTeardown()

	update_values1 := generateTimestamps(rrdUpdates)
	resp1 := driver.Update(RRD_FILE, update_values1...)
	verifyUpdateResponseForN(t, resp1, update_values1)

	update_values2 := generateTimestamps(rrdUpdates)
	resp2 := driver.Update(RRD_FILE, update_values2...)
	verifyUpdateResponseForN(t, resp2, update_values2)

	verifyStatsFresh(t, map[string]uint64{
		"UpdatesReceived": 2,
	})
}

func TestPending(t *testing.T) {
	testSetup(t)
	defer testTeardown()

	update_values := generateTimestamps(rrdUpdates)

	resp := driver.Update(RRD_FILE, update_values...)
	verifyUpdateResponseForN(t, resp, update_values)

	resp = driver.Pending(RRD_FILE)
	verifyPendingResponseForN(t, resp, update_values)

	verifyStatsFresh(t, map[string]uint64{
		"UpdatesReceived": 1,
		"UpdatesWritten":  0,
		"DataSetsWritten": 0,
	})
}

func TestFlush(t *testing.T) {
	testSetup(t)
	defer testTeardown()

	update_values := generateTimestamps(rrdUpdates)

	resp := driver.Update(RRD_FILE, update_values...)
	verifyUpdateResponseForN(t, resp, update_values)

	resp = driver.Flush(RRD_FILE)
	verifySuccessResponse(t, resp)

	verifyStatsFresh(t, map[string]uint64{
		"FlushesReceived": 1,
		"UpdatesReceived": 1,
	})
}

func TestFlushAll(t *testing.T) {
	testSetup(t)
	defer testTeardown()

	update_values := generateTimestamps(rrdUpdates)

	resp := driver.Update(RRD_FILE, update_values...)
	verifyUpdateResponseForN(t, resp, update_values)

	resp = driver.FlushAll()
	verifySuccessResponse(t, resp)

	verifyStatsFresh(t, map[string]uint64{
		"FlushesReceived": 0,
		"UpdatesReceived": 1,
	})
}

func TestForget(t *testing.T) {
	testSetup(t)
	defer testTeardown()

	update_values := generateTimestamps(rrdUpdates)

	resp := driver.Update(RRD_FILE, update_values...)
	verifyUpdateResponseForN(t, resp, update_values)

	resp = driver.Forget(RRD_FILE)
	verifySuccessResponse(t, resp)

	verifyStatsFresh(t, map[string]uint64{
		"FlushesReceived": 0,
		"UpdatesReceived": 1,
		"UpdatesWritten":  0,
		"DataSetsWritten": 0,
	})
}

func TestFirst(t *testing.T) {
	testSetup(t)
	defer testTeardown()

	resp1 := driver.First(RRD_FILE, 0)
	verifySuccessResponse(t, resp1)

	timestamp1, err1 := strconv.ParseUint(resp1.Message, 10, 64)
	if err1 != nil {
		t.Errorf("FIRST timestamp %v is not parseable: %v", resp1.Message, err1)
	}

	resp2 := driver.First(RRD_FILE, 1)
	verifySuccessResponse(t, resp2)

	timestamp2, err2 := strconv.ParseUint(resp2.Message, 10, 64)
	if err2 != nil {
		t.Errorf("FIRST timestamp %v is not parseable: %v", resp2.Message, err2)
	}

	if timestamp1 != timestamp2 {
		t.Errorf("FIRST timestamps are not consistent: %d != %d", timestamp1, timestamp2)
	}
}

func TestLast(t *testing.T) {
	testSetup(t)
	defer testTeardown()

	resp := driver.Last(RRD_FILE)
	verifySuccessResponse(t, resp)

	_, err := strconv.ParseUint(resp.Message, 10, 64)
	if err != nil {
		t.Errorf("LAST timestamp %v is not parseable: %v", resp.Message, err)
	}
}
