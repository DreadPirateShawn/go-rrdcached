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

var cmd *exec.Cmd
var rrdcached *Rrdcached
var daemon *tcptest.TCPTest

// ------------------------------------------
// Setup & Teardown

func testSetup(t *testing.T) {
	os.Remove(RRD_FILE) // Remove existing RRD file
	daemonStart()       // Start rrdcached daemon
	daemonConnect()     // Connect to rrdcached
	createFreshRRD(t)   // Create fresh RRD
}

func testTeardown() {
	rrdcached.Quit() // Not strictly necessary, but it feels nice to call this.
	daemonStop()     // Stop rrdcached daemon
}

func daemonStart() {
	// Note: '-g' flag is crucial ("run in foreground"), otherwise rrdcached will run as a forked child process,
	// and the parent (cmd) will exit and orphan the rrdcached process, leaving no (sane) way to tear it down.
	cmd_wrapper := func(port int) {
		cmd = exec.Command("rrdcached", "-g",
			"-p", fmt.Sprintf("%v/go-rrdached-test-%d.pid", TEST_DIR, port),
			"-B", "-b", TEST_DIR,
			"-l", SOCKET,
			"-l", fmt.Sprintf("0.0.0.0:%d", port))
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
	rrdcached = NewRrdcached("unix", SOCKET)
	rrdcached.Connect()
}

func createFreshRRD(t *testing.T) {
	ds := []string{"DS:test1:GAUGE:600:0:100", "DS:test2:GAUGE:600:0:100", "DS:test3:GAUGE:600:0:100", "DS:test4:GAUGE:600:0:100"}
	rra := []string{"RRA:MIN:0.5:12:1440", "RRA:MAX:0.5:12:1440", "RRA:AVERAGE:0.5:1:1440"}
	resp := rrdcached.Create(RRD_FILE, -1, -1, false, ds, rra)
	verifySuccessResponse(t, resp)
}

// ------------------------------------------
// Helper Fixtures

func generateTimestamps(values []string) {
	for i := 0; i < len(values); i++ {
		values[i] = NowString() + ":" + values[i]
		time.Sleep(1 * time.Second)
	}
}

func generateUpdateValues() []string {
	update_values := []string{"10:20:30:40", "90:80:70:60", "25:35:45:55", "55:65:75:85"}
	generateTimestamps(update_values)
	return update_values
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
	stats := rrdcached.GetStats()
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

func TestCreate(t *testing.T) {
	testSetup(t)
	defer testTeardown()

	ds := []string{"DS:test1:GAUGE:600:0:100", "DS:test2:GAUGE:600:0:100", "DS:test3:GAUGE:600:0:100", "DS:test4:GAUGE:600:0:100"}
	rra := []string{"RRA:MIN:0.5:12:1440", "RRA:MAX:0.5:12:1440", "RRA:AVERAGE:0.5:1:1440"}
	resp := rrdcached.Create(RRD_FILE, -1, -1, true, ds, rra)
	verifySuccessResponse(t, resp)

	verifyStatsFresh(t, map[string]uint64{
		"QueueLength":     0,
		"UpdatesReceived": 0,
		"FlushesReceived": 0,
		"UpdatesWritten":  0,
		"DataSetsWritten": 0,
		"TreeNodesNumber": 0,
		"TreeDepth":       0,
		"JournalBytes":    0,
		"JournalRotate":   0,
	})
}

func TestUpdate(t *testing.T) {
	testSetup(t)
	defer testTeardown()

	update_values1 := generateUpdateValues()
	resp1 := rrdcached.Update(RRD_FILE, update_values1...)
	verifyUpdateResponseForN(t, resp1, update_values1)

	update_values2 := generateUpdateValues()
	resp2 := rrdcached.Update(RRD_FILE, update_values2...)
	verifyUpdateResponseForN(t, resp2, update_values2)

	verifyStatsFresh(t, map[string]uint64{
		"UpdatesReceived": 2,
	})
}

func TestPending(t *testing.T) {
	testSetup(t)
	defer testTeardown()

	update_values := generateUpdateValues()

	resp := rrdcached.Update(RRD_FILE, update_values...)
	verifyUpdateResponseForN(t, resp, update_values)

	resp = rrdcached.Pending(RRD_FILE)
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

	update_values := generateUpdateValues()

	resp := rrdcached.Update(RRD_FILE, update_values...)
	verifyUpdateResponseForN(t, resp, update_values)

	resp = rrdcached.Flush(RRD_FILE)
	verifySuccessResponse(t, resp)

	verifyStatsFresh(t, map[string]uint64{
		"FlushesReceived": 1,
		"UpdatesReceived": 1,
		"UpdatesWritten":  1,
		"DataSetsWritten": 4,
	})
}

func TestFlushAll(t *testing.T) {
	testSetup(t)
	defer testTeardown()

	update_values := generateUpdateValues()

	resp := rrdcached.Update(RRD_FILE, update_values...)
	verifyUpdateResponseForN(t, resp, update_values)

	resp = rrdcached.FlushAll()
	verifySuccessResponse(t, resp)

	verifyStatsFresh(t, map[string]uint64{
		"FlushesReceived": 0,
		"UpdatesReceived": 1,
		"UpdatesWritten":  1,
		"DataSetsWritten": 4,
	})
}

func TestForget(t *testing.T) {
	testSetup(t)
	defer testTeardown()

	update_values := generateUpdateValues()

	resp := rrdcached.Update(RRD_FILE, update_values...)
	verifyUpdateResponseForN(t, resp, update_values)

	resp = rrdcached.Forget(RRD_FILE)
	verifySuccessResponse(t, resp)

	verifyStatsFresh(t, map[string]uint64{
		"FlushesReceived": 0,
		"UpdatesReceived": 1,
		"UpdatesWritten":  0,
		"DataSetsWritten": 0,
	})
}
