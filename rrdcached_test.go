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

func TestMain(m *testing.M) {
	// Note: Sub-function is here to allow defer usage, since os.Exit() bypasses defer.
	status_code := startDaemonRunTestsStopDaemon(m)

	// Exit with status code
	os.Exit(status_code)
}

func startDaemonRunTestsStopDaemon(m *testing.M) int {
	// Setup
	var cmd *exec.Cmd
	daemon := func(port int) {
		/*
			Note: '-g' flag is crucial ("run in foreground"), otherwise rrdcached will run as a forked child process,
			  and the parent (cmd) will exit and orphan the rrdcached process, leaving no (sane) way to tear it down.
		*/
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

	server, err := tcptest.Start(daemon, 30*time.Second)
	if err != nil {
		fmt.Println("Failed to start rrdcached:", err)
		panic(err)
	}
	fmt.Printf("rrdcached started on port %d", server.Port())

	// Teardown (deferred)
	defer func() {
		if cmd != nil && cmd.Process != nil {
			if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
				fmt.Println("SIGTERM failed:", err)
			}
			server.Wait()
		}
	}()

	// Run tests
	return m.Run()
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

func verifyStatsChange(t *testing.T, stats_pre *Stats, stats_post *Stats, stats_diff map[string]uint64) {
	fmt.Printf(">> PRE << %+v\n", stats_pre)
	fmt.Printf(">> POST << %+v\n", stats_post)

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

func TestUpdate(t *testing.T) {
	rrdcached := NewRrdcached("unix", SOCKET)
	rrdcached.Connect()
	rrdcached.FlushAll()
	stats_pre := rrdcached.GetStats()

	update_values1 := generateUpdateValues()
	resp1 := rrdcached.Update(RRD_FILE, update_values1...)
	verifyUpdateResponseForN(t, resp1, update_values1)

	update_values2 := generateUpdateValues()
	resp2 := rrdcached.Update(RRD_FILE, update_values2...)
	verifyUpdateResponseForN(t, resp2, update_values2)

	verifyStatsChange(t, stats_pre, rrdcached.GetStats(), map[string]uint64{
		"UpdatesReceived": 2,
	})

	rrdcached.Quit()
}

func TestPending(t *testing.T) {
	rrdcached := NewRrdcached("unix", SOCKET)
	rrdcached.Connect()
	rrdcached.FlushAll()
	stats_pre := rrdcached.GetStats()

	update_values := generateUpdateValues()

	resp := rrdcached.Update(RRD_FILE, update_values...)
	verifyUpdateResponseForN(t, resp, update_values)

	resp = rrdcached.Pending(RRD_FILE)
	verifyPendingResponseForN(t, resp, update_values)

	verifyStatsChange(t, stats_pre, rrdcached.GetStats(), map[string]uint64{
		"UpdatesReceived": 1,
		"UpdatesWritten":  0,
		"DataSetsWritten": 0,
	})

	rrdcached.Quit()
}

func TestFlush(t *testing.T) {
	rrdcached := NewRrdcached("unix", SOCKET)
	rrdcached.Connect()
	rrdcached.FlushAll()
	stats_pre := rrdcached.GetStats()

	update_values := generateUpdateValues()

	resp := rrdcached.Update(RRD_FILE, update_values...)
	verifyUpdateResponseForN(t, resp, update_values)

	resp = rrdcached.Flush(RRD_FILE)
	verifySuccessResponse(t, resp)

	verifyStatsChange(t, stats_pre, rrdcached.GetStats(), map[string]uint64{
		"FlushesReceived": 1,
		"UpdatesReceived": 1,
		"UpdatesWritten":  1,
		"DataSetsWritten": 4,
	})

	rrdcached.Quit()
}

func TestFlushAll(t *testing.T) {
	rrdcached := NewRrdcached("unix", SOCKET)
	rrdcached.Connect()
	rrdcached.FlushAll()
	stats_pre := rrdcached.GetStats()

	update_values := generateUpdateValues()

	resp := rrdcached.Update(RRD_FILE, update_values...)
	verifyUpdateResponseForN(t, resp, update_values)

	resp = rrdcached.FlushAll()
	verifySuccessResponse(t, resp)

	verifyStatsChange(t, stats_pre, rrdcached.GetStats(), map[string]uint64{
		"FlushesReceived": 0,
		"UpdatesReceived": 1,
		"UpdatesWritten":  1,
		"DataSetsWritten": 4,
	})

	rrdcached.Quit()
}

func TestForget(t *testing.T) {
	rrdcached := NewRrdcached("unix", SOCKET)
	rrdcached.Connect()
	rrdcached.FlushAll()
	stats_pre := rrdcached.GetStats()

	update_values := generateUpdateValues()

	resp := rrdcached.Update(RRD_FILE, update_values...)
	verifyUpdateResponseForN(t, resp, update_values)

	resp = rrdcached.Forget(RRD_FILE)
	verifySuccessResponse(t, resp)

	verifyStatsChange(t, stats_pre, rrdcached.GetStats(), map[string]uint64{
		"FlushesReceived": 0,
		"UpdatesReceived": 1,
		"UpdatesWritten":  0,
		"DataSetsWritten": 0,
	})

	rrdcached.Quit()
}
