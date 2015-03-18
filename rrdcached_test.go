package rrdcached

import (
    "fmt"
    "testing"
    "strings"
    "strconv"
    "time"
    "reflect"
)

const SOCKET = "/socks/rrdcached.sock"
const RRD_FILE = "/tmp/test.rrd"


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
    rrdcached := NewRrdcached( "unix", SOCKET )
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
    rrdcached := NewRrdcached( "unix", SOCKET )
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
        "UpdatesWritten": 0,
        "DataSetsWritten": 0,
    })

    rrdcached.Quit()
}

func TestFlush(t *testing.T) {
    rrdcached := NewRrdcached( "unix", SOCKET )
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
        "UpdatesWritten": 1,
        "DataSetsWritten": 4,
    })

    rrdcached.Quit()
}

func TestFlushAll(t *testing.T) {
    rrdcached := NewRrdcached( "unix", SOCKET )
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
        "UpdatesWritten": 1,
        "DataSetsWritten": 4,
    })

    rrdcached.Quit()
}

func TestForget(t *testing.T) {
    rrdcached := NewRrdcached( "unix", SOCKET )
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
        "UpdatesWritten": 0,
        "DataSetsWritten": 0,
    })

    rrdcached.Quit()
}
