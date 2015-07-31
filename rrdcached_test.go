package rrdcached

import (
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
)

type fakeDataTransport struct {
	written  string
	response string
}

func (rrdio *fakeDataTransport) WriteData(conn net.Conn, data string) {
	fmt.Print("===== WriteData =====\n")
	fmt.Printf("%+v\n", rrdio)
	rrdio.written = data
}

func (rrdio *fakeDataTransport) ReadData(r io.Reader) string {
	return rrdio.response
}

func prepTestData(fakeWritten string, fakeResponse string) (expected *fakeDataTransport, fakeDriver *Rrdcached) {
	expected = &fakeDataTransport{fakeWritten, fakeResponse}
	fakeDriver = &Rrdcached{Rrdio: &fakeDataTransport{response: fakeResponse}}
	return expected, fakeDriver
}

// ------------------------------------------
// Tests

var (
	testDefineDS  = []string{"DS:test1:GAUGE:600:0:100", "DS:test2:GAUGE:600:0:100"}
	testDefineRRA = []string{"RRA:MIN:0.5:12:1440", "RRA:MAX:0.5:12:1440", "RRA:AVERAGE:0.5:1:1440"}
)

func TestCreate(t *testing.T) {
	expected, fakeDriver := prepTestData(
		"CREATE foo.rrd -O DS:test1:GAUGE:600:0:100 DS:test2:GAUGE:600:0:100 RRA:MIN:0.5:12:1440 RRA:MAX:0.5:12:1440 RRA:AVERAGE:0.5:1:1440\n",
		"0 RRD created successfully (/tmp/foo.rrd)",
	)

	resp, err := fakeDriver.Create("foo.rrd", -1, -1, false, testDefineDS, testDefineRRA)

	assert.NoError(t, err)
	assert.Equal(t, 0, resp.Status)
	assert.Equal(t, expected, fakeDriver.Rrdio)
}

func TestCreateWithStartAndStepAndOverwrite(t *testing.T) {
	expected, fakeDriver := prepTestData(
		"CREATE foo.rrd -b 1438354678 -s 10 DS:test1:GAUGE:600:0:100 DS:test2:GAUGE:600:0:100 RRA:MIN:0.5:12:1440 RRA:MAX:0.5:12:1440 RRA:AVERAGE:0.5:1:1440\n",
		"0 RRD created successfully (/tmp/foo.rrd)",
	)

	resp, err := fakeDriver.Create("foo.rrd", 1438354678, 10, true, testDefineDS, testDefineRRA)

	assert.NoError(t, err)
	assert.Equal(t, 0, resp.Status)
	assert.Equal(t, expected, fakeDriver.Rrdio)
}

func TestCreateWithStart(t *testing.T) {
	expected, fakeDriver := prepTestData(
		"CREATE foo.rrd -b 1438354678 -O DS:test1:GAUGE:600:0:100 DS:test2:GAUGE:600:0:100 RRA:MIN:0.5:12:1440 RRA:MAX:0.5:12:1440 RRA:AVERAGE:0.5:1:1440\n",
		"0 RRD created successfully (/tmp/foo.rrd)",
	)

	resp, err := fakeDriver.Create("foo.rrd", 1438354678, -1, false, testDefineDS, testDefineRRA)

	assert.NoError(t, err)
	assert.Equal(t, 0, resp.Status)
	assert.Equal(t, expected, fakeDriver.Rrdio)
}

func TestCreateWithStep(t *testing.T) {
	expected, fakeDriver := prepTestData(
		"CREATE foo.rrd -s 10 -O DS:test1:GAUGE:600:0:100 DS:test2:GAUGE:600:0:100 RRA:MIN:0.5:12:1440 RRA:MAX:0.5:12:1440 RRA:AVERAGE:0.5:1:1440\n",
		"0 RRD created successfully (/tmp/foo.rrd)",
	)

	resp, err := fakeDriver.Create("foo.rrd", -1, 10, false, testDefineDS, testDefineRRA)

	assert.NoError(t, err)
	assert.Equal(t, 0, resp.Status)
	assert.Equal(t, expected, fakeDriver.Rrdio)
}

func TestCreateWithOverwrite(t *testing.T) {
	expected, fakeDriver := prepTestData(
		"CREATE foo.rrd DS:test1:GAUGE:600:0:100 DS:test2:GAUGE:600:0:100 RRA:MIN:0.5:12:1440 RRA:MAX:0.5:12:1440 RRA:AVERAGE:0.5:1:1440\n",
		"0 RRD created successfully (/tmp/foo.rrd)",
	)

	resp, err := fakeDriver.Create("foo.rrd", -1, -1, true, testDefineDS, testDefineRRA)

	assert.NoError(t, err)
	assert.Equal(t, 0, resp.Status)
	assert.Equal(t, expected, fakeDriver.Rrdio)
}

func TestCreateWithUnsupportedFlag(t *testing.T) {
	expected, fakeDriver := prepTestData(
		"CREATE foo.rrd -O DS:test1:GAUGE:600:0:100 DS:test2:GAUGE:600:0:100 RRA:MIN:0.5:12:1440 RRA:MAX:0.5:12:1440 RRA:AVERAGE:0.5:1:1440\n",
		"-1 Error while creating rrd (can't parse argument '-O')",
	)

	resp, err := fakeDriver.Create("foo.rrd", -1, -1, false, testDefineDS, testDefineRRA)

	assert.IsType(t, &UnrecognizedArgumentError{}, err)
	assert.Equal(t, -1, resp.Status)
	assert.Equal(t, expected, fakeDriver.Rrdio)
}

func TestUpdate(t *testing.T) {
	expected, fakeDriver := prepTestData(
		"UPDATE foo.rrd 1438354679:10:20:30:40 1438354680:90:80:70:60\n",
		"0 errors, enqueued 4 value(s).",
	)

	resp, err := fakeDriver.Update("foo.rrd", "1438354679:10:20:30:40", "1438354680:90:80:70:60")

	assert.NoError(t, err)
	assert.Equal(t, 0, resp.Status)
	assert.Equal(t, expected, fakeDriver.Rrdio)
}

func TestUpdateWithoutExistingRRD(t *testing.T) {
	expected, fakeDriver := prepTestData(
		"UPDATE foo.rrd 1438354679:10:20:30:40 1438354680:90:80:70:60\n",
		"-1 No such file: /tmp/foo.rrd",
	)

	resp, err := fakeDriver.Update("foo.rrd", "1438354679:10:20:30:40", "1438354680:90:80:70:60")

	assert.IsType(t, &FileDoesNotExistError{}, err)
	assert.Equal(t, -1, resp.Status)
	assert.Equal(t, expected, fakeDriver.Rrdio)
}

func TestPending(t *testing.T) {
	expected, fakeDriver := prepTestData(
		"PENDING foo.rrd\n",
		"4 updates pending",
	)

	resp, err := fakeDriver.Pending("foo.rrd")

	assert.NoError(t, err)
	assert.Equal(t, 4, resp.Status)
	assert.Equal(t, expected, fakeDriver.Rrdio)
}

func TestForget(t *testing.T) {
	expected, fakeDriver := prepTestData(
		"FORGET foo.rrd\n",
		"0 Gone!",
	)

	resp, err := fakeDriver.Forget("foo.rrd")

	assert.NoError(t, err)
	assert.Equal(t, 0, resp.Status)
	assert.Equal(t, expected, fakeDriver.Rrdio)
}

func TestFlush(t *testing.T) {
	expected, fakeDriver := prepTestData(
		"FLUSH foo.rrd\n",
		"0 Successfully flushed /tmp/foo.rrd.",
	)

	resp, err := fakeDriver.Flush("foo.rrd")

	assert.NoError(t, err)
	assert.Equal(t, 0, resp.Status)
	assert.Equal(t, expected, fakeDriver.Rrdio)
}

func TestFlushAll(t *testing.T) {
	expected, fakeDriver := prepTestData(
		"FLUSHALL\n",
		"0 Started flush.",
	)

	resp, err := fakeDriver.FlushAll()

	assert.NoError(t, err)
	assert.Equal(t, 0, resp.Status)
	assert.Equal(t, expected, fakeDriver.Rrdio)
}

func TestFirstUnsupported(t *testing.T) {
	expected, fakeDriver := prepTestData(
		"FIRST foo.rrd 1\n",
		"-1 Unknown command: FIRST",
	)

	resp, err := fakeDriver.First("foo.rrd", 1)

	assert.IsType(t, &UnknownCommandError{}, err)
	assert.Equal(t, -1, resp.Status)
	assert.Equal(t, expected, fakeDriver.Rrdio)
}

func TestLastUnsupported(t *testing.T) {
	expected, fakeDriver := prepTestData(
		"LAST foo.rrd\n",
		"-1 Unknown command: LAST",
	)

	resp, err := fakeDriver.Last("foo.rrd")

	assert.IsType(t, &UnknownCommandError{}, err)
	assert.Equal(t, -1, resp.Status)
	assert.Equal(t, expected, fakeDriver.Rrdio)
}

func TestStats(t *testing.T) {
	_, fakeDriver := prepTestData(
		"STATS\n",
		"10 Statistics follow\nQueueLength: 2\nCreatesReceived: 3\nUpdatesReceived: 5\nFlushesReceived: 7\nUpdatesWritten: 11\nDataSetsWritten: 13\nTreeNodesNumber: 17\nTreeDepth: 19\nJournalBytes: 23\nJournalRotate: 29",
	)

	stats := fakeDriver.GetStats()

	assert.Equal(t, 2, stats.QueueLength)
	assert.Equal(t, 3, stats.CreatesReceived)
	assert.Equal(t, 5, stats.UpdatesReceived)
	assert.Equal(t, 7, stats.FlushesReceived)
	assert.Equal(t, 11, stats.UpdatesWritten)
	assert.Equal(t, 13, stats.DataSetsWritten)
	assert.Equal(t, 17, stats.TreeNodesNumber)
	assert.Equal(t, 19, stats.TreeDepth)
	assert.Equal(t, 23, stats.JournalBytes)
	assert.Equal(t, 29, stats.JournalRotate)
}
