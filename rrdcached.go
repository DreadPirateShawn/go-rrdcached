package rrdcached

import (
    "io"
    "net"
    "fmt"
    "strings"
    "strconv"
    "reflect"
    "time"
)

type Rrdcached struct {
    protocol string
    socket string
}

func NewRrdcached(protocol string, socket string) *Rrdcached {
    return &Rrdcached{
        protocol: protocol,
        socket: socket,
    }
}

func (r *Rrdcached) Connect() net.Conn {
    conn, err := net.Dial(r.protocol, r.socket)
    if err != nil {
        panic(err)
    }
    return conn
}

type Stats struct {
    QueueLength uint64
    UpdatesReceived uint64
    FlushesReceived uint64
    UpdatesWritten uint64
    DataSetsWritten uint64
    TreeNodesNumber uint64
    TreeDepth uint64
    JournalBytes uint64
    JournalRotate uint64
}

// ---------------------------------------------
// Reflect:
//   http://stackoverflow.com/questions/6395076/in-golang-using-reflect-how-do-you-set-the-value-of-a-struct-field
//   http://stackoverflow.com/questions/24537525/reflect-value-fieldbyname-causing-panic
// TODO: Should I be using reflect?
//   https://groups.google.com/forum/#!topic/golang-nuts/wfmBXg3xML0
// ---------------------------------------------

func parseStats(data string) *Stats {
    lines := strings.Split(data, "\n")

    desc := strings.Split(lines[0], " ")
    count, _ := strconv.ParseInt(desc[0], 10, 64)

    stats := &Stats{}
    stats_struct := reflect.Indirect(reflect.ValueOf(stats))

    for i := 1; i <= int(count); i++ {
        stat := strings.Split(lines[i], ": ")
        stat_label := stat[0]
        stat_value, _ := strconv.ParseUint(stat[1], 10, 64)

        field := stats_struct.FieldByName(stat_label)
        if field.IsValid() && field.CanSet() {
            field.SetUint(stat_value)
        }
    }

    return stats
}

// -------------------------------------------------------------
// Pattern to read forever:
// http://stackoverflow.com/questions/2886719/unix-sockets-in-go
// -------------------------------------------------------------

func readOnce(r io.Reader) string {
    buf := make([]byte, 1024)

    n, err := r.Read(buf[:])
    if err != nil {
        panic(err)
    }
    data := string(buf[0:n])

    return data
}

func writeData(conn net.Conn, data string) {
    fmt.Printf("========== %v", data)

    _, err := conn.Write([]byte( data ))
    if err != nil {
        panic(err)
    }
}

type Response struct {
    Status int
    Message string
    Raw string
}

func checkResponse(conn net.Conn) *Response {
    data := readOnce(conn)
    data = strings.TrimSpace(data)
    fmt.Println(data)

    lines := strings.SplitN(data, " ", 2)

    status, _ := strconv.ParseInt(lines[0], 10, 0)

    return &Response{
        Status: int(status),
        Message: lines[1],
        Raw: data,
    }
}

func NowString() string {
    ms := float64(time.Now().UnixNano()) / float64(time.Second)
    return strconv.FormatFloat(ms, 'f', 3, 64)
}

// ----------------------------------------------------------

func (r *Rrdcached) GetStats() *Stats {
    conn := r.Connect()
    defer conn.Close()
    writeData(conn, "STATS\n")
    data := readOnce(conn)
    return parseStats(data)
}

func (r *Rrdcached) Update(filename string, values ...string) *Response {
    conn := r.Connect()
    defer conn.Close()
    writeData(conn, "UPDATE " + filename + " " + strings.Join(values," ") + "\n")
    return checkResponse(conn)
}

func (r *Rrdcached) Pending(filename string) *Response {
    conn := r.Connect()
    defer conn.Close()
    writeData(conn, "PENDING " + filename + "\n")
    return checkResponse(conn)
}

func (r *Rrdcached) Forget(filename string) *Response {
    conn := r.Connect()
    defer conn.Close()
    writeData(conn, "FORGET " + filename + "\n")
    return checkResponse(conn)
}

func (r *Rrdcached) Flush(filename string) *Response {
    conn := r.Connect()
    defer conn.Close()
    writeData(conn, "FLUSH " + filename + "\n")
    return checkResponse(conn)
}

func (r *Rrdcached) FlushAll() *Response {
    conn := r.Connect()
    defer conn.Close()
    writeData(conn, "FLUSHALL\n")
    return checkResponse(conn)
}

func (r *Rrdcached) Quit() {
    conn := r.Connect()
    defer conn.Close()
    writeData(conn, "QUIT\n")
}
