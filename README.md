# Go (golang) Bindings for rrdcached

This package implements [Go](http://golang.org) (golang) bindings for the [rrdcached](http://oss.oetiker.ch/rrdtool/doc/rrdcached.en.html) daemon.

## Install

```
go get github.com/dreadpirateshawn/rrdcached
```

## Requirements

rrdcached was developed against RRDCacheD 1.5.0-rc2 and expects CREATE support. Technically the pre-1.5 methods work with older libraries -- UPDATE, FLUSH, etc -- but the test suite leverages CREATE, so it's easiest to use out-of-the-box with the newer library.

RRDTool / RRDCacheD can be found here: https://github.com/oetiker/rrdtool-1.x

## Basic RRDCacheD test

### Unit tests

```
go test -v ./...
```

The go-rrdcached test suite starts and stops rrdcached daemon instances of its own, so you don't need to leave rrdcached running during development.

### Integration tests

```
rrdcached -p /tmp/go-rrdached-test.pid -B -b /tmp -l /tmp/go-rrdcached-test.sock -l 0.0.0.0:50081
go test -v ./... -tags=integration
```

### Manual validation

Verify socket connection using `nc`:

    $ echo "STATS" | sudo nc -U /tmp/go-rrdcached-test.sock
    9 Statistics follow
    QueueLength: 0
    UpdatesReceived: 0
    ... etc ...

Verify TCP connection using `telnet`:

    $ telnet 0.0.0.0 50081
    Trying 0.0.0.0...
    Connected to 0.0.0.0.
    Escape character is '^]'.
    STATS
    9 Statistics follow
    QueueLength: 0
    UpdatesReceived: 0
    ... etc ...
    ^]
    telnet> quit
    Connection closed.

Btw: [Telnet doesn't work for unix:socket.](https://github.com/tj/go-debug/issues/2)

## Troubleshooting

If you encounter permission problems accessing the socket from your Go program, here's what I've done to work around this. (TODO: Shouldn't this library be usable without doing this?)

  * **Change the default socket location.** Add `OPTS="-l unix:/socks/rrdcached.sock"` to `/etc/default/rrdcached`
  * **Start the rrdcached daemon.** `sudo /etc/init.d/rrdcached start`
  * **Ensure permissions.** `sudo chmod -R 777 /socks/`
  * **View syslogs.** `tail /var/log/syslog`

## Open Questions

  - RRD requires timestamp to increase by at least one second for each update value... does this library do enough to bubble up this error when it happens?

  - What if filenames with spaces are used?

  - What if Update is called with empty values? no-op or panic?

  - Tests cover one RRD with multiple dimensions, should they also cover multiple RRDs with one dimension each? [https://kb.op5.com/display/HOWTOs/Use+RRD+in+MULTIPLE+mode+for+separate+check+commands](https://kb.op5.com/display/HOWTOs/Use+RRD+in+MULTIPLE+mode+for+separate+check+commands)
