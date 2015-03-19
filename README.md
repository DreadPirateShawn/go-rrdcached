# Go (golang) Bindings for rrdcached

This package implements [Go](http://golang.org) (golang) bindings for the [rrdcached](http://oss.oetiker.ch/rrdtool/doc/rrdcached.en.html) daemon.

## Install

```
go get github.com/dreadpirateshawn/rrdcached
```

## Setup

rrdcached was developed against RRDCacheD 1.4.7, likely works on 1.4.x in general.

```
sudo apt-get install rrdcached rrdtool
```

### Configure rrdcached to use unix socket

The default socket can be seen by starting the daemon and checking the process:

    $ sudo /etc/init.d/rrdcached start
    Starting RRDtool data caching daemon: rrdcached.

    $ ps aux | grep rrd
    root     32608  0.0  0.1  52932   928 ?        Ssl  06:02   0:00 /usr/bin/rrdcached -l unix:/var/run/rrdcached.sock -j /var/lib/rrdcached/journal/ -F -b /var/lib/rrdcached/db/ -B -p /var/run/rrdcached.pid

Test communication with the socket using `socat` or `nc`. ([Telnet doesn't work for unix:socket.](https://github.com/tj/go-debug/issues/2))

```
echo "STATS" | sudo socat - UNIX-CONNECT:/var/run/rrdcached.sock
echo "STATS" | sudo nc -U /var/run/rrdcached.sock
```

You should see various stats appear:

```
9 Statistics follow
QueueLength: 0
UpdatesReceived: 30
etc...
```

If you encounter permission problems accessing the socket from your Go program, here's what I've done to work around this. (TODO: Shouldn't this library be usable without doing this?)

  * **Change the default socket location.** Add `OPTS="-l unix:/socks/rrdcached.sock"` to `/etc/default/rrdcached`
  * **Start the rrdcached daemon.** `sudo /etc/init.d/rrdcached start`
  * **Ensure permissions.** `sudo chmod -R 777 /socks/`

### View syslogs for info on errors

```
tail /var/log/syslog
```

### Create RRD file used by tests ([source](http://cuddletech.com/articles/rrd/ar01s02.html))

TODO: Shouldn't tests be runnable without doing this?

```
rrdtool create /tmp/go-rrdcached-test.rrd \
--start N --step 300 \
DS:test1:GAUGE:600:0:100 \
DS:test2:GAUGE:600:0:100 \
DS:test3:GAUGE:600:0:100 \
DS:test4:GAUGE:600:0:100 \
RRA:MIN:0.5:12:1440 \
RRA:MAX:0.5:12:1440 \
RRA:AVERAGE:0.5:1:1440

sudo chmod -R 777 /tmp/*.rrd

go test . -v
```

## Resources


## Open Questions

  - RRD requires timestamp to increase by at least one second for each update value... does this library do enough to bubble up this error when it happens?

  - What if filenames with spaces are used?

  - What if Update is called with empty values? no-op or panic?

  - Tests cover one RRD with multiple dimensions, should they also cover multiple RRDs with one dimension each? [https://kb.op5.com/display/HOWTOs/Use+RRD+in+MULTIPLE+mode+for+separate+check+commands](https://kb.op5.com/display/HOWTOs/Use+RRD+in+MULTIPLE+mode+for+separate+check+commands)
