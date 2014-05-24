package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"github.com/alk/maxi/core"
	"github.com/alk/maxi/memcached"
	"io"
	"log"
	"os"
	"runtime/pprof"
	"time"
)

var ZeroFlagsExpiration [8]byte

func encodeFlagsExpiration(flags uint32, expiration uint32) []byte {
	if flags == 0 && expiration == 0 {
		return ZeroFlagsExpiration[:]
	}
	rv := make([]byte, 8)
	binary.BigEndian.PutUint32(rv, flags)
	binary.BigEndian.PutUint32(rv[4:], expiration)
	return rv
}

func buildSetRequest(code memcached.CommandCode, key []byte, body []byte, flags uint32, expiration uint32) (rv memcached.MCRequest) {
	rv.Opcode = code
	rv.Key = key
	rv.Body = body
	if code != memcached.APPEND {
		rv.Extras = encodeFlagsExpiration(flags, expiration)
	}
	return
}

func buildGetRequest(key []byte) (rv memcached.MCRequest) {
	rv.Opcode = memcached.GET
	rv.Key = key
	return
}

type loaderReq struct {
	respChan core.SinkChan
	req      *memcached.MCRequest
}

func runRepliesReader(sink core.MCDSink, sentReqs chan loaderReq, sinkChanBuf chan core.SinkChan) {
	for sreq := range sentReqs {
		mcresp := <-sreq.respChan
		if mcresp == nil {
			log.Printf("Got bad response")
			break
		}
		mcreq := sreq.req
		status := mcresp.Status
		if status == memcached.TMPFAIL {
			// log.Printf("tmpfail for %v", *mcreq)
			// TODO: we're kinda silly here. Need delay
			// only on that server's queue
			time.Sleep(10 * time.Millisecond)
			sink.SendRequest(mcreq, sreq.respChan)
			sentReqs <- sreq
		} else {
			if status == memcached.SUCCESS {
				// log.Printf("Got ok reply for %v", *mcreq)
			} else if status == memcached.KEY_EEXISTS {
				// ignore
			} else {
				log.Printf("Got error for %v: %v", *mcreq, *mcresp)
			}
			// log.Printf("rep-reader: Sending back: %p", sreq.respChan)
			sinkChanBuf <- sreq.respChan
		}
	}
}

type stdinInnerFn func([]byte, []byte)

func runStdinLoop(fn stdinInnerFn) {
	mystdin := bufio.NewReader(os.Stdin)

	for {
		line, err := mystdin.ReadBytes('\n')
		if len(line) == 0 && err != nil {
			if err != io.EOF {
				log.Fatalf("Got error on stdin: ", err)
			}
			break
		}

		idx := bytes.IndexByte(line, ' ')
		if idx < 0 {
			continue
		}
		key := line[0:idx]
		value := bytes.TrimRight(line[idx+1:], "\n")
		fn(key, value)
	}
	// log.Printf("done")
}


func doRunSets(sink core.MCDSink, command memcached.CommandCode) {
	sinkChanBuf := make(chan core.SinkChan, core.QueueDepth)
	for i := 0; i < cap(sinkChanBuf); i++ {
		sinkChanBuf <- make(core.SinkChan, 1)
	}

	sentReqs := make(chan loaderReq, core.QueueDepth)

	go runRepliesReader(sink, sentReqs, sinkChanBuf)

	runStdinLoop(func(key, value []byte) {
		rch := <-sinkChanBuf
		mcreq := buildSetRequest(command, key, value, 0, 0)
		sink.SendRequest(&mcreq, rch)
		sentReqs <- loaderReq{
			req:      &mcreq,
			respChan: rch,
		}
	})
	close(sentReqs)

	for i := 0; i < cap(sinkChanBuf); i++ {
		_ = <-sinkChanBuf
	}
}

func runSets(sink core.MCDSink) {
	doRunSets(sink, memcached.SET)
}

func runAdds(sink core.MCDSink) {
	doRunSets(sink, memcached.ADD)
}

func runAppends(sink core.MCDSink) {
	doRunSets(sink, memcached.APPEND)
}

type verifyValue []byte

func (v verifyValue) OnResponse(req *memcached.MCRequest, resp *memcached.MCResponse) {
	if resp.Status != memcached.SUCCESS {
		log.Printf("Bad return on key: %s: %v", req.Key, resp.Status)
		return
	}
	if !bytes.Equal([]byte(v), resp.Body) {
		log.Printf("Value mismatch for: %s", req.Key)
	}
}

func runGets(sink core.MCDSink) {
	runStdinLoop(func (key, value []byte) {
		mcreq := buildGetRequest(key)
		sink.SendRequest(&mcreq, verifyValue(value))
	})
}

type noopCBType struct {}
var noopCB noopCBType

func (_ noopCBType) OnResponse(_ *memcached.MCRequest, _ *memcached.MCResponse) {
}

func runEvicts(sink core.MCDSink) {
	runStdinLoop(func (key, _ []byte) {
		mcreq := memcached.MCRequest {
			Opcode: memcached.EVICT_KEY,
			Key: key,
		}
		sink.SendRequest(&mcreq, noopCB)
	})
}

func runDeletes(sink core.MCDSink) {
	runStdinLoop(func (key, _ []byte) {
		mcreq := memcached.MCRequest {
			Opcode: memcached.DELETE,
			Key: key,
		}
		sink.SendRequest(&mcreq, noopCB)
	})
}

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var sinkURL = flag.String("sinkURL", "http://lh:9000/", "Couchbase URL i.e. http://<host>:8091/")
var bucketName = flag.String("bucket", "default", "bucket to use")
var verify = flag.Bool("verify", false, "do GETs to verify")
var evict = flag.Bool("evict", false, "evict keys instead of get/sets")
var add = flag.Bool("add", false, "add keys instead of get/sets")
var appends = flag.Bool("append", false, "append keys instead of get/sets")
var delete = flag.Bool("delete", false, "delete keys instead of get/sets")

func main() {
	flag.Parse()
	defer func() {
		fmt.Fprintf(os.Stdout, "Send reqs: %d, sends: %d\n", core.RequestsSent, core.Sends)
	}()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	sink, err := core.NewCouchbaseSink(*sinkURL, *bucketName)
	if err != nil {
		panic(err)
	}

	if *delete {
		runDeletes(sink)
	} else if *evict {
		runEvicts(sink)
	} else if *verify {
		runGets(sink)
	} else if *add {
		runAdds(sink)
	} else if *appends {
		runAppends(sink)
	} else {
		runSets(sink)
	}

	// TODO: graceful close of sink
	// time.Sleep(1 * time.Second)
}
