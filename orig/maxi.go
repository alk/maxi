package main

import (
	"log"
	"net"
	"fmt"
	"io"
	"github.com/dustin/go-couchbase"
	"github.com/dustin/gomemcached"
	"github.com/dustin/gomemcached/client"
	mcdServer "github.com/dustin/gomemcached/server"
	"flag"
	)

func mustConnect(url string) (c couchbase.Client) {
	c, err := couchbase.Connect(url)
	if err != nil {
		log.Fatalf("Error connecting: %v", err)
	}
	return c
}

func mustGetPool(c *couchbase.Client) (pool couchbase.Pool) {
	baseinfo := c.Info.Pools[0]
	pool, err := c.GetPool(baseinfo.Name)
	if err != nil {
		log.Fatalf("failed to get any pool: %v", err)
	}
	return pool
}

func mustGetBucket(c *couchbase.Client, bucketName string) couchbase.Bucket {
	pool := mustGetPool(c)
	var bucket *couchbase.Bucket
	bucket, err := pool.GetBucket(bucketName)
	if err != nil {
		log.Fatalf("failed to find bucket: %v", err)
	}
	return *bucket
}

type request struct {
	req gomemcached.MCRequest
	repChan chan *gomemcached.MCResponse
}

type handler struct {
	bucketInfo couchbase.Bucket
	vbucketMap [][]int
	serverList []string
	subHandlers []chan request
}

func main() {
	couchbaseURL := flag.String("url", "http://127.0.0.1:9000/", "couchbase host")
	bucketName := flag.String("bucket", "default", "")
	serverPort := flag.Int("port", 11211, "")
	flag.Parse()
	log.Printf("couchbaseURL: %v, bucketName: %v", *couchbaseURL, *bucketName)
	c := mustConnect(*couchbaseURL)
	bucketInfo := mustGetBucket(&c, *bucketName)
	log.Printf("bucketInfo: %v", bucketInfo)

	ls, e := net.Listen("tcp", fmt.Sprintf(":%d", *serverPort))
	if e != nil {
		log.Fatalf("Got an error:  %s", e)
	}
	log.Printf("Listening on port: %d", *serverPort)
	handler := buildHandler(bucketInfo)
	runAcceptor(ls, &handler)
}

func buildHandler(bucketInfo couchbase.Bucket) handler {
	serverList := bucketInfo.VBucketServerMap.ServerList
	subHandlers := make([]chan request, len(bucketInfo.VBucketServerMap.ServerList))
	h := handler{
		bucketInfo: bucketInfo,
		vbucketMap: bucketInfo.VBucketServerMap.VBucketMap,
		serverList: serverList,
		subHandlers: subHandlers,
	}
	for i, hostname := range serverList {
		reqChan := make(chan request, 32)
		spawnServerHandler(reqChan, hostname)
		subHandlers[i] = reqChan
	}
	return h
}

func spawnServerHandler(reqchan chan request, hostname string) {
	conn, err := memcached.Connect("tcp", hostname)
	if err != nil {
		log.Fatalf("Failed to connect %s: %v", hostname, err)
	}
	subchans := make([]chan request, 8)
	for i, _ := range subchans {
		sc := make(chan request, 32)
		subchans[i] = sc;
		go runDownstream(conn, sc)
	}
	go func () {
		idx := 0
		for req := range reqchan {
			subchans <- req
			idx = (idx + 1) % len(subchans)
		}
	}()
}

func runDownstreamReader(conn *memcached.Client, chans chan chan *gomemcached.MCResponse) {
	for respChan := range chans {
		resp, err := conn.Receive()
		if err != nil {
			log.Printf("Got error on reading downstream %v: %s", conn, err)
			close(chans)
			return
		}
		respChan <- resp
	}
}

func runDownstream(conn *memcached.Client, reqchan chan request) {
	defer conn.Close()
	respChans := make(chan chan *gomemcached.MCResponse, 32)
	go runDownstreamReader(conn, respChans)
	for req := range reqchan {
		err := conn.Transmit(&req.req)
		if err != nil {
			log.Printf("Failed to send to downstream %v: %s", conn, err)
			close(respChans)
		}
		respChans <- req.repChan
	}
}

func runAcceptor(ls net.Listener, h *handler) {
	for {
		sock, err := ls.Accept()
		if err != nil {
			log.Fatalf("Failed to accept: %v", err)
		}
		go handleIO(sock, h)
	}
}

func transmitResponse(o io.Writer, res *gomemcached.MCResponse) (err error) {
	if res == nil {
		// quiet command
		return
	}
	if len(res.Body) < 128 {
		_, err = o.Write(res.Bytes())
	} else {
		_, err = o.Write(res.HeaderBytes())
		if err == nil && len(res.Body) > 0 {
			_, err = o.Write(res.Body)
		}
	}
	return
}
func runReplyWriter(s io.Writer, replyChans chan chan *gomemcached.MCResponse) {
	for rch := range replyChans {
		resp := <-rch;
		log.Printf("Sending upstream reply: %v", *resp)
		err := transmitResponse(s, resp)
		if err != nil {
			log.Printf("Failed to send back reply: %v, assuming we're done", err)
			return
		}
	}
}

func handleIO(s io.ReadWriteCloser, h *handler) {
	defer s.Close()
	replyChans := make(chan chan *gomemcached.MCResponse, 32)
	defer close(replyChans)
	go runReplyWriter(s, replyChans)

	numVBuckets := len(h.vbucketMap)
	for {
		req, err := mcdServer.ReadPacket(s)
		if err != nil {
			log.Printf("Failed to read on %v: %s", s, err)
			return
		}
		log.Printf("K: %v, H: %d", req.Key, vbhash(req.Key))
		vbid := vbhash(req.Key) & (uint16(numVBuckets)-1)
		req.VBucket = vbid
		serverId := h.vbucketMap[vbid][0]
		log.Printf("Set vbucket to %d: %v", vbid, req)
		if (serverId < 0) {
			log.Panic("TODO")
		}
		rch := make(chan *gomemcached.MCResponse, 1)
		reqStruct := request {
			req: req,
			repChan: rch,
		}
		log.Printf("Sending request %v to server: %s", &reqStruct, h.serverList[serverId])
		h.subHandlers[serverId] <- reqStruct
		replyChans <- rch
	}
}

