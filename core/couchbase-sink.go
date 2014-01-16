package core

import (
	"github.com/alk/maxi/memcached"
	"github.com/dustin/go-couchbase"
	"log"
	"net"
	"runtime"
	"sync/atomic"
)

type SinkChan chan *memcached.MCResponse

type MCDSink interface {
	SendRequest(req *memcached.MCRequest, callback MCDCallback)
	// Stop()
}

type MCDCallback interface {
	OnResponse(req *memcached.MCRequest, resp *memcached.MCResponse)
}

func (sc SinkChan) OnResponse(req *memcached.MCRequest, resp *memcached.MCResponse) {
	sc <- resp
}

type request struct {
	req *memcached.MCRequest
	cb  MCDCallback
}

type sink struct {
	bucketInfo         couchbase.Bucket
	vbucketMap         [][]int
	numVBuckets        int
	serverList         []string
	subHandlers        [][]chan request
	connsPerDownstream int
	connsRRCounter     uint32
}

const QueueDepth = 10240
const ConnsPerDownstream = 4

var RequestsSent uint32
var Sends uint32

func NewCouchbaseSink(baseURL, bucketName string) (MCDSink, error) {
	// if baseURL[0:len("http:\/\/")] != "http:\/\/" {
	// 	baseURL = "http:\/\/" + baseURL
	// }
	bucketInfo, err := couchbase.GetBucket(baseURL, "default", bucketName)
	if err != nil {
		return nil, err
	}
	serverList := bucketInfo.VBSMJson.ServerList
	subHandlers := make([][]chan request, len(bucketInfo.VBSMJson.ServerList))
	vbucketMap := bucketInfo.VBSMJson.VBucketMap
	h := sink {
		bucketInfo: *bucketInfo,
		vbucketMap: vbucketMap,
		numVBuckets: len(vbucketMap),
		serverList: serverList,
		subHandlers: subHandlers,
		connsPerDownstream: ConnsPerDownstream,
	}
	for i, hostname := range serverList {
		allChans := make([]chan request, ConnsPerDownstream)
		for k := 0; k < ConnsPerDownstream; k++ {
			reqChan := make(chan request, QueueDepth)
			spawnServerHandler(reqChan, hostname, bucketName)
			allChans[k] = reqChan
		}
		subHandlers[i] = allChans
	}
	return &h, nil
}

func spawnServerHandler(reqchan chan request, hostname string, bucketName string) {
	sock, err := net.Dial("tcp", hostname)
	if err != nil {
		log.Panicf("Failed to connect %s: %v", hostname, err)
	}
	conn := memcached.ClientFromSock(sock)
	authCmd := memcached.MCRequest {
		Opcode: memcached.SASL_AUTH,
		Key: ([]byte)("PLAIN"),
		Body: ([]byte)("\000" + bucketName + "\000"),
	}
	sender := conn.Sender
	sender.TryEnqueueReq(&authCmd, true)
	err = sender.SendEnqueued()
	if err != nil {
		log.Panicf("Failed to authenticate %s: %v", hostname, err)
	}
	recver := conn.Recver
	recver.Fill()
	mcresp := memcached.MCResponse{}
	_, err = recver.TryUnpackResponse(&mcresp)
	if err != nil {
		log.Panicf("Failed to authenticate %s: %v", hostname, err)
	}
	go runDownstream(&conn, reqchan)
}

func runDownstreamReader(conn *memcached.Client, callbacks chan request) {
	recver := &conn.Recver
	for reqStruct := range callbacks {
		// log.Printf("dreader: got some Reqstruct: %v, %p", reqStruct, reqStruct.cb)
		mcresp := memcached.MCResponse{}
		for {
			actualResp, err := recver.TryUnpackResponse(&mcresp)
			if err != nil {
				log.Printf("Got error on reading downstream %v: %s", conn, err)
				reqStruct.cb.OnResponse(reqStruct.req, nil)
				close(callbacks)
				return
			}

			if actualResp != nil {
				break
			}

			// log.Printf("dreader: Entering fill: %p", reqStruct.cb)
			recver.Fill()
			// log.Printf("dreader: From fill: %p", reqStruct.cb)
		}

		// log.Printf("dreader: Got reply: %v: %p", mcresp, reqStruct.cb)
		// log.Printf("State: %v", recver)

		// TODO: exception handling?
		reqStruct.cb.OnResponse(reqStruct.req, &mcresp)
		// log.Printf("dreader: After onResponse: %p", reqStruct.cb)
	}
}

func peekMoreReq(c chan request) (r request, ok bool) {
	ok = false
	select {
	case r, ok = <- c:
	default:
	}
	return
}

func runDownstream(conn *memcached.Client, reqchan chan request) {
	defer conn.Close()

	respChans := make(chan request, QueueDepth)
	go runDownstreamReader(conn, respChans)

	sender := conn.Sender
	var err error

	for req := range reqchan {
	again:
		// log.Printf("Sending req: %s", req.req)
		// log.Printf("req-bytes: %v", req.req.Bytes())
		succeeded := sender.TryEnqueueReq(req.req, true)
		// time.Sleep(1*time.Millisecond)

		if !succeeded {
			log.Panicf("must succeed!")
		}

		atomic.AddUint32(&RequestsSent, 1)
		// log.Printf("downstream: before trying to batch more: %p", req.cb)
		respChans <- req
		// try to batch some more requests
		// log.Printf("downstream: trying to batch more: %p", req.cb)
		var ok bool
		var needRespChanSend bool = false
	moreReqLoop:
		for {
			runtime.Gosched()
			req, ok = peekMoreReq(reqchan)
			if !ok {
				break
			}
			succeeded = sender.TryEnqueueReq(req.req, false)
			if !succeeded {
				break
			}
			// log.Printf("downstream: Batched more: %p, %p", req.cb, req.req)
			// runtime.Gosched()
			atomic.AddUint32(&RequestsSent, 1)
			select {
			case respChans <- req:
			default:
				// log.Printf("downstream: But failed to send respChans: %p", req.cb)
				needRespChanSend = true
				break moreReqLoop
			}
		}
		// log.Printf("downstream: Sending stuff actualy")
		atomic.AddUint32(&Sends, 1)
		err = sender.SendEnqueued()
		if err != nil {
			// TODO: better error handling
			close(respChans)
			log.Panicf("Got error while sending request: %v", err)
		}

		if needRespChanSend {
			// log.Printf("downstream: Sending delayed thing to respChans: %p", req.cb)
			respChans <- req
			// log.Printf("downstream: Done sending to respChans")
			continue
		}

		if ok {
			// log.Printf("downstream: again")
			goto again
		}
	}
}

func (s *sink) SendRequest(req *memcached.MCRequest, cb MCDCallback) {
	// rch = make(chan *memcached.MCResponse, 1)
	reqStruct := request{
		req: req,
		cb:  cb,
	}
	var vbid uint16
	if req.VBucket == 0 {
		hash := vbhash(req.Key)
		vbid = hash & (uint16(s.numVBuckets) - 1)
		// log.Printf("K: %v, H: %d, vbid: %d", req.Key, hash, vbid)
		req.VBucket = vbid
	} else {
		req.VBucket = req.VBucket - 1
		vbid = 0
	}
	serverId := s.vbucketMap[vbid][0]
	counter := atomic.AddUint32(&s.connsRRCounter, 1)
	counter = counter % uint32(s.connsPerDownstream)
	// log.Printf("Sending request %v to server: %s", reqStruct, s.serverList[serverId])
	s.subHandlers[serverId][counter] <- reqStruct
}
