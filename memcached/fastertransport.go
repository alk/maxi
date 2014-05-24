package memcached

// NOTE: this has a bunch of stuff copied (and heavily modified, thus
// all bugs are mine) from dustin's gomemcached:
// http://github.com/dustin/gomemcached

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
)

const (
	REQ_MAGIC = 0x80
	RES_MAGIC = 0x81
)

type CommandCode uint8

const (
	GET        = CommandCode(0x00)
	SET        = CommandCode(0x01)
	ADD        = CommandCode(0x02)
	REPLACE    = CommandCode(0x03)
	DELETE     = CommandCode(0x04)
	INCREMENT  = CommandCode(0x05)
	DECREMENT  = CommandCode(0x06)
	QUIT       = CommandCode(0x07)
	FLUSH      = CommandCode(0x08)
	GETQ       = CommandCode(0x09)
	NOOP       = CommandCode(0x0a)
	VERSION    = CommandCode(0x0b)
	GETK       = CommandCode(0x0c)
	GETKQ      = CommandCode(0x0d)
	APPEND     = CommandCode(0x0e)
	PREPEND    = CommandCode(0x0f)
	STAT       = CommandCode(0x10)
	SETQ       = CommandCode(0x11)
	ADDQ       = CommandCode(0x12)
	REPLACEQ   = CommandCode(0x13)
	DELETEQ    = CommandCode(0x14)
	INCREMENTQ = CommandCode(0x15)
	DECREMENTQ = CommandCode(0x16)
	QUITQ      = CommandCode(0x17)
	FLUSHQ     = CommandCode(0x18)
	APPENDQ    = CommandCode(0x19)
	PREPENDQ   = CommandCode(0x1a)
	HELLO      = CommandCode(0x1f)
	RGET       = CommandCode(0x30)
	RSET       = CommandCode(0x31)
	RSETQ      = CommandCode(0x32)
	RAPPEND    = CommandCode(0x33)
	RAPPENDQ   = CommandCode(0x34)
	RPREPEND   = CommandCode(0x35)
	RPREPENDQ  = CommandCode(0x36)
	RDELETE    = CommandCode(0x37)
	RDELETEQ   = CommandCode(0x38)
	RINCR      = CommandCode(0x39)
	RINCRQ     = CommandCode(0x3a)
	RDECR      = CommandCode(0x3b)
	RDECRQ     = CommandCode(0x3c)

	// this is ep-engine specific. Evicts key's value from ram if
	// it's not dirty
	EVICT_KEY = CommandCode(0x93)

	SASL_LIST_MECHS = CommandCode(0x20)
	SASL_AUTH       = CommandCode(0x21)
	SASL_STEP       = CommandCode(0x22)

	TAP_CONNECT          = CommandCode(0x40)
	TAP_MUTATION         = CommandCode(0x41)
	TAP_DELETE           = CommandCode(0x42)
	TAP_FLUSH            = CommandCode(0x43)
	TAP_OPAQUE           = CommandCode(0x44)
	TAP_VBUCKET_SET      = CommandCode(0x45)
	TAP_CHECKPOINT_START = CommandCode(0x46)
	TAP_CHECKPOINT_END   = CommandCode(0x47)
)

type Status uint16

const (
	SUCCESS         = Status(0x00)
	KEY_ENOENT      = Status(0x01)
	KEY_EEXISTS     = Status(0x02)
	E2BIG           = Status(0x03)
	EINVAL          = Status(0x04)
	NOT_STORED      = Status(0x05)
	DELTA_BADVAL    = Status(0x06)
	NOT_MY_VBUCKET  = Status(0x07)
	UNKNOWN_COMMAND = Status(0x81)
	ENOMEM          = Status(0x82)
	TMPFAIL         = Status(0x86)
)

const (
	BACKFILL          = 0x01
	DUMP              = 0x02
	LIST_VBUCKETS     = 0x04
	TAKEOVER_VBUCKETS = 0x08
	SUPPORT_ACK       = 0x10
	REQUEST_KEYS_ONLY = 0x20
	CHECKPOINT        = 0x40
	REGISTERED_CLIENT = 0x80
)

// Number of bytes in a binary protocol header.
const HDR_LEN = 24

// Mapping of CommandCode -> name of command (not exhaustive)
var CommandNames map[CommandCode]string

var StatusNames map[Status]string

func init() {
	CommandNames = make(map[CommandCode]string)
	CommandNames[GET] = "GET"
	CommandNames[SET] = "SET"
	CommandNames[ADD] = "ADD"
	CommandNames[REPLACE] = "REPLACE"
	CommandNames[DELETE] = "DELETE"
	CommandNames[INCREMENT] = "INCREMENT"
	CommandNames[DECREMENT] = "DECREMENT"
	CommandNames[QUIT] = "QUIT"
	CommandNames[FLUSH] = "FLUSH"
	CommandNames[GETQ] = "GETQ"
	CommandNames[NOOP] = "NOOP"
	CommandNames[VERSION] = "VERSION"
	CommandNames[GETK] = "GETK"
	CommandNames[GETKQ] = "GETKQ"
	CommandNames[APPEND] = "APPEND"
	CommandNames[PREPEND] = "PREPEND"
	CommandNames[STAT] = "STAT"
	CommandNames[SETQ] = "SETQ"
	CommandNames[ADDQ] = "ADDQ"
	CommandNames[REPLACEQ] = "REPLACEQ"
	CommandNames[DELETEQ] = "DELETEQ"
	CommandNames[INCREMENTQ] = "INCREMENTQ"
	CommandNames[DECREMENTQ] = "DECREMENTQ"
	CommandNames[QUITQ] = "QUITQ"
	CommandNames[FLUSHQ] = "FLUSHQ"
	CommandNames[APPENDQ] = "APPENDQ"
	CommandNames[PREPENDQ] = "PREPENDQ"
	CommandNames[RGET] = "RGET"
	CommandNames[RSET] = "RSET"
	CommandNames[RSETQ] = "RSETQ"
	CommandNames[RAPPEND] = "RAPPEND"
	CommandNames[RAPPENDQ] = "RAPPENDQ"
	CommandNames[RPREPEND] = "RPREPEND"
	CommandNames[RPREPENDQ] = "RPREPENDQ"
	CommandNames[RDELETE] = "RDELETE"
	CommandNames[RDELETEQ] = "RDELETEQ"
	CommandNames[RINCR] = "RINCR"
	CommandNames[RINCRQ] = "RINCRQ"
	CommandNames[RDECR] = "RDECR"
	CommandNames[RDECRQ] = "RDECRQ"

	CommandNames[SASL_LIST_MECHS] = "SASL_LIST_MECHS"
	CommandNames[SASL_AUTH] = "SASL_AUTH"
	CommandNames[SASL_STEP] = "SASL_STEP"

	CommandNames[TAP_CONNECT] = "TAP_CONNECT"
	CommandNames[TAP_MUTATION] = "TAP_MUTATION"
	CommandNames[TAP_DELETE] = "TAP_DELETE"
	CommandNames[TAP_FLUSH] = "TAP_FLUSH"
	CommandNames[TAP_OPAQUE] = "TAP_OPAQUE"
	CommandNames[TAP_VBUCKET_SET] = "TAP_VBUCKET_SET"
	CommandNames[TAP_CHECKPOINT_START] = "TAP_CHECKPOINT_START"
	CommandNames[TAP_CHECKPOINT_END] = "TAP_CHECKPOINT_END"

	StatusNames = make(map[Status]string)
	StatusNames[SUCCESS] = "SUCCESS"
	StatusNames[KEY_ENOENT] = "KEY_ENOENT"
	StatusNames[KEY_EEXISTS] = "KEY_EEXISTS"
	StatusNames[E2BIG] = "E2BIG"
	StatusNames[EINVAL] = "EINVAL"
	StatusNames[NOT_STORED] = "NOT_STORED"
	StatusNames[DELTA_BADVAL] = "DELTA_BADVAL"
	StatusNames[NOT_MY_VBUCKET] = "NOT_MY_VBUCKET"
	StatusNames[UNKNOWN_COMMAND] = "UNKNOWN_COMMAND"
	StatusNames[ENOMEM] = "ENOMEM"
	StatusNames[TMPFAIL] = "TMPFAIL"

}

// String an op code.
func (o CommandCode) String() (rv string) {
	rv = CommandNames[o]
	if rv == "" {
		rv = fmt.Sprintf("0x%02x", int(o))
	}
	return rv
}

// String an op code.
func (s Status) String() (rv string) {
	rv = StatusNames[s]
	if rv == "" {
		rv = fmt.Sprintf("0x%02x", int(s))
	}
	return rv
}

// A Memcached Request
type MCRequest struct {
	// The command being issued
	Opcode CommandCode
	// The CAS (if applicable, or 0)
	Cas uint64
	// An opaque value to be returned with this request
	Opaque uint32
	// The vbucket to which this command belongs
	VBucket  uint16
	Datatype byte
	// Command extras, key, and body
	Extras, Key, Body []byte
}

var be = binary.BigEndian

// The number of bytes this request requires.
func (req *MCRequest) Size() int {
	return HDR_LEN + len(req.Extras) + len(req.Key) + len(req.Body)
}

// A debugging string representation of this request
func (req *MCRequest) String() string {
	return fmt.Sprintf("{MCRequest opcode=%s, bodylen=%d, key='%s'}",
		req.Opcode, len(req.Body), req.Key)
}

func (req *MCRequest) FillBytes(data []byte) {

	pos := 0
	data[pos] = REQ_MAGIC
	pos++
	data[pos] = byte(req.Opcode)
	pos++
	binary.BigEndian.PutUint16(data[pos:],
		uint16(len(req.Key)))
	pos += 2

	// 4
	data[pos] = byte(len(req.Extras))
	pos++
	data[pos] = req.Datatype
	pos++
	binary.BigEndian.PutUint16(data[pos:pos+2], req.VBucket)
	pos += 2

	// 8
	binary.BigEndian.PutUint32(data[pos:pos+4],
		uint32(len(req.Body)+len(req.Key)+len(req.Extras)))
	pos += 4

	// 12
	binary.BigEndian.PutUint32(data[pos:pos+4], req.Opaque)
	pos += 4

	// 16
	if req.Cas != 0 {
		binary.BigEndian.PutUint64(data[pos:pos+8], req.Cas)
	}
	pos += 8

	if len(req.Extras) > 0 {
		copy(data[pos:pos+len(req.Extras)], req.Extras)
		pos += len(req.Extras)
	}

	if len(req.Key) > 0 {
		copy(data[pos:pos+len(req.Key)], req.Key)
		pos += len(req.Key)
	}

	if len(req.Body) > 0 {
		copy(data[pos:pos+len(req.Body)], req.Body)
	}
}

// A memcached response
type MCResponse struct {
	// The command opcode of the command that sent the request
	Opcode CommandCode
	// The status of the response
	Status Status
	// The opaque sent in the request
	Opaque uint32
	// The CAS identifier (if applicable)
	Cas uint64
	Datatype byte
	// Extras, key, and body for this response
	Extras, Key, Body []byte
}

// A debugging string representation of this response
func (res *MCResponse) String() string {
	return fmt.Sprintf("{MCResponse status=%v keylen=%d, extralen=%d, bodylen=%d}",
		res.Status, len(res.Key), len(res.Extras), len(res.Body))
}

// Response as an error.
func (res MCResponse) Error() string {
	return fmt.Sprintf("MCResponse status=%v, msg: %s",
		res.Status, string(res.Body))
}

func (res *MCResponse) TryUnpack(slice []byte) (movedBy uint32, err error) {
	if len(slice) < HDR_LEN {
		return 0, nil
	}

	if slice[0] != RES_MAGIC {
		return 0, fmt.Errorf("Bad magic: 0x%02x", slice[0])
	}

	totalSize := be.Uint32(slice[8:12])

	if uint32(len(slice)) < HDR_LEN+totalSize {
		return 0, nil
	}

	var pos uint32 = HDR_LEN

	keySize := uint32(be.Uint16(slice[2:4]))
	key := slice[pos : pos+keySize]
	pos += keySize

	extraSize := uint32(slice[4])
	extra := slice[pos : pos+extraSize]
	pos += extraSize

	body := slice[pos : HDR_LEN+totalSize]

	res.Opcode = CommandCode(slice[1])
	res.Key = key
	res.Extras = extra
	res.Status = Status(be.Uint16(slice[6:8]))
	res.Datatype = slice[5]
	res.Opaque = be.Uint32(slice[12:16])
	res.Cas = be.Uint64(slice[16:24])
	res.Body = body

	return HDR_LEN + totalSize, nil
}

type SendClient struct {
	writeConn io.Writer
	originalBuffer []byte
	sendBuffer []byte
}

const ClientBufSize = 16384

func (c *SendClient) TryEnqueueReq(req *MCRequest, must bool) bool {
	if c.sendBuffer == nil {
		if c.originalBuffer == nil {
			c.originalBuffer = make([]byte, ClientBufSize)
		}
		c.sendBuffer = c.originalBuffer[0:0]
	}

	needed := req.Size()

	blen := len(c.sendBuffer)
	bcap := cap(c.sendBuffer)

	if bcap-blen < needed {
		if must {
			if len(c.sendBuffer) > 0 {
				log.Panicf("something queued when must = true")
			}
			c.sendBuffer = make([]byte, needed)[0:0]
			return c.TryEnqueueReq(req, false)
		}
		return false
	}

	c.sendBuffer = c.sendBuffer[0 : blen+needed]

	req.FillBytes(c.sendBuffer[blen:])

	return true
}

func (c *SendClient) SendEnqueued() (err error) {
	// log.Printf("Sending size: %d", len(c.sendBuffer))
	_, err = c.writeConn.Write(c.sendBuffer)
	if err != nil {
		return
	}

	c.sendBuffer = nil
	return
}

type RecvClient struct {
	reader io.Reader
	buf    []byte
	r, w   uint32
	err    error
}

func NewRecvClient(reader io.Reader, bufsize int) (rc RecvClient) {
	rc.buf = make([]byte, bufsize)
	rc.reader = reader
	return
}

func (b *RecvClient) String() string {
	return fmt.Sprintf("RecvClient{reader = %v, r = %v, w = %v, err = %v}",
		b.reader, b.r, b.w, b.err)
}

func (b *RecvClient) GoString() string {
	return b.String()
}

// fill reads a new chunk into the buffer.
func (b *RecvClient) Fill() {
	// Slide existing data to beginning.
	blen := uint32(len(b.buf))
	if b.w > blen/2 && b.r > 0 {
		copy(b.buf, b.buf[b.r:b.w])
		b.w -= b.r
		b.r = 0
	} else if b.r == 0 && b.w == blen {
		// TODO: consider shrinking buffer
		newbuf := make([]byte, len(b.buf)*2)
		copy(newbuf, b.buf)
		b.buf = newbuf
	}

	// Read new data.
	n, e := b.reader.Read(b.buf[b.w:])
	b.w += uint32(n)
	if e != nil {
		b.err = e
	}
}

func (b *RecvClient) MaxSlice() (slice []byte, err error) {
	if b.err != nil {
		return nil, b.err
	}
	return b.buf[b.r:b.w], nil
}

// func (b *RecvClient) MoveBy(amt uint32) {
// 	newR := b.r + amt
// 	if newR > b.w {
// 		log.Panicf("to big MoveBy amt: %v", amt)
// 	}
// 	b.r += amt
// }

func (b *RecvClient) ConsumeError() (err error) {
	err = b.err
	b.err = nil
	return
}

func (b *RecvClient) TryUnpackResponse(resp *MCResponse) (*MCResponse, error) {
	slice, err := b.MaxSlice()
	if err != nil {
		return nil, err
	}
	movedBy, err := resp.TryUnpack(slice)
	if err != nil {
		return nil, err
	}
	if movedBy > 0 {
		b.r += movedBy
		return resp, nil
	}
	return nil, nil
}

type Client struct {
	Sender SendClient
	Recver RecvClient
	Socket io.ReadWriteCloser
}

const RecverBufSize = 16384

func ClientFromSock(sock io.ReadWriteCloser) (cl Client) {
	cl.Recver = NewRecvClient(sock, RecverBufSize)
	cl.Sender.writeConn = sock
	cl.Socket = sock
	return
}

func (c *Client) Close() {
	c.Socket.Close()
}
