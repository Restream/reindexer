package cproto

import (
	"fmt"
	"io"
	"net"
	"sync"

	"bufio"
	"bytes"
	"github.com/restream/reindexer/cjson"
)

type sig chan []byte

const bufsCap = 16 * 1024
const queueSize = 64

const cprotoMagic = 0xEEDD1132
const cprotoVersion = 0x100
const cprotoHdrLen = 20

const (
	cmdPing            = 0
	cmdOpenNamespace   = 1
	cmdCloseNamespace  = 2
	cmdDropNamespace   = 3
	cmdRenameNamespace = 4
	cmdCloneNamespace  = 5
	cmdAddIndex        = 6
	cmdEnumNamespaces  = 7
	cmdConfigureIndex  = 8
	cmdCommit          = 16
	cmdModifyItem      = 17
	cmdDeleteQuery     = 18
	cmdSelect          = 32
	cmdSelectSQL       = 33
	cmdFetchResults    = 34
	cmdGetMeta         = 48
	cmdPutMeta         = 49
	cmdEnumMeta        = 50
	cmdCodeMax         = 128
)

type connection struct {
	owner *NetCProto
	conn  net.Conn

	wrBuf, wrBuf2 *bytes.Buffer
	wrKick        chan struct{}

	rdBuf *bufio.Reader
	repl  [queueSize]sig
	seqs  chan int
	lock  sync.RWMutex

	err   error
	errCh chan struct{}
}

func newConnection(owner *NetCProto) (c *connection, err error) {
	c = &connection{
		owner:  owner,
		wrBuf:  bytes.NewBuffer(make([]byte, 0, bufsCap)),
		wrBuf2: bytes.NewBuffer(make([]byte, 0, bufsCap)),
		wrKick: make(chan struct{}, 1),
		seqs:   make(chan int, queueSize),
	}
	for i := 0; i < queueSize; i++ {
		c.seqs <- i
		c.repl[i] = make(sig)
	}
	if err = c.connect(); err != nil {
		c.err = err
	}
	return
}

func (c *connection) connect() (err error) {
	c.conn, err = net.Dial("tcp", c.owner.url.Host)
	if err != nil {
		c.err = err
		return err
	}
	c.conn.(*net.TCPConn).SetNoDelay(true)
	c.rdBuf = bufio.NewReaderSize(c.conn, bufsCap)
	c.errCh = make(chan struct{})

	go c.writeLoop()
	go c.readLoop()
	return
}

func (c *connection) readLoop() {
	var err error
	var hdr = make([]byte, cprotoHdrLen)
	for {
		if err = c.readReply(hdr); err != nil {
			c.onError(err)
			return
		}
	}
}

func (c *connection) readReply(hdr []byte) (err error) {
	if _, err = io.ReadFull(c.rdBuf, hdr); err != nil {
		return
	}
	ser := cjson.NewSerializer(hdr)
	magic := ser.GetUInt32()
	version := ser.GetUInt32()
	size := ser.GetUInt32()
	_ = int(ser.GetUInt32())
	rseq := int32(ser.GetUInt32())
	if magic != cprotoMagic {
		return fmt.Errorf("Invalid cproto magic '%08X'", magic)
	}

	if version != cprotoVersion {
		return fmt.Errorf("Invalid version '%08X'", version)
	}

	repCh := c.repl[rseq]
	answ := make([]byte, size)

	if _, err = io.ReadFull(c.rdBuf, answ); err != nil {
		return
	}

	if repCh != nil {
		repCh <- answ
	} else {
		return fmt.Errorf("unexpected answer: %v", answ)
	}
	return
}

func (c *connection) write(buf []byte) {
	c.lock.Lock()
	c.wrBuf.Write(buf)
	c.lock.Unlock()
	select {
	case c.wrKick <- struct{}{}:
	default:
	}
}

func (c *connection) writeLoop() {
	for {
		<-c.wrKick

		c.lock.Lock()
		if c.wrBuf.Len() == 0 {
			err := c.err
			c.lock.Unlock()
			if err == nil {
				continue
			} else {
				return
			}
		}
		c.wrBuf, c.wrBuf2 = c.wrBuf2, c.wrBuf
		c.lock.Unlock()

		if _, err := c.wrBuf2.WriteTo(c.conn); err != nil {
			c.onError(err)
			return
		}
	}
}

func (c *connection) rpcCall(cmd int, args ...interface{}) (ret []interface{}, err error) {
	seq := <-c.seqs
	reply := c.repl[seq]
	in := newRPCEncoder(cmd, seq)
	for _, a := range args {
		switch t := a.(type) {
		case int:
			in.intArg(t)
		case int32:
			in.intArg(int(t))
		case int64:
			in.int64Arg(t)
		case string:
			in.stringArg(t)
		case []byte:
			in.bytesArg(t)
		case []int32:
			in.int32ArrArg(t)
		}
	}

	c.write(in.ser.Bytes())
	in.ser.Close()

	var answer []byte
	select {
	case answer = <-reply:
	case <-c.errCh:
		c.lock.RLock()
		err = c.err
		c.lock.RUnlock()
	}
	c.seqs <- seq
	if err != nil {
		return
	}

	out := newRPCDecoder(answer)
	err = out.errCode()
	if err != nil {
		// fmt.Printf("error: %s\n", err.Error())
		return nil, err
	}
	retCount := out.argsCount()
	ret = make([]interface{}, retCount, retCount)
	for i := 0; i < retCount; i++ {
		ret[i] = out.intfArg()
	}
	// fmt.Printf("ret %v\n", ret)

	return ret, nil
}

func (c *connection) onError(err error) {
	c.lock.Lock()
	c.err = err
	c.conn.Close()
	c.lock.Unlock()
	close(c.errCh)
}

func (c *connection) hasError() (has bool) {
	c.lock.RLock()
	has = c.err != nil
	c.lock.RUnlock()
	return
}
