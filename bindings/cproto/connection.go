package cproto

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/restream/reindexer/cjson"
)

type sig chan *NetBuffer

const bufsCap = 16 * 1024
const queueSize = 40

const cprotoMagic = 0xEEDD1132
const cprotoVersion = 0x101
const cprotoHdrLen = 16

const (
	cmdPing             = 0
	cmdLogin            = 1
	cmdOpenDatabase     = 2
	cmdCloseDatabase    = 3
	cmdDropDatabase     = 4
	cmdOpenNamespace    = 16
	cmdCloseNamespace   = 17
	cmdDropNamespace    = 18
	cmdAddIndex         = 21
	cmdEnumNamespaces   = 22
	cmdDropIndex        = 24
	cmdUpdateIndex      = 25
	сmdStartTransaction = 25
	сmdAddTxItem        = 26
	сmdCommitTx         = 27
	сmdRollbackTx       = 28
	cmdCommit           = 32
	cmdModifyItem       = 33
	cmdDeleteQuery      = 34
	cmdSelect           = 48
	cmdSelectSQL        = 49
	cmdFetchResults     = 50
	cmdCloseResults     = 51
	cmdGetMeta          = 64
	cmdPutMeta          = 65
	cmdEnumMeta         = 66
	cmdCodeMax          = 128
)

type connection struct {
	owner *NetCProto
	conn  net.Conn

	wrBuf, wrBuf2 *bytes.Buffer
	wrKick        chan struct{}

	rdBuf *bufio.Reader
	repl  [queueSize]sig

	seqs chan int
	lock sync.RWMutex

	err   error
	errCh chan struct{}

	lastReadStamp int64
}

func newConnection(owner *NetCProto) (c *connection, err error) {
	c = &connection{
		owner:  owner,
		wrBuf:  bytes.NewBuffer(make([]byte, 0, bufsCap)),
		wrBuf2: bytes.NewBuffer(make([]byte, 0, bufsCap)),
		wrKick: make(chan struct{}, 1),
		seqs:   make(chan int, queueSize),
		errCh:  make(chan struct{}),
	}
	for i := 0; i < queueSize; i++ {
		c.seqs <- i
		c.repl[i] = make(sig)
	}
	if err = c.connect(); err != nil {
		c.onError(err)
		return
	}
	if err = c.login(owner); err != nil {
		c.onError(err)
		return
	}
	return
}

func (c *connection) connect() (err error) {
	c.conn, err = net.Dial("tcp", c.owner.url.Host)
	if err != nil {
		return err
	}
	c.conn.(*net.TCPConn).SetNoDelay(true)
	c.rdBuf = bufio.NewReaderSize(c.conn, bufsCap)

	go c.writeLoop()
	go c.readLoop()
	return
}

func (c *connection) login(owner *NetCProto) (err error) {
	password, username, path := "", "", owner.url.Path
	if owner.url.User != nil {
		username = owner.url.User.Username()
		password, _ = owner.url.User.Password()
	}
	if len(path) > 0 && path[0] == '/' {
		path = path[1:]
	}

	buf, err := c.rpcCall(cmdLogin, username, password, path)
	if err != nil {
		c.err = err
		return
	}
	defer buf.Free()

	if len(buf.args) > 1 {
		owner.checkServerStartTime(buf.args[1].(int64))
	}
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
		atomic.StoreInt64(&c.lastReadStamp, time.Now().Unix())
	}
}

func (c *connection) readReply(hdr []byte) (err error) {
	if _, err = io.ReadFull(c.rdBuf, hdr); err != nil {
		return
	}
	ser := cjson.NewSerializer(hdr)
	magic := ser.GetUInt32()
	version := ser.GetUInt16()
	_ = int(ser.GetUInt16())
	size := int(ser.GetUInt32())
	rseq := int32(ser.GetUInt32())
	if magic != cprotoMagic {
		return fmt.Errorf("Invalid cproto magic '%08X'", magic)
	}

	if version < cprotoVersion {
		return fmt.Errorf("Unsupported cproto version '%04X'. This client expects reindexer server v1.9.8+", version)
	}

	repCh := c.repl[rseq]
	answ := newNetBuffer()
	answ.reset(size, c)

	if _, err = io.ReadFull(c.rdBuf, answ.buf); err != nil {
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
		select {
		case <-c.errCh:
			return
		case <-c.wrKick:
		}
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

func (c *connection) rpcCall(cmd int, args ...interface{}) (buf *NetBuffer, err error) {
	seq := <-c.seqs
	reply := c.repl[seq]
	in := newRPCEncoder(cmd, seq)
	for _, a := range args {
		switch t := a.(type) {
		case bool:
			in.boolArg(t)
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

	select {
	case buf = <-reply:
	case <-c.errCh:
		c.lock.RLock()
		err = c.err
		c.lock.RUnlock()
	}
	c.seqs <- seq
	if err != nil {
		return
	}
	if err = buf.parseArgs(); err != nil {
		return
	}
	return
}

func (c *connection) onError(err error) {
	c.lock.Lock()
	if c.err == nil {
		c.err = err
		if c.conn != nil {
			c.conn.Close()
		}
		select {
		case <-c.errCh:
		default:
			close(c.errCh)
		}
	}
	c.lock.Unlock()
}

func (c *connection) hasError() (has bool) {
	c.lock.RLock()
	has = c.err != nil
	c.lock.RUnlock()
	return
}

func (c *connection) lastReadTime() time.Time {
	stamp := atomic.LoadInt64(&c.lastReadStamp)
	return time.Unix(stamp, 0)
}
