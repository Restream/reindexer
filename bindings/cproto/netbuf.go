package cproto

import (
	"fmt"
	"sync"

	"github.com/restream/reindexer/bindings"
)

var bufPool sync.Pool

type NetBuffer struct {
	buf       []byte
	result    []byte
	conn      *connection
	reqID     int
	args      []interface{}
	closed    bool
	needClose bool
}

func (buf *NetBuffer) Fetch(offset, limit int, withItems bool) (err error) {
	flags := 0
	if withItems {
		flags |= bindings.ResultsWithJson
	} else {
		flags |= bindings.ResultsWithCJson
	}
	//fmt.Printf("cmdFetchResults(reqId=%d, offset=%d, limit=%d, json=%v, flags=%v)\n", buf.reqID, offset, limit, withItems, flags)
	fetchBuf, err := buf.conn.rpcCall(cmdFetchResults, buf.reqID, flags, offset, limit, int64(-1))
	defer fetchBuf.Free()
	if err != nil {
		buf.close()
		return
	}
	fetchBuf.buf, buf.buf = buf.buf, fetchBuf.buf

	if err = buf.parseArgs(); err != nil {
		buf.close()
		return
	}
	buf.result = buf.args[0].([]byte)
	if buf.args[1].(int) == -1 {
		buf.closed = true
	}
	return
}

func (buf *NetBuffer) Free() {
	if buf != nil {
		buf.close()
		buf.result = nil
		bufPool.Put(buf)
	}
}

func (buf *NetBuffer) GetBuf() []byte {
	return buf.result
}

func (buf *NetBuffer) reset(size int, conn *connection) {
	if cap(buf.buf) >= size {
		buf.buf = buf.buf[:size]
	} else {
		buf.buf = make([]byte, size)
	}
	buf.conn = conn
	buf.closed = false
	buf.needClose = false
}

func (buf *NetBuffer) parseArgs() (err error) {
	if buf.args != nil {
		buf.args = buf.args[:0]
	}
	dec := newRPCDecoder(buf.buf)
	if err = dec.errCode(); err != nil {
		return
	}
	retCount := dec.argsCount()
	if retCount > 0 {
		for i := 0; i < retCount; i++ {
			buf.args = append(buf.args, dec.intfArg())
		}
	}
	return
}

func (buf *NetBuffer) close() {
	if buf.needClose && !buf.closed {
		buf.closed = true
		if err := buf.conn.rpcCallNoResults(cmdCloseResults, buf.reqID); err != nil {
			fmt.Printf("query close error: %v", err)
		}
	}
}

func newNetBuffer() *NetBuffer {
	obj := bufPool.Get()
	if obj != nil {
		return obj.(*NetBuffer)
	}
	return &NetBuffer{}
}
