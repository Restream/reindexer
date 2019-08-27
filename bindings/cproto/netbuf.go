package cproto

import (
	"context"
	"fmt"
	"sync"
	"time"

	"git.itv.restr.im/itv-backend/reindexer/bindings"
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

func (buf *NetBuffer) Fetch(ctx context.Context, offset, limit int, asJson bool) (err error) {
	flags := 0
	if asJson {
		flags |= bindings.ResultsJson
	} else {
		flags |= bindings.ResultsCJson | bindings.ResultsWithItemID
	}
	//fmt.Printf("cmdFetchResults(reqId=%d, offset=%d, limit=%d, json=%v, flags=%v)\n", buf.reqID, offset, limit, asJson, flags)
	netTimeout := uint32(buf.conn.owner.timeouts.RequestTimeout / time.Second)
	fetchBuf, err := buf.conn.rpcCall(ctx, cmdFetchResults, netTimeout, buf.reqID, flags, offset, limit)
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
		if rerr, ok := err.(bindings.Error); ok {
			if rerr.Code() == bindings.ErrTimeout {
				err = context.DeadlineExceeded
			} else if rerr.Code() == bindings.ErrCanceled {
				err = context.Canceled
			}
		}
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
		netTimeout := uint32(buf.conn.owner.timeouts.RequestTimeout / time.Second)
		closeBuf, err := buf.conn.rpcCall(context.TODO(), cmdCloseResults, netTimeout, buf.reqID)
		if err != nil {
			fmt.Printf("rx: query close error: %v", err)
		}
		closeBuf.Free()
	}
}

func newNetBuffer() *NetBuffer {
	obj := bufPool.Get()
	if obj != nil {
		return obj.(*NetBuffer)
	}
	return &NetBuffer{}
}
