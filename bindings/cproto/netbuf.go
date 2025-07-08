package cproto

import (
	"context"
	"sync"

	"github.com/restream/reindexer/v5/bindings"
	"github.com/golang/snappy"
)

var bufPool sync.Pool

type NetBuffer struct {
	buf   []byte
	conn  connection
	reqID int
	uid   int64
	args  []interface{}
}

func (buf *NetBuffer) Fetch(ctx context.Context, offset, limit int, asJson bool) (err error) {
	flags := 0
	if asJson {
		flags |= bindings.ResultsJson
	} else {
		flags |= bindings.ResultsCJson | bindings.ResultsWithItemID
	}
	flags |= bindings.ResultsSupportIdleTimeout
	fetchBuf, err := buf.conn.rpcCall(ctx, cmdFetchResults, buf.conn.getRequestTimeout(), buf.reqID, flags, offset, limit, buf.uid)
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
	if buf.args[1].(int) == -1 {
		buf.reqID = -1
		buf.uid = -1
	}
	return
}

func (buf *NetBuffer) Free() {
	if buf != nil {
		buf.close()
		bufPool.Put(buf)
	}
}

func (buf *NetBuffer) FreeNoReply(seq uint32) {
	if buf != nil {
		buf.closeNoReply(seq)
		bufPool.Put(buf)
	}
}

func (buf *NetBuffer) GetBuf() []byte {
	return buf.args[0].([]byte)
}

func (buf *NetBuffer) needClose() bool {
	return buf.reqID != -1
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
	if buf.needClose() {
		closeBuf, err := buf.conn.rpcCall(context.TODO(), cmdCloseResults, buf.conn.getRequestTimeout(), buf.reqID, buf.uid)
		buf.reqID = -1
		buf.uid = -1
		if err != nil {
			buf.conn.logMsg(bindings.ERROR, "rq: query close error: %v\n", err)
		}
		closeBuf.Free()
	}
}

func (buf *NetBuffer) closeNoReply(seq uint32) {
	if buf.needClose() {
		buf.conn.rpcCallNoReply(context.TODO(), cmdCloseResults, buf.conn.getRequestTimeout(), seq, buf.reqID, buf.uid, true)
		buf.reqID = -1
		buf.uid = -1
	}
}

func newNetBuffer(size int, conn connection) (buf *NetBuffer) {
	obj := bufPool.Get()
	if obj != nil {
		buf = obj.(*NetBuffer)
	} else {
		buf = &NetBuffer{}
	}
	if cap(buf.buf) >= size {
		buf.buf = buf.buf[:size]
	} else {
		buf.buf = make([]byte, size)
	}
	buf.conn = conn
	buf.reqID = -1
	buf.uid = -1
	if len(buf.args) > 0 {
		buf.args = buf.args[:0]
	}

	return buf
}

func (buf *NetBuffer) decompress() (err error) {
	buf.buf, err = snappy.Decode(nil, buf.buf)
	return err
}
