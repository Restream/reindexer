package cproto

import (
	"context"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/restream/reindexer/v5/bindings"
)

type MockConnection struct {
	t        *testing.T
	rec      *recMockConnection
	expCalls map[string]*call
}

type recMockConnection struct {
	args []interface{}
	mock *MockConnection
}

type MockConnFactory struct {
	t        *testing.T
	rec      *recMockConnFactory
	expCalls map[string]*call
}

type recMockConnFactory struct {
	args []interface{}
	mock *MockConnFactory
}

func (mcf *MockConnFactory) expect() *recMockConnFactory {
	return mcf.rec
}

func (mc *MockConnection) expect() *recMockConnection {
	return mc.rec
}

func NewMockConnFactory(t *testing.T) *MockConnFactory {
	mock := &MockConnFactory{
		t:        t,
		expCalls: make(map[string]*call, 0),
	}
	mock.rec = &recMockConnFactory{mock: mock}
	return mock
}

func NewMockConnection(t *testing.T) *MockConnection {
	mock := &MockConnection{
		t:        t,
		expCalls: make(map[string]*call, 0),
	}
	mock.rec = &recMockConnection{mock: mock}
	return mock
}

func (mcf *MockConnFactory) newConnection(ctx context.Context, params newConnParams, loggerOwner LoggerOwner, eh bindings.EventsHandler) (connection, string, int64, error) {
	c, ok := mcf.expCalls["newConnection"]
	if !ok {
		mcf.t.Fatalf("unexpected call newConnection")
	}
	ret0, _ := c.rets[0].(connection)
	ret1, _ := c.rets[1].(string)
	ret2, _ := c.rets[2].(int64)
	ret3, _ := c.rets[3].(error)
	return ret0, ret1, ret2, ret3
}

func (rec *recMockConnFactory) newConnection(ctx context.Context, params newConnParams, loggerOwner LoggerOwner) *call {
	c := &call{
		method: reflect.TypeOf((*MockConnFactory)(nil).newConnection),
	}
	rec.mock.expCalls["newConnection"] = c
	return c
}

func (mc *MockConnection) hasError() bool {
	c, ok := mc.expCalls["hasError"]
	if !ok {
		mc.t.Fatalf("unexpected call hasError")
	}
	ret0, _ := c.rets[0].(bool)
	return ret0
}

func (rec *recMockConnection) hasError() *call {
	c := &call{
		method: reflect.TypeOf((*MockConnection)(nil).hasError),
	}
	rec.mock.expCalls["hasError"] = c
	return c
}

func (rec *recMockConnection) rpcCall() *call {
	c := &call{
		method: reflect.TypeOf((*MockConnection)(nil).rpcCall),
	}
	rec.mock.expCalls["rpcCall"] = c
	return c
}

func (mc *MockConnection) rpcCall(ctx context.Context, cmd int, netTimeout uint32, args ...interface{}) (buf *NetBuffer, err error) {
	c, ok := mc.expCalls["rpcCall"]
	if !ok {
		mc.t.Fatalf("unexpected call rpcCall")
	}
	ret0, _ := c.rets[0].(*NetBuffer)
	ret1, _ := c.rets[1].(error)
	return ret0, ret1
}
func (mc *MockConnection) rpcCallNoResults(ctx context.Context, cmd int, netTimeout uint32, args ...interface{}) error {
	return nil
}
func (mc *MockConnection) rpcCallAsync(ctx context.Context, cmd int, netTimeout uint32, cmpl bindings.RawCompletion, args ...interface{}) {
}
func (mc *MockConnection) onError(err error) {
}
func (mc *MockConnection) rpcCallNoReply(ctx context.Context, cmd int, netTimeout uint32, seq uint32, args ...interface{}) {
}
func (mc *MockConnection) curError() error {
	c, ok := mc.expCalls["curError"]
	if !ok {
		mc.t.Fatalf("unexpected call curError")
	}
	ret0, _ := c.rets[0].(error)
	return ret0
}

func (rec *recMockConnection) curError() *call {
	c := &call{
		method: reflect.TypeOf((*MockConnection)(nil).curError),
	}
	rec.mock.expCalls["curError"] = c
	return c
}
func (mc *MockConnection) lastReadTime() time.Time {
	return time.Time{}
}
func (mc *MockConnection) finalize() error {
	return nil
}
func (mc *MockConnection) getConnection() net.Conn {
	return nil
}
func (mc *MockConnection) getSeqs() chan uint32 {
	return nil
}
func (mc *MockConnection) getRequestTimeout() uint32 {
	return 0
}
func (mc *MockConnection) logMsg(level int, fmt string, msg ...interface{}) {
}

type call struct {
	rets   []interface{}
	args   []interface{}
	method reflect.Type
}

func (c *call) Return(rets ...interface{}) []interface{} {
	c.rets = rets
	return rets
}
