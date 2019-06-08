package cproto

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"testing"
)

func TestCprotoPool(t *testing.T) {
	t.Run("connection errors", func(t *testing.T) {
		invalidAddr, _ := url.Parse("cproto://1234567890")
		c := new(NetCProto)
		err := c.Init(invalidAddr)
		t.Logf("init error: %v", err)
		if err == nil {
			t.Errorf("Must be error for invalid address, but got nil")
			return
		}

		pingErr := c.Ping(context.Background())
		if pingErr == nil || pingErr.Error() != err.Error() {
			t.Errorf("Must be connection error, but got: %v; want: %v", pingErr, err)
		}
	})

	t.Run("success connection", func(t *testing.T) {
		t.Skip("think about mock login")
		serv, addr, err := runTestServer()
		if err != nil {
			t.Skipf("Can't run test server: %v", err)
			return
		}
		defer serv.Close()

		c := new(NetCProto)
		err = c.Init(addr)
		if err != nil {
			t.Errorf("Can't init client: %v", err)
			return
		}

		if len(serv.conns) != connPoolSize {
			t.Errorf("Unexpected connections count. Got %d; (want: %d)", len(serv.conns), connPoolSize)
		}
	})

}

func runTestServer() (s *testServer, addr *url.URL, err error) {
	startPort := 40000
	var l net.Listener
	var port int
	for port = startPort; port < startPort+10; port++ {
		if l, err = net.Listen("tcp", fmt.Sprintf(":%d", port)); err == nil {
			break
		}
	}
	if err != nil {
		return
	}
	s = &testServer{l: l}
	go s.acceptLoop()
	addr, _ = url.Parse(fmt.Sprintf("cproto://127.0.0.01:%d", port))
	return
}

type testServer struct {
	l     net.Listener
	conns []net.Conn
}

func (s *testServer) acceptLoop() {
	for {
		conn, err := s.l.Accept()
		if err != nil {
			return
		}
		s.conns = append(s.conns, conn)
	}
}

func (s *testServer) Close() {
	s.l.Close()
}
