//go:build !unittest
// +build !unittest

package cproto

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/restream/reindexer/v5"
	"github.com/restream/reindexer/v5/bindings"
	"github.com/restream/reindexer/v5/test/helpers"
)

type testServer struct {
	l     net.Listener
	conns []net.Conn
}

var benchmarkSeed = flag.Int64("seed", time.Now().Unix(), "seed number for random")

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
	addr, _ = url.Parse(fmt.Sprintf("cproto://127.0.0.1:%d", port))
	return
}

func BenchmarkGetConn(b *testing.B) {
	srv1 := helpers.TestServer{T: nil, RpcPort: "6651", HttpPort: "9951", DbName: "cproto", SrvType: helpers.ServerTypeBuiltin}
	if err := srv1.Run(); err != nil {
		panic(err)
	}
	defer srv1.Stop()

	binding := NetCProto{}
	u, _ := url.Parse(fmt.Sprintf("cproto://127.0.0.1:%s/%s_%s", srv1.RpcPort, srv1.DbName, srv1.RpcPort))
	dsn := []url.URL{*u}
	err := binding.Init(dsn, nil, bindings.OptionConnect{CreateDBIfMissing: true})
	if err != nil {
		panic(err)
	}

	b.Run("getConn", func(b *testing.B) {
		var conn connection
		ctx := context.Background()
		for i := 0; i < b.N; i++ {
			conn, err = binding.getConnection(ctx)
			if err != nil {
				panic(err)
			}
		}

		_ = conn
	})

}

func TestCprotoPool(t *testing.T) {
	t.Run("success connection", func(t *testing.T) {
		t.Skip("think about mock login")
		serv, addr, err := runTestServer()
		require.NoError(t, err)
		defer serv.Close()

		c := new(NetCProto)
		err = c.Init([]url.URL{*addr}, nil)
		require.NoError(t, err)

		assert.Equal(t, defConnPoolSize, len(serv.conns))
	})

	t.Run("rotate connections on each getConn", func(t *testing.T) {
		srv1 := helpers.TestServer{T: t, RpcPort: "6661", HttpPort: "9961", DbName: "cproto", SrvType: helpers.ServerTypeBuiltin}
		dsn := fmt.Sprintf("cproto://127.0.0.1:%s/%s_%s", srv1.RpcPort, srv1.DbName, srv1.RpcPort)

		err := srv1.Run()
		require.NoError(t, err)
		defer srv1.Stop()

		u, err := url.Parse(dsn)
		require.NoError(t, err)
		c := new(NetCProto)
		err = c.Init([]url.URL{*u}, nil, reindexer.WithConnPoolLoadBalancing(bindings.LBRoundRobin))
		require.NoError(t, err)

		conns := make(map[connection]bool)
		for i := 0; i < defConnPoolSize; i++ {
			conn, err := c.getConnection(context.Background())
			require.NoError(t, err)
			if _, ok := conns[conn]; ok {
				t.Fatalf("getConn not rotate conn")
			}
			conns[conn] = true
		}

		// return anew from the pool
		conn, err := c.getConnection(context.Background())
		require.NoError(t, err)
		if _, ok := conns[conn]; !ok {
			t.Fatalf("getConn not rotate conn")
		}
	})
}

func TestCprotoStatus(t *testing.T) {
	t.Run("check error status", func(t *testing.T) {
		srv := helpers.TestServer{T: t, RpcPort: "6661", HttpPort: "9961", DbName: "cproto", SrvType: helpers.ServerTypeBuiltin}
		dsn := srv.GetDSN()

		err := srv.Run()
		assert.NoError(t, err)
		defer srv.Stop()

		u, err := url.Parse(dsn)
		assert.NoError(t, err)
		c := new(NetCProto)
		err = c.Init([]url.URL{*u}, nil)
		assert.NoError(t, err)

		status := c.Status(context.Background())
		assert.NoError(t, status.Err)
		srv.Stop()
		status = c.Status(context.Background())
		assert.Error(t, status.Err)
		err = srv.Run()
		status = c.Status(context.Background())
		assert.NoError(t, status.Err)
	})
}

func TestStatusError(t *testing.T) {
	expectedError := "failed to connect with provided dsn; dial tcp 127.0.0.1:6661: connect: connection refused"

	srv1 := helpers.TestServer{T: t, RpcPort: "6661", HttpPort: "9961", DbName: "reindex_test_status_db", SrvType: helpers.ServerTypeBuiltin}
	defer srv1.Clean()

	// Check for connection error - server not started
	dsn := fmt.Sprintf("cproto://127.0.0.1:%s/%s_%s", srv1.RpcPort, srv1.DbName, srv1.RpcPort)
	_, err := reindexer.NewReindex(dsn, reindexer.WithCreateDBIfMissing())
	assert.ErrorContains(t, err, expectedError)

	// Start server
	err = srv1.Run()
	assert.NoError(t, err)

	// Check no error after server run
	db, err := reindexer.NewReindex(dsn, reindexer.WithCreateDBIfMissing())
	require.NoError(t, err)
	defer db.Close()

	// Stop server
	err = srv1.Stop()
	assert.NoError(t, err)

	// Check for connection error - server stopped
	status := db.Status()
	assert.Equal(t, expectedError, status.Err.Error())
}

func TestInvalidDSNFromGetStatus(t *testing.T) {
	var tests = []struct {
		dsn  string
		want string
	}{
		{
			"cproto://127.0.0.1::6661/some",
			"failed to connect with provided dsn; dial tcp: address 127.0.0.1::6661: too many colons in address",
		},
		{
			"cproto:///127.0.0.1:6661/some",
			"failed to connect with provided dsn; dial tcp: missing address",
		},
		{
			"cproto://127.0..0.1:6661/some",
			"failed to connect with provided dsn; dial tcp: lookup 127.0..0.1: no such host",
		},
		{
			"cproto:://127.0.0.1:6661/some",
			"failed to connect with provided dsn; dial tcp: missing address",
		},
	}

	for _, tt := range tests {
		testname := fmt.Sprintf("Test for dsn: %s", tt.dsn)
		t.Run(testname, func(t *testing.T) {
			dsn := fmt.Sprintf(tt.dsn)
			_, err := reindexer.NewReindex(dsn, reindexer.WithCreateDBIfMissing())
			assert.ErrorContains(t, err, tt.want)
		})
	}
}
