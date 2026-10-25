package reindexer

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/restream/reindexer/v5"
	"github.com/restream/reindexer/v5/events"
	"github.com/restream/reindexer/v5/test/helpers"
	"github.com/stretchr/testify/require"
)

type subscriptionLagItem struct {
	ID int `reindex:"id,,pk"`
}

// Forwards cproto connections and may stop reading server's responses, so the server's side of the subscription starts to lag
type pausableProxy struct {
	listener net.Listener
	target   string
	mtx      sync.Mutex
	resumed  *sync.Cond
	paused   bool
	closed   bool
	conns    []net.Conn
}

const (
	testSubscriptionLagNs = "test_subscription_lag"

	subscriptionLagTimeout = 20 * time.Second
	subscriptionLagTxSize  = 1000
	// Has to be large enough to overflow socket buffers between the server and a paused proxy
	subscriptionLagEvents = 100 * subscriptionLagTxSize
)

func newPausableProxy(t *testing.T, target string) *pausableProxy {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	p := &pausableProxy{listener: listener, target: target}
	p.resumed = sync.NewCond(&p.mtx)
	go p.acceptLoop()
	t.Cleanup(p.close)
	return p
}

func (p *pausableProxy) dsn(dbName string) string {
	return fmt.Sprintf("cproto://%s/%s", p.listener.Addr().String(), dbName)
}

func (p *pausableProxy) acceptLoop() {
	for {
		client, err := p.listener.Accept()
		if err != nil {
			return
		}
		server, err := net.Dial("tcp", p.target)
		if err != nil {
			client.Close()
			continue
		}
		// Limits buffering on the proxy's side, but still has to exceed 2*MSS (64KB on loopback) to avoid delayed-ACK throttling
		server.(*net.TCPConn).SetReadBuffer(256 * 1024)

		p.mtx.Lock()
		if p.closed {
			p.mtx.Unlock()
			client.Close()
			server.Close()
			return
		}
		p.conns = append(p.conns, client, server)
		p.mtx.Unlock()

		go func() {
			io.Copy(server, client)
			server.Close()
		}()
		go p.forwardResponses(client, server)
	}
}

func (p *pausableProxy) forwardResponses(client, server net.Conn) {
	defer client.Close()
	buf := make([]byte, 16*1024)
	for {
		p.mtx.Lock()
		for p.paused {
			p.resumed.Wait()
		}
		p.mtx.Unlock()

		n, err := server.Read(buf)
		if n > 0 {
			if _, werr := client.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func (p *pausableProxy) pause() {
	p.mtx.Lock()
	defer p.mtx.Unlock()
	p.paused = true
}

func (p *pausableProxy) resume() {
	p.mtx.Lock()
	defer p.mtx.Unlock()
	p.paused = false
	p.resumed.Broadcast()
}

func (p *pausableProxy) close() {
	p.listener.Close()
	p.mtx.Lock()
	defer p.mtx.Unlock()
	p.closed = true
	p.paused = false
	p.resumed.Broadcast()
	for _, c := range p.conns {
		c.Close()
	}
	p.conns = nil
}

func startSubscriptionLagServer(t *testing.T) *helpers.TestServer {
	srv := &helpers.TestServer{T: t, RpcPort: "26540", HttpPort: "29095", DbName: "subscription_lag", SrvType: helpers.ServerTypeBuiltin}
	require.NoError(t, srv.Clean())
	require.NoError(t, srv.Run())
	t.Cleanup(func() {
		// The server may be stuck in the unsubscription, so its stop would hang the whole tests run
		if !t.Failed() {
			require.NoError(t, srv.Stop())
			srv.Clean()
		}
	})
	require.NoError(t, srv.DB().OpenNamespace(testSubscriptionLagNs, reindexer.DefaultNamespaceOptions(), subscriptionLagItem{}))
	return srv
}

func connectSubscriber(t *testing.T, dsn string, opts *events.EventsStreamOptions) *atomic.Int64 {
	db, err := reindexer.NewReindex(dsn, reindexer.WithConnPoolSize(1))
	require.NoError(t, err)
	stream := db.Subscribe(opts)
	require.NoError(t, stream.Error())
	t.Cleanup(func() {
		if !t.Failed() {
			db.Close()
		}
	})

	upserts := &atomic.Int64{}
	go func() {
		for event := range stream.Chan() {
			if event.Type() == events.EventTypeItemUpsertTx {
				upserts.Add(1)
			}
		}
	}()
	return upserts
}

func upsertSubscriptionLagItems(t *testing.T, db *reindexer.Reindexer, from int, count int) {
	for txFrom := from; txFrom < from+count; txFrom += subscriptionLagTxSize {
		tx, err := db.BeginTx(testSubscriptionLagNs)
		require.NoError(t, err)
		for id := txFrom; id < txFrom+subscriptionLagTxSize; id++ {
			require.NoError(t, tx.UpsertAsync(&subscriptionLagItem{ID: id}, func(error) {}))
		}
		require.NoError(t, tx.Commit())
	}
}

func requireSubscriptionIsResponsive(t *testing.T, dsn string) {
	done := make(chan error, 1)
	go func() {
		db, err := reindexer.NewReindex(dsn)
		if err != nil {
			done <- err
			return
		}
		defer db.Close()
		stream := db.Subscribe(events.DefaultEventsStreamOptions())
		if err = stream.Error(); err != nil {
			done <- err
			return
		}
		done <- stream.Close(context.Background())
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(subscriptionLagTimeout):
		require.FailNow(t, "Subscription and unsubscription have not finished in time")
	}
}

func waitSubscriptionLag(condition func() bool) {
	deadline := time.Now().Add(subscriptionLagTimeout)
	for !condition() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
}

func TestSubscriptionUnsubscribeSlowestWhileOthersLag(t *testing.T) {
	srv := startSubscriptionLagServer(t)
	proxyLagging := newPausableProxy(t, srv.GetRpcAddr())
	proxySlowest := newPausableProxy(t, srv.GetRpcAddr())
	opts := events.DefaultEventsStreamOptions().WithNamespacesList(testSubscriptionLagNs)
	lagging := connectSubscriber(t, proxyLagging.dsn(srv.GetDbName()), opts)
	connectSubscriber(t, proxySlowest.dsn(srv.GetDbName()), opts)

	proxySlowest.pause()
	upsertSubscriptionLagItems(t, srv.DB(), 0, subscriptionLagEvents)
	waitSubscriptionLag(func() bool { return lagging.Load() == subscriptionLagEvents })
	require.Equal(t, int64(subscriptionLagEvents), lagging.Load())

	proxyLagging.pause()
	upsertSubscriptionLagItems(t, srv.DB(), subscriptionLagEvents, subscriptionLagEvents)
	proxySlowest.close()
	requireSubscriptionIsResponsive(t, srv.GetDSN())

	proxyLagging.resume()
	waitSubscriptionLag(func() bool { return lagging.Load() == 2*subscriptionLagEvents })
	require.Equal(t, int64(2*subscriptionLagEvents), lagging.Load())
}
