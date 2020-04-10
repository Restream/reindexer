package reindexer

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/restream/reindexer"
	_ "github.com/restream/reindexer/bindings/builtinserver"
	"github.com/restream/reindexer/bindings/builtinserver/config"
)

var httpCl = http.DefaultClient

func TestMultipleDSN(t *testing.T) {
	ns := "items"
	type Item struct {
		Name string `json:"name" reindex:"name,,pk"`
	}

	t.Run("connect to next dsn if current not available", func(t *testing.T) {
		t.Parallel()
		srv1 := TestServer{t: t, rpcPort: "6661", httpPort: "9991"}
		srv2 := TestServer{t: t, rpcPort: "6662", httpPort: "9992"}
		srv3 := TestServer{t: t, rpcPort: "6663", httpPort: "9993"}
		dsn := []string{
			fmt.Sprintf("cproto://127.0.0.1:%s/reindex_test_multi_dsn_%s", srv1.rpcPort, srv1.rpcPort),
			fmt.Sprintf("cproto://127.0.0.1:%s/reindex_test_multi_dsn_%s", srv2.rpcPort, srv2.rpcPort),
			fmt.Sprintf("cproto://127.0.0.1:%s/reindex_test_multi_dsn_%s", srv3.rpcPort, srv3.rpcPort),
		}

		srv1.Run()
		srv2.Run()
		srv3.Run()

		defer srv1.Clean()
		defer srv2.Clean()
		defer srv3.Clean()

		db := reindexer.NewReindex(dsn, reindexer.WithCreateDBIfMissing())
		require.NotNil(t, db)

		err := db.OpenNamespace(ns, reindexer.DefaultNamespaceOptions().DropOnIndexesConflict(), Item{})
		require.NoError(t, err)

		itemExp := Item{Name: "some_name"}
		err = db.Upsert(ns, itemExp)
		require.NoError(t, err)

		item, err := db.Query(ns).Exec().FetchAll()
		require.NoError(t, err)
		assert.Equal(t, itemExp, *item[0].(*Item))
		require.Equal(t, srv1.Addr(), db.Status().CProto.ConnAddr)

		srv1.Stop()
		srv2.Stop()

		// check change dsn to third address, with skip second stopped
		err = db.OpenNamespace(ns, reindexer.DefaultNamespaceOptions().DropOnIndexesConflict(), Item{})
		require.NoError(t, err)
		_, err = db.Insert(ns, itemExp)
		require.NoError(t, err)

		item, err = db.Query(ns).Exec().FetchAll()
		require.NoError(t, err)
		assert.Equal(t, itemExp, *item[0].(*Item))
		require.Equal(t, srv3.Addr(), db.Status().CProto.ConnAddr)

		srv3.Stop()
		srv1.Run()

		// check reconnect to first
		err = db.OpenNamespace(ns, reindexer.DefaultNamespaceOptions().DropOnIndexesConflict(), Item{})
		require.NoError(t, err)
		item, err = db.Query(ns).Exec().FetchAll()
		require.NoError(t, err)
		assert.Equal(t, itemExp, *item[0].(*Item))
		require.Equal(t, srv1.Addr(), db.Status().CProto.ConnAddr)
		srv1.Stop()
	})

	t.Run("return err if all srv stopped", func(t *testing.T) {
		t.Parallel()
		srv1 := TestServer{t: t, rpcPort: "6664", httpPort: "9994"}
		srv2 := TestServer{t: t, rpcPort: "6665", httpPort: "9995"}
		srv3 := TestServer{t: t, rpcPort: "6666", httpPort: "9996"}
		dsn := []string{
			fmt.Sprintf("cproto://127.0.0.1:%s/reindex_test_multi_dsn_%s", srv1.rpcPort, srv1.rpcPort),
			fmt.Sprintf("cproto://127.0.0.1:%s/reindex_test_multi_dsn_%s", srv2.rpcPort, srv2.rpcPort),
			fmt.Sprintf("cproto://127.0.0.1:%s/reindex_test_multi_dsn_%s", srv3.rpcPort, srv3.rpcPort),
		}

		errExp := "failed to connect with provided dsn; dial tcp 127.0.0.1:6665: connect: connection refused; dial tcp 127.0.0.1:6666: connect: connection refused; dial tcp 127.0.0.1:6664: connect: connection refused"
		db := reindexer.NewReindex(dsn, reindexer.WithCreateDBIfMissing())
		require.NotNil(t, db)

		err := db.OpenNamespace(ns, reindexer.DefaultNamespaceOptions().DropOnIndexesConflict(), Item{})
		require.Error(t, err)
		assert.Equal(t, errExp, err.Error())
	})
}

func TestRaceConditionsMultiDSN(t *testing.T) {
	srv1 := TestServer{t: t, rpcPort: "6667", httpPort: "9997"}
	srv2 := TestServer{t: t, rpcPort: "6668", httpPort: "9998"}

	t.Run("on reconnect to next dsn", func(t *testing.T) {
		t.Parallel()
		ns := "items"
		type Item struct {
			Name string `json:"name" reindex:"name,,pk"`
		}

		srv1.Run()
		srv2.Run()
		dsn := []string{
			fmt.Sprintf("cproto://127.0.0.1:%s/reindex_test_multi_dsn_%s", srv1.rpcPort, srv1.rpcPort),
			fmt.Sprintf("cproto://127.0.0.1:%s/reindex_test_multi_dsn_%s", srv2.rpcPort, srv2.rpcPort),
		}

		defer srv1.Clean()
		defer srv2.Clean()

		db := reindexer.NewReindex(dsn, reindexer.WithCreateDBIfMissing())
		require.NotNil(t, db)

		err := db.OpenNamespace(ns, reindexer.DefaultNamespaceOptions().DropOnIndexesConflict(), Item{})
		require.NoError(t, err)

		itemExp := Item{Name: "some_name"}
		_, err = db.Insert(ns, itemExp)
		require.NoError(t, err)

		srv1.Stop()

		dbTmp := reindexer.NewReindex(dsn, reindexer.WithCreateDBIfMissing())
		require.NotNil(t, db)
		err = dbTmp.OpenNamespace(ns, reindexer.DefaultNamespaceOptions().DropOnIndexesConflict(), Item{})
		require.NoError(t, err)
		itemExp = Item{Name: "some_name"}
		_, err = dbTmp.Insert(ns, itemExp)
		require.NoError(t, err)
		dbTmp.Close()

		cnt := 100
		wg := sync.WaitGroup{}
		wg.Add(cnt)
		for i := 0; i < cnt; i++ {
			go func() {
				defer wg.Done()
				db.Query(ns).Exec().FetchOne()
			}()
		}
		wg.Wait()

		srv2.Stop()
	})
}

type TestServer struct {
	t        *testing.T
	rpcPort  string
	httpPort string
	db       *reindexer.Reindexer
}

func (srv *TestServer) Addr() string {
	return "127.0.0.1:" + srv.rpcPort
}

func (srv *TestServer) Run() {
	cfg := config.DefaultServerConfig()
	cfg.Net.RPCAddr = "127.0.0.1:" + srv.rpcPort
	cfg.Net.HTTPAddr = "127.0.0.1:" + srv.httpPort
	cfg.Storage.Path = "/tmp/reindex_" + srv.rpcPort
	cfg.Logger.LogLevel = "error"
	cfg.Logger.ServerLog = ""
	cfg.Logger.HTTPLog = ""
	cfg.Logger.RPCLog = ""
	db := reindexer.NewReindex("builtinserver://reindex_test_multi_dsn_"+srv.rpcPort, reindexer.WithServerConfig(100*time.Second, cfg))
	srv.db = db

	timeout := false
	go func() {
		<-time.After(3 * time.Second)
		timeout = true
	}()

	for {
		time.Sleep(100 * time.Millisecond)
		resp, _ := httpCl.Get("http://127.0.0.1:" + srv.httpPort + "/api/v1/check")
		if resp == nil {
			continue
		}
		body, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		require.NoError(srv.t, err)

		if resp.StatusCode == 200 && string(body) != "" {
			break
		}

		if timeout {
			srv.t.Fatal("server run timeout")
		}
	}
}

func (srv *TestServer) Clean() {
	err := os.RemoveAll("/tmp/reindex_" + srv.rpcPort)
	require.NoError(srv.t, err)
}

func (srv *TestServer) Stop() {
	if srv.db == nil {
		return
	}
	srv.db.Close()

	timeout := false
	go func() {
		<-time.After(3 * time.Second)
		timeout = true
	}()

	for {
		time.Sleep(100 * time.Millisecond)
		resp, err := httpCl.Get("http://127.0.0.1:" + srv.httpPort + "/api/v1/check")
		if err == nil {
			resp.Body.Close()
		}

		if resp == nil {
			break
		}

		if timeout {
			srv.t.Fatal("server stop timeout")
		}
	}
}
