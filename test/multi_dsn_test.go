package reindexer

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/restream/reindexer"
	"github.com/restream/reindexer/test/helpers"
)

func TestMultipleDSN(t *testing.T) {
	if len(DB.slaveList) > 0 || len(DB.clusterList) > 0 {
		return
	}

	ns := "items"
	type Item struct {
		Name string `json:"name" reindex:"name,,pk"`
	}

	t.Run("connect to next dsn if current not available", func(t *testing.T) {
		t.Parallel()
		srv1 := helpers.TestServer{T: t, RpcPort: "6661", HttpPort: "9961", ClusterPort: "9861", DbName: "reindex_test_multi_dsn"}
		srv2 := helpers.TestServer{T: t, RpcPort: "6662", HttpPort: "9962", ClusterPort: "9862", DbName: "reindex_test_multi_dsn"}
		srv3 := helpers.TestServer{T: t, RpcPort: "6663", HttpPort: "9963", ClusterPort: "9863", DbName: "reindex_test_multi_dsn"}
		dsn := []string{
			fmt.Sprintf("cproto://127.0.0.1:%s/%s_%s", srv1.RpcPort, srv1.DbName, srv1.RpcPort),
			fmt.Sprintf("cproto://127.0.0.1:%s/%s_%s", srv2.RpcPort, srv2.DbName, srv2.RpcPort),
			fmt.Sprintf("cproto://127.0.0.1:%s/%s_%s", srv3.RpcPort, srv3.DbName, srv3.RpcPort),
		}

		err := srv1.Run()
		require.NoError(t, err)
		err = srv2.Run()
		require.NoError(t, err)
		err = srv3.Run()
		require.NoError(t, err)

		defer srv1.Clean()
		defer srv2.Clean()
		defer srv3.Clean()

		db := reindexer.NewReindex(dsn, reindexer.WithCreateDBIfMissing())
		require.NotNil(t, db)

		err = db.OpenNamespace(ns, reindexer.DefaultNamespaceOptions().DropOnIndexesConflict(), Item{})
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
		err = srv1.Run()
		require.NoError(t, err)

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
		srv1 := helpers.TestServer{T: t, RpcPort: "6671", HttpPort: "9971", ClusterPort: "9871", DbName: "reindex_test_multi_dsn"}
		srv2 := helpers.TestServer{T: t, RpcPort: "6672", HttpPort: "9972", ClusterPort: "9872", DbName: "reindex_test_multi_dsn"}
		srv3 := helpers.TestServer{T: t, RpcPort: "6673", HttpPort: "9973", ClusterPort: "9873", DbName: "reindex_test_multi_dsn"}
		dsn := []string{
			fmt.Sprintf("cproto://127.0.0.1:%s/%s_%s", srv1.RpcPort, srv1.DbName, srv1.RpcPort),
			fmt.Sprintf("cproto://127.0.0.1:%s/%s_%s", srv2.RpcPort, srv2.DbName, srv2.RpcPort),
			fmt.Sprintf("cproto://127.0.0.1:%s/%sn_%s", srv3.RpcPort, srv3.DbName, srv3.RpcPort),
		}

		errExp := "failed to connect with provided dsn; dial tcp 127.0.0.1:6671: connect: connection refused; dial tcp 127.0.0.1:6672: connect: connection refused; dial tcp 127.0.0.1:6673: connect: connection refused"
		db := reindexer.NewReindex(dsn, reindexer.WithCreateDBIfMissing())
		require.NotNil(t, db)

		err := db.OpenNamespace(ns, reindexer.DefaultNamespaceOptions().DropOnIndexesConflict(), Item{})
		require.Error(t, err)
		assert.Equal(t, errExp, err.Error())
	})

	t.Run("should be possible Close(), after all srv stopped", func(t *testing.T) {
		t.Parallel()

		srv1 := helpers.TestServer{T: t, RpcPort: "6681", HttpPort: "9981", ClusterPort: "9881", DbName: "reindex_test_multi_dsn"}
		srv2 := helpers.TestServer{T: t, RpcPort: "6682", HttpPort: "9982", ClusterPort: "9882", DbName: "reindex_test_multi_dsn"}
		dsn := []string{
			fmt.Sprintf("cproto://127.0.0.1:%s/%s_%s", srv1.RpcPort, srv1.DbName, srv1.RpcPort),
			fmt.Sprintf("cproto://127.0.0.1:%s/%s_%s", srv2.RpcPort, srv2.DbName, srv2.RpcPort),
		}
		err := srv1.Run()
		require.NoError(t, err)
		err = srv2.Run()
		require.NoError(t, err)
		db := reindexer.NewReindex(dsn, reindexer.WithCreateDBIfMissing())
		require.NotNil(t, db)

		err = db.OpenNamespace(ns, reindexer.DefaultNamespaceOptions().DropOnIndexesConflict(), Item{})
		require.NoError(t, err)

		itemExp := Item{Name: "some_name"}
		_, err = db.Insert(ns, itemExp)
		require.NoError(t, err)

		done := make(chan bool)
		defer func() {
			time.Sleep(1 * time.Second) // for try queries while and after Close()
			done <- true
		}()
		go func() {
			for {
				select {
				case <-done:
					return
				default:
					db.Query(ns).Exec().FetchOne()
				}
			}
		}()

		srv1.Stop()
		srv2.Stop()

		db.Close()
	})

	t.Run("should reset cache after restart server and reconnect", func(t *testing.T) {
		t.Parallel()

		srv := helpers.TestServer{T: t, RpcPort: "6683", HttpPort: "9983", ClusterPort: "9883", DbName: "reindex_test_multi_dsn"}
		dsn := []string{
			fmt.Sprintf("cproto://127.0.0.1:%s/%s_%s", srv.RpcPort, srv.DbName, srv.RpcPort),
		}
		err := srv.Run()
		require.NoError(t, err)
		db := reindexer.NewReindex(dsn, reindexer.WithCreateDBIfMissing())
		require.NotNil(t, db)

		err = db.OpenNamespace(ns, reindexer.DefaultNamespaceOptions().DropOnIndexesConflict(), NamespaceCacheItem{})
		require.NoError(t, err)

		// fill cache
		itemExp := NamespaceCacheItem{ID: 1}
		err = db.Upsert(ns, itemExp)
		require.NoError(t, err)

		itemExp = NamespaceCacheItem{ID: 2}
		err = db.Upsert(ns, itemExp)
		require.NoError(t, err)

		_, err = db.Query(ns).Exec().FetchAll()
		require.NoError(t, err)

		status := db.Status()
		assert.Equal(t, int64(2), status.Cache.CurSize)

		// restart
		err = srv.Stop()
		require.NoError(t, err)
		err = srv.Run()
		require.NoError(t, err)

		// check after reset
		it := db.Query(ns).Exec()

		status = db.Status()
		assert.Equal(t, int64(0), status.Cache.CurSize)

		it.Next()
		status = db.Status()
		assert.Equal(t, int64(1), status.Cache.CurSize)

		it.Next()
		status = db.Status()
		assert.Equal(t, int64(2), status.Cache.CurSize)

		require.NoError(t, it.Error())
		it.Close()
		db.Close()
	})
}

func TestRaceConditionsMultiDSN(t *testing.T) {
	if len(DB.slaveList) > 0 || len(DB.clusterList) > 0 {
		return
	}

	srv1 := helpers.TestServer{T: t, RpcPort: "6651", HttpPort: "9951", ClusterPort: "9851", DbName: "reindex_test_multi_dsn"}
	srv2 := helpers.TestServer{T: t, RpcPort: "6652", HttpPort: "9952", ClusterPort: "9852", DbName: "reindex_test_multi_dsn"}

	t.Run("on reconnect to next dsn", func(t *testing.T) {
		t.Parallel()
		ns := "items"
		type Item struct {
			Name string `json:"name" reindex:"name,,pk"`
		}

		err := srv1.Run()
		require.NoError(t, err)
		err = srv2.Run()
		require.NoError(t, err)
		dsn := []string{
			fmt.Sprintf("cproto://127.0.0.1:%s/%s_%s", srv1.RpcPort, srv1.DbName, srv1.RpcPort),
			fmt.Sprintf("cproto://127.0.0.1:%s/%s_%s", srv2.RpcPort, srv2.DbName, srv2.RpcPort),
		}

		defer srv1.Clean()
		defer srv2.Clean()

		db := reindexer.NewReindex(dsn, reindexer.WithCreateDBIfMissing())
		require.NotNil(t, db)

		err = db.OpenNamespace(ns, reindexer.DefaultNamespaceOptions().DropOnIndexesConflict(), Item{})
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
				db.Status()
			}()
		}
		wg.Wait()

		srv2.Stop()
	})
}
