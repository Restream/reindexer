package reindexer

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/restream/reindexer/v5/bindings"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/restream/reindexer/v5"
	"github.com/restream/reindexer/v5/test/helpers"
)

type MultiDSNItem struct {
	Name string `json:"name" reindex:"name,,pk"`
}

const ghEnv = "REINDEXER_GH_CI_ASAN"

const testMultiDSNNs = "multi_dsn_items"

func TestReconnectWithStrategy(t *testing.T) {
	const ns = testMultiDSNNs

	t.Run("raft cluster", func(t *testing.T) {
		t.Run("reconnect use OptionReconnectionStrategy", func(t *testing.T) {
			if os.Getenv(ghEnv) != "" {
				t.Skip() // Skip this test on github CI due to asan and coroutines conflict
			}
			t.Parallel()
			servers := []*helpers.TestServer{
				{T: t, RpcPort: "6691", HttpPort: "9991", DbName: "reindex_test_multi_dsn", SrvType: helpers.ServerTypeBuiltin},
				{T: t, RpcPort: "6692", HttpPort: "9992", DbName: "reindex_test_multi_dsn", SrvType: helpers.ServerTypeBuiltin},
				{T: t, RpcPort: "6693", HttpPort: "9993", DbName: "reindex_test_multi_dsn", SrvType: helpers.ServerTypeBuiltin},
			}
			dsn := []string{servers[0].GetDSN(), servers[1].GetDSN(), servers[2].GetDSN()}
			defer servers[0].Clean()
			defer servers[1].Clean()
			defer servers[2].Clean()
			helpers.CreateCluster(t, servers, ns, MultiDSNItem{})

			db, err := reindexer.NewReindex(dsn, reindexer.WithCreateDBIfMissing(), reindexer.WithReconnectionStrategy(reindexer.ReconnectStrategySynchronized, true))
			require.NoError(t, err)

			require.NoError(t, db.OpenNamespace(ns, reindexer.DefaultNamespaceOptions().DropOnIndexesConflict(), MultiDSNItem{}))
			itemExp := MultiDSNItem{Name: "some_name"}
			require.NoError(t, db.Upsert(ns, itemExp))

			item, err := db.Query(ns).Exec().FetchAll()
			require.NoError(t, err)
			assert.Equal(t, itemExp, *item[0].(*MultiDSNItem))
			require.Equal(t, servers[0].GetRpcAddr(), db.Status().CProto.ConnAddr)

			require.NoError(t, servers[0].Stop())

			// Await explicit connections drop to avoid connection error on the first call
			time.Sleep(5 * time.Second)
			// check change dsn to second or third address
			err = db.OpenNamespace(ns, reindexer.DefaultNamespaceOptions().DropOnIndexesConflict(), MultiDSNItem{})
			require.NoError(t, err)
			_, err = db.Insert(ns, itemExp)
			require.NoError(t, err)

			item, err = db.Query(ns).Exec().FetchAll()
			require.NoError(t, err)
			assert.Equal(t, itemExp, *item[0].(*MultiDSNItem))
			require.Contains(t, []string{servers[1].GetRpcAddr(), servers[2].GetRpcAddr()}, db.Status().CProto.ConnAddr)

			require.NoError(t, servers[1].Stop())
			require.NoError(t, servers[2].Stop())
		})
	})

	t.Run("async replication", func(t *testing.T) {
		t.Run("reconnect with reconnectStrategyPrefferRead", func(t *testing.T) {
			if os.Getenv(ghEnv) != "" {
				t.Skip() // Skip this test on github CI due to asan and coroutines conflict
			}
			t.Parallel()
			servers := []*helpers.TestServer{
				{T: t, RpcPort: "6631", HttpPort: "9931", DbName: "reindex_test_multi_dsn", SrvType: helpers.ServerTypeBuiltin},
				{T: t, RpcPort: "6632", HttpPort: "9932", DbName: "reindex_test_multi_dsn", SrvType: helpers.ServerTypeBuiltin},
				{T: t, RpcPort: "6633", HttpPort: "9933", DbName: "reindex_test_multi_dsn", SrvType: helpers.ServerTypeBuiltin},
			}
			dsn := []string{servers[0].GetDSN(), servers[1].GetDSN(), servers[2].GetDSN()}
			defer servers[0].Clean()
			defer servers[1].Clean()
			defer servers[2].Clean()

			helpers.CreateReplication(t, servers[0], servers[1:], ns, MultiDSNItem{})
			db, err := reindexer.NewReindex(dsn, reindexer.WithCreateDBIfMissing(), reindexer.WithReconnectionStrategy(reindexer.ReconnectStrategyPrefferRead, true))
			require.NoError(t, err)

			err = db.OpenNamespace(ns, reindexer.DefaultNamespaceOptions().DropOnIndexesConflict(), MultiDSNItem{})
			require.NoError(t, err)
			itemExp := MultiDSNItem{Name: "some_name"}
			err = db.Upsert(ns, itemExp)
			require.NoError(t, err)

			item, err := db.Query(ns).Exec().FetchAll()
			require.NoError(t, err)
			assert.Equal(t, itemExp, *item[0].(*MultiDSNItem))
			require.Equal(t, servers[0].GetRpcAddr(), db.Status().CProto.ConnAddr)

			require.NoError(t, servers[0].Stop())

			// check change dsn to follower, while leader offline
			_, err = db.Insert(ns, itemExp)
			require.Equal(t, "can't use WithReconnectionStrategy without configured cluster or async replication", err.Error())

			require.NoError(t, servers[1].Stop())
			require.NoError(t, servers[0].Run())

			// check change dsn to leader
			_, err = db.Insert(ns, itemExp)
			require.NoError(t, err)

			require.NoError(t, servers[0].Stop())
			require.NoError(t, servers[2].Stop())
		})
	})

	t.Run("should error if use strategy and not configured cluster or replication", func(t *testing.T) {
		t.Parallel()
		servers := []*helpers.TestServer{
			{T: t, RpcPort: "6621", HttpPort: "9921", DbName: "reindex_test_multi_dsn", SrvType: helpers.ServerTypeBuiltin},
			{T: t, RpcPort: "6622", HttpPort: "9922", DbName: "reindex_test_multi_dsn", SrvType: helpers.ServerTypeBuiltin},
		}
		dsn := []string{servers[0].GetDSN(), servers[1].GetDSN()}
		defer servers[0].Clean()
		defer servers[1].Clean()
		servers[0].Run()
		servers[1].Run()

		db, err := reindexer.NewReindex(dsn, reindexer.WithCreateDBIfMissing(), reindexer.WithReconnectionStrategy(reindexer.ReconnectStrategyPrefferRead, true))
		require.NoError(t, err)
		err = db.OpenNamespace(ns, reindexer.DefaultNamespaceOptions().DropOnIndexesConflict(), MultiDSNItem{})
		require.NoError(t, err)
		require.Equal(t, servers[0].GetRpcAddr(), db.Status().CProto.ConnAddr)
		require.NoError(t, servers[0].Stop())

		err = db.OpenNamespace(ns, reindexer.DefaultNamespaceOptions().DropOnIndexesConflict(), MultiDSNItem{})
		errExp := bindings.NewError("can't use WithReconnectionStrategy without configured cluster or async replication", bindings.ErrParams)
		require.Equal(t, errExp, err)
	})
}

func TestMultipleDSN(t *testing.T) {
	if len(DB.slaveList) > 0 || len(DB.clusterList) > 0 {
		t.Skip()
	}
	const ns = testMultiDSNNs

	t.Run("connect to next dsn if current not available", func(t *testing.T) {
		t.Parallel()
		srv1 := helpers.TestServer{T: t, RpcPort: "6661", HttpPort: "9961", DbName: "reindex_test_multi_dsn", SrvType: helpers.ServerTypeBuiltin}
		srv2 := helpers.TestServer{T: t, RpcPort: "6662", HttpPort: "9962", DbName: "reindex_test_multi_dsn", SrvType: helpers.ServerTypeBuiltin}
		srv3 := helpers.TestServer{T: t, RpcPort: "6663", HttpPort: "9963", DbName: "reindex_test_multi_dsn", SrvType: helpers.ServerTypeBuiltin}
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

		db, err := reindexer.NewReindex(dsn, reindexer.WithCreateDBIfMissing())
		require.NoError(t, err)

		err = db.OpenNamespace(ns, reindexer.DefaultNamespaceOptions().DropOnIndexesConflict(), MultiDSNItem{})
		require.NoError(t, err)

		itemExp := MultiDSNItem{Name: "some_name"}
		err = db.Upsert(ns, itemExp)
		require.NoError(t, err)

		item, err := db.Query(ns).Exec().FetchAll()
		require.NoError(t, err)
		assert.Equal(t, itemExp, *item[0].(*MultiDSNItem))
		require.Equal(t, srv1.GetRpcAddr(), db.Status().CProto.ConnAddr)

		srv1.Stop()
		srv2.Stop()

		// check change dsn to third address, with skip second stopped
		err = db.OpenNamespace(ns, reindexer.DefaultNamespaceOptions().DropOnIndexesConflict(), MultiDSNItem{})
		require.NoError(t, err)
		_, err = db.Insert(ns, itemExp)
		require.NoError(t, err)

		item, err = db.Query(ns).Exec().FetchAll()
		require.NoError(t, err)
		assert.Equal(t, itemExp, *item[0].(*MultiDSNItem))
		require.Equal(t, srv3.GetRpcAddr(), db.Status().CProto.ConnAddr)

		srv3.Stop()
		err = srv1.Run()
		require.NoError(t, err)

		// check reconnect to first
		err = db.OpenNamespace(ns, reindexer.DefaultNamespaceOptions().DropOnIndexesConflict(), MultiDSNItem{})
		require.NoError(t, err)
		item, err = db.Query(ns).Exec().FetchAll()
		require.NoError(t, err)
		assert.Equal(t, itemExp, *item[0].(*MultiDSNItem))
		require.Equal(t, srv1.GetRpcAddr(), db.Status().CProto.ConnAddr)
		srv1.Stop()
	})

	t.Run("return err if all srv stopped", func(t *testing.T) {
		t.Parallel()
		srv1 := helpers.TestServer{T: t, RpcPort: "6671", HttpPort: "9971", DbName: "reindex_test_multi_dsn", SrvType: helpers.ServerTypeBuiltin}
		srv2 := helpers.TestServer{T: t, RpcPort: "6672", HttpPort: "9972", DbName: "reindex_test_multi_dsn", SrvType: helpers.ServerTypeBuiltin}
		srv3 := helpers.TestServer{T: t, RpcPort: "6673", HttpPort: "9973", DbName: "reindex_test_multi_dsn", SrvType: helpers.ServerTypeBuiltin}
		dsn := []string{
			fmt.Sprintf("cproto://127.0.0.1:%s/%s_%s", srv1.RpcPort, srv1.DbName, srv1.RpcPort),
			fmt.Sprintf("cproto://127.0.0.1:%s/%s_%s", srv2.RpcPort, srv2.DbName, srv2.RpcPort),
			fmt.Sprintf("cproto://127.0.0.1:%s/%sn_%s", srv3.RpcPort, srv3.DbName, srv3.RpcPort),
		}

		errExp := "failed to connect with provided dsn; dial tcp 127.0.0.1:6671: connect: connection refused; dial tcp 127.0.0.1:6672: connect: connection refused; dial tcp 127.0.0.1:6673: connect: connection refused"
		db, err := reindexer.NewReindex(dsn, reindexer.WithCreateDBIfMissing())
		require.ErrorContains(t, err, errExp)

		err = db.OpenNamespace(ns, reindexer.DefaultNamespaceOptions().DropOnIndexesConflict(), MultiDSNItem{})
		require.ErrorContains(t, err, errExp)
	})

	t.Run("should be possible Close(), after all srv stopped", func(t *testing.T) {
		t.Parallel()

		srv1 := helpers.TestServer{T: t, RpcPort: "6681", HttpPort: "9981", DbName: "reindex_test_multi_dsn", SrvType: helpers.ServerTypeBuiltin}
		srv2 := helpers.TestServer{T: t, RpcPort: "6682", HttpPort: "9982", DbName: "reindex_test_multi_dsn", SrvType: helpers.ServerTypeBuiltin}
		dsn := []string{
			fmt.Sprintf("cproto://127.0.0.1:%s/%s_%s", srv1.RpcPort, srv1.DbName, srv1.RpcPort),
			fmt.Sprintf("cproto://127.0.0.1:%s/%s_%s", srv2.RpcPort, srv2.DbName, srv2.RpcPort),
		}
		err := srv1.Run()
		require.NoError(t, err)
		err = srv2.Run()
		require.NoError(t, err)
		db, err := reindexer.NewReindex(dsn, reindexer.WithCreateDBIfMissing())
		require.NoError(t, err)

		err = db.OpenNamespace(ns, reindexer.DefaultNamespaceOptions().DropOnIndexesConflict(), MultiDSNItem{})
		require.NoError(t, err)

		itemExp := MultiDSNItem{Name: "some_name"}
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
	})

	t.Run("should reset cache after restart server and reconnect", func(t *testing.T) {
		t.Parallel()

		srv := helpers.TestServer{T: t, RpcPort: "6683", HttpPort: "9983", DbName: "reindex_test_multi_dsn", SrvType: helpers.ServerTypeBuiltin}
		dsn := []string{srv.GetDSN()}
		err := srv.Run()
		require.NoError(t, err)
		db, err := reindexer.NewReindex(dsn, reindexer.WithCreateDBIfMissing())
		require.NoError(t, err)

		err = db.OpenNamespace(ns, reindexer.DefaultNamespaceOptions().DropOnIndexesConflict(), TestItemPk{})
		require.NoError(t, err)

		// fill cache
		itemExp := TestItemPk{ID: 1}
		err = db.Upsert(ns, itemExp)
		require.NoError(t, err)

		itemExp = TestItemPk{ID: 2}
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
		t.Skip()
	}
	const ns = testMultiDSNNs

	t.Run("on reconnect to next dsn", func(t *testing.T) {
		t.Parallel()

		srv1 := helpers.TestServer{T: t, RpcPort: "6651", HttpPort: "9951", DbName: "reindex_test_multi_dsn", SrvType: helpers.ServerTypeBuiltin}
		srv2 := helpers.TestServer{T: t, RpcPort: "6652", HttpPort: "9952", DbName: "reindex_test_multi_dsn", SrvType: helpers.ServerTypeBuiltin}

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

		db, err := reindexer.NewReindex(dsn, reindexer.WithCreateDBIfMissing())
		require.NoError(t, err)

		err = db.OpenNamespace(ns, reindexer.DefaultNamespaceOptions().DropOnIndexesConflict(), MultiDSNItem{})
		require.NoError(t, err)
		require.Equal(t, srv1.GetRpcAddr(), db.Status().CProto.ConnAddr)

		itemExp := MultiDSNItem{Name: "some_name"}
		_, err = db.Insert(ns, itemExp)
		require.NoError(t, err)

		srv1.Stop()

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

	t.Run("on reconnect to next dsn with strategy", func(t *testing.T) {
		t.Parallel()
		srv1 := helpers.TestServer{T: t, RpcPort: "6653", HttpPort: "9953", DbName: "reindex_test_multi_dsn", SrvType: helpers.ServerTypeBuiltin}
		srv2 := helpers.TestServer{T: t, RpcPort: "6654", HttpPort: "9954", DbName: "reindex_test_multi_dsn", SrvType: helpers.ServerTypeBuiltin}

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

		db, err := reindexer.NewReindex(dsn, reindexer.WithCreateDBIfMissing(), reindexer.WithReconnectionStrategy(reindexer.ReconnectStrategyPrefferRead, true))
		require.NoError(t, err)

		err = db.OpenNamespace(ns, reindexer.DefaultNamespaceOptions().DropOnIndexesConflict(), MultiDSNItem{})
		require.NoError(t, err)
		require.Equal(t, srv1.GetRpcAddr(), db.Status().CProto.ConnAddr)

		itemExp := MultiDSNItem{Name: "some_name"}
		_, err = db.Insert(ns, itemExp)
		require.NoError(t, err)

		srv1.Stop()

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
