package reindexer

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/restream/reindexer/v4"
	"github.com/restream/reindexer/v4/bindings/builtin"
)

func init() {
	tnamespaces["test_items_race"] = TestItem{}
	tnamespaces["test_join_items_race"] = TestJoinItem{}
	tnamespaces["test_items_race_tx"] = TestItem{}
	tnamespaces["test_join_items_race_tx"] = TestJoinItem{}
}

func TestRaceConditions(t *testing.T) {
	t.Parallel()
	FillTestJoinItems(7000, 2000, "test_join_items_race")
	done := make(chan bool)
	wg := sync.WaitGroup{}
	writer := func() {
		defer func() {
			if p := recover(); p != nil {
				fmt.Println("Panic silenced:", p)
				wg.Done()
			}
		}()
		for {
			select {
			case <-done:
				wg.Done()
				return
			case <-time.After(time.Millisecond * 1):
				ctx, cancel := context.WithCancel(context.Background())
				DB.UpsertCtx(ctx, "test_items_race", newTestItem(1000+rand.Intn(100), 5))
				cancel()
			}
		}
	}
	reader := func() {
		defer func() {
			if p := recover(); p != nil {
				fmt.Println("Panic silenced:", p)
				wg.Done()
			}
		}()
		for {
			select {
			case <-done:
				wg.Done()
				return
			default:
				ctx, cancel := context.WithCancel(context.Background())
				q := DB.Query("test_items_race").Limit(2)

				if rand.Int()%100 > 50 {
					q.WhereInt("year", reindexer.GT, 2010)
				}
				if rand.Int()%100 > 50 {
					q.WhereInt("genre", reindexer.SET, 1, 2)
				}
				if rand.Int()%100 > 50 {
					q.WhereString("name", reindexer.EQ, randString())
				}
				if rand.Int()%100 > 80 {
					qj1 := DB.Query("test_join_items_race").Where("device", reindexer.EQ, "ottstb").Sort("name", false)
					qj2 := DB.Query("test_join_items_race").Where("device", reindexer.EQ, "android")
					qj3 := DB.Query("test_join_items_race").Where("device", reindexer.EQ, "iphone")
					q.LeftJoin(qj1, "prices").On("price_id", reindexer.SET, "id")
					q.LeftJoin(qj2, "pricesx").On("location", reindexer.EQ, "location").On("price_id", reindexer.SET, "id")
					q.LeftJoin(qj3, "pricesx").On("location", reindexer.LT, "location").Or().On("price_id", reindexer.SET, "id")
				}

				it := q.ExecCtx(t, ctx)
				_ = it.TotalCount()
				for it.Next() {
					_ = it.Object().(*TestItem)
				}
				it.Close()
				cancel()
				_ = q
			}
		}
	}
	openCloser := func() {
		defer func() {
			if p := recover(); p != nil {
				fmt.Println("Panic silenced:", p)
				wg.Done()
			}
		}()
		for {
			select {
			case <-done:
				wg.Done()
				return
			case <-time.After(time.Millisecond * 10):
				DB.CloseNamespace("test_items_race")
				DB.OpenNamespace("test_items_race", reindexer.DefaultNamespaceOptions(), TestItem{})
				tx, _ := DB.BeginTx("test_join_items_race")
				tx.Upsert(TestJoinItem{ID: 7000})
				tx.Commit()
			}
		}
	}

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go writer()
		wg.Add(1)
		go reader()
		wg.Add(1)
		go openCloser()
	}

	time.Sleep(time.Millisecond * 15000)
	close(done)
	wg.Wait()
}

func setNsCopyConfigs(t *testing.T, namespace string) {
	nsConfig := make([]reindexer.DBNamespacesConfig, 1)
	nsConfig[0].StartCopyPolicyTxSize = 10000
	nsConfig[0].StartCopyPolicyTxSize = 10
	nsConfig[0].StartCopyPolicyTxSize = 100000
	nsConfig[0].Namespace = namespace
	item := reindexer.DBConfigItem{
		Type:       "namespaces",
		Namespaces: &nsConfig,
	}
	err := DB.Upsert(reindexer.ConfigNamespaceName, item)
	assert.NoError(t, err)
}

func TestRaceConditionsTx(t *testing.T) {
	t.Parallel()
	FillTestJoinItems(7000, 2000, "test_join_items_race_tx")
	setNsCopyConfigs(t, "test_items_iter_race_tx")
	setNsCopyConfigs(t, "test_join_items_race_tx")
	done := make(chan bool)
	wg := sync.WaitGroup{}
	writer := func() {
		defer func() {
			if p := recover(); p != nil {
				fmt.Println("Panic silenced:", p)
				wg.Done()
			}
		}()
		for {
			select {
			case <-done:
				wg.Done()
				return
			case <-time.After(time.Millisecond * 1):
				ctx, cancel := context.WithCancel(context.Background())
				DB.UpsertCtx(ctx, "test_items_race_tx", newTestItem(rand.Intn(20000), 5))
				cancel()
			}
		}
	}
	reader := func() {
		defer func() {
			if p := recover(); p != nil {
				fmt.Println("Panic silenced:", p)
				wg.Done()
			}
		}()
		for {
			select {
			case <-done:
				wg.Done()
				return
			default:
				ctx, cancel := context.WithCancel(context.Background())
				q := DB.Query("test_items_race_tx").Limit(2)

				if rand.Int()%100 > 50 {
					q.WhereInt("year", reindexer.GT, 2010)
				}
				if rand.Int()%100 > 50 {
					q.WhereInt("genre", reindexer.SET, 1, 2)
				}
				if rand.Int()%100 > 50 {
					q.WhereString("name", reindexer.EQ, randString())
				}
				if rand.Int()%100 > 80 {
					qj1 := DB.Query("test_join_items_race_tx").Where("device", reindexer.EQ, "ottstb").Sort("name", false)
					qj2 := DB.Query("test_join_items_race_tx").Where("device", reindexer.EQ, "android")
					qj3 := DB.Query("test_join_items_race_tx").Where("device", reindexer.EQ, "iphone")
					q.LeftJoin(qj1, "prices").On("price_id", reindexer.SET, "id")
					q.LeftJoin(qj2, "pricesx").On("location", reindexer.EQ, "location").On("price_id", reindexer.SET, "id")
					q.LeftJoin(qj3, "pricesx").On("location", reindexer.LT, "location").Or().On("price_id", reindexer.SET, "id")
				}

				it := q.ExecCtx(t, ctx)
				_ = it.TotalCount()
				for it.Next() {
					_ = it.Object().(*TestItem)
				}
				it.Close()
				cancel()
				_ = q
			}
		}
	}
	txWriter := func() {
		defer func() {
			if p := recover(); p != nil {
				fmt.Println("Panic silenced:", p)
				wg.Done()
			}
		}()
		for {
			select {
			case <-done:
				wg.Done()
				return
			case <-time.After(time.Millisecond * 10):
				tx, _ := DB.BeginTx("test_join_items_race_tx")
				bigTx := rand.Intn(2) > 0
				txItemsCount := 1000
				if bigTx {
					txItemsCount = 20000
				}
				for i := 0; i < txItemsCount; i++ {
					tx.UpsertAsync(TestJoinItem{ID: i}, func(err error) {
						if err != nil {
							panic(err)
						}
					})
				}
				tx.Commit()
			}
		}
	}
	deleter := func() {
		defer func() {
			if p := recover(); p != nil {
				fmt.Println("Panic silenced:", p)
				wg.Done()
			}
		}()
		for {
			select {
			case <-done:
				wg.Done()
				return
			case <-time.After(time.Millisecond * 10):
				startID := rand.Intn(20000)
				_, err := DB.Query("test_join_items_race_tx").Where("id", reindexer.GE, startID).Where("id", reindexer.LE, startID+rand.Intn(1000)).Delete()
				assert.NoError(t, err)
			}
		}
	}

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go writer()
		wg.Add(1)
		go reader()
		wg.Add(1)
		go txWriter()
		wg.Add(1)
		go deleter()
	}

	time.Sleep(time.Millisecond * 15000)
	close(done)
	wg.Wait()
}

func TestCtxWatcherRace(t *testing.T) {
	t.Run("Checking the correctness of processing a closed chan if StopWatchOnCtx called after Finalize", func(t *testing.T) {
		const defWatchersPoolSize = 4
		const defCtxWatchDelay = time.Millisecond * 100

		watcher := builtin.NewCtxWatcher(defWatchersPoolSize, defCtxWatchDelay)

		// cancel context required for StopWatchOnCtx func
		ctx, _ := context.WithCancel(context.Background())
		ctxInfo, _ := watcher.StartWatchOnCtx(ctx)

		wg := sync.WaitGroup{}
		wg.Add(2)

		go func() {
			watcher.Finalize()
			wg.Done()
		}()
		go func() {
			watcher.StopWatchOnCtx(ctxInfo)
			wg.Done()
		}()

		wg.Wait()
	})
}
