package reindexer

import (
	"context"
	"encoding/json"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/restream/reindexer/v5"
	"github.com/restream/reindexer/v5/bindings/builtin"
	"github.com/restream/reindexer/v5/events"
)

type TestItemWithExtraFields1 struct {
	TestItem
	SomeInt      int
	RandomString string
	Array        []int
}

type TestItemWithExtraFields2Nested1 struct {
	SomeNestedInt      int
	RandomNestedString string
	NestedArray        []int
}

type TestItemWithExtraFields2Nested2 struct {
	Nested2 TestItemWithExtraFields2Nested1
}

type TestItemWithExtraFields2 struct {
	TestItem
	Nested1 TestItemWithExtraFields2Nested2
}

const (
	testItemsRaceNs       = "test_items_race"
	testJoinItemsRaceNs   = "test_join_items_race"
	testItemsRaceTxNs     = "test_items_race_tx"
	testJoinItemsRaceTxNs = "test_join_items_race_tx"
)

func init() {
	tnamespaces[testItemsRaceNs] = TestItem{}
	tnamespaces[testJoinItemsRaceNs] = TestJoinItem{}
	tnamespaces[testItemsRaceTxNs] = TestItem{}
	tnamespaces[testJoinItemsRaceTxNs] = TestJoinItem{}
}

func newTestItemWithExtraFields1(id int, pkgsCount int) interface{} {
	return &TestItemWithExtraFields1{
		TestItem:     *newTestItem(id, pkgsCount).(*TestItem),
		SomeInt:      rand.Int()%50 + 2000,
		RandomString: randString(),
		Array:        randIntArr(10, 1000, 1000),
	}
}

func newTestItemWithExtraFields2(id int, pkgsCount int) interface{} {
	return &TestItemWithExtraFields2{
		TestItem: *newTestItem(id, pkgsCount).(*TestItem),
		Nested1: TestItemWithExtraFields2Nested2{
			Nested2: TestItemWithExtraFields2Nested1{
				SomeNestedInt:      rand.Int()%50 + 2000,
				RandomNestedString: randString(),
				NestedArray:        randIntArr(10, 1000, 1000)}},
	}
}

func subscriberRaceRoutine(t *testing.T, subsWg *sync.WaitGroup, subsDone chan (bool), opts *events.EventsStreamOptions) {
	defer subsWg.Done()
	stream := DBD.Subscribe(opts)
	defer stream.Close(context.Background())
	require.NoError(t, stream.Error())
	events := 0
	for {
		select {
		case <-subsDone:
			require.Greater(t, events, 0, "Opts: %v", opts)
			require.NoError(t, stream.Error(), "Opts: %v", opts)
			return
		case <-stream.Chan():
			require.NoError(t, stream.Error(), "Opts: %v", opts)
			events += 1
		}
	}
}

func subUnsubRaceRoutine(t *testing.T, subsWg *sync.WaitGroup, subsDone chan (bool), opts *events.EventsStreamOptions) {
	defer subsWg.Done()
	for {
		select {
		case <-subsDone:
			return
		default:
			stream := DBD.Subscribe(opts)
			require.NoError(t, stream.Error())
			time.Sleep(100 * time.Millisecond)
			err := stream.Close(context.Background())
			require.NoError(t, err)
			time.Sleep(20 * time.Millisecond)
		}
	}
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

func TestRaceConditions(t *testing.T) {
	t.Parallel()
	FillTestJoinItems(7000, 2000, testJoinItemsRaceNs)

	done := make(chan bool)
	wg := sync.WaitGroup{}
	writer := func() {
		for {
			select {
			case <-done:
				wg.Done()
				return
			case <-time.After(time.Millisecond * 1):
				ctx, cancel := context.WithCancel(context.Background())
				DB.UpsertCtx(ctx, testItemsRaceNs, newTestItem(1000+rand.Intn(100), 5))
				cancel()
			}
		}
	}
	writerJSON := func() {
		counter := 0
		for {
			select {
			case <-done:
				wg.Done()
				return
			case <-time.After(time.Millisecond * 5):
				// Check race conditions on the new field, added via JSON
				var item interface{}
				if counter%2 == 0 {
					item = newTestItemWithExtraFields1(1000+rand.Intn(100), 5)
				} else {
					item = newTestItemWithExtraFields2(1000+rand.Intn(100), 5)
				}
				j, err := json.Marshal(item)
				require.NoError(t, err)
				counter += 1
				ctx, cancel := context.WithCancel(context.Background())
				err = DB.UpsertCtx(ctx, testItemsRaceNs, j)
				cancel()
				if rerr, ok := err.(reindexer.Error); !ok ||
					(rerr.Code() != reindexer.ErrCodeParams && rerr.Code() != reindexer.ErrCodeNotFound) {
					require.NoError(t, err)
				}
			}
		}
	}
	reader := func() {
		for {
			select {
			case <-done:
				wg.Done()
				return
			default:
				ctx, cancel := context.WithCancel(context.Background())
				q := DB.Query(testItemsRaceNs).Limit(2)

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
					qj1 := DB.Query(testJoinItemsRaceNs).Where("device", reindexer.EQ, "ottstb").Sort("name", false)
					qj2 := DB.Query(testJoinItemsRaceNs).Where("device", reindexer.EQ, "android")
					qj3 := DB.Query(testJoinItemsRaceNs).Where("device", reindexer.EQ, "iphone")
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
	updater := func() {
		for {
			select {
			case <-done:
				wg.Done()
				return
			default:
				isUpdate := rand.Int()%100 >= 50

				ctx, cancel := context.WithCancel(context.Background())
				if rand.Int()%100 > 90 {
					q := DB.Query(testItemsRaceNs)
					if isUpdate {
						q.Set("name", randString()).Set("company_name", randString()).Limit(5)
					} else {
						q.Limit(1)
					}

					qj1 := DB.Query(testJoinItemsRaceNs).Where("device", reindexer.EQ, "ottstb").Sort("name", false)
					qj2 := DB.Query(testJoinItemsRaceNs).Where("device", reindexer.EQ, "android")
					qj3 := DB.Query(testJoinItemsRaceNs).Where("device", reindexer.EQ, "iphone")
					q.InnerJoin(qj1, "prices").On("price_id", reindexer.SET, "id")
					q.InnerJoin(qj2, "pricesx").On("location", reindexer.EQ, "location").On("price_id", reindexer.SET, "id")
					q.InnerJoin(qj3, "pricesx").On("location", reindexer.LT, "location").Or().On("price_id", reindexer.SET, "id")

					if isUpdate {
						q.UpdateCtx(ctx).FetchAll()
					} else {
						q.DeleteCtx(ctx)
					}
				} else if rand.Int()%100 > 90 {
					q := DB.Query(testJoinItemsRaceNs)
					if isUpdate {
						q.Set("name", randString()).Set("device", randDevice()).Limit(5)
					} else {
						q.Limit(1)
					}
					qj := DB.Query(testItemsRaceNs)

					if rand.Int()%100 >= 50 {
						qj.Where("genre", reindexer.SET, []int{1, 2})
					} else {
						qj.Where("genre", reindexer.GE, 2)
					}
					qj.JoinHandler("prices", func(field string, item interface{}, subitems []interface{}) bool {
						return false
					})
					q.InnerJoin(qj, "prices").On("id", reindexer.SET, "price_id")

					if isUpdate {
						q.UpdateCtx(ctx).FetchAll()
					} else {
						q.DeleteCtx(ctx)
					}
				}
				cancel()
				time.Sleep(1 * time.Millisecond)
			}
		}
	}
	openCloser := func() {
		for {
			select {
			case <-done:
				wg.Done()
				return
			case <-time.After(time.Millisecond * 10):
				DB.CloseNamespace(testItemsRaceNs)
				DB.OpenNamespace(testItemsRaceNs, reindexer.DefaultNamespaceOptions(), TestItem{})
				tx, _ := DB.BeginTx(testJoinItemsRaceNs)
				tx.Upsert(TestJoinItem{ID: 7000})
				tx.Commit()
			}
		}
	}

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go writer()
		wg.Add(1)
		go writerJSON()
		wg.Add(1)
		go reader()
		wg.Add(1)
		go openCloser()
		wg.Add(1)
		go updater()
	}
	subsWg := sync.WaitGroup{}
	subsDone := make(chan bool)
	subsWg.Add(3)
	go subUnsubRaceRoutine(t, &subsWg, subsDone, events.DefaultEventsStreamOptions().WithDocModifyEvents())
	go subscriberRaceRoutine(t, &subsWg, subsDone, events.DefaultEventsStreamOptions().WithTransactionCommitEvents().WithConfigNamespace())
	if strings.HasPrefix(DB.dsn, "builtin://") {
		go subscriberRaceRoutine(t, &subsWg, subsDone, events.DefaultEventsStreamOptions().WithIndexModifyEvents().WithNamespaceOperationEvents())
	} else {
		go subscriberRaceRoutine(t, &subsWg, subsDone, events.DefaultEventsStreamOptions().WithDocModifyEvents())
	}

	time.Sleep(time.Millisecond * 15000)
	close(done)
	wg.Wait()
	close(subsDone)
	subsWg.Wait()
}

func TestRaceConditionsTx(t *testing.T) {
	t.Parallel()
	FillTestJoinItems(7000, 2000, testJoinItemsRaceTxNs)
	setNsCopyConfigs(t, "test_items_iter_race_tx")
	setNsCopyConfigs(t, testJoinItemsRaceTxNs)

	done := make(chan bool)
	wg := sync.WaitGroup{}
	writer := func() {
		for {
			select {
			case <-done:
				wg.Done()
				return
			case <-time.After(time.Millisecond * 1):
				ctx, cancel := context.WithCancel(context.Background())
				DB.UpsertCtx(ctx, testItemsRaceTxNs, newTestItem(rand.Intn(20000), 5))
				cancel()
			}
		}
	}
	reader := func() {
		for {
			select {
			case <-done:
				wg.Done()
				return
			default:
				ctx, cancel := context.WithCancel(context.Background())
				q := DB.Query(testItemsRaceTxNs).Limit(2)

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
					qj1 := DB.Query(testJoinItemsRaceTxNs).Where("device", reindexer.EQ, "ottstb").Sort("name", false)
					qj2 := DB.Query(testJoinItemsRaceTxNs).Where("device", reindexer.EQ, "android")
					qj3 := DB.Query(testJoinItemsRaceTxNs).Where("device", reindexer.EQ, "iphone")
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
		defer wg.Done()
		txs := 0
		for {
			select {
			case <-done:
				require.Greater(t, txs, 0)
				return
			case <-time.After(time.Millisecond * 10):
				tx, err := DB.BeginTx(testJoinItemsRaceTxNs)
				if err == nil {
					txs += 1
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
	}
	deleter := func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			case <-time.After(time.Millisecond * 10):
				startID := rand.Intn(20000)
				_, err := DB.Query(testJoinItemsRaceTxNs).Where("id", reindexer.GE, startID).Where("id", reindexer.LE, startID+rand.Intn(1000)).Delete()
				assert.NoError(t, err)
			}
		}
	}

	updater := func() {
		for {
			select {
			case <-done:
				wg.Done()
				return
			default:
				ctx, cancel := context.WithCancel(context.Background())
				if rand.Int()%100 > 90 {
					q := DB.Query(testItemsRaceTxNs).
						Set("name", randString()).
						Set("company_name", randString()).
						Limit(5)

					qj1 := DB.Query(testJoinItemsRaceTxNs).Where("device", reindexer.EQ, "ottstb").Sort("name", false)
					qj3 := DB.Query(testJoinItemsRaceTxNs).Where("device", reindexer.EQ, "iphone")
					q.InnerJoin(qj1, "prices").On("price_id", reindexer.SET, "id")
					q.InnerJoin(qj3, "pricesx").On("location", reindexer.LT, "location").Or().On("price_id", reindexer.SET, "id")

					q.UpdateCtx(ctx).FetchAll()
				} else if rand.Int()%100 > 90 {
					q := DB.Query(testJoinItemsRaceTxNs).
						Set("name", randString()).
						Set("device", randDevice()).
						Limit(5)
					qj := DB.Query(testItemsRaceTxNs)

					if rand.Int()%100 >= 33 {
						qj.Where("genre", reindexer.SET, []int{1, 2})
					} else if rand.Int()%100 >= 33 {
						qj.Where("genre", reindexer.GE, 2)
					} else {
						qj.Where("id", reindexer.SET,
							[]int{rand.Intn(2000), rand.Intn(2000), rand.Intn(2000), rand.Intn(2000), rand.Intn(2000)})
					}
					qj.JoinHandler("prices", func(field string, item interface{}, subitems []interface{}) bool {
						return false
					})
					q.InnerJoin(qj, "prices").On("id", reindexer.SET, "price_id")

					q.UpdateCtx(ctx).FetchAll()
				} else if rand.Int()%100 > 90 {
					q := DB.Query(testItemsRaceTxNs).
						Set("name", randString()).
						Set("device", randDevice()).
						Limit(1)
					qj := DB.Query(testItemsRaceTxNs)

					if rand.Int()%100 >= 50 {
						qj.Where("genre", reindexer.SET, []int{1, 2})
					} else {
						qj.Where("genre", reindexer.GE, 2)
					}
					q.InnerJoin(qj, "prices").On("id", reindexer.SET, "id")

					q.UpdateCtx(ctx).FetchAll()
				}
				cancel()
				time.Sleep(5 * time.Millisecond)
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
		if i%2 == 0 {
			wg.Add(1)
			go updater()
		}

	}
	subsWg := sync.WaitGroup{}
	subsDone := make(chan bool)
	subsWg.Add(4)
	go subUnsubRaceRoutine(t, &subsWg, subsDone, events.DefaultEventsStreamOptions().WithAllTransactionEvents())
	go subscriberRaceRoutine(t, &subsWg, subsDone, events.DefaultEventsStreamOptions().WithConfigNamespace())
	go subscriberRaceRoutine(t, &subsWg, subsDone, events.DefaultEventsStreamOptions().WithTransactionCommitEvents())
	go subscriberRaceRoutine(t, &subsWg, subsDone, events.DefaultEventsStreamOptions().WithAllTransactionEvents())

	time.Sleep(time.Millisecond * 15000)
	close(done)
	wg.Wait()
	close(subsDone)
	subsWg.Wait()
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
