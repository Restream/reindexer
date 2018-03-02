package reindexer

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/restream/reindexer"
)

func init() {
	tnamespaces["test_items_iter"] = TestItem{}
}

func TestQueryIter(t *testing.T) {
	total := 10
	for i := 0; i < total; i++ {
		err := DB.Upsert("test_items_iter", newTestItem(1000+i, 5))
		if err != nil {
			panic(err)
		}
	}

	// check next
	limit := 5
	iter := DB.Query("test_items_iter").ReqTotal().Limit(limit).Exec()
	defer iter.Close()
	i := 0
	for iter.Next() {
		_ = iter.Object()
		i++
	}
	if iter.Error() != nil {
		t.Fatalf("Iter has error: %v", iter.Error())
	}
	if i != limit {
		t.Errorf("Unexpected result count: %d (want %d)", i, limit)
	}

	// check all
	items, err := DB.Query("test_items_iter").Exec().FetchAll()
	if err != nil {
		t.Fatalf("Iter has error: %v", iter.Error())
	}
	if len(items) != total {
		t.Errorf("Unexpected result count: %d (want %d)", len(items), total)
	}

	// check one
	item, err := DB.Query("test_items_iter").Exec().FetchOne()
	if err != nil {
		t.Fatalf("Iter has error: %v", iter.Error())
	}
	if item == nil {
		t.Errorf("Iterator.FetchOne: item is nil")
	}
}

func TestRaceConditions(t *testing.T) {
	FillTestJoinItems(7000, 2000)
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
				DB.Upsert("test_items_iter", newTestItem(1000+rand.Intn(100), 5))
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
				q := DB.Query("test_items_iter").Limit(2)

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
					qj1 := DB.Query("test_join_items").Where("device", reindexer.EQ, "ottstb").Sort("name", false)
					qj2 := DB.Query("test_join_items").Where("device", reindexer.EQ, "android")
					qj3 := DB.Query("test_join_items").Where("device", reindexer.EQ, "iphone")
					q.LeftJoin(qj1, "prices").On("price_id", reindexer.SET, "id")
					q.LeftJoin(qj2, "pricesx").On("location", reindexer.EQ, "location").On("price_id", reindexer.SET, "id")
					q.LeftJoin(qj3, "pricesx").On("location", reindexer.LT, "location").Or().On("price_id", reindexer.SET, "id")
				}

				it := q.Exec()
				_ = it.TotalCount()
				for it.Next() {
					_ = it.Object().(*TestItem)
				}
				it.Close()
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
				DB.CloseNamespace("test_items_iter")
				DB.OpenNamespace("test_items_iter", reindexer.DefaultNamespaceOptions(), TestItem{})
				tx, _ := DB.BeginTx("test_join_items")
				tx.Upsert(TestJoinItem{ID: 7000})
				tx.Commit(nil)
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

	time.Sleep(time.Millisecond * 20000)
	close(done)
	wg.Wait()
}
