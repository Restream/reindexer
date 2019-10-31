package reindexer

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/restream/reindexer"
	"github.com/restream/reindexer/test/custom_struct_another"
)

func init() {
	tnamespaces["test_items_iter"] = TestItem{}
	tnamespaces["test_items_iter_next_obj"] = TestItem{}
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
				ctx, cancel := context.WithCancel(context.Background())
				DB.UpsertCtx(ctx, "test_items_iter", newTestItem(1000+rand.Intn(100), 5))
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

				it := q.ExecCtx(ctx)
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
				DB.CloseNamespace("test_items_iter")
				DB.OpenNamespace("test_items_iter", reindexer.DefaultNamespaceOptions(), TestItem{})
				tx, _ := DB.BeginTx("test_join_items")
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

	time.Sleep(time.Millisecond * 20000)
	close(done)
	wg.Wait()
}

func TestNextObj(t *testing.T) {
	ctx := context.Background()

	err := DB.OpenNamespace("test_items_iter_next_obj", reindexer.DefaultNamespaceOptions(), TestItem{})
	if err != nil {
		panic(err)
	}

	itemsExp := []*TestItem{
		newTestItem(20000, 5).(*TestItem),
		newTestItem(20001, 5).(*TestItem),
		newTestItem(20002, 5).(*TestItem),
	}
	itemCustomExp := &TestItemCustom{
		ID:                itemsExp[1].ID,
		Name:              itemsExp[1].Name,
		Genre:             itemsExp[1].Genre,
		Year:              itemsExp[1].Year,
		CustomUniqueField: rand.Int(),
		Actor:             itemsExp[1].Actor,
	}
	for _, v := range itemsExp {
		if err := DB.Upsert("test_items_iter_next_obj", v); err != nil {
			panic(err)
		}
	}

	// should use two structs in one test, to use cTagsCache
	t.Run("parse to custom and original structs", func(t *testing.T) {
		it := DB.Query("test_items_iter_next_obj").
			WhereInt("id", reindexer.SET, itemsExp[0].ID, itemsExp[1].ID, itemsExp[2].ID).
			ExecCtx(ctx)
		defer it.Close()

		// use one original struct
		item := TestItem{}
		if it.NextObj(&item) {
			if itemsExp[0].ID != item.ID {
				t.Errorf("unexpected item, exp=%#v, current=%#v", itemsExp[0], item)
			}
		}

		// use custom struct
		itemCustom := TestItemCustom{}
		if it.NextObj(&itemCustom) {
			if itemCustomExp.ID != itemCustom.ID || itemCustomExp.Name != itemCustom.Name || itemCustomExp.Genre != itemCustom.Genre ||
				itemCustomExp.Year != itemCustom.Year || itemCustomExp.Actor != itemCustom.Actor || 0 != itemCustom.CustomUniqueField {
				t.Errorf("unexpected item, exp=%#v, current=%#v", itemCustomExp, itemCustom)
			}
		}

		// and second original struct
		item2 := TestItem{}
		if it.NextObj(&item2) {
			if itemsExp[2].ID != item2.ID {
				t.Errorf("unexpected item, exp=%#v, current=%#v", itemsExp[2], item2)
			}
		}
		if it.Error() != nil {
			panic(it.Error())
		}
	})

	t.Run("parse to the same struct from separate packages", func(t *testing.T) {
		it := DB.Query("test_items_iter_next_obj").
			WhereInt("id", reindexer.SET, itemsExp[0].ID, itemsExp[1].ID).
			ExecCtx(ctx)
		defer it.Close()

		// use another custom struct
		itemCustomAnother := custom_struct_another.TestItemCustom{}
		if it.NextObj(&itemCustomAnother) {
			if itemsExp[0].ID != itemCustomAnother.ID {
				t.Errorf("unexpected item, exp=%#v, current=%#v", itemsExp[0], itemCustomAnother)
			}
		}

		// use custom struct
		itemCustom := TestItemCustom{}
		if it.NextObj(&itemCustom) {
			if itemCustomExp.ID != itemCustom.ID || itemCustomExp.Name != itemCustom.Name || itemCustomExp.Genre != itemCustom.Genre ||
				itemCustomExp.Year != itemCustom.Year || itemCustomExp.Actor != itemCustom.Actor || 0 != itemCustom.CustomUniqueField {
				t.Errorf("unexpected item, exp=%#v, current=%#v", itemCustomExp, itemCustom)
			}
		}

		if it.Error() != nil {
			panic(it.Error())
		}
	})
}
