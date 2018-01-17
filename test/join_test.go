package reindexer

import (
	"reflect"
	"testing"

	"github.com/restream/reindexer"

	"fmt"

	"encoding/json"
)

func init() {
	tnamespaces["test_items_for_join"] = TestItem{}
	tnamespaces["test_join_items"] = TestJoinItem{}
}

type TestJoinItem struct {
	ID       int    `reindex:"id,,pk"`
	Name     string `reindex:"name,tree"`
	Location string `reindex:"location"`
	Device   string `reindex:"device"`
	Price    int
}

func (item *TestItem) Join(field string, subitems []interface{}, context interface{}) {
	switch field {
	case "prices":
		if item.Prices == nil {
			item.Prices = make([]*TestJoinItem, 0, len(subitems))
		}
		for _, srcItem := range subitems {
			item.Prices = append(item.Prices, srcItem.(*TestJoinItem))
		}
	case "pricesx":
		if item.Pricesx == nil {
			item.Pricesx = make([]*TestJoinItem, 0, len(subitems))
		}
		for _, srcItem := range subitems {
			item.Pricesx = append(item.Pricesx, srcItem.(*TestJoinItem))
		}
	}
}

func TestJoin(t *testing.T) {
	FillTestItems("test_items_for_join", 0, 10000, 20)
	FillTestJoinItems(7000, 500)
	CheckTestItemsJoinQueries(true, false, false)
	CheckTestItemsJoinQueries(false, true, true)
	CheckTestItemsJoinQueries(true, true, true)
	CheckTestItemsJoinQueries(false, true, false)
	CheckTestItemsJoinQueries(true, true, false)

}

func FillTestJoinItems(start int, count int) {
	tx := newTestTx(DB, "test_join_items")

	for i := 0; i < count; i++ {
		if err := tx.Upsert(&TestJoinItem{
			ID:       i + start,
			Name:     "price_" + randString(),
			Location: randLocation(),
			Device:   randDevice(),
		}); err != nil {
			panic(err)
		}
	}
	tx.MustCommit(nil)
}

func CheckTestItemsJoinQueries(left, inner, or bool) {
	qj1 := DB.Query("test_join_items").Where("device", reindexer.EQ, "ottstb").Sort("name", true)
	qj2 := DB.Query("test_join_items").Where("device", reindexer.EQ, "android")
	qj3 := DB.Query("test_join_items").Where("device", reindexer.EQ, "iphone")

	qjoin := DB.Query("test_items_for_join").Where("genre", reindexer.EQ, 10).Limit(10).Debug(reindexer.TRACE)

	if left {
		qjoin.LeftJoin(qj1, "prices").On("price_id", reindexer.SET, "id")
	}
	if inner {
		qjoin.InnerJoin(qj2, "pricesx").On("location", reindexer.EQ, "location").On("price_id", reindexer.SET, "id")

		if or {
			qjoin.Or()
		}
		qjoin.InnerJoin(qj3, "pricesx").On("location", reindexer.LT, "location").Or().On("price_id", reindexer.SET, "id")
	}

	rjoin, _ := qjoin.MustExec().FetchAll()

	// for _, rr := range rjoin {
	// 	item := rr.(*TestItem)
	// 	log.Printf("%#v %d -> %#d,%#d\n", item.PricesIDs, item.LocationID, len(item.Pricesx), len(item.Prices))
	// }

	// Verify join results with manual join
	r1, _ := DB.Query("test_items_for_join").Where("genre", reindexer.EQ, 10).MustExec().FetchAll()
	rjcheck := make([]interface{}, 0, 1000)

	for _, iitem := range r1 {

		item := iitem.(*TestItem)
		if left {
			rj1, _ := DB.Query("test_join_items").
				Where("device", reindexer.EQ, "ottstb").
				Where("id", reindexer.SET, item.PricesIDs).
				Sort("name", true).
				MustExec().FetchAll()

			if len(rj1) != 0 {
				item.Prices = make([]*TestJoinItem, 0, len(rj1))
				for _, rrj := range rj1 {
					item.Prices = append(item.Prices, rrj.(*TestJoinItem))
				}
			}
		}

		if inner {
			rj2 := DB.Query("test_join_items").
				Where("device", reindexer.EQ, "android").
				Where("id", reindexer.SET, item.PricesIDs).
				Where("location", reindexer.EQ, item.LocationID).
				MustExec()

			rj3 := DB.Query("test_join_items").
				Where("device", reindexer.EQ, "iphone").
				Where("id", reindexer.SET, item.PricesIDs).Or().
				Where("location", reindexer.LT, item.LocationID).MustExec()

			if (or && (rj2.Count() != 0 || rj3.Count() != 0)) || (!or && (rj2.Count() != 0 && rj3.Count() != 0)) {
				item.Pricesx = make([]*TestJoinItem, 0)
				for rj2.Next() {
					item.Pricesx = append(item.Pricesx, rj2.Object().(*TestJoinItem))
				}
				for rj3.Next() {
					item.Pricesx = append(item.Pricesx, rj3.Object().(*TestJoinItem))
				}
			} else {
				continue
			}

		}
		rjcheck = append(rjcheck, item)

		if len(rjcheck) == 10 {
			break
		}
	}

	if len(rjcheck) != len(rjoin) {
		panic(fmt.Errorf("%d != %d", len(rjcheck), len(rjoin)))
	}
	for i := 0; i < len(rjcheck); i++ {
		i1 := rjcheck[i].(*TestItem)
		i2 := rjoin[i].(*TestItem)
		if !reflect.DeepEqual(i1, i2) {
			i1s, _ := json.Marshal(i1)
			i2s, _ := json.Marshal(i2)

			panic(fmt.Errorf("-----expect:\n%s\n-----got:\n%s", string(i1s), string(i2s)))

		}
	}

}

func TestJoinQueryResultsOnIterator(t *testing.T) {
	qjoin := DB.Query("test_items_for_join").Where("genre", reindexer.EQ, 10).Limit(10).Debug(reindexer.TRACE)
	qj1 := DB.Query("test_join_items").Where("device", reindexer.EQ, "ottstb").Sort("name", false)
	qj2 := DB.Query("test_join_items").Where("device", reindexer.EQ, "android")
	qjoin.LeftJoin(qj1, "prices").On("price_id", reindexer.SET, "id").
		InnerJoin(qj2, "pricesx").On("location", reindexer.EQ, "location").On("price_id", reindexer.SET, "id")

	var handlerSubitems []interface{}

	qjoin.JoinHandler("prices", func(field string, item interface{}, subitems []interface{}) (isContinue bool) {
		if field != "prices" {
			t.Errorf("field expected: '%v'; actual: '%v'", "prices", field)
		}
		if item == nil {
			t.Errorf("item in handler is nil")
		}
		handlerSubitems = subitems
		return true
	})

	iter := qjoin.MustExec()
	defer iter.Close()

	for iter.Next() {
		item := iter.Object().(*TestItem)
		joinResultsPrices, err := iter.JoinedObjects("prices")
		if err != nil {
			t.Fatalf("Can't get join objects from iterator: %v", err)
		}
		joinResultsPricesx, err := iter.JoinedObjects("pricesx")
		if err != nil {
			t.Fatalf("Can't get join objects from iterator: %v", err)
		}

		for i := range item.Prices {
			if !reflect.DeepEqual(item.Prices[i], joinResultsPrices[i]) {
				i1s, _ := json.Marshal(item.Prices[i])
				i2s, _ := json.Marshal(joinResultsPrices[i])

				panic(fmt.Errorf("-----expect:\n%s\n-----got:\n%s", string(i1s), string(i2s)))
			}
			if !reflect.DeepEqual(item.Prices[i], handlerSubitems[i]) {
				i1s, _ := json.Marshal(item.Prices[i])
				i2s, _ := json.Marshal(handlerSubitems[i])

				panic(fmt.Errorf("-----expect:\n%s\n-----got:\n%s", string(i1s), string(i2s)))
			}
		}
		for i := range item.Pricesx {
			if !reflect.DeepEqual(item.Pricesx[i], joinResultsPricesx[i]) {
				i1s, _ := json.Marshal(item.Pricesx[i])
				i2s, _ := json.Marshal(joinResultsPricesx[i])

				panic(fmt.Errorf("-----expect:\n%s\n-----got:\n%s", string(i1s), string(i2s)))
			}
		}
	}
}
