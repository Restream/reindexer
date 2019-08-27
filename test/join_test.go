package reindexer

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"testing"

	"git.itv.restr.im/itv-backend/reindexer"
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
	Amount   int    `reindex:"amount,tree"`
	Price    int
}

func (item *TestItem) Join(field string, subitems []interface{}, context interface{}) {
	switch strings.ToLower(field) {
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
	CheckJoinsAsWhereCondition()
}

func FillTestJoinItems(start int, count int) {
	tx := newTestTx(DB, "test_join_items")

	for i := 0; i < count; i++ {
		if err := tx.Upsert(&TestJoinItem{
			ID:       i + start,
			Name:     "price_" + randString(),
			Location: randLocation(),
			Device:   randDevice(),
			Amount:   rand.Int() % 10,
		}); err != nil {
			panic(err)
		}
	}
	tx.MustCommit()
}

type byID []*TestJoinItem

func (s byID) Len() int {
	return len(s)
}
func (s byID) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s byID) Less(i, j int) bool {
	return s[i].ID < s[j].ID
}

func CheckJoinsAsWhereCondition() {
	qj1 := DB.Query("test_join_items").Where("DEVICE", reindexer.EQ, "ottstb").Sort("NAME", true)
	qj2 := DB.Query("test_join_items").Where("DEVICE", reindexer.EQ, "android").Where("AMOUNT", reindexer.GT, 2)
	qj3 := DB.Query("test_join_items").Where("DEVICE", reindexer.EQ, "iphone")

	qjoin := DB.Query("test_join_items").Where("GENRE", reindexer.GE, 1).Limit(100).Debug(reindexer.TRACE)
	qjoin.InnerJoin(qj1, "PRICES").On("PRICE_ID", reindexer.SET, "ID")
	qjoin.Or().Where("DEVICE", reindexer.EQ, "android")
	qjoin.Or().InnerJoin(qj2, "PRICESX").On("LOCATION", reindexer.EQ, "LOCATION").On("PRICE_ID", reindexer.SET, "ID")
	qjoin.Or().InnerJoin(qj3, "PRICESX").On("LOCATION", reindexer.LT, "LOCATION").Or().On("PRICE_ID", reindexer.SET, "ID")
	qjoin.Or().Where("DEVICE", reindexer.EQ, "iphone")

	rjcheck := make([]interface{}, 0, 1000)

	jr, _ := DB.Query("test_join_items").Where("GENRE", reindexer.GE, 1).Limit(100).MustExec().FetchAll()
	for _, iitem := range jr {
		item := iitem.(*TestItem)
		rj1, _ := DB.Query("test_join_items").
			Where("DEVICE", reindexer.EQ, "ottstb").
			Where("ID", reindexer.SET, item.PricesIDs).
			Sort("NAME", true).
			MustExec().FetchAll()

		if len(rj1) != 0 {
			item.Prices = make([]*TestJoinItem, 0, len(rj1))
			for _, rrj := range rj1 {
				item.Prices = append(item.Prices, rrj.(*TestJoinItem))
			}
		} else {
			jitem := iitem.(*TestJoinItem)
			if jitem.Device != "android" {
				rj2 := DB.Query("test_join_items").
					Where("DEVICE", reindexer.EQ, "android").
					Where("AMOUNT", reindexer.GT, 2).
					Where("ID", reindexer.SET, item.PricesIDs).
					Where("LOCATION", reindexer.EQ, item.LocationID).
					Sort("NAME", true).
					Limit(30).
					MustExec()

				rj3 := DB.Query("test_join_items").
					Where("DEVICE", reindexer.EQ, "iphone").
					Where("ID", reindexer.SET, item.PricesIDs).Or().
					Where("LOCATION", reindexer.LT, item.LocationID).
					MustExec()

				item.Pricesx = make([]*TestJoinItem, 0)
				for rj2.Next() {
					item.Pricesx = append(item.Pricesx, rj2.Object().(*TestJoinItem))
				}
				for rj3.Next() {
					item.Pricesx = append(item.Pricesx, rj3.Object().(*TestJoinItem))
				}
				rj2.Close()
				rj3.Close()
			}
		}
		rjcheck = append(rjcheck, item)
		if len(rjcheck) == 100 {
			break
		}
	}

	rjoin, _ := qjoin.MustExec().FetchAll()
	if len(rjcheck) != len(rjoin) {
		panic(fmt.Errorf("%d != %d", len(rjcheck), len(rjoin)))
	}
	for i := 0; i < len(rjcheck); i++ {
		i1 := rjcheck[i].(*TestItem)
		i2 := rjoin[i].(*TestItem)
		sort.Sort(byID(i1.Pricesx))
		sort.Sort(byID(i2.Pricesx))
		sort.Sort(byID(i1.Prices))
		sort.Sort(byID(i2.Prices))
		if !reflect.DeepEqual(i1, i2) {
			i1s, _ := json.Marshal(i1)
			i2s, _ := json.Marshal(i2)
			panic(fmt.Errorf("%d:-----expect:\n%s\n-----got:\n%s", i, string(i1s), string(i2s)))

		}
	}
}

func CheckTestItemsJoinQueries(left, inner, or bool) {
	qj1 := DB.Query("test_join_items").Where("DEVICE", reindexer.EQ, "ottstb").Sort("NAME", true)
	qj2 := DB.Query("test_join_items").Where("DEVICE", reindexer.EQ, "android").Where("AMOUNT", reindexer.GT, 2)
	qj3 := DB.Query("test_join_items").Where("DEVICE", reindexer.EQ, "iphone")

	qjoin := DB.Query("test_items_for_join").Where("GENRE", reindexer.EQ, 10).Limit(10).Debug(reindexer.TRACE)

	if left {
		qjoin.LeftJoin(qj1, "PRICES").On("PRICE_ID", reindexer.SET, "ID")
	}
	if inner {
		qjoin.InnerJoin(qj2, "PRICESX").On("LOCATION", reindexer.EQ, "LOCATION").On("PRICE_ID", reindexer.SET, "ID")

		if or {
			qjoin.Or()
		}
		qjoin.InnerJoin(qj3, "PRICESX").
			On("LOCATION", reindexer.LT, "LOCATION").
			Or().On("PRICE_ID", reindexer.SET, "ID")
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
				Where("DEVICE", reindexer.EQ, "ottstb").
				Where("ID", reindexer.SET, item.PricesIDs).
				Sort("NAME", true).
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
				Where("DEVICE", reindexer.EQ, "android").
				Where("AMOUNT", reindexer.GT, 2).
				Where("ID", reindexer.SET, item.PricesIDs).
				Where("LOCATION", reindexer.EQ, item.LocationID).
				Sort("NAME", true).
				Limit(30).
				MustExec()

			rj3 := DB.Query("test_join_items").
				Where("DEVICE", reindexer.EQ, "iphone").
				Where("ID", reindexer.SET, item.PricesIDs).Or().
				Where("LOCATION", reindexer.LT, item.LocationID).
				MustExec()

			if (or && (rj2.Count() != 0 || rj3.Count() != 0)) || (!or && (rj2.Count() != 0 && rj3.Count() != 0)) {
				item.Pricesx = make([]*TestJoinItem, 0)
				for rj2.Next() {
					item.Pricesx = append(item.Pricesx, rj2.Object().(*TestJoinItem))
				}
				for rj3.Next() {
					item.Pricesx = append(item.Pricesx, rj3.Object().(*TestJoinItem))
				}
				rj2.Close()
				rj3.Close()
			} else {
				rj2.Close()
				rj3.Close()
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
		sort.Sort(byID(i1.Pricesx))
		sort.Sort(byID(i2.Pricesx))
		sort.Sort(byID(i1.Prices))
		sort.Sort(byID(i2.Prices))
		if !reflect.DeepEqual(i1, i2) {
			i1s, _ := json.Marshal(i1)
			i2s, _ := json.Marshal(i2)

			panic(fmt.Errorf("%d:-----expect:\n%s\n-----got:\n%s", i, string(i1s), string(i2s)))

		}
	}
}

func TestJoinQueryResultsOnIterator(t *testing.T) {
	qjoin := DB.Query("test_items_for_join").Where("GENRE", reindexer.EQ, 10).Limit(10).Debug(reindexer.TRACE)
	qj1 := DB.Query("test_join_items").Where("DEVICE", reindexer.EQ, "ottstb").Sort("name", false)
	qj2 := DB.Query("test_join_items").Where("DEVICE", reindexer.EQ, "android")
	qjoin.LeftJoin(qj1, "PRICES").On("PRICE_ID", reindexer.SET, "ID").
		InnerJoin(qj2, "PRICESX").On("LOCATION", reindexer.EQ, "LOCATION").On("PRICE_ID", reindexer.SET, "Id")

	var handlerSubitems []interface{}

	qjoin.JoinHandler("PRICES", func(field string, item interface{}, subitems []interface{}) (isContinue bool) {
		if !strings.EqualFold(field, "prices") {
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
		joinResultsPrices, err := iter.JoinedObjects("PRICES")
		if err != nil {
			t.Fatalf("Can't get join objects from iterator: %v", err)
		}
		joinResultsPricesx, err := iter.JoinedObjects("PRICESX")
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
