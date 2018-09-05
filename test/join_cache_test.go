package reindexer

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"reflect"
	"sync"
	"testing"

	"github.com/restream/reindexer"
)

func TestJoinCache(t *testing.T) {
	log.Printf("DO JOIN CACHE TESTS")

	FillTestItems("test_items_for_join", 0, 10000, 20)
	FillTestJoinItems(7000, 500)
	RunInMultiThread(CheckTestCachedItemsJoinLeftQueries, 20)
	RunInMultiThread(CheckTestCachedItemsJoinInnerQueries, 20)
	RunInMultiThread(CheckTestCachedItemsJoinSortQueries, 8)

}
func RunInMultiThread(fn func(*sync.WaitGroup), thread_count int) {
	var wg sync.WaitGroup
	wg.Add(thread_count)
	for i := 0; i < thread_count; i++ {
		go fn(&wg)
	}
	wg.Wait()
}

func PrepareJoinQueryResult(sort1 string, sort2 string) []interface{} {
	qj1 := DB.Query("test_join_items").Where("DEVICE", reindexer.EQ, "ottstb")
	if sort1 != "" {
		qj1.Sort(sort1, true)
	}

	qjoin := DB.Query("TEST_ITEMS_FOR_JOIN").Where("GENRE", reindexer.EQ, 10).Limit(10).Debug(reindexer.TRACE)
	if sort2 != "" {
		qjoin.Sort(sort2, true)
	}

	qjoin.LeftJoin(qj1, "PRICES").On("PRICE_ID", reindexer.SET, "ID")
	rjoin, _ := qjoin.MustExec().FetchAll()
	return rjoin
}

func CheckTestCachedItemsJoinLeftQueries(wg *sync.WaitGroup) {
	defer wg.Done()
	resultSort1 := PrepareJoinQueryResult("device", "name")

	for i := 0; i < 20; i++ {
		result := PrepareJoinQueryResult("device", "name")
		if !reflect.DeepEqual(result, resultSort1) {
			i1s, _ := json.Marshal(resultSort1)
			i2s, _ := json.Marshal(result)
			panic(fmt.Errorf("%d:-----expect:\n%s\n-----got:\n%s", i, string(i1s), string(i2s)))

		}
	}
}

func CheckTestCachedItemsJoinInnerQueries(wg *sync.WaitGroup) {
	defer wg.Done()
	var result_without_cahce []interface{}
	for i := 0; i < 20; i++ {
		qj1 := DB.Query("test_join_items").Where("DEVICE", reindexer.EQ, "android").Where("AMOUNT", reindexer.GT, 2)
		qj2 := DB.Query("test_join_items").Where("DEVICE", reindexer.EQ, "iphone")

		qjoin := DB.Query("test_items_for_join").Where("GENRE", reindexer.EQ, 10).Limit(10).Debug(reindexer.TRACE)
		qjoin.InnerJoin(qj1, "PRICESX").On("LOCATION", reindexer.EQ, "location").On("price_id", reindexer.SET, "id")
		qjoin.Or()
		qjoin.InnerJoin(qj2, "PRICESX").
			On("location", reindexer.LT, "LOCATION").
			Or().On("PRICE_ID", reindexer.SET, "id")

		rjoin, _ := qjoin.MustExec().FetchAll()
		if i == 0 {
			result_without_cahce = append([]interface{}(nil), rjoin...)
		} else if !reflect.DeepEqual(rjoin, result_without_cahce) {
			i1s, _ := json.Marshal(result_without_cahce)
			i2s, _ := json.Marshal(rjoin)
			panic(fmt.Errorf("%d:-----expect:\n%s\n-----got:\n%s", i, string(i1s), string(i2s)))

		}
	}
}

func CheckTestCachedItemsJoinSortQueries(wg *sync.WaitGroup) {
	defer wg.Done()
	resultSort1 := PrepareJoinQueryResult("device", "genre")
	resultSort2 := PrepareJoinQueryResult("location", "name")
	resultSort3 := PrepareJoinQueryResult("name", "name")
	resultSort4 := PrepareJoinQueryResult("amount", "rate")
	resultSort5 := PrepareJoinQueryResult("", "")

	for i := 0; i < 100; i++ {
		op := rand.Intn(5)

		if op == 0 {
			res := PrepareJoinQueryResult("device", "genre")
			if !reflect.DeepEqual(res, resultSort1) {
				i1s, _ := json.Marshal(resultSort1)
				i2s, _ := json.Marshal(res)
				panic(fmt.Errorf("%d:-----expect:\n%s\n-----got:\n%s", i, string(i1s), string(i2s)))

			}
		} else if op == 1 {
			res := PrepareJoinQueryResult("location", "name")
			if !reflect.DeepEqual(res, resultSort2) {
				i1s, _ := json.Marshal(resultSort2)
				i2s, _ := json.Marshal(res)
				panic(fmt.Errorf("%d:-----expect:\n%s\n-----got:\n%s", i, string(i1s), string(i2s)))

			}
		} else if op == 2 {
			res := PrepareJoinQueryResult("name", "name")
			if !reflect.DeepEqual(res, resultSort3) {
				i1s, _ := json.Marshal(resultSort3)
				i2s, _ := json.Marshal(res)
				panic(fmt.Errorf("%d:-----expect:\n%s\n-----got:\n%s", i, string(i1s), string(i2s)))

			}
		} else if op == 3 {
			res := PrepareJoinQueryResult("amount", "rate")
			if !reflect.DeepEqual(res, resultSort4) {
				i1s, _ := json.Marshal(resultSort4)
				i2s, _ := json.Marshal(res)
				panic(fmt.Errorf("%d:-----expect:\n%s\n-----got:\n%s", i, string(i1s), string(i2s)))

			}
		} else if op == 4 {
			res := PrepareJoinQueryResult("", "")
			if !reflect.DeepEqual(res, resultSort5) {
				i1s, _ := json.Marshal(resultSort5)
				i2s, _ := json.Marshal(res)
				panic(fmt.Errorf("%d:-----expect:\n%s\n-----got:\n%s", i, string(i1s), string(i2s)))

			}
		}

	}

}
