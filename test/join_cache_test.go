package reindexer

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"reflect"
	"sync"
	"testing"

	"git.itv.restr.im/itv-backend/reindexer"
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

func expectSlicesEqual(expect []interface{}, got []interface{}) {
	if len(expect) != len(got) {
		panic(fmt.Errorf("Result len is not equal"))
	}
	wasErr := false
	for j := 0; j < len(expect); j++ {
		if !reflect.DeepEqual(expect[j], got[j]) {
			i1s, _ := json.Marshal(expect[j])
			i2s, _ := json.Marshal(got[j])
			fmt.Printf("%d:-----expect:\n%s\n-----got:\n%s\n", j, string(i1s), string(i2s))
			wasErr = true
		}
	}
	if wasErr {
		panic(fmt.Errorf("Error occuried"))
	}
}

func CheckTestCachedItemsJoinLeftQueries(wg *sync.WaitGroup) {
	defer wg.Done()
	resultSort1 := PrepareJoinQueryResult("device", "name")

	for i := 0; i < 20; i++ {
		expectSlicesEqual(resultSort1, PrepareJoinQueryResult("device", "name"))
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
		} else {
			expectSlicesEqual(rjoin, result_without_cahce)
		}
	}
}

func CheckTestCachedItemsJoinSortQueries(wg *sync.WaitGroup) {
	defer wg.Done()
	resultSort := [][]interface{}{PrepareJoinQueryResult("device", "genre"),
		PrepareJoinQueryResult("location", "name"),
		PrepareJoinQueryResult("name", "name"),
		PrepareJoinQueryResult("amount", "rate"),
		PrepareJoinQueryResult("", ""),
	}

	for i := 0; i < 100; i++ {
		op := rand.Intn(5)
		switch op {
		case 0:
			expectSlicesEqual(resultSort[op], PrepareJoinQueryResult("device", "genre"))
		case 1:
			expectSlicesEqual(resultSort[op], PrepareJoinQueryResult("location", "name"))
		case 2:
			expectSlicesEqual(resultSort[op], PrepareJoinQueryResult("name", "name"))
		case 3:
			expectSlicesEqual(resultSort[op], PrepareJoinQueryResult("amount", "rate"))
		case 4:
			expectSlicesEqual(resultSort[op], PrepareJoinQueryResult("", ""))
		}
	}
}
