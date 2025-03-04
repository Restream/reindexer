package reindexer

import (
	"math/rand"
	"sync"
	"testing"

	"github.com/restream/reindexer/v5"
	"github.com/stretchr/testify/assert"
)

func TestJoinCache(t *testing.T) {

	FillTestItems("test_items_for_join", 0, 10000, 20)
	FillTestJoinItems(7000, 500, "test_join_items")
	RunInMultiThread(t, CheckTestCachedItemsJoinLeftQueries, 20)
	RunInMultiThread(t, CheckTestCachedItemsJoinInnerQueries, 20)
	RunInMultiThread(t, CheckTestCachedItemsJoinSortQueries, 8)

}
func RunInMultiThread(t *testing.T, fn func(*testing.T, *sync.WaitGroup), threadCount int) {
	var wg sync.WaitGroup
	wg.Add(threadCount)
	for i := 0; i < threadCount; i++ {
		go fn(t, &wg)
	}
	wg.Wait()
}

func PrepareJoinQueryResult(t *testing.T, sort1 string, sort2 string) []interface{} {
	qj1 := DB.Query("test_join_items").Where("DEVICE", reindexer.EQ, "ottstb")
	if sort1 != "" {
		qj1.Sort(sort1, true)
	}

	qjoin := DB.Query("TEST_ITEMS_FOR_JOIN").Where("GENRE", reindexer.EQ, 10).Limit(10).Debug(reindexer.TRACE)
	if sort2 != "" {
		qjoin.Sort(sort2, true)
	}

	qjoin.LeftJoin(qj1, "PRICES").On("PRICE_ID", reindexer.SET, "ID")
	rjoin, _ := qjoin.MustExec(t).FetchAll()
	return rjoin
}

func CheckTestCachedItemsJoinLeftQueries(t *testing.T, wg *sync.WaitGroup) {
	defer wg.Done()
	resultSort1 := PrepareJoinQueryResult(t, "device", "name")

	for i := 0; i < 20; i++ {
		assert.Equal(t, resultSort1, PrepareJoinQueryResult(t, "device", "name"))
	}
}

func CheckTestCachedItemsJoinInnerQueries(t *testing.T, wg *sync.WaitGroup) {
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

		rjoin, _ := qjoin.MustExec(t).FetchAll()
		if i == 0 {
			result_without_cahce = append([]interface{}(nil), rjoin...)
		} else {
			assert.Equal(t, rjoin, result_without_cahce)
		}
	}
}

func CheckTestCachedItemsJoinSortQueries(t *testing.T, wg *sync.WaitGroup) {
	defer wg.Done()
	resultSort := [][]interface{}{PrepareJoinQueryResult(t, "device", "genre"),
		PrepareJoinQueryResult(t, "location", "name"),
		PrepareJoinQueryResult(t, "name", "name"),
		PrepareJoinQueryResult(t, "amount", "rate"),
		PrepareJoinQueryResult(t, "", ""),
	}

	for i := 0; i < 100; i++ {
		op := rand.Intn(5)
		switch op {
		case 0:
			assert.Equal(t, resultSort[op], PrepareJoinQueryResult(t, "device", "genre"))
		case 1:
			assert.Equal(t, resultSort[op], PrepareJoinQueryResult(t, "location", "name"))
		case 2:
			assert.Equal(t, resultSort[op], PrepareJoinQueryResult(t, "name", "name"))
		case 3:
			assert.Equal(t, resultSort[op], PrepareJoinQueryResult(t, "amount", "rate"))
		case 4:
			assert.Equal(t, resultSort[op], PrepareJoinQueryResult(t, "", ""))
		}
	}
}
