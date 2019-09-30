package reindexer

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/restream/reindexer"
)

type TextTxItem struct {
	ID   int    `reindex:"id,,pk"`
	Name string `reindex:"name,text"`
	Data string
}

const testTxItemNs = "test_tx_item"
const testTxAsyncItemNs = "test_tx_async_item"
const testTxQueryItemNs = "test_tx_queries_item"

func init() {
	tnamespaces[testTxItemNs] = TextTxItem{}
	tnamespaces[testTxAsyncItemNs] = TextTxItem{}
	tnamespaces[testTxQueryItemNs] = TextTxItem{}
}

func FillTextTxItem1Tx(count int, tx *txTest) {
	for i := 0; i < count; i++ {
		if err := tx.Upsert(&TextTxItem{
			ID:   0 + i,
			Name: strconv.Itoa(i),
			Data: randString(),
		}); err != nil {
			panic(err)
		}
	}
}

func FillTextTxFullItems(count int) {
	tx := newTestTx(DB, testTxItemNs)
	FillTextTxItem1Tx(count, tx)
	tx.MustCommit()

}

func CheckTYx(ns string, count int) {
	q1 := DB.Query(ns)
	res, _ := q1.MustExec().FetchAll()
	resMap := make(map[int]string)
	for _, item := range res {
		some := item.(*TextTxItem)
		if strconv.Itoa(some.ID) != some.Name {
			panic("Unexpected item content after tx")
		}
		_, ok := resMap[some.ID]
		if ok {
			panic("Duplicate item after tx")
		}
		resMap[some.ID] = some.Name
	}
	if count != len(resMap) {
		panic(fmt.Errorf("Expect %d results, but got %d", count, len(resMap)))
	}
}

func TestTx(t *testing.T) {
	count := 3
	FillTextTxFullItems(count)
	CheckTYx(testTxItemNs, count)
}

func FillTextTxItemAsync1Tx(count int, tx *txTest) {
	for i := 0; i < count; i++ {
		tx.InsertAsync(&TextTxItem{
			ID:   i,
			Name: strconv.Itoa(i),
		}, func(err error) {
			if err != nil {
				panic(err)
			}
		})
	}
	resCount := tx.MustCommit()
	if resCount != count {
		panic("Unexpected items count on commit")
	}
}

func TestAsyncTx(t *testing.T) {
	tx := newTestTx(DB, testTxAsyncItemNs)
	count := 5000
	FillTextTxItemAsync1Tx(count, tx)
	CheckTYx(testTxAsyncItemNs, count)
}

func TestTxQueries(t *testing.T) {
	tx := newTestTx(DB, testTxQueryItemNs)
	count := 5000
	FillTextTxItem1Tx(count, tx)
	tx.MustCommit()
	CheckTYx(testTxQueryItemNs, count)

	tx = newTestTx(DB, testTxQueryItemNs)
	tx.Query().WhereInt("id", reindexer.LT, 100).Delete()
	tx.MustCommit()

	CheckTYx(testTxQueryItemNs, count-100)

	tx = newTestTx(DB, testTxQueryItemNs)
	tx.Query().WhereInt("id", reindexer.EQ, 1000).Set("Data", "testdata").Update()
	tx.MustCommit()

	item, err := DB.Query(testTxQueryItemNs).WhereInt("id", reindexer.EQ, 1000).MustExec().FetchOne()
	if err != nil {
		panic(err)
	}
	some := item.(*TextTxItem)
	if some.Data != "testdata" {
		panic(fmt.Errorf("expect %s, got %s", "testdata", some.Data))
	}

}
