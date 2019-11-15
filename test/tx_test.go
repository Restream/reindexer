package reindexer

import (
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"testing"

	"github.com/restream/reindexer"
)

type TextTxItem struct {
	ID   int    `reindex:"id,,pk"`
	Name string `reindex:"name,text"`
	Data string
}

type UntaggedTxItem struct {
	ID         int64  `json:"id" reindex:"id,,pk"`
	Data       int64  `json:"data"`
	DataString string `json:"data_string"`
}

const testTxItemNs = "test_tx_item"
const testTxAsyncItemNs = "test_tx_async_item"
const testTxQueryItemNs = "test_tx_queries_item"
const testTxConcurrentTagsItemNs = "test_tx_concurrent_tags_item"

func init() {
	tnamespaces[testTxItemNs] = TextTxItem{}
	tnamespaces[testTxAsyncItemNs] = TextTxItem{}
	tnamespaces[testTxQueryItemNs] = TextTxItem{}
	tnamespaces[testTxConcurrentTagsItemNs] = UntaggedTxItem{}
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

func newUntaggedItems(itemID int64, count int) []*UntaggedTxItem {
	items := make([]*UntaggedTxItem, count)
	for i := range items {
		items[i] = &UntaggedTxItem{
			ID:         itemID,
			Data:       rand.Int63(),
			DataString: randString(),
		}
	}
	return items
}

func TestConcurrentTagsTx(t *testing.T) {
	const concurrency = 20
	wg := sync.WaitGroup{}
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			cnt := rand.Intn(20) + 1
			for i := 0; i < cnt; i++ {
				ID := int64(rand.Intn(20) + 1)
				items := newUntaggedItems(ID, rand.Intn(5))
				tx, err := DB.BeginTx(testTxConcurrentTagsItemNs)
				if err != nil {
					panic(err)
				}
				for i := range items {
					err := tx.Upsert(items[i])
					if err != nil {
						panic(err)
					}
				}
				count := tx.MustCommit()
				if count != len(items) {
					panic("Wrong items count in Tx")
				}
			}
		}()
	}
	wg.Wait()
	DB.SetSynced(false)
	items, err := DB.Query(testTxConcurrentTagsItemNs).MustExec().FetchAll()
	if err != nil {
		panic(err)
	}
	if len(items) == 0 {
		panic("Empty items array")
	}
}
