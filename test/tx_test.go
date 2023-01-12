package reindexer

import (
	"context"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/restream/reindexer/v3"
	"github.com/restream/reindexer/v3/bindings"
	"github.com/stretchr/testify/assert"
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
const testTxAsyncTimeoutItemNs = "test_tx_async_timeout_item"
const testTxQueryItemNs = "test_tx_queries_item"
const testTxConcurrentTagsItemNs = "test_tx_concurrent_tags_item"

func init() {
	tnamespaces[testTxItemNs] = TextTxItem{}
	tnamespaces[testTxAsyncItemNs] = TextTxItem{}
	tnamespaces[testTxAsyncTimeoutItemNs] = TextTxItem{}
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

func CheckTYx(t *testing.T, ns string, count int) {
	q1 := DB.Query(ns)
	res, _ := q1.MustExec(t).FetchAll()
	resMap := make(map[int]string)
	for _, item := range res {
		some := item.(*TextTxItem)
		assert.Equal(t, strconv.Itoa(some.ID), some.Name, "Unexpected item content after tx")
		_, ok := resMap[some.ID]
		assert.False(t, ok, "Duplicate item after tx")
		resMap[some.ID] = some.Name
	}
	assert.Equal(t, count, len(resMap), "Expect %d results, but got %d", count, len(resMap))

}

func TestTx(t *testing.T) {
	count := 3
	FillTextTxFullItems(count)
	CheckTYx(t, testTxItemNs, count)
}

func FillTextTxItemAsync1Tx(t *testing.T, count int, tx *txTest) {
	for i := 0; i < count; i++ {
		tx.InsertAsync(&TextTxItem{
			ID:   i,
			Name: strconv.Itoa(i),
		}, func(err error) {
			assert.NoError(t, err)
		})
	}
	resCount := tx.MustCommit()
	assert.Equal(t, resCount, count, "Unexpected items count on commit")
}

func TestAsyncTx(t *testing.T) {
	tx := newTestTx(DB, testTxAsyncItemNs)
	count := 5000
	FillTextTxItemAsync1Tx(t, count, tx)
	CheckTYx(t, testTxAsyncItemNs, count)
}

func TestTxQueries(t *testing.T) {
	tx := newTestTx(DB, testTxQueryItemNs)
	count := 5000
	FillTextTxItem1Tx(count, tx)
	tx.MustCommit()
	assert.NoError(t, tx.Rollback())
	CheckTYx(t, testTxQueryItemNs, count)

	tx = newTestTx(DB, testTxQueryItemNs)
	tx.Query().WhereInt("id", reindexer.LT, 100).Delete()
	tx.MustCommit()

	CheckTYx(t, testTxQueryItemNs, count-100)

	tx = newTestTx(DB, testTxQueryItemNs)
	tx.Query().WhereInt("id", reindexer.EQ, 1000).Set("Data", "testdata").Update()
	tx.MustCommit()

	item, err := DB.Query(testTxQueryItemNs).WhereInt("id", reindexer.EQ, 1000).MustExec(t).FetchOne()
	if err != nil {
		panic(err)
	}
	some := item.(*TextTxItem)
	assert.Equal(t, some.Data, "testdata", "expect %s, got %s", "testdata", some.Data)
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
					assert.NoError(t, tx.Upsert(items[i]))
				}
				count := tx.MustCommit()
				assert.Equal(t, count, len(items), "Wrong items count in Tx")
			}
		}()
	}
	wg.Wait()
	DB.SetSyncRequired()
	items, err := DB.Query(testTxConcurrentTagsItemNs).MustExec(t).FetchAll()
	assert.NoError(t, err)
	assert.NotEqual(t, len(items), 0, "Empty items array")
}

func TestAsyncTxTimeout(t *testing.T) {
	tx1 := newTestTx(DB, testTxAsyncTimeoutItemNs)
	count1 := 1000
	FillTextTxItemAsync1Tx(t, count1, tx1)
	CheckTYx(t, testTxAsyncTimeoutItemNs, count1)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	tx2 := newTestTxCtx(ctx, DB, testTxAsyncTimeoutItemNs)
	hadSucceedRollback := false
	hadError := false
	count2 := 2000
	for i := 0; i < count2/10; i++ {
		for j := 0; j < 100; j++ {
			errAsync := tx2.UpsertAsync(&TextTxItem{
				ID:   i + (j+1)*1000,
				Name: strconv.Itoa(i + (j+1)*1000),
			}, func(err error) {
			})
			if errAsync != nil {
				hadError = true
				if !hadSucceedRollback {
					if ctx.Err() == nil {
						assert.Equal(t, errAsync, context.DeadlineExceeded)
					} else {
						assert.Equal(t, errAsync, ctx.Err())
					}
				}
				if tx2.Rollback() == nil {
					hadSucceedRollback = true
				}
			} else {
				assert.False(t, hadError)
			}
		}
		time.Sleep(time.Millisecond * 50)
	}
	_, err := tx2.Commit()
	assert.NotNil(t, ctx.Err())
	assert.True(t, hadSucceedRollback)
	rerr, ok := err.(bindings.Error)
	assert.True(t, ok)
	assert.Equal(t, rerr.Code(), reindexer.ErrCodeLogic)

	// Check, that there are no changes in namespace
	CheckTYx(t, testTxAsyncTimeoutItemNs, count1)
}
