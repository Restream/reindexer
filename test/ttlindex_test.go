package reindexer

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type TestItemWithTtl struct {
	ID   int    `reindex:"id,,pk" json:"id"`
	Date int64  `reindex:"date,ttl,,expire_after=1" json:"date"`
	Data string `reindex:"data" json:"data"`
}

type ItemExpireTTL struct {
	ID        string `json:"id" reindex:"id,,pk"`
	CreatedAt int64  `json:"created_at" reindex:"created_at,ttl,,expire_after=200"`
}

type ItemExpireTTL2 struct {
	ID        string `json:"id" reindex:"id,,pk"`
	CreatedAt int64  `json:"created_at" reindex:"created_at,ttl,,expire_after=1"`
}

const (
	testItemsWithTTLNs = "test_items_with_ttl"
	testExpireTTLNs1   = "test_expire_ttl1"
	testExpireTTLNs2   = "test_expire_ttl2"
)

func init() {
	tnamespaces[testItemsWithTTLNs] = TestItemWithTtl{}
	tnamespaces[testExpireTTLNs1] = ItemExpireTTL{}
	tnamespaces[testExpireTTLNs2] = ItemExpireTTL2{}
}

func TestBasicCheckItemsTtlExpired(t *testing.T) {
	tx := newTestTx(DB, testItemsWithTTLNs)
	for i := 0; i < 1000; i++ {
		new_item := TestItemWithTtl{
			ID:   i,
			Date: time.Now().Unix(),
			Data: randString(),
		}
		assert.NoError(t, tx.Upsert(new_item))
	}
	time.Sleep(3)

	results, err := DB.Query(testItemsWithTTLNs).Exec(t).FetchAll()
	if err != nil {
		panic(err)
	}
	assert.Equal(t, len(results), 0, "Namespace should be empty!")
}

func TestExpireTTL(t *testing.T) {
	t.Run("Can insert index with expire_after", func(t *testing.T) {
		for index := 0; index < 10; index++ {
			err := DB.Upsert(testExpireTTLNs1, ItemExpireTTL{strconv.Itoa(index), time.Now().Unix()})
			assert.NoError(t, err, "Can't Upsert data to namespace")
		}
	})

	t.Run("Items are deleted after expire", func(t *testing.T) {
		countItems := 0
		for i := 1; i < 20; i++ {
			time.Sleep(1 * time.Second)
			q := DB.Query(testExpireTTLNs2)
			it := q.Exec(t)
			defer it.Close()
			countItems = it.Count()
			if countItems == 0 {
				break
			}
		}
		assert.Equal(t, 0, countItems)
	})

}
