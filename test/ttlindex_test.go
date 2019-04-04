package reindexer

import (
	"fmt"
	"testing"
	"time"
)

type TestItemWithTtl struct {
	ID   int    `reindex:"id,,pk" json:"id"`
	Date int64  `reindex:"date,ttl,,expire_after=1" json:"date"`
	Data string `reindex:"data" json:"data"`
}

func init() {
	tnamespaces["test_items_with_ttl"] = TestItemWithTtl{}
}

func newTestItemWithTtlObject(id int, date int64) *TestItemWithTtl {
	return &TestItemWithTtl{
		ID:   id,
		Date: date,
		Data: randString(),
	}
}

func makeTestItemsReady() {
	tx := newTestTx(DB, "test_items_with_ttl")
	for i := 0; i < 1000; i++ {
		if err := tx.Upsert(newTestItemWithTtlObject(i, time.Now().Unix())); err != nil {
			panic(err)
		}
	}
}

func TestBasicCheckItemsTtlExpired(t *testing.T) {
	makeTestItemsReady()

	time.Sleep(3)

	results, err := DB.Query("test_items_with_ttl").Exec().FetchAll()
	if err != nil {
		panic(err)
	}

	if len(results) != 0 {
		panic(fmt.Errorf("Namespace should be empty!"))
	}
}
