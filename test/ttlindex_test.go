package reindexer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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

func TestBasicCheckItemsTtlExpired(t *testing.T) {
	tx := newTestTx(DB, "test_items_with_ttl")
	for i := 0; i < 1000; i++ {
		assert.NoError(t, tx.Upsert(newTestItemWithTtlObject(i, time.Now().Unix())))
	}

	time.Sleep(3)

	results, err := DB.Query("test_items_with_ttl").Exec(t).FetchAll()
	if err != nil {
		panic(err)
	}

	assert.Equal(t, len(results), 0, "Namespace should be empty!")

}
