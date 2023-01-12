package reindexer

import (
	"strconv"
	"testing"
	"time"

	"github.com/restream/reindexer/v3"
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

type ItemExpireTTL struct {
	ID        string `json:"id" reindex:"id,,pk"`
	CreatedAt int64  `json:"created_at" reindex:"created_at,ttl,,expire_after=100"`
}

type ItemExpireTTL2 struct {
	ID        string `json:"id" reindex:"id,,pk"`
	CreatedAt int64  `json:"created_at" reindex:"created_at,ttl,,expire_after=1"`
}

func TestExpireTTL(t *testing.T) {

	const testNamespace = "test_expire_ttl"

	err := DB.OpenNamespace(testNamespace, reindexer.DefaultNamespaceOptions(), ItemExpireTTL{})
	assert.NoError(t, err, "Can't open namespace \"%s\"", testNamespace)
	for index := 0; index < 10; index++ {
		err = DB.Upsert(testNamespace, ItemExpireTTL{strconv.Itoa(index), time.Now().Unix()})
		assert.NoError(t, err, "Can't Upsert data to namespace")
	}
	DB.CloseNamespace(testNamespace)
	err = DB.OpenNamespace(testNamespace, reindexer.DefaultNamespaceOptions(), ItemExpireTTL2{})
	assert.NoError(t, err)

	countItems := 0
	for i := 1; i < 10; i++ {
		time.Sleep(1 * time.Second)
		q := DB.Query(testNamespace)
		it := q.Exec(t)
		defer it.Close()
		countItems = it.Count()
		if countItems == 0 {
			break
		}
	}
	assert.Equal(t, 0, countItems)
}
