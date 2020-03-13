package reindexer

import (
	"testing"
)

type TestItemHuge struct {
	ID   int `reindex:"id,,pk"`
	Data []int
}

func init() {
	tnamespaces["test_items_huge"] = TestItemHuge{}
}

func FillTestItemHuge(start int, count int) {
	tx := newTestTx(DB, "test_items_huge")

	for i := 0; i < count; i++ {
		dataCount := i * 1024 * 16
		item := &TestItemHuge{
			ID:   mkID(start + i),
			Data: randIntArr(dataCount, 10000, 50),
		}

		if err := tx.Upsert(item); err != nil {
			panic(err)
		}
	}
	tx.MustCommit()
}

func TestItemsHuge(t *testing.T) {
	t.Parallel()

	// Fill items by cjson encoder
	FillTestItemHuge(0, 50)

	// get and decode all items by cjson decoder
	newTestQuery(DB, "test_items_huge").ExecAndVerify(t)

}
