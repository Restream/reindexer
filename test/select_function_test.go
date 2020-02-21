package reindexer

import (
	"testing"

	"github.com/restream/reindexer"
	"github.com/stretchr/testify/assert"
)

type TestSelectTextItem struct {
	ID   int    `reindex:"id,,pk"`
	Name string `reindex:"name,text"`
}

func init() {
	tnamespaces["test_select_text_item"] = TestSelectTextItem{}
}

func FillTestSelectTextItemsTx(count int, tx *txTest) {
	for i := 0; i < count; i++ {
		if err := tx.Upsert(&TestSelectTextItem{
			ID:   mkID(i),
			Name: randLangString(),
		}); err != nil {
			panic(err)
		}
	}
}
func FillTestSelectTextItems(count int) {
	tx := newTestTx(DB, "test_select_text_item")
	FillTestSelectTextItemsTx(count, tx)
	tx.MustCommit()
}

func TestSelectFunction(t *testing.T) {
	FillTestSelectTextItems(50)
	CheckSelectItemsQueries(t)
}

func CheckSelectItemsQueries(t *testing.T) {

	first := randLangString()

	q1 := DB.Query("test_select_text_item").Where("name", reindexer.EQ, first).Functions("name.snippet(<b>,<b>,3,3)")

	res, _, err := q1.MustExec().FetchAllWithRank()
	assert.NoError(t, err)

	for _, item := range res {
		_, ok := item.(*TestSelectTextItem)
		assert.True(t, ok, "Unknown type after merge ")
	}

}
