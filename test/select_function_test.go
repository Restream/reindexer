package reindexer

import (
	"log"
	"testing"

	"git.itv.restr.im/itv-backend/reindexer"

	"fmt"
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
	CheckSelectItemsQueries()
}

func CheckSelectItemsQueries() {
	log.Printf("DO SELECT FUCNTIONS TESTS")

	first := randLangString()

	q1 := DB.Query("test_select_text_item").Where("name", reindexer.EQ, first).Functions("name.snippet(<b>,<b>,3,3)")

	res, _, er := q1.MustExec().FetchAllWithRank()

	if er != nil {
		panic(fmt.Errorf("query error:[%v;]", er))
	}

	for _, item := range res {
		switch item.(type) {
		case *TestSelectTextItem:

			_ = item.(*TestSelectTextItem).Name
		default:
			panic(fmt.Errorf("Unknown type after merge "))
		}
	}

}
