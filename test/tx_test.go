package reindexer

import (
	"fmt"
	"strconv"
	"testing"
)

type TextTxItem struct {
	ID   int    `reindex:"id,,pk"`
	Name string `reindex:"name,text"`
}

func init() {
	tnamespaces["test_tx_item"] = TextTxItem{}

}

func FillTextTxItem1Tx(count int, tx *txTest) {
	for i := 0; i < count; i++ {
		if err := tx.Upsert(&TextTxItem{
			ID:   0 + i,
			Name: strconv.Itoa(i),
		}); err != nil {
			panic(err)
		}
	}
}

func FillTextTxFullItems(count int) {
	tx := newTestTx(DB, "test_tx_item")
	FillTextTxItem1Tx(count, tx)
	tx.MustCommit(nil)

}

func TestTx(t *testing.T) {
	FillTextTxFullItems(3)
	CheckTYx()
}

func CheckTYx() {

	q1 := DB.Query("test_tx_item")

	res, _ := q1.MustExec().FetchAll()
	for _, item := range res {
		some := item.(*TextTxItem)
		fmt.Printf("Name : %s , id: %d \n", some.Name, some.ID)

	}
}
