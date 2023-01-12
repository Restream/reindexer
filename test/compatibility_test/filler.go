package main

import (
	"flag"
	"fmt"
	"math/rand"
	"strconv"

	"github.com/restream/reindexer/v3"
	_ "github.com/restream/reindexer/v3/bindings/cproto"
)

type testItem struct {
	ID       int    `reindex:"id,,pk"`
	Name     string `reindex:"name,tree"`
	Location string `reindex:"location"`
	Amount   int    `reindex:"amount,tree"`
}

const ns = "items"

func fillTxItems(db *reindexer.Reindexer, offset int, count int) int {
	tx := db.MustBeginTx(ns)
	for i := offset; i < count+offset; i++ {
		tx.UpsertAsync(&testItem{
			ID:       int(i),
			Name:     "SomeName " + strconv.Itoa(i),
			Location: "SomeLocation " + strconv.Itoa(rand.Intn(100)),
			Amount:   rand.Intn(200)}, func(err error) {
			if err != nil {
				fmt.Println("Error on upsert: ", err.Error())
			}
		})
	}
	return tx.MustCommit()
}

func main() {
	dsn := flag.String("dsn", "cproto://127.0.0.1:6534/testdb", "reindex db dsn")
	count := flag.Int("count", 100, "items count")
	offset := flag.Int("offset", 0, "items offset")
	flag.Parse()
	db := reindexer.NewReindex(*dsn, reindexer.WithCreateDBIfMissing())
	err := db.OpenNamespace(ns, reindexer.DefaultNamespaceOptions(), testItem{})
	if err != nil {
		panic(err)
	}
	actualCnt := fillTxItems(db, *offset, *count)
	if actualCnt != *count {
		panic(err)
	}
}
