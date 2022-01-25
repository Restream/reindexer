package main

import "C"

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/restream/reindexer"
	_ "github.com/restream/reindexer/bindings/builtinserver"
	"github.com/restream/reindexer/bindings/builtinserver/config"
)

type TestItem struct {
	ID       int    `reindex:"id,,pk"`
	Genre    int64  `reindex:"genre,tree,sparse"`
	Name     string `reindex:"name,tree"`
	Location string `reindex:"location"`
	Amount   int    `reindex:"amount,tree"`
}

func main() {
	const NsName = "test_items"
	cfg := config.DefaultServerConfig()
	cfg.Storage.Path = "rx_storage"
	db := reindexer.NewReindex("builtinserver://rdx_test_db", reindexer.WithServerConfig(time.Second*100, cfg))
	if db.Status().Err != nil {
		panic(db.Status().Err)
	}

	err := db.OpenNamespace(NsName, reindexer.DefaultNamespaceOptions(), TestItem{})
	if err != nil {
		panic(err)
	}

	err = db.Upsert(NsName, &TestItem{
		ID:       int(rand.Int() % 100),
		Genre:    int64(rand.Int() % 100),
		Name:     "SomeName " + string(rand.Int()%100),
		Location: "SomeLocation " + string(rand.Int()%100),
		Amount:   rand.Int() % 200}, "id=serial()")
	if err != nil {
		panic(err)
	}

	it := db.Query(NsName).ExecToJson()
	if it.Count() != 1 {
		panic("Unexpected items count")
	}
	for it.Next() {
		fmt.Println(string(it.JSON()))
	}

	db.DropNamespace(NsName)
}
