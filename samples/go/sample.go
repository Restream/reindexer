package main

import "C"

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/restream/reindexer/v5"
	_ "github.com/restream/reindexer/v5/bindings/builtinserver"
	"github.com/restream/reindexer/v5/bindings/builtinserver/config"
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
	// Create server config with custom storage path
	cfg := config.DefaultServerConfig()
	cfg.Storage.Path = "path/to/rx_storage"
	// Initialize reindexer binding in builtinserver mode
	db, err := reindexer.NewReindex("builtinserver://rdx_test_db", reindexer.WithServerConfig(time.Second*100, cfg))
	// Check if DB was initialized correctly
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Create or open namespace with indexes and schema from struct TestItem
	err = db.OpenNamespace(NsName, reindexer.DefaultNamespaceOptions(), TestItem{})
	if err != nil {
		panic(err)
	}

	// Add some data to namespace
	err = db.Upsert(NsName, &TestItem{
		ID:       int(rand.Int() % 100),
		Genre:    int64(rand.Int() % 100),
		Name:     "SomeName " + string(rand.Int()%100),
		Location: "SomeLocation " + string(rand.Int()%100),
		Amount:   rand.Int() % 200}, "id=serial()")
	if err != nil {
		panic(err)
	}

	// Get data as JSON
	it := db.Query(NsName).ExecToJson()
	if it.Count() != 1 {
		panic("Unexpected items count")
	}
	// Iterate over select results
	for it.Next() {
		fmt.Println(string(it.JSON()))
	}

	// Delete namespace (also removes it's storage from disk)
	db.DropNamespace(NsName)
}
