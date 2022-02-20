//go:build sharding_test
// +build sharding_test

package sharding

import (
	"flag"
	"os"
	"reflect"
	"strconv"
	"testing"

	"github.com/restream/reindexer"
	"github.com/stretchr/testify/assert"
)

var dsn = flag.String("dsn", "builtin://", "reindex db dsn")

func TestMain(m *testing.M) {
	flag.Parse()
	retCode := m.Run()
	os.Exit(retCode)

}

type TestItemSharding struct {
	ID       int `reindex:"id,hash,"`
	Data     string
	Location string   `reindex:"location,hash"`
	_        struct{} `reindex:"id+location,,composite,pk"`
}

func TestSharding(t *testing.T) {

	rx := reindexer.NewReindex(*dsn, reindexer.WithCreateDBIfMissing())
	defer rx.Close()

	const testNamespace = "ns"

	err := rx.OpenNamespace(testNamespace, reindexer.DefaultNamespaceOptions(), TestItemSharding{})
	assert.NoError(t, err, "Can't open namespace \"%s\" what=", testNamespace, err)

	index := 0
	itemsOnShard := 3
	shardCount := 3

	idSetBaseAll := map[int]TestItemSharding{}
	idSetBaseShard := make([]map[int]TestItemSharding, shardCount)

	for k := 0; k < shardCount; k++ {
		idSetBaseShard[k] = map[int]TestItemSharding{}
		for ; index < itemsOnShard+k*itemsOnShard; index++ {
			item := TestItemSharding{ID: index, Data: "data", Location: "key" + strconv.Itoa(k)}
			err = rx.Upsert(testNamespace, item)
			assert.NoError(t, err, "Can't Upsert data to namespace")
			idSetBaseAll[index] = item
			idSetBaseShard[k][index] = item
		}
	}

	check := func() {
		for m := 0; m < 2; m++ {
			{
				q := rx.Query(testNamespace)
				it := q.Exec()
				defer it.Close()
				it.AllowUnsafe(true)
				testData, err := it.FetchAll()
				assert.NoError(t, err, "Can't get data from namespace")
				idSetBaseOneShard := map[int]TestItemSharding{}
				for _, v := range testData {
					item, _ := v.(*TestItemSharding)
					idSetBaseOneShard[item.ID] = *item
				}
				eq := reflect.DeepEqual(idSetBaseAll, idSetBaseOneShard)
				assert.True(t, eq)
			}

			for i := 0; i < shardCount; i++ {
				q := rx.Query(testNamespace).WhereString("location", reindexer.EQ, "key"+strconv.Itoa(i))
				it := q.Exec()
				defer it.Close()
				it.AllowUnsafe(true)
				testData, err := it.FetchAll()
				assert.NoError(t, err, "Can't get data from namespace")
				idSetBaseOneShard := map[int]TestItemSharding{}
				for _, v := range testData {
					item, _ := v.(*TestItemSharding)
					idSetBaseOneShard[item.ID] = *item
				}
				eq := reflect.DeepEqual(idSetBaseShard[i], idSetBaseOneShard)
				assert.True(t, eq)
			}
		}
	}

	check()
	for k := 0; k < shardCount; k++ {
		itemToChange := 2 + k*itemsOnShard
		item := TestItemSharding{ID: itemToChange, Data: "datanew", Location: "key" + strconv.Itoa(k)}
		count, err := rx.Update(testNamespace, item)
		assert.NoError(t, err, "Can't Update data in namespace")
		assert.True(t, count == 1)
		idSetBaseAll[itemToChange] = item
		idSetBaseShard[k][itemToChange] = item

	}
	check()
	for k := 0; k < shardCount; k++ {
		itemToChange := 2 + k*itemsOnShard
		itemDel := TestItemSharding{ID: itemToChange, Data: "datanew", Location: "key" + strconv.Itoa(k)}
		err := rx.Delete(testNamespace, itemDel)
		assert.NoError(t, err, "Can't Delete data from namespace")
		itemIns := TestItemSharding{ID: itemToChange, Data: "datanew2", Location: "key" + strconv.Itoa(k)}
		count, err := rx.Insert(testNamespace, itemIns)
		assert.NoError(t, err, "Can't Insert data to namespace")
		assert.True(t, count == 1)
		idSetBaseAll[itemToChange] = itemIns
		idSetBaseShard[k][itemToChange] = itemIns
	}

	check()

}
