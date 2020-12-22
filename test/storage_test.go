package reindexer

import (
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/restream/reindexer"
	"github.com/stretchr/testify/assert"
)

type SubStruct struct {
	F5 string `reindex:"f5"`
	F7 []int  `reindex:"f7"`
	F8 []int
}

type TestItemV1 struct {
	ID int `reindex:"id,,pk"`
	F1 int `reindex:"f1"`
	B1 bool
	F2 string `reindex:"f2"`
	T2 SubStruct
	F3 int `reindex:"f3"`
	T6 string
	T0 string
}

// Modified struct. Must pass test
type TestItemV2 struct {
	ID int    `reindex:"id,,pk"`
	F1 int    `reindex:"f1"`
	T1 string // new field
	//	T2 SubStruct - removed
	B1 bool   `reindex:"b1"`
	F2 string `reindex:"f2"`
	// F3 int `reindex:"f3"` - removed
	F4 int `reindex:"f4"` // new field
}

// Modified struct. Must pass test
type TestItemV3 struct {
	F1 int `reindex:"f1"` // changed order
	T2 SubStruct
	ID int `reindex:"id,,pk"` // pk position changed order
	F4 int `reindex:"f4"`     // new field
	F3 int `reindex:"f3"`
}

// Modified struct. Must raise error
type TestItemV4 struct {
	F1 int `reindex:"f1"`
	T1 string
	ID int    `reindex:"id,,pk"`
	F4 int    `reindex:"f4"`
	F3 string `reindex:"f3"` // changed type of indexed field. Can't convert int to string
}

// Modified struct. Must raise error
type TestItemV5 struct {
	F1 int `reindex:"f1"`
	T0 int // changed type of unindexed field. Can't convert string to int
	ID int `reindex:"id,,pk"`
	F4 int `reindex:"f4"`
}

// Modified struct. Must drop
type TestItemV6 struct {
	F1  int `reindex:"f1"`
	ID1 int `reindex:"id,,pk"` // Modified struct - pk field name changed
	F4  int `reindex:"f4"`
}

func init() {
	tnamespaces["test_items_storage"] = TestItemV1{}
}

func TestStorageChangeFormat(t *testing.T) {

	err := DB.Upsert(reindexer.ConfigNamespaceName, reindexer.DBConfigItem{
		Type:      "profiling",
		Profiling: &reindexer.DBProfilingConfig{MemStats: true},
	})

	if err != nil {
		panic(err)
	}

	tx := DB.MustBeginTx("test_items_storage")

	tx.Upsert(&TestItemV1{
		ID: 1,
		F1: 100,
		B1: true,
		F2: "f2val",
		F3: 300,
		T2: SubStruct{
			F5: "f5val",
			F7: []int{1, 2},
			F8: []int{7, 8},
		},
		T0: "t0val",
		T6: "t6val",
	})

	beforeUpdate := time.Now().UTC()

	tx.MustCommit()

	afterUpdate := time.Now().UTC()

	// Test2
	assert.NoError(t, DB.CloseNamespace("test_items_storage"))
	assert.NoError(t, DB.OpenNamespace("test_items_storage", reindexer.DefaultNamespaceOptions(), TestItemV2{}))

	stat, err := DB.GetNamespaceMemStat("test_items_storage")
	assert.NoError(t, err)

	updatedAt := time.Unix(0, stat.Replication.UpdatedUnixNano).UTC()
	assert.False(t, beforeUpdate.Before(updatedAt) && afterUpdate.After(updatedAt), "%v must be between %v and %v", updatedAt, beforeUpdate, afterUpdate)

	item, ok := DB.Query("test_items_storage").WhereInt("id", reindexer.EQ, 1).Get()

	if !ok {
		j, _ := DB.Query("test_items_storage").ExecToJson().FetchAll()
		fmt.Printf("%s", string(j))
		panic(fmt.Errorf("Not found test item after update fields struct"))
	}
	itemv2 := item.(*TestItemV2)

	if itemv2.ID != 1 || itemv2.F1 != 100 || itemv2.F2 != "f2val" || itemv2.B1 != true {
		j, _ := DB.Query("test_items_storage").ExecToJson().FetchAll()
		fmt.Printf("%s", string(j))
		panic(fmt.Errorf("%v", *itemv2))
	}

	// Test3
	assert.NoError(t, DB.CloseNamespace("test_items_storage"))
	assert.NoError(t, DB.OpenNamespace("test_items_storage", reindexer.DefaultNamespaceOptions(), TestItemV3{}))

	item, ok = DB.Query("test_items_storage").WhereInt("id", reindexer.EQ, 1).Get()

	assert.True(t, ok, "Not found test item after update fields struct")

	itemv3 := item.(*TestItemV3)

	assert.False(t, itemv3.ID != 1 || itemv3.F1 != 100 || itemv3.F3 != 300 || itemv3.T2.F5 != "f5val" ||
		itemv3.T2.F7[0] != 1 || itemv3.T2.F7[1] != 2 ||
		itemv3.T2.F8[0] != 7 || itemv3.T2.F8[1] != 8, "%v", *itemv2)

	// Test4
	assert.NoError(t, DB.CloseNamespace("test_items_storage"))
	assert.Error(t, DB.OpenNamespace("test_items_storage", reindexer.DefaultNamespaceOptions(), TestItemV4{}), "expecting storage error on index conflict, but it's ok")

	// Test5
	// Open namespace with different non indexed field type
	DB.CloseNamespace("test_items_storage")
	assert.NoError(t, DB.OpenNamespace("test_items_storage", reindexer.DefaultNamespaceOptions(), TestItemV5{}))

	iterator := DB.Query("test_items_storage").WhereInt("id", reindexer.EQ, 1).DeepReplEqual().Exec()
	assert.NoError(t, iterator.Error())
	assert.Equal(t, iterator.Count(), 1, "Expecting 1 item, found %d ", iterator.Count())
	iterator.Next()
	assert.Error(t, iterator.Error(), "expecting iterator error on wrong type cast, but it's ok")
	iterator.Close()
	// Test6
	assert.NoError(t, DB.CloseNamespace("test_items_storage"))
	assert.NoError(t, DB.OpenNamespace("test_items_storage", reindexer.DefaultNamespaceOptions().DropOnIndexesConflict(), TestItemV6{}))
	stat, err = DB.GetNamespaceMemStat("test_items_storage")
	assert.NoError(t, err)

	assert.Equal(t, int(stat.ItemsCount), 0, "expected 0 items in ns,found %d", stat.ItemsCount)

	udsn, err := url.Parse(*dsn)
	assert.NoError(t, err)

	if udsn.Scheme == "builtin" {
		// Test7
		// Try to create DB on exists file path - must fail
		ioutil.WriteFile(udsn.Path+"blocked_storage", []byte{}, os.ModePerm)
		assert.Error(t, DB.OpenNamespace("blocked_storage", reindexer.DefaultNamespaceOptions(), TestItemV1{}), "Expecting storage error, but it's ok")
	}
}
