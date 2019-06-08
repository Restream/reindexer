package reindexer

import (
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/restream/reindexer"
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

	setUpdatedAt := time.Now().Add(-time.Duration(100 * time.Second)).UTC()

	tx.MustCommit(&setUpdatedAt)

	// Test2
	if err := DB.CloseNamespace("test_items_storage"); err != nil {
		panic(err)
	}

	if err := DB.OpenNamespace("test_items_storage", reindexer.DefaultNamespaceOptions(), TestItemV2{}); err != nil {
		panic(err)
	}

	vrfUpdatedAt, err := DB.GetUpdatedAt("test_items_storage")

	if err != nil {
		panic(err)
	}
	if vrfUpdatedAt == nil {
		panic(fmt.Errorf("GetUpdatedAt = nil,expected %v", setUpdatedAt))
	}

	if setUpdatedAt != *vrfUpdatedAt {
		panic(fmt.Errorf("%v != %v", setUpdatedAt, *vrfUpdatedAt))
	}

	stat, err := DB.GetNamespaceMemStat("test_items_storage")

	if err != nil {
		panic(err)
	}

	vrfDescUpdatedAt := time.Unix(0, stat.UpdatedUnixNano).UTC()
	if setUpdatedAt.Round(time.Millisecond) != vrfDescUpdatedAt.Round(time.Millisecond) {
		panic(fmt.Errorf("%v != %v", setUpdatedAt, vrfDescUpdatedAt))
	}

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
	if err := DB.CloseNamespace("test_items_storage"); err != nil {
		panic(err)
	}
	if err := DB.OpenNamespace("test_items_storage", reindexer.DefaultNamespaceOptions(), TestItemV3{}); err != nil {
		panic(err)
	}

	item, ok = DB.Query("test_items_storage").WhereInt("id", reindexer.EQ, 1).Get()

	if !ok {
		panic(fmt.Errorf("Not found test item after update fields struct"))
	}
	itemv3 := item.(*TestItemV3)

	if itemv3.ID != 1 || itemv3.F1 != 100 || itemv3.F3 != 300 || itemv3.T2.F5 != "f5val" ||
		itemv3.T2.F7[0] != 1 || itemv3.T2.F7[1] != 2 ||
		itemv3.T2.F8[0] != 7 || itemv3.T2.F8[1] != 8 {
		panic(fmt.Errorf("%v", *itemv2))
	}

	// Test4
	if err := DB.CloseNamespace("test_items_storage"); err != nil {
		panic(err)
	}

	if err := DB.OpenNamespace("test_items_storage", reindexer.DefaultNamespaceOptions(), TestItemV4{}); err == nil {
		panic(fmt.Errorf("expecting storage error on index conflict, but it's ok"))
	}

	// Test5
	// Open namespace with different non indexed field type

	DB.CloseNamespace("test_items_storage")
	if err := DB.OpenNamespace("test_items_storage", reindexer.DefaultNamespaceOptions(), TestItemV5{}); err != nil {
		panic(err)
	}

	iterator := DB.Query("test_items_storage").WhereInt("id", reindexer.EQ, 1).DeepReplEqual().Exec()

	if err = iterator.Error(); err != nil {
		panic(err)
	}

	if iterator.Count() != 1 {
		panic(fmt.Errorf("Expecting 1 item, found %d ", iterator.Count()))
	}

	iterator.Next()

	if err = iterator.Error(); err == nil {
		panic(fmt.Errorf("expecting iterator error on wrong type cast, but it's ok"))
	}

	// Test6
	if err := DB.CloseNamespace("test_items_storage"); err != nil {
		panic(err)
	}

	if err := DB.OpenNamespace("test_items_storage", reindexer.DefaultNamespaceOptions().DropOnIndexesConflict(), TestItemV6{}); err != nil {
		panic(err)
	}

	stat, err = DB.GetNamespaceMemStat("test_items_storage")

	if err != nil {
		panic(err)
	}
	if stat.ItemsCount != 0 {
		panic(fmt.Errorf("expected 0 items in ns,found %d", stat.ItemsCount))
	}

	udsn, err := url.Parse(*dsn)
	if err != nil {
		panic(err)
	}
	if udsn.Scheme == "builtin" {
		// Test7
		// Try to create DB on exists file path - must fail
		ioutil.WriteFile(udsn.Path+"blocked_storage", []byte{}, os.ModePerm)
		err = DB.OpenNamespace("blocked_storage", reindexer.DefaultNamespaceOptions(), TestItemV1{})

		if err == nil {
			panic(fmt.Errorf("Expecting storage error, but it's ok"))
		}
	}
}
