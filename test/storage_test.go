package reindexer

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net/url"
	"os"
	"path"
	"testing"
	"time"

	"github.com/restream/reindexer/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

// Item with regular indexes
type TestIndexesCompatibilityRegularItem struct {
	ID       int    `reindex:"id,,pk"`
	StrField string `reindex:"str_field"`
	IntField int    `reindex:"int_field,tree"`
}

// Item with dense indexes
type TestIndexesCompatibilityDenseItem struct {
	ID       int    `reindex:"id,,pk"`
	StrField string `reindex:"str_field,,dense"`
	IntField int    `reindex:"int_field,tree,dense,is_no_column"`
}

const (
	testWalNs                         = "test_wal"
	testItemsStorageNs                = "test_items_storage"
	testIndexesCompatibilityRegularNs = "indexes_compat_r_d"
	testIndexesCompatibilityDenseNs   = "indexes_compat_d_r"
)

func init() {
	tnamespaces[testItemsStorageNs] = TestItemV1{}
	tnamespaces[testWalNs] = TestItem{}
	tnamespaces[testIndexesCompatibilityRegularNs] = TestIndexesCompatibilityRegularItem{}
	tnamespaces[testIndexesCompatibilityDenseNs] = TestIndexesCompatibilityDenseItem{}
}

func getLastLsnCounter(t *testing.T, ns string) int64 {
	stat, err := DB.GetNamespaceMemStat(ns)
	require.NoError(t, err)
	return stat.Replication.LastLSN.Counter
}

func newTestIndexesCompatibilityRegularItem(id int) interface{} {
	return &TestIndexesCompatibilityRegularItem{
		ID:       1000000 + id,
		StrField: randString(),
		IntField: rand.Intn(100000),
	}
}

func newTestIndexesCompatibilityDenseItem(id int) interface{} {
	return &TestIndexesCompatibilityDenseItem{
		ID:       1000000 + id,
		StrField: randString(),
		IntField: rand.Intn(100000),
	}
}

func TestStorageChangeFormat(t *testing.T) {
	if len(DB.slaveList) > 0 {
		t.Skip()
	}

	const ns = testItemsStorageNs

	tx := DB.MustBeginTx(ns)
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

	t.Run("Test storage", func(t *testing.T) {
		assert.NoError(t, DB.CloseNamespace(ns))
		assert.NoError(t, DB.OpenNamespace(ns, reindexer.DefaultNamespaceOptions(), TestItemV2{}))

		stat, err := DB.GetNamespaceMemStat(ns)
		assert.NoError(t, err)
		updatedAt := time.Unix(0, stat.Replication.UpdatedUnixNano).UTC()
		assert.False(t, beforeUpdate.Before(updatedAt) && afterUpdate.After(updatedAt), "%v must be between %v and %v", updatedAt, beforeUpdate, afterUpdate)
		item, ok := DB.Query(ns).WhereInt("id", reindexer.EQ, 1).Get()
		if !ok {
			j, _ := DB.Query(ns).ExecToJson().FetchAll()
			fmt.Printf("%s", string(j))
			panic(fmt.Errorf("Not found test item after update fields struct"))
		}

		itemv2 := item.(*TestItemV2)
		if itemv2.ID != 1 || itemv2.F1 != 100 || itemv2.F2 != "f2val" || itemv2.B1 != true {
			j, _ := DB.Query(ns).ExecToJson().FetchAll()
			fmt.Printf("%s", string(j))
			panic(fmt.Errorf("%v", *itemv2))
		}

		// Check there are right items after update fields struct
		assert.NoError(t, DB.CloseNamespace(ns))
		assert.NoError(t, DB.OpenNamespace(ns, reindexer.DefaultNamespaceOptions(), TestItemV3{}))

		item, ok = DB.Query(ns).WhereInt("id", reindexer.EQ, 1).Get()
		assert.True(t, ok, "Not found test item after update fields struct")
		itemv3 := item.(*TestItemV3)
		assert.False(t, itemv3.ID != 1 || itemv3.F1 != 100 || itemv3.F3 != 300 || itemv3.T2.F5 != "f5val" ||
			itemv3.T2.F7[0] != 1 || itemv3.T2.F7[1] != 2 ||
			itemv3.T2.F8[0] != 7 || itemv3.T2.F8[1] != 8, "%v", *itemv2)
	})

	t.Run("Check storage error on index conflict", func(t *testing.T) {
		assert.NoError(t, DB.CloseNamespace(ns))
		assert.Error(t, DB.OpenNamespace(ns, reindexer.DefaultNamespaceOptions(), TestItemV4{}), "expected storage error on index conflict")
	})

	t.Run("Open namespace with different non indexed field type", func(t *testing.T) {
		DB.CloseNamespace(ns)
		assert.NoError(t, DB.OpenNamespace(ns, reindexer.DefaultNamespaceOptions(), TestItemV5{}))

		iterator := DB.Query(ns).WhereInt("id", reindexer.EQ, 1).DeepReplEqual().Exec(t)
		defer iterator.Close()
		assert.NoError(t, iterator.Error())
		assert.Equal(t, iterator.Count(), 1, "Expecting 1 item, found %d ", iterator.Count())
		iterator.Next()
		assert.Error(t, iterator.Error(), "expecting iterator error on wrong type cast, but it's ok")
	})

	t.Run("Check that NamespaceMemStat has 0 items after Open namespace", func(t *testing.T) {
		assert.NoError(t, DB.CloseNamespace(ns))
		assert.NoError(t, DB.OpenNamespace(ns, reindexer.DefaultNamespaceOptions().DropOnIndexesConflict(), TestItemV6{}))

		stat, err := DB.GetNamespaceMemStat(ns)
		assert.NoError(t, err)
		assert.Equal(t, int(stat.ItemsCount), 0, "expected 0 items in ns,found %d", stat.ItemsCount)
	})

	t.Run("Can't create DB on existing file path", func(t *testing.T) {
		udsn, err := url.Parse(*dsn)
		assert.NoError(t, err)
		if udsn.Scheme == "builtin" {
			os.WriteFile(path.Join(udsn.Path, "blocked_storage"), []byte{}, os.ModePerm)
			err = DB.OpenNamespace("blocked_storage", reindexer.DefaultNamespaceOptions(), TestItemV1{})
			assert.Errorf(t, err, "Expecting storage error, but it's ok (path: %s)", udsn.Path+"blocked_storage")
		}
	})
}

func TestWal(t *testing.T) {
	const ns = testWalNs

	t.Run("AddIndex method doesn't create new entries in WAL if index already exists", func(t *testing.T) {
		lastLsn0 := getLastLsnCounter(t, ns)
		index := reindexer.IndexDef{
			Name:      "new_index",
			JSONPaths: []string{"new_index"},
			IndexType: "hash",
			FieldType: "string",
		}
		// add new index - lsn counter increased
		err := DB.AddIndex(ns, index)
		require.NoError(t, err)
		lastLsn1 := getLastLsnCounter(t, ns)
		require.Equal(t, lastLsn0+1, lastLsn1)
		// try to add the same index - no lsn counter increase
		err = DB.AddIndex(ns, index)
		require.NoError(t, err)
		lastLsn2 := getLastLsnCounter(t, ns)
		require.Equal(t, lastLsn1, lastLsn2)
	})

	t.Run("UpdateIndex method doesn't create new entries in WAL if update to the same index", func(t *testing.T) {
		lastLsn0 := getLastLsnCounter(t, ns)
		index := reindexer.IndexDef{
			Name:      "name",
			JSONPaths: []string{"name"},
			IndexType: "hash",
			FieldType: "string",
		}
		// there is index update - lsn counter increased
		err := DB.UpdateIndex(ns, index)
		require.NoError(t, err)
		lastLsn1 := getLastLsnCounter(t, ns)
		require.Equal(t, lastLsn0+1, lastLsn1)
		// update to the same index - no lsn counter increase
		err = DB.UpdateIndex(ns, index)
		require.NoError(t, err)
		lastLsn2 := getLastLsnCounter(t, ns)
		require.Equal(t, lastLsn1, lastLsn2)
	})
}

func TestDenseIndexesCompatibility(t *testing.T) {
	getJSONContent := func(t *testing.T, ns string) []string {
		var ret []string
		it := DB.Query(ns).Sort("id", false).MustExec(t)
		defer it.Close()
		require.NoError(t, it.Error())
		for it.Next() {
			require.NoError(t, it.Error())
			j, err := json.Marshal(it.Object())
			require.NoError(t, err)
			ret = append(ret, string(j))
		}
		return ret
	}

	testImpl := func(t *testing.T, ns string, oldNewItem func(id int) interface{}, newNewItem func(id int) interface{}, newItemType interface{}) {
		const inserts = 100
		for i := 0; i < inserts; i++ {
			upd, err := DB.Insert(ns, oldNewItem(i))
			require.NoError(t, err)
			require.Equal(t, 1, upd)
		}
		initialJSONs := getJSONContent(t, ns)

		err := DB.CloseNamespace(ns)
		require.NoError(t, err)
		err = DB.OpenNamespace(ns, reindexer.DefaultNamespaceOptions(), newItemType)
		require.NoError(t, err)
		reopenedJSONs := getJSONContent(t, ns)
		require.Equal(t, initialJSONs, reopenedJSONs)
		require.Equal(t, len(reopenedJSONs), inserts)

		const offset = inserts / 2
		var newJSONs []string
		for i := offset; i < inserts+offset; i++ {
			item := newNewItem(i)
			j, err := json.Marshal(item)
			require.NoError(t, err)
			newJSONs = append(newJSONs, string(j))
			err = DB.Upsert(ns, item)
			require.NoError(t, err)
		}
		finalJSONs := getJSONContent(t, ns)
		require.Equal(t, initialJSONs[0:offset], finalJSONs[0:offset])
		require.Equal(t, newJSONs, finalJSONs[offset:])
	}

	t.Run("Binding is able to reopen namespace with dense indexes", func(t *testing.T) {
		testImpl(t, testIndexesCompatibilityRegularNs, newTestIndexesCompatibilityRegularItem,
			newTestIndexesCompatibilityDenseItem, TestIndexesCompatibilityDenseItem{})
	})

	t.Run("Binding is able to reopen namespace with regular indexes", func(t *testing.T) {
		testImpl(t, testIndexesCompatibilityDenseNs, newTestIndexesCompatibilityDenseItem,
			newTestIndexesCompatibilityRegularItem, TestIndexesCompatibilityRegularItem{})
	})
}
