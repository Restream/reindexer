package reindexer

import (
	"encoding/json"
	"log"
	"testing"

	"github.com/restream/reindexer/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type Account struct {
	Id    int64  `json:"id" reindex:"id,,pk"`
	SAN   string `json:"san" reindex:"san,hash"`
	Count string `json:"-" reindex:"count,hash"`
}

type Orders struct {
	OrderId         int64 `json:"-"`
	OrderOwnerFName string
	OrderTime       uint32 `reindex:"ordertime"`
	OrderCity       string `reindex:"ordercity,tree"`
}

type TestOpenNamespace struct {
	Id     int64 `json:"id" reindex:"id,,pk"`
	First  int   `reindex:"first,-" json:"first"`
	Age1   int64 `json:"-"`
	Age2   int64 `reindex:"-"`
	Field  int64
	Parent *Orders `reindex:"-"`

	privateSimple string   `json:"-"`
	privateSlice  string   `json:"-"`
	privateStruct struct{} `json:"-"`

	Accounts1 []Account `json:"-" reindex:"accounts,,joined"`
	Accounts2 []Account `json:"-"`
	Accounts3 []Account `reindex:"accounts,,joined"`

	Obj        []Orders `json:"Ord1,omitempty"`
	privateObj []Orders `json:"Ord2,omitempty"`

	_                 struct{} `json:"-" reindex:"id+first,,composite"`
	privateComposite1 struct{} `reindex:"first+id,,composite"`
}

type FailSimple struct {
	Id  int64  `json:"id" reindex:"id,,pk"`
	Age string `json:"-" reindex:"age,hash"`
}

type FailPrivate struct {
	Id      int64  `json:"id" reindex:"id,,pk"`
	private string `json:"private" reindex:"private,hash"`
}

type FailPrivateJoin struct {
	Id              int64     `json:"id" reindex:"id,,pk"`
	privateAccounts []Account `json:"-" reindex:"accounts,,joined"`
}

type FailJoinScalar struct {
	Id       int64  `json:"id" reindex:"id,,pk"`
	Accounts string `json:"-" reindex:"accounts,,joined"`
}

type FailJoinSingleStruct struct {
	Id       int64   `json:"id" reindex:"id,,pk"`
	Accounts Account `json:"-" reindex:"accounts,,joined"`
}

type FailComposite struct {
	Id        int64    `json:"id" reindex:"id,,pk"`
	Composite []Orders `json:"-" reindex:"ordertime+ordercity,,composite"`
}

type ExpectedIndexDef struct {
	Name      string
	JSONPaths []string
	IndexType string
	FieldType string
}

type testGenre struct {
	ID   int    `json:"id" reindex:"genres"`
	Name string `json:"name" reindex:"genres_names"`
}

type multiGenresInvalidItem struct {
	Genres    []testGenre `json:"genres"`
	MainGenre testGenre   `json:"main_genre"`
}

type testGenreRecursive struct {
	ID   int    `json:"id" reindex:"genres"`
	Name string `json:"name" reindex:"genres_names"`

	RecursiveGenre *testGenreRecursive `json:"recursive_genre"`
}

type multiGenresInvalidRecursiveItem struct {
	Genres    []testGenreRecursive `json:"genres"`
	MainGenre testGenreRecursive   `json:"main_genre"`
}

type testAppendableGenre struct {
	ID   int    `json:"id" reindex:"genres,,appendable"`
	Name string `json:"name" reindex:"genres_names,,appendable"`
}

type multiGenresItem struct {
	Genres    []testAppendableGenre `json:"genres"`
	MainGenre testAppendableGenre   `json:"main_genre"`
}

type testGenreRecursiveAppendable struct {
	ID   int    `json:"id" reindex:"genres,,appendable"`
	Name string `json:"name" reindex:"genres_names,,appendable"`

	RecursiveGenre *testGenreRecursiveAppendable `json:"recursive_genre"`
}

type multiGenresRecursiveItem struct {
	Genres    []testGenreRecursiveAppendable `json:"genres"`
	MainGenre testGenreRecursiveAppendable   `json:"main_genre"`
}

const (
	testNamespaceOpenNs        = "test_namespace_open"
	testNoNamespaceOpenNs      = "test_no_namespace_open"
	testMultigenresNs          = "test_multigenres_namespace"
	testMultigenresRecursiveNs = "test_multigenres_recursive_namespace"
)

func TestOpenNs(t *testing.T) {
	t.Parallel()

	validateIndexes := func(t *testing.T, ns string, expected []ExpectedIndexDef) {
		desc, err := DB.DescribeNamespace(ns)
		require.NoError(t, err)
		actual := make([]ExpectedIndexDef, 0)
		for _, idx := range desc.Indexes {
			actual = append(actual, ExpectedIndexDef{
				idx.IndexDef.Name,
				idx.IndexDef.JSONPaths,
				idx.IndexDef.IndexType,
				idx.IndexDef.FieldType,
			})
		}
		assert.Equal(t, expected, actual)
	}

	t.Run("open namespace: check indexes creation", func(t *testing.T) {
		if len(DB.slaveList) > 0 {
			t.Skip() // This test contains ns open/close and won't work with our replication testing logic
		}

		const ns = testNamespaceOpenNs
		err := DB.OpenNamespace(ns, reindexer.DefaultNamespaceOptions(), TestOpenNamespace{})
		defer DB.CloseNamespace(ns)
		require.NoError(t, err)

		validateIndexes(t, ns, []ExpectedIndexDef{
			{Name: "id", JSONPaths: []string{"id"}, IndexType: "hash", FieldType: "int64"},
			{Name: "first", JSONPaths: []string{"first"}, IndexType: "-", FieldType: "int64"},
			{Name: "ordertime", JSONPaths: []string{"Ord1.OrderTime"}, IndexType: "hash", FieldType: "int"},
			{Name: "ordercity", JSONPaths: []string{"Ord1.OrderCity"}, IndexType: "tree", FieldType: "string"},
			{Name: "id+first", JSONPaths: []string{"id", "first"}, IndexType: "hash", FieldType: "composite"},
			{Name: "first+id", JSONPaths: []string{"first", "id"}, IndexType: "hash", FieldType: "composite"},
		})
	})

	t.Run("no open namespace: check indexes are not created", func(t *testing.T) {
		const ns = testNoNamespaceOpenNs

		err := DB.OpenNamespace(ns, reindexer.DefaultNamespaceOptions(), FailSimple{})
		assert.ErrorContains(t, err,
			"non-composite/non-joined field ('Age'), marked with `json:-` can not have explicit reindex tags, but it does (reindex:\"age,hash\")")

		err = DB.OpenNamespace(ns, reindexer.DefaultNamespaceOptions(), FailPrivate{})
		assert.ErrorContains(t, err,
			"unexported non-composite field ('private') can not have reindex tags, but it does (reindex:\"private,hash\")")

		err = DB.OpenNamespace(ns, reindexer.DefaultNamespaceOptions(), FailPrivateJoin{})
		assert.ErrorContains(t, err,
			"unexported non-composite field ('privateAccounts') can not have reindex tags, but it does (reindex:\"accounts,,joined\")")

		err = DB.OpenNamespace(ns, reindexer.DefaultNamespaceOptions(), FailJoinScalar{})
		assert.ErrorContains(t, err,
			"joined index must be a slice of objects/pointers, but it is a scalar value")

		err = DB.OpenNamespace(ns, reindexer.DefaultNamespaceOptions(), FailJoinSingleStruct{})
		assert.ErrorContains(t, err,
			"joined index must be a slice of structs/pointers, but it is a single struct")

		err = DB.OpenNamespace(ns, reindexer.DefaultNamespaceOptions(), FailComposite{})
		assert.ErrorContains(t, err,
			"'composite' tag allowed only on empty on structs: Invalid tags '[ordertime+ordercity  composite]' on field 'Composite'")
	})

	t.Run("error on non-appendable jsonpaths", func(t *testing.T) {
		if len(DB.slaveList) > 0 {
			t.Skip() // This test contains ns open/close and won't work with our replication testing logic
		}

		DB.DropNamespace(testMultigenresNs)
		err := DB.OpenNamespace(testMultigenresNs, reindexer.DefaultNamespaceOptions(), multiGenresInvalidItem{})
		assert.ErrorContains(t, err, "index 'genres' is not appendable")

		err = DB.OpenNamespace(testMultigenresNs, reindexer.DefaultNamespaceOptions(), multiGenresInvalidRecursiveItem{})
		assert.ErrorContains(t, err, "index 'genres' is not appendable")
	})

	t.Run("appendable jsonpaths list", func(t *testing.T) {
		if len(DB.slaveList) > 0 {
			t.Skip() // This test contains ns open/close and won't work with our replication testing logic
		}

		ns := testMultigenresRecursiveNs
		DB.DropNamespace(ns)
		err := DB.OpenNamespace(ns, reindexer.DefaultNamespaceOptions(), multiGenresItem{})
		require.NoError(t, err)

		desc, err := DB.DescribeNamespace(ns)
		require.NoError(t, err)
		actual := make([]ExpectedIndexDef, 0)
		for _, idx := range desc.Indexes {
			actual = append(actual, ExpectedIndexDef{
				idx.IndexDef.Name,
				idx.IndexDef.JSONPaths,
				idx.IndexDef.IndexType,
				idx.IndexDef.FieldType,
			})
		}
		j, err := json.Marshal(actual)
		log.Println(string(j))
		require.NoError(t, err)

		validateIndexes(t, ns, []ExpectedIndexDef{
			{Name: "genres", JSONPaths: []string{"genres.id", "main_genre.id"}, IndexType: "hash", FieldType: "int64"},
			{Name: "genres_names", JSONPaths: []string{"genres.name", "main_genre.name"}, IndexType: "hash", FieldType: "string"},
		})

	})

	t.Run("appendable jsonpaths list with recusrion", func(t *testing.T) {
		if len(DB.slaveList) > 0 {
			t.Skip() // This test contains ns open/close and won't work with our replication testing logic
		}

		ns := testMultigenresRecursiveNs
		DB.DropNamespace(ns)
		err := DB.OpenNamespace(ns, reindexer.DefaultNamespaceOptions(), multiGenresRecursiveItem{})
		require.NoError(t, err)

		desc, err := DB.DescribeNamespace(ns)
		require.NoError(t, err)
		actual := make([]ExpectedIndexDef, 0)
		for _, idx := range desc.Indexes {
			actual = append(actual, ExpectedIndexDef{
				idx.IndexDef.Name,
				idx.IndexDef.JSONPaths,
				idx.IndexDef.IndexType,
				idx.IndexDef.FieldType,
			})
		}
		j, err := json.Marshal(actual)
		log.Println(string(j))
		require.NoError(t, err)
		validateIndexes(t, ns, []ExpectedIndexDef{
			{Name: "genres", JSONPaths: []string{"genres.id", "main_genre.id"}, IndexType: "hash", FieldType: "int64"},
			{Name: "genres_names", JSONPaths: []string{"genres.name", "main_genre.name"}, IndexType: "hash", FieldType: "string"},
		})
	})
}
