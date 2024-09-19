package reindexer

import (
	"testing"

	"github.com/restream/reindexer/v4"
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

func TestOpenNs(t *testing.T) {
	t.Parallel()

	t.Run("open namespase: check indexes creation", func(t *testing.T) {
		if len(DB.slaveList) > 0 {
			t.Skip() // This test contains ns open/close and won't work with our replication testing logic
		}

		const ns = "test_namespace_open"
		err := DB.OpenNamespace(ns, reindexer.DefaultNamespaceOptions(), TestOpenNamespace{})
		defer DB.CloseNamespace(ns)
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
		expected := []ExpectedIndexDef{
			{Name: "id", JSONPaths: []string{"id"}, IndexType: "hash", FieldType: "int64"},
			{Name: "first", JSONPaths: []string{"first"}, IndexType: "-", FieldType: "int64"},
			{Name: "ordertime", JSONPaths: []string{"Ord1.OrderTime"}, IndexType: "hash", FieldType: "int"},
			{Name: "ordercity", JSONPaths: []string{"Ord1.OrderCity"}, IndexType: "tree", FieldType: "string"},
			{Name: "id+first", JSONPaths: []string{"id", "first"}, IndexType: "hash", FieldType: "composite"},
			{Name: "first+id", JSONPaths: []string{"first", "id"}, IndexType: "hash", FieldType: "composite"},
		}
		assert.Equal(t, expected, actual)
	})

	t.Run("no open namespace: check indexes are not created", func(t *testing.T) {
		const ns = "test_no_namespace_open"

		err := DB.OpenNamespace(ns, reindexer.DefaultNamespaceOptions(), FailSimple{})
		assert.ErrorContains(t, err,
			"non-composite/non-joined field ('Age'), marked with `json:-` can not have explicit reindex tags, but it does ('age,hash')")

		err = DB.OpenNamespace(ns, reindexer.DefaultNamespaceOptions(), FailPrivate{})
		assert.ErrorContains(t, err,
			"unexported non-composite field ('private') can not have reindex tags, but it does ('private,hash')")

		err = DB.OpenNamespace(ns, reindexer.DefaultNamespaceOptions(), FailPrivateJoin{})
		assert.ErrorContains(t, err,
			"unexported non-composite field ('privateAccounts') can not have reindex tags, but it does ('accounts,,joined')")

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
}
