package reindexer

import (
	"reflect"
	"testing"

	"github.com/restream/reindexer/v5"
	"github.com/stretchr/testify/require"
)

type TestDescribeStruct struct {
	ID        int64   `reindex:"id,,pk"`
	Foo       string  `reindex:"foo,hash"`
	Qwe       string  `reindex:"qwe,tree"`
	Bar       []int32 `reindex:"bar"`
	Search    string  `reindex:"search,fuzzytext"`
	SubStruct TestDescribeSubStruct
	TestDescribeBuiltinSubStruct
	Bla string
}

type TestDescribeSubStruct struct {
	Name string `reindex:"name"`
}

type TestDescribeBuiltinSubStruct struct {
	Type string `reindex:"type"`
}

func init() {
	tnamespaces["test_describe"] = TestDescribeStruct{}

}

func TestDescribe(t *testing.T) {
	testDescribeStruct := TestDescribeStruct{ID: 42}
	generalConds := []string{"SET", "ALLSET", "EQ", "LT", "LE", "GT", "GE", "RANGE"}
	arrayConds := []string{"SET", "ALLSET", "EQ", "ANY", "EMPTY"}
	fulltextConds := []string{"EQ"}

	err := DB.Upsert("test_describe", &testDescribeStruct)
	require.NoError(t, err)

	results, err := DB.ExecSQL("SELECT * FROM " + reindexer.NamespacesNamespaceName + " WHERE name='test_describe'").FetchAll()
	require.NoError(t, err)
	require.Equal(t, 1, len(results))

	result := results[0]

	desc, ok := result.(*reindexer.NamespaceDescription)
	require.True(t, ok, "wait %T, got %T", reindexer.NamespaceDescription{}, result)
	require.Equal(t, "test_describe", desc.Name)
	require.Equal(t, 7, len(desc.Indexes))
	require.Equal(t, "id", desc.Indexes[0].Name)
	require.True(t, desc.Indexes[0].IsPK)
	require.Equal(t, reflect.Int64.String(), desc.Indexes[0].FieldType)
	require.True(t, desc.Indexes[0].IsSortable)
	require.Equal(t, generalConds, desc.Indexes[0].Conditions)

	require.Equal(t, "foo", desc.Indexes[1].Name)
	require.False(t, desc.Indexes[1].IsPK)
	require.Equal(t, reflect.String.String(), desc.Indexes[1].FieldType)
	require.True(t, desc.Indexes[1].IsSortable)
	require.Equal(t, generalConds, desc.Indexes[1].Conditions)

	require.Equal(t, "qwe", desc.Indexes[2].Name)
	require.False(t, desc.Indexes[2].IsPK)
	require.Equal(t, reflect.String.String(), desc.Indexes[2].FieldType)
	require.True(t, desc.Indexes[2].IsSortable)
	require.Equal(t, generalConds, desc.Indexes[2].Conditions)

	require.Equal(t, "bar", desc.Indexes[3].Name)
	require.False(t, desc.Indexes[3].IsPK)
	require.Equal(t, reflect.Int.String(), desc.Indexes[3].FieldType)
	require.True(t, desc.Indexes[3].IsArray)
	require.False(t, desc.Indexes[3].IsSortable)
	require.Equal(t, arrayConds, desc.Indexes[3].Conditions)

	require.Equal(t, "search", desc.Indexes[4].Name)
	require.False(t, desc.Indexes[4].IsPK)
	require.Equal(t, reflect.String.String(), desc.Indexes[4].FieldType)
	require.False(t, desc.Indexes[4].IsSortable)
	require.Equal(t, fulltextConds, desc.Indexes[4].Conditions)

	require.Equal(t, "name", desc.Indexes[5].Name)

	require.Equal(t, "type", desc.Indexes[6].Name)

	results, err = DB.ExecSQL("SELECT * FROM " + reindexer.NamespacesNamespaceName).FetchAll()
	require.NoError(t, err)

	ksystemNamespaceCount := 8
	require.Equal(t, len(tnamespaces)+ksystemNamespaceCount, len(results))
}
