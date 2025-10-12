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
	Bla        string
	OneMoreTtl int64 `reindex:"one_more_ttl,ttl,,expire_after=10"`
}

type TestDescribeSubStruct struct {
	Name string `reindex:"name"`
	Time int    `reindex:"time,ttl,expire_after=100,is_no_column"`
}

type TestDescribeBuiltinSubStruct struct {
	Type     string `reindex:"type,,dense,is_no_column"`
	ExpireAt int64  `reindex:"expire_at,ttl,is_no_column,expire_after=999,dense"`
}

const testDescribeNS = "test_describe"

func init() {
	tnamespaces[testDescribeNS] = TestDescribeStruct{}

}

func TestDescribe(t *testing.T) {
	generalConds := []string{"SET", "ALLSET", "EQ", "LT", "LE", "GT", "GE", "RANGE"}
	arrayConds := []string{"SET", "ALLSET", "EQ", "ANY", "EMPTY"}
	fulltextConds := []string{"EQ", "SET"}

	testDescribeStruct := TestDescribeStruct{ID: 42}
	err := DB.Upsert(testDescribeNS, &testDescribeStruct)
	require.NoError(t, err)

	results, err := DB.ExecSQL("SELECT * FROM " + reindexer.NamespacesNamespaceName + " WHERE name='" + testDescribeNS + "'").FetchAll()
	require.NoError(t, err)
	require.Equal(t, 1, len(results))

	result := results[0]

	desc, ok := result.(*reindexer.NamespaceDescription)
	require.True(t, ok, "wait %T, got %T", reindexer.NamespaceDescription{}, result)
	require.Equal(t, testDescribeNS, desc.Name)
	require.Equal(t, 10, len(desc.Indexes))
	require.Equal(t, "id", desc.Indexes[0].Name)
	idx := desc.Indexes[0]
	require.True(t, idx.IsPK)
	require.Equal(t, reflect.Int64.String(), idx.FieldType)
	require.True(t, idx.IsSortable)
	require.Equal(t, generalConds, idx.Conditions)

	idx = desc.Indexes[1]
	require.Equal(t, "foo", idx.Name)
	require.False(t, idx.IsPK)
	require.Equal(t, reflect.String.String(), idx.FieldType)
	require.True(t, idx.IsSortable)
	require.Equal(t, generalConds, idx.Conditions)

	idx = desc.Indexes[2]
	require.Equal(t, "qwe", idx.Name)
	require.False(t, idx.IsPK)
	require.Equal(t, reflect.String.String(), idx.FieldType)
	require.True(t, idx.IsSortable)
	require.Equal(t, generalConds, idx.Conditions)

	idx = desc.Indexes[3]
	require.Equal(t, "bar", idx.Name)
	require.False(t, idx.IsPK)
	require.Equal(t, reflect.Int.String(), idx.FieldType)
	require.True(t, idx.IsArray)
	require.False(t, idx.IsSortable)
	require.Equal(t, arrayConds, idx.Conditions)

	idx = desc.Indexes[4]
	require.Equal(t, "search", desc.Indexes[4].Name)
	require.False(t, idx.IsPK)
	require.Equal(t, reflect.String.String(), idx.FieldType)
	require.False(t, idx.IsSortable)
	require.Equal(t, fulltextConds, idx.Conditions)

	idx = desc.Indexes[5]
	require.Equal(t, "name", idx.Name)
	require.False(t, idx.IsDense)
	require.False(t, idx.IsArray)
	require.False(t, idx.IsSparse)
	require.False(t, idx.IsNoColumn)
	require.Equal(t, 0, idx.ExpireAfter)

	idx = desc.Indexes[6]
	require.Equal(t, "time", idx.Name)
	require.False(t, idx.IsDense)
	require.False(t, idx.IsArray)
	require.False(t, idx.IsSparse)
	require.True(t, idx.IsNoColumn)
	require.Equal(t, 100, idx.ExpireAfter)

	idx = desc.Indexes[7]
	require.Equal(t, "type", idx.Name)
	require.True(t, idx.IsDense)
	require.False(t, idx.IsArray)
	require.False(t, idx.IsSparse)
	require.True(t, idx.IsNoColumn)
	require.Equal(t, 0, idx.ExpireAfter)

	idx = desc.Indexes[8]
	require.Equal(t, "expire_at", idx.Name)
	require.True(t, idx.IsDense)
	require.False(t, idx.IsArray)
	require.False(t, idx.IsSparse)
	require.True(t, idx.IsNoColumn)
	require.Equal(t, 999, idx.ExpireAfter)

	idx = desc.Indexes[9]
	require.Equal(t, "one_more_ttl", idx.Name)
	require.False(t, idx.IsDense)
	require.False(t, idx.IsArray)
	require.False(t, idx.IsSparse)
	require.False(t, idx.IsNoColumn)
	require.Equal(t, 10, idx.ExpireAfter)

	results, err = DB.ExecSQL("SELECT * FROM " + reindexer.NamespacesNamespaceName).FetchAll()
	require.NoError(t, err)

	ksystemNamespaceCount := 8
	require.Equal(t, len(tnamespaces)+ksystemNamespaceCount, len(results))
}
