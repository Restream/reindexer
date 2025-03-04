package reindexer

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/restream/reindexer/v5"
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

	if err := DB.Upsert("test_describe", &testDescribeStruct); err != nil {
		panic(err)
	}

	results, err := DB.ExecSQL("SELECT * FROM " + reindexer.NamespacesNamespaceName + " WHERE name='test_describe'").FetchAll()
	if err != nil {
		panic(err)
	}

	if len(results) != 1 {
		panic(fmt.Sprintf("wait []NamespaceDescription of length %d, got %d", 1, len(results)))
	}

	result := results[0]

	desc, ok := result.(*reindexer.NamespaceDescription)
	if !ok {
		panic(fmt.Sprintf("wait %T, got %T", reindexer.NamespaceDescription{}, result))
	}

	if desc.Name != "test_describe" {
		panic(fmt.Sprintf("wait %s, got %s", "test_describe", desc.Name))
	}

	if len(desc.Indexes) != 7 {
		panic(fmt.Sprintf("wait []IndexDescription of length %d, got %d", 7, len(desc.Indexes)))
	}

	if desc.Indexes[0].Name != "id" {
		panic(fmt.Sprintf("wait field's name '%s', got '%s'", "id", desc.Indexes[0].Name))
	}

	if !desc.Indexes[0].IsPK {
		panic(fmt.Sprintf("field 'id' must be PK %#v", desc.Indexes[0]))
	}

	if desc.Indexes[0].FieldType != reflect.Int64.String() {
		panic(fmt.Sprintf("wait field type %s, got %s", reflect.Int64, desc.Indexes[0].FieldType))
	}

	if desc.Indexes[1].Name != "foo" {
		panic(fmt.Sprintf("wait field's name '%s', got '%s'", "foo", desc.Indexes[1].Name))
	}

	if desc.Indexes[1].IsPK {
		panic("field 'foo' must not be PK")
	}

	if desc.Indexes[1].FieldType != reflect.String.String() {
		panic(fmt.Sprintf("wait field type %s, got %s", reflect.String, desc.Indexes[1].FieldType))
	}

	if desc.Indexes[2].Name != "qwe" {
		panic(fmt.Sprintf("wait field's name '%s', got '%s'", "qwe", desc.Indexes[2].Name))
	}

	if desc.Indexes[2].IsPK {
		panic("field 'qwe' must not be PK")
	}

	if desc.Indexes[2].FieldType != reflect.String.String() {
		panic(fmt.Sprintf("wait field type %s, got %s", reflect.String, desc.Indexes[2].FieldType))
	}

	if desc.Indexes[3].Name != "bar" {
		panic(fmt.Sprintf("wait field's name '%s', got '%s'", "bar", desc.Indexes[3].Name))
	}

	if desc.Indexes[3].IsPK {
		panic("field 'bar' must not be PK")
	}

	if desc.Indexes[3].FieldType != reflect.Int.String() {
		panic(fmt.Sprintf("wait field type %s, got %s", reflect.Int, desc.Indexes[3].FieldType))
	}

	if !desc.Indexes[3].IsArray {
		panic("wait field is array, got not")
	}

	if desc.Indexes[4].Name != "search" {
		panic(fmt.Sprintf("wait field's name '%s', got '%s'", "search", desc.Indexes[4].Name))
	}

	if desc.Indexes[4].IsPK {
		panic("field 'search' must not be PK")
	}

	if desc.Indexes[4].FieldType != reflect.String.String() {
		panic(fmt.Sprintf("wait field type %s, got %s", reflect.String, desc.Indexes[4].FieldType))
	}

	if desc.Indexes[5].Name != "name" {
		panic(fmt.Sprintf("wait field's name '%s', got '%s'", "name", desc.Indexes[5].Name))
	}

	if desc.Indexes[6].Name != "type" {
		panic(fmt.Sprintf("wait field's name '%s', got '%s'", "type", desc.Indexes[6].Name))
	}

	results, err = DB.ExecSQL("SELECT * FROM " + reindexer.NamespacesNamespaceName).FetchAll()
	if err != nil {
		panic(err)
	}

	ksystemNamespaceCount := 8
	if len(results) != len(tnamespaces)+ksystemNamespaceCount {
		panic(fmt.Sprintf("wait %d namespaces, got %d", len(tnamespaces)+ksystemNamespaceCount, len(results)))
	}
}
