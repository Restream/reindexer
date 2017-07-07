package reindexer

import (
	"fmt"
	"reflect"
)

type TestDescribeStruct struct {
	ID        int    `reindex:"id,,pk"`
	Foo       string `reindex:"foo,hash"`
	Qwe       string `reindex:"qwe,tree"`
	Bar       []int  `reindex:"bar"`
	Search    string `reindex:"search,fulltext"`
	SubStruct TestDescribeSubStruct
	TestDescribeBuiltinSubStruct
	Bla string `reindex:",,dict"`
}

type TestDescribeSubStruct struct {
	Name string `reindex:"name"`
}

type TestDescribeBuiltinSubStruct struct {
	Type string `reindex:"type"`
}

func CheckTestDescribeQuery() {
	if err := DB.CreateIndex("test_describe", TestDescribeStruct{}); err != nil {
		panic(err)
	}

	testDescribeStruct := TestDescribeStruct{ID: 42}

	if err := DB.Upsert("test_describe", &testDescribeStruct); err != nil {
		panic(err)
	}

	results, err := DB.Select("DESCRIBE test_describe").PtrSlice()
	if err != nil {
		panic(err)
	}

	if len(results) != 1 {
		panic(fmt.Sprintf("wait []NamespaceDescription of length %d, got %d", 1, len(results)))
	}

	result := results[0]

	desc, ok := result.(*NamespaceDescription)
	if !ok {
		panic(fmt.Sprintf("wait %T, got %T", NamespaceDescription{}, result))
	}

	if desc.Name != "test_describe" {
		panic(fmt.Sprintf("wait %s, got %s", "test_describe", desc.Name))
	}

	if len(desc.Indices) != 7 {
		panic(fmt.Sprintf("wait []IndexDescription of length %d, got %d", 7, len(desc.Indices)))
	}

	if desc.Indices[0].Name != "id" {
		panic(fmt.Sprintf("wait field's name '%s', got '%s'", "id", desc.Indices[0].Name))
	}

	if !desc.Indices[0].PK {
		panic(fmt.Sprint("field 'id' must be PK"))
	}

	if desc.Indices[0].Sortable {
		panic(fmt.Sprint("index must not be sortable"))
	}

	if desc.Indices[0].Fulltext {
		panic(fmt.Sprint("index must not be fulltext"))
	}

	if desc.Indices[0].FieldType != reflect.Int.String() {
		panic(fmt.Sprintf("wait field type %s, got %s", reflect.Int, desc.Indices[0].FieldType))
	}

	for i, cond := range desc.Indices[0].Conditions {
		if cond != condsByType(IndexIntHash)[i] {
			panic(fmt.Sprintf("wait conditions %s, got %s", condsByType(IndexInt), desc.Indices[0].Conditions))
		}
	}

	if desc.Indices[1].Name != "foo" {
		panic(fmt.Sprintf("wait field's name '%s', got '%s'", "foo", desc.Indices[1].Name))
	}

	if desc.Indices[1].PK {
		panic(fmt.Sprint("field 'foo' must not be PK"))
	}

	if desc.Indices[1].Sortable {
		panic(fmt.Sprint("index must not be sortable"))
	}

	if desc.Indices[1].Fulltext {
		panic(fmt.Sprint("index must not be fulltext"))
	}

	if desc.Indices[1].FieldType != reflect.String.String() {
		panic(fmt.Sprintf("wait field type %s, got %s", reflect.String, desc.Indices[1].FieldType))
	}

	for i, cond := range desc.Indices[1].Conditions {
		if cond != condsByType(IndexHash)[i] {
			panic(fmt.Sprintf("wait conditions %s, got %s", condsByType(IndexHash), desc.Indices[1].Conditions))
		}
	}

	if desc.Indices[2].Name != "qwe" {
		panic(fmt.Sprintf("wait field's name '%s', got '%s'", "qwe", desc.Indices[2].Name))
	}

	if desc.Indices[2].PK {
		panic(fmt.Sprint("field 'qwe' must not be PK"))
	}

	if !desc.Indices[2].Sortable {
		panic(fmt.Sprint("index must be sortable"))
	}

	if desc.Indices[2].Fulltext {
		panic(fmt.Sprint("index must not be fulltext"))
	}

	if desc.Indices[2].FieldType != reflect.String.String() {
		panic(fmt.Sprintf("wait field type %s, got %s", reflect.String, desc.Indices[2].FieldType))
	}

	for i, cond := range desc.Indices[2].Conditions {
		if cond != condsByType(IndexTree)[i] {
			panic(fmt.Sprintf("wait conditions %s, got %s", condsByType(IndexTree), desc.Indices[2].Conditions))
		}
	}

	if desc.Indices[3].Name != "bar" {
		panic(fmt.Sprintf("wait field's name '%s', got '%s'", "bar", desc.Indices[3].Name))
	}

	if desc.Indices[3].PK {
		panic(fmt.Sprint("field 'bar' must not be PK"))
	}

	if desc.Indices[3].Sortable {
		panic(fmt.Sprint("index must not be sortable"))
	}

	if desc.Indices[3].Fulltext {
		panic(fmt.Sprint("index must not be fulltext"))
	}

	if desc.Indices[3].FieldType != reflect.Int.String() {
		panic(fmt.Sprintf("wait field type %s, got %s", reflect.Int, desc.Indices[3].FieldType))
	}

	if !desc.Indices[3].IsArray {
		panic("wait field is array, got not")
	}

	for i, cond := range desc.Indices[3].Conditions {
		if cond != condsByType(IndexHash)[i] {
			panic(fmt.Sprintf("wait conditions %s, got %s", condsByType(IndexHash), desc.Indices[3].Conditions))
		}
	}

	if desc.Indices[4].Name != "search" {
		panic(fmt.Sprintf("wait field's name '%s', got '%s'", "search", desc.Indices[4].Name))
	}

	if desc.Indices[4].PK {
		panic(fmt.Sprint("field 'search' must not be PK"))
	}

	if desc.Indices[4].Sortable {
		panic(fmt.Sprint("index must not be sortable"))
	}

	if !desc.Indices[4].Fulltext {
		panic(fmt.Sprint("index must be fulltext"))
	}

	if desc.Indices[4].FieldType != reflect.String.String() {
		panic(fmt.Sprintf("wait field type %s, got %s", reflect.String, desc.Indices[4].FieldType))
	}

	for i, cond := range desc.Indices[4].Conditions {
		if cond != condsByType(IndexFullText)[i] {
			panic(fmt.Sprintf("wait conditions %s, got %s", condsByType(IndexFullText), desc.Indices[4].Conditions))
		}
	}

	if desc.Indices[5].Name != "name" {
		panic(fmt.Sprintf("wait field's name '%s', got '%s'", "name", desc.Indices[5].Name))
	}

	if desc.Indices[6].Name != "type" {
		panic(fmt.Sprintf("wait field's name '%s', got '%s'", "type", desc.Indices[6].Name))
	}

	results, err = DB.Select("DESCRIBE *").PtrSlice()
	if err != nil {
		panic(err)
	}

	if len(results) != 7 {
		panic(fmt.Sprintf("wait %d namespaces, got %d", 7, len(results)))
	}
}
