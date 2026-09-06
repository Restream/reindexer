package reindexer

import (
	"fmt"
	"math/rand"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/restream/reindexer/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestAllCombinationsStruct struct {
	ID          int       `reindex:"id,,pk"`
	StrHash     string    `reindex:"str_hash,hash"`
	StrTree     string    `reindex:"str_tree,tree"`
	StrStore    string    `reindex:"str_store,-"`
	StrText     string    `reindex:"str_text,text"`
	IntHash     int32     `reindex:"int_hash,hash"`
	IntTree     int32     `reindex:"int_tree,tree"`
	IntStore    int32     `reindex:"int_store,-"`
	Int64Hash   int64     `reindex:"int64_hash,hash"`
	Int64Tree   int64     `reindex:"int64_tree,tree"`
	Int64Ttl    int64     `reindex:"int64_ttl,ttl,,expire_after=999999999999"`
	Int64Store  int64     `reindex:"int64_store,-"`
	DoubleTree  float64   `reindex:"double_tree,tree"`
	DoubleStore float64   `reindex:"double_store,-"`
	BoolStore   bool      `reindex:"bool_store,-"`
	UuidHash    string    `reindex:"uuid_hash,hash,uuid"`
	UuidStore   string    `reindex:"uuid_store,-,uuid"`
	VecHnsw     []float32 `reindex:"vec_hnsw,hnsw,m=4,ef_construction=10,start_size=10,metric=l2,dimension=2"`
	VecBf       []float32 `reindex:"vec_bf,vec_bf,start_size=10,metric=inner_product,dimension=2"`
	VecIvf      []float32 `reindex:"vec_ivf,ivf,centroids_count=4,metric=cosine,dimension=2"`
	_           struct{}  `reindex:"int_hash+str_hash,hash,composite"`
	_           struct{}  `reindex:"double_tree+str_tree,tree,composite"`
	_           struct{}  `reindex:"str_text+str_hash,text,composite"`
}

type TestAllCombinationsArrayStruct struct {
	ID          int             `reindex:"id,,pk"`
	StrHash     []string        `reindex:"str_hash,hash"`
	StrTree     []string        `reindex:"str_tree,tree"`
	StrStore    []string        `reindex:"str_store,-"`
	StrText     []string        `reindex:"str_text,text"`
	IntHash     []int32         `reindex:"int_hash,hash"`
	IntTree     []int32         `reindex:"int_tree,tree"`
	IntStore    []int32         `reindex:"int_store,-"`
	Int64Hash   []int64         `reindex:"int64_hash,hash"`
	Int64Tree   []int64         `reindex:"int64_tree,tree"`
	Int64Ttl    []int64         `reindex:"int64_ttl,ttl,,expire_after=999999999999"`
	Int64Store  []int64         `reindex:"int64_store,-"`
	DoubleTree  []float64       `reindex:"double_tree,tree"`
	DoubleStore []float64       `reindex:"double_store,-"`
	BoolStore   []bool          `reindex:"bool_store,-"`
	UuidHash    []string        `reindex:"uuid_hash,hash,uuid"`
	UuidStore   []string        `reindex:"uuid_store,-,uuid"`
	Rtree       reindexer.Point `reindex:"rtree,rtree,rstar"`
	// TODO 2232
	// VecHnsw     [][]float32     `reindex:"vec_hnsw,hnsw,m=4,ef_construction=10,start_size=10,metric=l2,dimension=2"`
	// VecBf       [][]float32     `reindex:"vec_bf,vec_bf,start_size=10,metric=inner_product,dimension=2"`
	// VecIvf      [][]float32     `reindex:"vec_ivf,ivf,centroids_count=4,metric=cosine,dimension=2"`
}

type TestAllCombinationsSparseStruct struct {
	ID          int     `reindex:"id,,pk"`
	StrHash     string  `reindex:"str_hash,hash,sparse"`
	StrTree     string  `reindex:"str_tree,tree,sparse"`
	StrStore    string  `reindex:"str_store,-,sparse"`
	StrText     string  `reindex:"str_text,text,sparse"`
	IntHash     int32   `reindex:"int_hash,hash,sparse"`
	IntTree     int32   `reindex:"int_tree,tree,sparse"`
	IntStore    int32   `reindex:"int_store,-,sparse"`
	Int64Hash   int64   `reindex:"int64_hash,hash,sparse"`
	Int64Tree   int64   `reindex:"int64_tree,tree,sparse"`
	Int64Ttl    int64   `reindex:"int64_ttl,ttl,sparse,expire_after=999999999999"`
	Int64Store  int64   `reindex:"int64_store,-,sparse"`
	DoubleTree  float64 `reindex:"double_tree,tree,sparse"`
	DoubleStore float64 `reindex:"double_store,-,sparse"`
	BoolStore   bool    `reindex:"bool_store,-,sparse"`
}

type TestAllCombinationsSparseArrayStruct struct {
	ID          int             `reindex:"id,,pk"`
	StrHash     []string        `reindex:"str_hash,hash,sparse"`
	StrTree     []string        `reindex:"str_tree,tree,sparse"`
	StrStore    []string        `reindex:"str_store,-,sparse"`
	StrText     []string        `reindex:"str_text,text,sparse"`
	IntHash     []int32         `reindex:"int_hash,hash,sparse"`
	IntTree     []int32         `reindex:"int_tree,tree,sparse"`
	IntStore    []int32         `reindex:"int_store,-,sparse"`
	Int64Hash   []int64         `reindex:"int64_hash,hash,sparse"`
	Int64Tree   []int64         `reindex:"int64_tree,tree,sparse"`
	Int64Ttl    []int64         `reindex:"int64_ttl,ttl,sparse,expire_after=999999999999"`
	Int64Store  []int64         `reindex:"int64_store,-,sparse"`
	DoubleTree  []float64       `reindex:"double_tree,tree,sparse"`
	DoubleStore []float64       `reindex:"double_store,-,sparse"`
	BoolStore   []bool          `reindex:"bool_store,-,sparse"`
	Rtree       reindexer.Point `reindex:"rtree,rtree,rstar,sparse"`
}

const (
	testIndexDescriptionNS            = "test_index_description"
	testIndexDescriptionArrayNS       = "test_index_description_array"
	testIndexDescriptionSparseNS      = "test_index_description_sparse"
	testIndexDescriptionSparseArrayNS = "test_index_description_sparse_array"
)

var (
	generalConds          = []string{"SET", "ALLSET", "EQ", "LT", "LE", "GT", "GE", "RANGE"}
	generalCondsArray     = []string{"SET", "ALLSET", "EQ", "ANY", "EMPTY", "LT", "LE", "GT", "GE", "RANGE"}
	generalCondsSparse    = []string{"SET", "ALLSET", "EQ", "ANY", "EMPTY", "LT", "LE", "GT", "GE", "RANGE"}
	generalCondsStr       = []string{"SET", "ALLSET", "EQ", "LT", "LE", "GT", "GE", "RANGE", "LIKE"}
	generalCondsStrArray  = []string{"SET", "ALLSET", "EQ", "ANY", "EMPTY", "LT", "LE", "GT", "GE", "RANGE", "LIKE"}
	generalCondsStrSparse = []string{"SET", "ALLSET", "EQ", "ANY", "EMPTY", "LT", "LE", "GT", "GE", "RANGE", "LIKE"}
	fulltextConds         = []string{"EQ", "SET"}
	rtreeCond             = []string{"DWITHIN"}
	knnConds              = []string{"KNN", "ANY", "EMPTY"}
)

func init() {
	tnamespaces[testIndexDescriptionNS] = TestAllCombinationsStruct{}
	tnamespaces[testIndexDescriptionArrayNS] = TestAllCombinationsArrayStruct{}
	tnamespaces[testIndexDescriptionSparseNS] = TestAllCombinationsSparseStruct{}
	tnamespaces[testIndexDescriptionSparseArrayNS] = TestAllCombinationsSparseArrayStruct{}
}

func fillTestItemsAllCombinations(t *testing.T, ns string, count int) []TestAllCombinationsStruct {
	tx := newTestTx(DB, ns)
	items := []TestAllCombinationsStruct{}
	for i := 0; i < count; i++ {
		item := TestAllCombinationsStruct{
			ID:          i,
			StrHash:     randString(),
			StrTree:     randString(),
			StrStore:    randString(),
			StrText:     randString(),
			IntHash:     rand.Int31(),
			IntTree:     rand.Int31(),
			IntStore:    rand.Int31(),
			Int64Hash:   rand.Int63(),
			Int64Tree:   rand.Int63(),
			Int64Ttl:    rand.Int63(),
			Int64Store:  rand.Int63(),
			DoubleTree:  randFloat(-100, 100),
			DoubleStore: randFloat(-100, 100),
			BoolStore:   randBool(),
			UuidHash:    randUuid(),
			UuidStore:   randUuid(),
			VecHnsw:     randVect(2),
			VecBf:       randVect(2),
			VecIvf:      randVect(2),
		}
		err := tx.Upsert(item)
		require.NoError(t, err)
		items = append(items, item)
	}
	tx.MustCommit()
	return items
}

func fillTestItemsAllCombinationsArray(t *testing.T, ns string, count int) []TestAllCombinationsArrayStruct {
	tx := newTestTx(DB, ns)
	items := []TestAllCombinationsArrayStruct{}
	for i := 0; i < count; i++ {
		// random array min elements count = 1, max elements count = 3
		item := TestAllCombinationsArrayStruct{
			ID:          i,
			StrHash:     randStringArr(rand.Intn(3) + 1),
			StrTree:     randStringArr(rand.Intn(3) + 1),
			StrStore:    randStringArr(rand.Intn(3) + 1),
			StrText:     randStringArr(rand.Intn(3) + 1),
			IntHash:     randInt32Arr(rand.Intn(3)+1, 0, 1000),
			IntTree:     randInt32Arr(rand.Intn(3)+1, 0, 1000),
			IntStore:    randInt32Arr(rand.Intn(3)+1, 0, 1000),
			Int64Hash:   randInt64Arr(rand.Intn(3)+1, 0, 1000000),
			Int64Tree:   randInt64Arr(rand.Intn(3)+1, 0, 1000000),
			Int64Ttl:    randInt64Arr(rand.Intn(3)+1, 0, 1000000),
			Int64Store:  randInt64Arr(rand.Intn(3)+1, 0, 1000000),
			DoubleTree:  randFloat64Arr(rand.Intn(3)+1, -100, 100),
			DoubleStore: randFloat64Arr(rand.Intn(3)+1, -100, 100),
			BoolStore:   randBoolArr(rand.Intn(3) + 1),
			UuidHash:    randUuidArray(rand.Intn(3) + 1),
			UuidStore:   randUuidArray(rand.Intn(3) + 1),
			Rtree:       randPoint(),
			// TODO 2232
			// VecHnsw:     randVectArr(rand.Intn(3)+1, 2),
			// VecBf:       randVectArr(rand.Intn(3)+1, 2),
			// VecIvf:      randVectArr(rand.Intn(3)+1, 2),
		}
		err := tx.Upsert(item)
		require.NoError(t, err)
		items = append(items, item)
	}
	tx.MustCommit()
	return items
}

func fillTestItemsAllCombinationsSparse(t *testing.T, ns string, count int) []TestAllCombinationsSparseStruct {
	tx := newTestTx(DB, ns)
	items := []TestAllCombinationsSparseStruct{}
	for i := 0; i < count; i++ {
		item := TestAllCombinationsSparseStruct{
			ID:          i,
			StrHash:     randString(),
			StrTree:     randString(),
			StrStore:    randString(),
			StrText:     randString(),
			IntHash:     rand.Int31(),
			IntTree:     rand.Int31(),
			IntStore:    rand.Int31(),
			Int64Hash:   rand.Int63(),
			Int64Tree:   rand.Int63(),
			Int64Ttl:    rand.Int63(),
			Int64Store:  rand.Int63(),
			DoubleTree:  randFloat(-100, 100),
			DoubleStore: randFloat(-100, 100),
			BoolStore:   randBool(),
		}
		err := tx.Upsert(item)
		require.NoError(t, err)
		items = append(items, item)
	}
	tx.MustCommit()
	return items
}

func fillTestItemsAllCombinationsSparseArray(t *testing.T, ns string, count int) []TestAllCombinationsSparseArrayStruct {
	tx := newTestTx(DB, ns)

	items := []TestAllCombinationsSparseArrayStruct{}
	for i := 0; i < count; i++ {
		item := TestAllCombinationsSparseArrayStruct{
			ID:          i,
			StrHash:     randStringArr(rand.Intn(3) + 1),
			StrTree:     randStringArr(rand.Intn(3) + 1),
			StrStore:    randStringArr(rand.Intn(3) + 1),
			StrText:     randStringArr(rand.Intn(3) + 1),
			IntHash:     randInt32Arr(rand.Intn(3)+1, 0, 1000),
			IntTree:     randInt32Arr(rand.Intn(3)+1, 0, 1000),
			IntStore:    randInt32Arr(rand.Intn(3)+1, 0, 1000),
			Int64Hash:   randInt64Arr(rand.Intn(3)+1, 0, 1000000),
			Int64Tree:   randInt64Arr(rand.Intn(3)+1, 0, 1000000),
			Int64Ttl:    randInt64Arr(rand.Intn(3)+1, 0, 1000000),
			Int64Store:  randInt64Arr(rand.Intn(3)+1, 0, 1000000),
			DoubleTree:  randFloat64Arr(rand.Intn(3)+1, -100, 100),
			DoubleStore: randFloat64Arr(rand.Intn(3)+1, -100, 100),
			BoolStore:   randBoolArr(rand.Intn(3) + 1),
			Rtree:       randPoint(),
		}
		err := tx.Upsert(item)
		require.NoError(t, err)
		items = append(items, item)
	}
	tx.MustCommit()
	return items
}

func getIndexesDescription(t *testing.T, ns string) *reindexer.NamespaceDescription {
	resNamespaces, err := DB.ExecSQL("SELECT * FROM " + reindexer.NamespacesNamespaceName + " WHERE name='" + ns + "'").FetchAll()
	require.NoError(t, err)
	require.Equal(t, 1, len(resNamespaces))
	resNs := resNamespaces[0]
	desc, ok := resNs.(*reindexer.NamespaceDescription)
	require.True(t, ok, "wait %T, got %T", reindexer.NamespaceDescription{}, resNs)
	require.Equal(t, ns, desc.Name)
	return desc
}

func getIndexByName(t *testing.T, name string, indexMap map[string]reindexer.IndexDescription) reindexer.IndexDescription {
	idx, ok := indexMap[name]
	require.True(t, ok, "index %s not found", name)
	return idx
}

func getRandomItemValue(t *testing.T, fieldName string, items any) any {
	slice := reflect.ValueOf(items)
	require.True(t, slice.Kind() == reflect.Slice, "items must be a slice")
	randomItem := slice.Index(rand.Intn(slice.Len()))
	field := randomItem.FieldByName(fieldName)
	require.True(t, field.IsValid(), "Field %s not found", fieldName)
	return field.Interface()
}

func getQueryValues(t *testing.T, fieldName string, items any) (singleValue any, arrayValue []any) {
	value := getRandomItemValue(t, fieldName, items)
	v := reflect.ValueOf(value)
	isSlice := v.Kind() == reflect.Slice
	if isSlice {
		singleValue = GetRandomElementFromSlice(value)
		sliceLen := v.Len()
		arrayValue = make([]any, sliceLen)
		for i := 0; i < sliceLen; i++ {
			arrayValue[i] = v.Index(i).Interface()
		}
	} else {
		singleValue = value
		value2 := getRandomItemValue(t, fieldName, items)
		arrayValue = []any{value, value2}
	}
	return singleValue, arrayValue
}

func covertFieldType(fieldType string) string {
	if fieldType == "int64" {
		return "int"
	} else if fieldType == "uuid" {
		return "UUID"
	} else {
		return fieldType
	}
}

func generateStrValue(fieldType string) string {
	if fieldType == "int" {
		return fmt.Sprint(rand.Int31())
	} else if fieldType == "double" {
		return fmt.Sprint(rand.Int31())
	} else if fieldType == "bool" {
		return fmt.Sprint(randBool())
	} else if fieldType == "uuid" {
		return randUuid()
	} else {
		return randString()
	}
}

func getRangeValues(t *testing.T, singleValue any, arrayValue any) []any {
	v := reflect.ValueOf(arrayValue)
	if !(v.Kind() == reflect.Slice || v.Kind() == reflect.Array) {
		t.Fatalf("expected slice/array for RANGE, got %v", v.Kind())
	}

	var result []any
	for i := 0; i < v.Len(); i++ {
		result = append(result, v.Index(i).Interface())
	}

	switch len(result) {
	case 0:
		return []any{singleValue, singleValue}
	case 1:
		return []any{result[0], result[0]}
	case 2:
		return result
	default:
		return result[:2]
	}
}

func checkSortableComposite(t *testing.T, ns string, indexName string, fieldNames []string, items any) {
	desc := randBool()
	resItems, err := DB.Query(ns).Sort(indexName, desc).MustExec(t).FetchAll()
	require.NoError(t, err)

	itemsValue := reflect.ValueOf(items)
	require.Equal(t, itemsValue.Len(), len(resItems))
	require.Equal(t, reflect.Slice, itemsValue.Kind())

	sortedExpected := reflect.MakeSlice(itemsValue.Type(), itemsValue.Len(), itemsValue.Len())
	reflect.Copy(sortedExpected, itemsValue)

	sort.SliceStable(sortedExpected.Interface(), func(i, j int) bool {
		itemI := sortedExpected.Index(i)
		itemJ := sortedExpected.Index(j)

		for _, fieldName := range fieldNames {
			valI := itemI.FieldByName(fieldName)
			valJ := itemJ.FieldByName(fieldName)
			cmp := compareValues(t, valI, valJ)
			if cmp != 0 {
				return (cmp < 0) != desc
			}
		}
		return false
	})

	for i := 0; i < sortedExpected.Len(); i++ {
		expected := sortedExpected.Index(i)
		actual := resItems[i]

		for _, fieldName := range fieldNames {
			expectedVal := expected.FieldByName(fieldName).Interface()
			actualVal := reflect.ValueOf(actual).Elem().FieldByName(fieldName).Interface()
			assert.Equal(t, expectedVal, actualVal)
		}
	}
}

func checkSortable(t *testing.T, ns string, index reindexer.IndexDescription, items any) {
	require.True(t, index.IsSortable)
	checkSortableComposite(t, ns, index.Name, index.JSONPaths, items)
}

func checkSortableRtree(t *testing.T, ns string, index reindexer.IndexDescription, items any) {
	require.True(t, index.IsSortable)
	checkSortableComposite(t, ns, index.Name, index.JSONPaths, items)
	_, err := DBD.Query(ns).SortStPointDistance(index.Name, randPoint(), true).MustExec().FetchAll()
	require.NoError(t, err)
}

func checkNotSortable(t *testing.T, ns string, index reindexer.IndexDescription, errMsg string) {
	require.False(t, index.IsSortable)
	_, err := DBD.Query(ns).Sort(index.Name, randBool()).Exec().FetchAll()
	assert.ErrorContains(t, err, errMsg)
}

func checkErrNullConds(t *testing.T, ns string, indexName string) {
	_, err := DBD.Query(ns).Where(indexName, reindexer.ANY, nil).Exec().FetchAll()
	assert.ErrorContains(t, err, "The 'NOT NULL' condition is supported only by 'sparse' or 'array' indexes")
	_, err = DBD.Query(ns).Where(indexName, reindexer.EMPTY, nil).Exec().FetchAll()
	assert.ErrorContains(t, err, "The 'is NULL' condition is supported only by 'sparse' or 'array' indexes")
}

func checkErrCondLike(t *testing.T, ns string, indexName string, fieldType string, value string) {
	_, err := DBD.Query(ns).Where(indexName, reindexer.LIKE, value).Exec().FetchAll()
	assert.ErrorContains(t, err, fmt.Sprintf("Condition CondLike with type %s", fieldType))
}

func checkErrCondDwithin(t *testing.T, ns string, indexName string, fieldType string) {
	errPattern := fmt.Sprintf("(Condition CondDWithin with type %s)|(DWITHIN query on index '%s')", fieldType, regexp.QuoteMeta(indexName))
	_, err := DBD.Query(ns).Where(indexName, reindexer.DWITHIN, randFloat64Arr(3, 1, 10)).Exec().FetchAll()
	assert.Regexp(t, regexp.MustCompile(errPattern), err.Error())
	_, err = DBD.Query(ns).DWithin(indexName, randPoint(), randFloat(0, 2)).Exec().FetchAll()
	assert.Regexp(t, regexp.MustCompile(errPattern), err.Error())
}

func checkErrCondKnn(t *testing.T, ns string, indexName string) {
	bfSearchParams, err := reindexer.NewIndexBFSearchParam(reindexer.BaseKnnSearchParam{}.SetK(20))
	require.NoError(t, err)
	_, err = DBD.Query(ns).WhereKnn(indexName, randVect(2), bfSearchParams).Exec().FetchAll()
	assert.ErrorContains(t, err, fmt.Sprintf("KNN allowed only for float vector index; %s is not float vector index", indexName))
}

func checkFulltextConds(t *testing.T, ns string, index reindexer.IndexDescription, items any) {
	errorMsg := fmt.Sprintf("Full text index (%s) support only EQ or SET condition with 1 or 2 parameter", index.Name)
	_, err := DBD.Query(ns).Where(index.Name, reindexer.ANY, nil).Exec().FetchAll()
	assert.ErrorContains(t, err, errorMsg)
	_, err = DBD.Query(ns).Where(index.Name, reindexer.EMPTY, nil).Exec().FetchAll()
	assert.ErrorContains(t, err, errorMsg)
	_, err = DBD.Query(ns).Where(index.Name, reindexer.ALLSET, []string{"abc", "def"}).Exec().FetchAll()
	assert.ErrorContains(t, err, errorMsg)
	_, err = DBD.Query(ns).Where(index.Name, reindexer.RANGE, []int{1, 2}).Exec().FetchAll()
	assert.ErrorContains(t, err, errorMsg)
	_, err = DBD.Query(ns).Where(index.Name, reindexer.GT, 1).Exec().FetchAll()
	assert.ErrorContains(t, err, errorMsg)
	_, err = DBD.Query(ns).Where(index.Name, reindexer.GE, 1).Exec().FetchAll()
	assert.ErrorContains(t, err, errorMsg)
	_, err = DBD.Query(ns).Where(index.Name, reindexer.LT, 1).Exec().FetchAll()
	assert.ErrorContains(t, err, errorMsg)
	_, err = DBD.Query(ns).Where(index.Name, reindexer.LE, 1).Exec().FetchAll()
	assert.ErrorContains(t, err, errorMsg)
	_, err = DBD.Query(ns).Where(index.Name, reindexer.LIKE, "abc").Exec().FetchAll()
	assert.ErrorContains(t, err, errorMsg)
	_, err = DBD.Query(ns).Where(index.Name, reindexer.DWITHIN, randFloat(0, 2)).Exec().FetchAll()
	assert.ErrorContains(t, err, "Expected point and distance for DWithin")
	_, err = DBD.Query(ns).DWithin(index.Name, randPoint(), randFloat(0, 2)).Exec().FetchAll()
	assert.ErrorContains(t, err, errorMsg)
	bfSearchParams, _ := reindexer.NewIndexBFSearchParam(reindexer.BaseKnnSearchParam{}.SetK(10))
	_, err = DBD.Query(ns).WhereKnn(index.Name, randVect(2), bfSearchParams).Exec().FetchAll()
	assert.ErrorContains(t, err, fmt.Sprintf("KNN allowed only for float vector index; %s is not float vector index", index.Name))

	singleValue, _ := getQueryValues(t, index.JSONPaths[0], items)
	_, err = DBD.Query(ns).Where(index.Name, reindexer.EQ, singleValue).MustExec().FetchAll()
	require.NoError(t, err)
	_, err = DBD.Query(ns).Where(index.Name, reindexer.SET, singleValue).MustExec().FetchAll()
	require.NoError(t, err)
}

func checkGeneralConds(t *testing.T, ns string, index reindexer.IndexDescription, items any) {
	fieldType := covertFieldType(index.FieldType)
	singleValue, arrayValue := getQueryValues(t, index.JSONPaths[0], items)

	if !(index.IsArray || index.IsSparse) {
		checkErrNullConds(t, ns, index.Name)
	} else {
		DB.Query(ns).Where(index.Name, reindexer.ANY, nil).SelectAllFields().ExecAndVerify(t)
		DB.Query(ns).Where(index.Name, reindexer.EMPTY, nil).SelectAllFields().ExecAndVerify(t)
	}
	if fieldType != "string" {
		checkErrCondLike(t, ns, index.Name, fieldType, generateStrValue(strings.ToLower(fieldType)))
	} else {
		DB.Query(ns).Where(index.Name, reindexer.LIKE, singleValue).SelectAllFields().ExecAndVerify(t)
	}
	checkErrCondDwithin(t, ns, index.Name, fieldType)
	checkErrCondKnn(t, ns, index.Name)

	DB.Query(ns).Where(index.Name, reindexer.EQ, singleValue).SelectAllFields().ExecAndVerify(t)
	DB.Query(ns).Where(index.Name, reindexer.GT, singleValue).SelectAllFields().ExecAndVerify(t)
	DB.Query(ns).Where(index.Name, reindexer.GE, singleValue).SelectAllFields().ExecAndVerify(t)
	DB.Query(ns).Where(index.Name, reindexer.LT, singleValue).SelectAllFields().ExecAndVerify(t)
	DB.Query(ns).Where(index.Name, reindexer.LE, singleValue).SelectAllFields().ExecAndVerify(t)
	DB.Query(ns).Where(index.Name, reindexer.SET, arrayValue).SelectAllFields().ExecAndVerify(t)
	DB.Query(ns).Where(index.Name, reindexer.ALLSET, arrayValue).SelectAllFields().ExecAndVerify(t)
	rangeValues := getRangeValues(t, singleValue, arrayValue)
	DB.Query(ns).Where(index.Name, reindexer.RANGE, rangeValues).SelectAllFields().ExecAndVerify(t)
}

func checkPointConds(t *testing.T, ns string, index reindexer.IndexDescription) {
	errorMsg := "Only DWithin condition is available for RTree index"
	_, err := DBD.Query(ns).Where(index.Name, reindexer.ANY, nil).Exec().FetchAll()
	assert.ErrorContains(t, err, errorMsg)
	_, err = DBD.Query(ns).Where(index.Name, reindexer.EMPTY, nil).Exec().FetchAll()
	assert.ErrorContains(t, err, errorMsg)
	_, err = DBD.Query(ns).Where(index.Name, reindexer.EQ, 1).Exec().FetchAll()
	assert.ErrorContains(t, err, errorMsg)
	_, err = DBD.Query(ns).Where(index.Name, reindexer.SET, []int{1, 2}).Exec().FetchAll()
	assert.ErrorContains(t, err, errorMsg)
	_, err = DBD.Query(ns).Where(index.Name, reindexer.ALLSET, []int{1, 2}).Exec().FetchAll()
	assert.ErrorContains(t, err, errorMsg)
	_, err = DBD.Query(ns).Where(index.Name, reindexer.RANGE, []int{1, 2}).Exec().FetchAll()
	assert.ErrorContains(t, err, errorMsg)
	_, err = DBD.Query(ns).Where(index.Name, reindexer.GT, 1).Exec().FetchAll()
	assert.ErrorContains(t, err, errorMsg)
	_, err = DBD.Query(ns).Where(index.Name, reindexer.GE, 1).Exec().FetchAll()
	assert.ErrorContains(t, err, errorMsg)
	_, err = DBD.Query(ns).Where(index.Name, reindexer.LT, 1).Exec().FetchAll()
	assert.ErrorContains(t, err, errorMsg)
	_, err = DBD.Query(ns).Where(index.Name, reindexer.LE, 1).Exec().FetchAll()
	assert.ErrorContains(t, err, errorMsg)
	_, err = DBD.Query(ns).Where(index.Name, reindexer.LIKE, "1").Exec().FetchAll()
	assert.ErrorContains(t, err, errorMsg)
	checkErrCondKnn(t, ns, index.Name)

	DB.Query(ns).DWithin(index.Name, randPoint(), randFloat(1, 5)).SelectAllFields().ExecAndVerify(t)
}

func checkVectorConds(t *testing.T, ns string, index reindexer.IndexDescription) {
	errorMsg := "Valid conditions for float vector index are KNN, Empty, Any; attempt to use '%s' on field '%s'"
	_, err := DBD.Query(ns).Where(index.Name, reindexer.EQ, 1).Exec().FetchAll()
	assert.ErrorContains(t, err, fmt.Sprintf(errorMsg, "=", index.Name))
	_, err = DBD.Query(ns).Where(index.Name, reindexer.SET, []int{1, 2}).Exec().FetchAll()
	assert.ErrorContains(t, err, fmt.Sprintf(errorMsg, "IN", index.Name))
	_, err = DBD.Query(ns).Where(index.Name, reindexer.ALLSET, []int{1, 2}).Exec().FetchAll()
	assert.ErrorContains(t, err, fmt.Sprintf(errorMsg, "ALLSET", index.Name))
	_, err = DBD.Query(ns).Where(index.Name, reindexer.RANGE, []int{1, 2}).Exec().FetchAll()
	assert.ErrorContains(t, err, fmt.Sprintf(errorMsg, "RANGE", index.Name))
	_, err = DBD.Query(ns).Where(index.Name, reindexer.GT, 1).Exec().FetchAll()
	assert.ErrorContains(t, err, fmt.Sprintf(errorMsg, ">", index.Name))
	_, err = DBD.Query(ns).Where(index.Name, reindexer.GE, 1).Exec().FetchAll()
	assert.ErrorContains(t, err, fmt.Sprintf(errorMsg, ">=", index.Name))
	_, err = DBD.Query(ns).Where(index.Name, reindexer.LT, 1).Exec().FetchAll()
	assert.ErrorContains(t, err, fmt.Sprintf(errorMsg, "<", index.Name))
	_, err = DBD.Query(ns).Where(index.Name, reindexer.LE, 1).Exec().FetchAll()
	assert.ErrorContains(t, err, fmt.Sprintf(errorMsg, "<=", index.Name))
	_, err = DBD.Query(ns).Where(index.Name, reindexer.LIKE, "1").Exec().FetchAll()
	assert.ErrorContains(t, err, fmt.Sprintf(errorMsg, "LIKE", index.Name))
	_, err = DBD.Query(ns).Where(index.Name, reindexer.DWITHIN, randFloat64Arr(3, 1, 10)).Exec().FetchAll()
	assert.ErrorContains(t, err, fmt.Sprintf(errorMsg, "DWITHIN", index.Name))
	_, err = DBD.Query(ns).DWithin(index.Name, randPoint(), randFloat(0, 2)).Exec().FetchAll()
	assert.ErrorContains(t, err, fmt.Sprintf(errorMsg, "DWITHIN", index.Name))

	var params reindexer.KnnSearchParam
	switch index.IndexType {
	case "hnsw":
		params, err = reindexer.NewIndexHnswSearchParam(20, reindexer.BaseKnnSearchParam{}.SetK(20))
	case "ivf":
		params, err = reindexer.NewIndexIvfSearchParam(10, reindexer.BaseKnnSearchParam{}.SetK(20))
	default:
		params, err = reindexer.NewIndexBFSearchParam(reindexer.BaseKnnSearchParam{}.SetK(20))
	}
	require.NoError(t, err)
	res, err := DBD.Query(ns).WhereKnn(index.Name, randVect(2), params).SelectAllFields().MustExec().FetchAll()
	require.NoError(t, err)
	assert.NotNil(t, res)
}

func checkCompositeGeneralConds(t *testing.T, ns string, index reindexer.IndexDescription, fieldNames []string, items any) {
	checkErrNullConds(t, ns, index.Name)
	_, err := DBD.Query(ns).Where(index.Name, reindexer.LIKE, "1").Exec().FetchAll()
	assert.ErrorContains(t, err, "Invalid count of arguments for composite index, expected 2, got 1")
	_, err = DBD.Query(ns).Where(index.Name, reindexer.LIKE, []string{"1", "2"}).Exec().FetchAll()
	assert.ErrorContains(t, err, "Condition CondLike must have exact 1 argument, but 2 arguments were provided")
	checkErrCondDwithin(t, ns, index.Name, "composite")
	checkErrCondKnn(t, ns, index.Name)

	singleValue1, arrayValue1 := getQueryValues(t, fieldNames[0], items)
	singleValue2, arrayValue2 := getQueryValues(t, fieldNames[1], items)
	compositeValueSingle := []any{[]any{singleValue1, singleValue2}}
	compositeValueArray := []any{
		[]any{arrayValue1[0], arrayValue2[1]},
		[]any{arrayValue1[1], arrayValue2[1]},
	}
	DB.Query(ns).Where(index.Name, reindexer.EQ, compositeValueSingle).SelectAllFields().ExecAndVerify(t)
	DB.Query(ns).Where(index.Name, reindexer.GT, compositeValueSingle).SelectAllFields().ExecAndVerify(t)
	DB.Query(ns).Where(index.Name, reindexer.GE, compositeValueSingle).SelectAllFields().ExecAndVerify(t)
	DB.Query(ns).Where(index.Name, reindexer.LT, compositeValueSingle).SelectAllFields().ExecAndVerify(t)
	DB.Query(ns).Where(index.Name, reindexer.LE, compositeValueSingle).SelectAllFields().ExecAndVerify(t)
	DB.Query(ns).Where(index.Name, reindexer.SET, compositeValueArray).SelectAllFields().ExecAndVerify(t)
	DB.Query(ns).Where(index.Name, reindexer.ALLSET, compositeValueArray).SelectAllFields().ExecAndVerify(t)
	DB.Query(ns).Where(index.Name, reindexer.RANGE, compositeValueArray).SelectAllFields().ExecAndVerify(t)
}

func checkCompositeFulltextConds(t *testing.T, ns string, index reindexer.IndexDescription, items any) {
	checkFulltextConds(t, ns, index, items)
}

func TestIndexDescription(t *testing.T) {
	t.Parallel()

	const ns = testIndexDescriptionNS
	items := fillTestItemsAllCombinations(t, ns, 20)

	desc := getIndexesDescription(t, ns)
	randomItem := items[rand.Intn(len(items))]
	require.Equal(t, reflect.TypeOf(randomItem).NumField(), len(desc.Indexes))

	indexMap := make(map[string]reindexer.IndexDescription)
	for i := range desc.Indexes {
		indexMap[desc.Indexes[i].Name] = desc.Indexes[i]
	}

	t.Run("check index description: int hash PK", func(t *testing.T) {
		idx := desc.Indexes[0]
		require.Equal(t, "id", idx.Name)
		require.True(t, idx.IsPK)
		require.Equal(t, reflect.Int64.String(), idx.FieldType)
		require.True(t, idx.IsSortable)
		require.Equal(t, generalConds, idx.Conditions)
	})

	t.Run("check index description: str hash", func(t *testing.T) {
		idx := getIndexByName(t, "str_hash", indexMap)
		checkSortable(t, ns, idx, items)
		require.Equal(t, generalCondsStr, idx.Conditions)
		checkGeneralConds(t, ns, idx, items)
	})

	t.Run("check index description: str tree", func(t *testing.T) {
		idx := getIndexByName(t, "str_tree", indexMap)
		checkSortable(t, ns, idx, items)
		require.Equal(t, generalCondsStr, idx.Conditions)
		checkGeneralConds(t, ns, idx, items)
	})

	t.Run("check index description: str store", func(t *testing.T) {
		idx := getIndexByName(t, "str_store", indexMap)
		checkSortable(t, ns, idx, items)
		require.Equal(t, generalCondsStr, idx.Conditions)
		checkGeneralConds(t, ns, idx, items)
	})

	t.Run("check index description: str text", func(t *testing.T) {
		idx := getIndexByName(t, "str_text", indexMap)
		checkSortable(t, ns, idx, items)
		require.Equal(t, fulltextConds, idx.Conditions)
		checkFulltextConds(t, ns, idx, items)
	})

	t.Run("check index description: int hash", func(t *testing.T) {
		idx := getIndexByName(t, "int_hash", indexMap)
		checkSortable(t, ns, idx, items)
		require.Equal(t, generalConds, idx.Conditions)
		checkGeneralConds(t, ns, idx, items)
	})

	t.Run("check index description: int tree", func(t *testing.T) {
		idx := getIndexByName(t, "int_tree", indexMap)
		checkSortable(t, ns, idx, items)
		require.Equal(t, generalConds, idx.Conditions)
		checkGeneralConds(t, ns, idx, items)
	})

	t.Run("check index description: int store", func(t *testing.T) {
		idx := getIndexByName(t, "int_store", indexMap)
		checkSortable(t, ns, idx, items)
		require.Equal(t, generalConds, idx.Conditions)
		checkGeneralConds(t, ns, idx, items)
	})

	t.Run("check index description: int64 hash", func(t *testing.T) {
		idx := getIndexByName(t, "int64_hash", indexMap)
		checkSortable(t, ns, idx, items)
		require.Equal(t, generalConds, idx.Conditions)
		checkGeneralConds(t, ns, idx, items)
	})

	t.Run("check index description: int64 tree", func(t *testing.T) {
		idx := getIndexByName(t, "int64_tree", indexMap)
		checkSortable(t, ns, idx, items)
		require.Equal(t, generalConds, idx.Conditions)
		checkGeneralConds(t, ns, idx, items)
	})

	t.Run("check index description: int64 ttl", func(t *testing.T) {
		idx := getIndexByName(t, "int64_ttl", indexMap)
		checkSortable(t, ns, idx, items)
		require.Equal(t, generalConds, idx.Conditions)
		checkGeneralConds(t, ns, idx, items)
	})

	t.Run("check index description: int64 store", func(t *testing.T) {
		idx := getIndexByName(t, "int64_store", indexMap)
		checkSortable(t, ns, idx, items)
		require.Equal(t, generalConds, idx.Conditions)
		checkGeneralConds(t, ns, idx, items)
	})

	t.Run("check index description: double tree", func(t *testing.T) {
		idx := getIndexByName(t, "double_tree", indexMap)
		checkSortable(t, ns, idx, items)
		require.Equal(t, generalConds, idx.Conditions)
		checkGeneralConds(t, ns, idx, items)
	})

	t.Run("check index description: double store", func(t *testing.T) {
		idx := getIndexByName(t, "double_store", indexMap)
		checkSortable(t, ns, idx, items)
		require.Equal(t, generalConds, idx.Conditions)
		checkGeneralConds(t, ns, idx, items)
	})

	t.Run("check index description: bool store", func(t *testing.T) {
		idx := getIndexByName(t, "bool_store", indexMap)
		checkSortable(t, ns, idx, items)
		require.Equal(t, generalConds, idx.Conditions)
		checkGeneralConds(t, ns, idx, items)
	})

	t.Run("check index description: uuid hash", func(t *testing.T) {
		idx := getIndexByName(t, "uuid_hash", indexMap)
		checkSortable(t, ns, idx, items)
		require.Equal(t, generalConds, idx.Conditions)
		checkGeneralConds(t, ns, idx, items)
	})

	t.Run("check index description: uuid store", func(t *testing.T) {
		idx := getIndexByName(t, "uuid_store", indexMap)
		checkSortable(t, ns, idx, items)
		require.Equal(t, generalConds, idx.Conditions)
		checkGeneralConds(t, ns, idx, items)
	})

	t.Run("check index description: float vector hnsw", func(t *testing.T) {
		idx := getIndexByName(t, "vec_hnsw", indexMap)
		checkNotSortable(t, ns, idx, fmt.Sprintf("Ordering by float vector index is not allowed: '%s'", idx.Name))
		require.Equal(t, knnConds, idx.Conditions)
		checkVectorConds(t, ns, idx)
	})

	t.Run("check index description: float vector bf", func(t *testing.T) {
		idx := getIndexByName(t, "vec_bf", indexMap)
		checkNotSortable(t, ns, idx, fmt.Sprintf("Ordering by float vector index is not allowed: '%s'", idx.Name))
		require.Equal(t, knnConds, idx.Conditions)
		checkVectorConds(t, ns, idx)
	})

	t.Run("check index description: float vector ivf", func(t *testing.T) {
		idx := getIndexByName(t, "vec_ivf", indexMap)
		checkNotSortable(t, ns, idx, fmt.Sprintf("Ordering by float vector index is not allowed: '%s'", idx.Name))
		require.Equal(t, knnConds, idx.Conditions)
		checkVectorConds(t, ns, idx)
	})

	t.Run("check index description: composite hash", func(t *testing.T) {
		idx := getIndexByName(t, "int_hash+str_hash", indexMap)
		require.True(t, idx.IsSortable)
		includedStructFields := []string{"IntHash", "StrHash"}
		checkSortableComposite(t, ns, idx.Name, includedStructFields, items)
		require.Equal(t, generalConds, idx.Conditions)
		checkCompositeGeneralConds(t, ns, idx, includedStructFields, items)
	})

	t.Run("check index description: composite tree", func(t *testing.T) {
		idx := getIndexByName(t, "double_tree+str_tree", indexMap)
		require.True(t, idx.IsSortable)
		includedStructFields := []string{"DoubleTree", "StrTree"}
		checkSortableComposite(t, ns, idx.Name, includedStructFields, items)
		require.Equal(t, generalConds, idx.Conditions)
		checkCompositeGeneralConds(t, ns, idx, includedStructFields, items)
	})

	t.Run("check index description: composite text", func(t *testing.T) {
		idx := getIndexByName(t, "str_text+str_hash", indexMap)
		require.True(t, idx.IsSortable)
		includedStructFields := []string{"StrText", "StrHash"}
		checkSortableComposite(t, ns, idx.Name, includedStructFields, items)
		require.Equal(t, fulltextConds, idx.Conditions)
		idx.JSONPaths = includedStructFields
		checkCompositeFulltextConds(t, ns, idx, items)
	})
}

func TestIndexDescriptionArray(t *testing.T) {
	t.Parallel()

	const ns = testIndexDescriptionArrayNS
	items := fillTestItemsAllCombinationsArray(t, ns, 20)

	desc := getIndexesDescription(t, ns)
	randomItem := items[rand.Intn(len(items))]
	require.Equal(t, reflect.TypeOf(randomItem).NumField(), len(desc.Indexes))

	indexMap := make(map[string]reindexer.IndexDescription)
	for i := range desc.Indexes {
		indexMap[desc.Indexes[i].Name] = desc.Indexes[i]
	}

	t.Run("check index description: int hash PK", func(t *testing.T) {
		idx := desc.Indexes[0]
		require.Equal(t, "id", idx.Name)
		require.True(t, idx.IsPK)
		require.Equal(t, reflect.Int64.String(), idx.FieldType)
		require.True(t, idx.IsSortable)
		require.Equal(t, generalConds, idx.Conditions)
	})

	t.Run("check index description: str hash", func(t *testing.T) {
		idx := getIndexByName(t, "str_hash", indexMap)
		checkSortable(t, ns, idx, items)
		require.Equal(t, generalCondsStrArray, idx.Conditions)
		checkGeneralConds(t, ns, idx, items)
	})

	t.Run("check index description: str tree", func(t *testing.T) {
		idx := getIndexByName(t, "str_tree", indexMap)
		checkSortable(t, ns, idx, items)
		require.Equal(t, generalCondsStrArray, idx.Conditions)
		checkGeneralConds(t, ns, idx, items)
	})

	t.Run("check index description: str store", func(t *testing.T) {
		idx := getIndexByName(t, "str_store", indexMap)
		checkSortable(t, ns, idx, items)
		require.Equal(t, generalCondsStrArray, idx.Conditions)
		checkGeneralConds(t, ns, idx, items)
	})

	t.Run("check index description: str text", func(t *testing.T) {
		idx := getIndexByName(t, "str_text", indexMap)
		checkSortable(t, ns, idx, items)
		require.Equal(t, fulltextConds, idx.Conditions)
		checkFulltextConds(t, ns, idx, items)
	})

	t.Run("check index description: int hash", func(t *testing.T) {
		idx := getIndexByName(t, "int_hash", indexMap)
		checkSortable(t, ns, idx, items)
		require.Equal(t, generalCondsArray, idx.Conditions)
		checkGeneralConds(t, ns, idx, items)
	})

	t.Run("check index description: int tree", func(t *testing.T) {
		idx := getIndexByName(t, "int_tree", indexMap)
		checkSortable(t, ns, idx, items)
		require.Equal(t, generalCondsArray, idx.Conditions)
		checkGeneralConds(t, ns, idx, items)
	})

	t.Run("check index description: int store", func(t *testing.T) {
		idx := getIndexByName(t, "int_store", indexMap)
		checkSortable(t, ns, idx, items)
		require.Equal(t, generalCondsArray, idx.Conditions)
		checkGeneralConds(t, ns, idx, items)
	})

	t.Run("check index description: int64 hash", func(t *testing.T) {
		idx := getIndexByName(t, "int64_hash", indexMap)
		checkSortable(t, ns, idx, items)
		require.Equal(t, generalCondsArray, idx.Conditions)
		checkGeneralConds(t, ns, idx, items)
	})

	t.Run("check index description: int64 tree", func(t *testing.T) {
		idx := getIndexByName(t, "int64_tree", indexMap)
		checkSortable(t, ns, idx, items)
		require.Equal(t, generalCondsArray, idx.Conditions)
		checkGeneralConds(t, ns, idx, items)
	})

	t.Run("check index description: int64 ttl", func(t *testing.T) {
		idx := getIndexByName(t, "int64_ttl", indexMap)
		checkSortable(t, ns, idx, items)
		require.Equal(t, generalCondsArray, idx.Conditions)
		checkGeneralConds(t, ns, idx, items)
	})

	t.Run("check index description: int64 store", func(t *testing.T) {
		idx := getIndexByName(t, "int64_store", indexMap)
		checkSortable(t, ns, idx, items)
		require.Equal(t, generalCondsArray, idx.Conditions)
		checkGeneralConds(t, ns, idx, items)
	})

	t.Run("check index description: double tree", func(t *testing.T) {
		idx := getIndexByName(t, "double_tree", indexMap)
		checkSortable(t, ns, idx, items)
		require.Equal(t, generalCondsArray, idx.Conditions)
		checkGeneralConds(t, ns, idx, items)
	})

	t.Run("check index description: double store", func(t *testing.T) {
		idx := getIndexByName(t, "double_store", indexMap)
		checkSortable(t, ns, idx, items)
		require.Equal(t, generalCondsArray, idx.Conditions)
		checkGeneralConds(t, ns, idx, items)
	})

	t.Run("check index description: bool store", func(t *testing.T) {
		idx := getIndexByName(t, "bool_store", indexMap)
		checkSortable(t, ns, idx, items)
		require.Equal(t, generalCondsArray, idx.Conditions)
		checkGeneralConds(t, ns, idx, items)
	})

	t.Run("check index description: uuid hash", func(t *testing.T) {
		idx := getIndexByName(t, "uuid_hash", indexMap)
		checkSortable(t, ns, idx, items)
		require.Equal(t, generalCondsArray, idx.Conditions)
		checkGeneralConds(t, ns, idx, items)
	})

	t.Run("check index description: uuid store", func(t *testing.T) {
		idx := getIndexByName(t, "uuid_store", indexMap)
		checkSortable(t, ns, idx, items)
		require.Equal(t, generalCondsArray, idx.Conditions)
		checkGeneralConds(t, ns, idx, items)
	})

	t.Run("check index description: rtree", func(t *testing.T) {
		idx := getIndexByName(t, "rtree", indexMap)
		checkSortableRtree(t, ns, idx, items)
		require.Equal(t, rtreeCond, idx.Conditions)
		checkPointConds(t, ns, idx)
	})

	t.Run("check index description: float vector hnsw", func(t *testing.T) {
		t.Skip() // TODO 2232
		idx := getIndexByName(t, "vec_hnsw", indexMap)
		checkNotSortable(t, ns, idx, fmt.Sprintf("Ordering by float vector index is not allowed: '%s'", idx.Name))
		require.Equal(t, knnConds, idx.Conditions)
		checkVectorConds(t, ns, idx)
	})

	t.Run("check index description: float vector bf", func(t *testing.T) {
		t.Skip() // TODO 2232
		idx := getIndexByName(t, "vec_bf", indexMap)
		checkNotSortable(t, ns, idx, fmt.Sprintf("Ordering by float vector index is not allowed: '%s'", idx.Name))
		require.Equal(t, knnConds, idx.Conditions)
		checkVectorConds(t, ns, idx)
	})

	t.Run("check index description: float vector ivf", func(t *testing.T) {
		t.Skip() // TODO 2232
		idx := getIndexByName(t, "vec_ivf", indexMap)
		checkNotSortable(t, ns, idx, fmt.Sprintf("Ordering by float vector index is not allowed: '%s'", idx.Name))
		require.Equal(t, knnConds, idx.Conditions)
		checkVectorConds(t, ns, idx)
	})
}

func TestIndexDescriptionSparse(t *testing.T) {
	t.Parallel()

	const ns = testIndexDescriptionSparseNS
	items := fillTestItemsAllCombinationsSparse(t, ns, 20)

	desc := getIndexesDescription(t, ns)
	randomItem := items[rand.Intn(len(items))]
	require.Equal(t, reflect.TypeOf(randomItem).NumField(), len(desc.Indexes))

	indexMap := make(map[string]reindexer.IndexDescription)
	for i := range desc.Indexes {
		indexMap[desc.Indexes[i].Name] = desc.Indexes[i]
	}

	t.Run("check index description: int hash PK", func(t *testing.T) {
		idx := desc.Indexes[0]
		require.Equal(t, "id", idx.Name)
		require.True(t, idx.IsPK)
		require.Equal(t, reflect.Int64.String(), idx.FieldType)
		require.True(t, idx.IsSortable)
		require.Equal(t, generalConds, idx.Conditions)
	})

	t.Run("check index description: str hash", func(t *testing.T) {
		idx := getIndexByName(t, "str_hash", indexMap)
		checkSortable(t, ns, idx, items)
		require.Equal(t, generalCondsStrSparse, idx.Conditions)
		checkGeneralConds(t, ns, idx, items)
	})

	t.Run("check index description: str tree", func(t *testing.T) {
		idx := getIndexByName(t, "str_tree", indexMap)
		checkSortable(t, ns, idx, items)
		require.Equal(t, generalCondsStrSparse, idx.Conditions)
		checkGeneralConds(t, ns, idx, items)
	})

	t.Run("check index description: str store", func(t *testing.T) {
		idx := getIndexByName(t, "str_store", indexMap)
		checkSortable(t, ns, idx, items)
		require.Equal(t, generalCondsStrSparse, idx.Conditions)
		checkGeneralConds(t, ns, idx, items)
	})

	t.Run("check index description: str text", func(t *testing.T) {
		idx := getIndexByName(t, "str_text", indexMap)
		checkSortable(t, ns, idx, items)
		require.Equal(t, fulltextConds, idx.Conditions)
		checkFulltextConds(t, ns, idx, items)
	})

	t.Run("check index description: int hash", func(t *testing.T) {
		idx := getIndexByName(t, "int_hash", indexMap)
		checkSortable(t, ns, idx, items)
		require.Equal(t, generalCondsSparse, idx.Conditions)
		checkGeneralConds(t, ns, idx, items)
	})

	t.Run("check index description: int tree", func(t *testing.T) {
		idx := getIndexByName(t, "int_tree", indexMap)
		checkSortable(t, ns, idx, items)
		require.Equal(t, generalCondsSparse, idx.Conditions)
		checkGeneralConds(t, ns, idx, items)
	})

	t.Run("check index description: int store", func(t *testing.T) {
		idx := getIndexByName(t, "int_store", indexMap)
		checkSortable(t, ns, idx, items)
		require.Equal(t, generalCondsSparse, idx.Conditions)
		checkGeneralConds(t, ns, idx, items)
	})

	t.Run("check index description: int64 hash", func(t *testing.T) {
		idx := getIndexByName(t, "int64_hash", indexMap)
		checkSortable(t, ns, idx, items)
		require.Equal(t, generalCondsSparse, idx.Conditions)
		checkGeneralConds(t, ns, idx, items)
	})

	t.Run("check index description: int64 tree", func(t *testing.T) {
		idx := getIndexByName(t, "int64_tree", indexMap)
		checkSortable(t, ns, idx, items)
		require.Equal(t, generalCondsSparse, idx.Conditions)
		checkGeneralConds(t, ns, idx, items)
	})

	t.Run("check index description: int64 ttl", func(t *testing.T) {
		idx := getIndexByName(t, "int64_ttl", indexMap)
		checkSortable(t, ns, idx, items)
		require.Equal(t, generalCondsSparse, idx.Conditions)
		checkGeneralConds(t, ns, idx, items)
	})

	t.Run("check index description: int64 store", func(t *testing.T) {
		idx := getIndexByName(t, "int64_store", indexMap)
		checkSortable(t, ns, idx, items)
		require.Equal(t, generalCondsSparse, idx.Conditions)
		checkGeneralConds(t, ns, idx, items)
	})

	t.Run("check index description: double tree", func(t *testing.T) {
		idx := getIndexByName(t, "double_tree", indexMap)
		checkSortable(t, ns, idx, items)
		require.Equal(t, generalCondsSparse, idx.Conditions)
		checkGeneralConds(t, ns, idx, items)
	})

	t.Run("check index description: double store", func(t *testing.T) {
		idx := getIndexByName(t, "double_store", indexMap)
		checkSortable(t, ns, idx, items)
		require.Equal(t, generalCondsSparse, idx.Conditions)
		checkGeneralConds(t, ns, idx, items)
	})

	t.Run("check index description: bool store", func(t *testing.T) {
		idx := getIndexByName(t, "bool_store", indexMap)
		checkSortable(t, ns, idx, items)
		require.Equal(t, generalCondsSparse, idx.Conditions)
		checkGeneralConds(t, ns, idx, items)
	})
}

func TestIndexDescriptionSparseArray(t *testing.T) {
	t.Parallel()

	const ns = testIndexDescriptionSparseArrayNS
	items := fillTestItemsAllCombinationsSparseArray(t, ns, 20)

	desc := getIndexesDescription(t, ns)
	randomItem := items[rand.Intn(len(items))]
	require.Equal(t, reflect.TypeOf(randomItem).NumField(), len(desc.Indexes))

	indexMap := make(map[string]reindexer.IndexDescription)
	for i := range desc.Indexes {
		indexMap[desc.Indexes[i].Name] = desc.Indexes[i]
	}

	t.Run("check index description: int hash PK", func(t *testing.T) {
		idx := desc.Indexes[0]
		require.Equal(t, "id", idx.Name)
		require.True(t, idx.IsPK)
		require.Equal(t, reflect.Int64.String(), idx.FieldType)
		require.True(t, idx.IsSortable)
		require.Equal(t, generalConds, idx.Conditions)
	})

	t.Run("check index description: str hash", func(t *testing.T) {
		idx := getIndexByName(t, "str_hash", indexMap)
		checkSortable(t, ns, idx, items)
		require.Equal(t, generalCondsStrArray, idx.Conditions)
		checkGeneralConds(t, ns, idx, items)
	})

	t.Run("check index description: str tree", func(t *testing.T) {
		idx := getIndexByName(t, "str_tree", indexMap)
		checkSortable(t, ns, idx, items)
		require.Equal(t, generalCondsStrArray, idx.Conditions)
		checkGeneralConds(t, ns, idx, items)
	})

	t.Run("check index description: str store", func(t *testing.T) {
		idx := getIndexByName(t, "str_store", indexMap)
		checkSortable(t, ns, idx, items)
		require.Equal(t, generalCondsStrArray, idx.Conditions)
		checkGeneralConds(t, ns, idx, items)
	})

	t.Run("check index description: str text", func(t *testing.T) {
		idx := getIndexByName(t, "str_text", indexMap)
		checkSortable(t, ns, idx, items)
		require.Equal(t, fulltextConds, idx.Conditions)
		checkFulltextConds(t, ns, idx, items)
	})

	t.Run("check index description: int hash", func(t *testing.T) {
		idx := getIndexByName(t, "int_hash", indexMap)
		checkSortable(t, ns, idx, items)
		require.Equal(t, generalCondsArray, idx.Conditions)
		checkGeneralConds(t, ns, idx, items)
	})

	t.Run("check index description: int tree", func(t *testing.T) {
		idx := getIndexByName(t, "int_tree", indexMap)
		checkSortable(t, ns, idx, items)
		require.Equal(t, generalCondsArray, idx.Conditions)
		checkGeneralConds(t, ns, idx, items)
	})

	t.Run("check index description: int store", func(t *testing.T) {
		idx := getIndexByName(t, "int_store", indexMap)
		checkSortable(t, ns, idx, items)
		require.Equal(t, generalCondsArray, idx.Conditions)
		checkGeneralConds(t, ns, idx, items)
	})

	t.Run("check index description: int64 hash", func(t *testing.T) {
		idx := getIndexByName(t, "int64_hash", indexMap)
		checkSortable(t, ns, idx, items)
		require.Equal(t, generalCondsArray, idx.Conditions)
		checkGeneralConds(t, ns, idx, items)
	})

	t.Run("check index description: int64 tree", func(t *testing.T) {
		idx := getIndexByName(t, "int64_tree", indexMap)
		checkSortable(t, ns, idx, items)
		require.Equal(t, generalCondsArray, idx.Conditions)
		checkGeneralConds(t, ns, idx, items)
	})

	t.Run("check index description: int64 ttl", func(t *testing.T) {
		idx := getIndexByName(t, "int64_ttl", indexMap)
		checkSortable(t, ns, idx, items)
		require.Equal(t, generalCondsArray, idx.Conditions)
		checkGeneralConds(t, ns, idx, items)
	})

	t.Run("check index description: int64 store", func(t *testing.T) {
		idx := getIndexByName(t, "int64_store", indexMap)
		checkSortable(t, ns, idx, items)
		require.Equal(t, generalCondsArray, idx.Conditions)
		checkGeneralConds(t, ns, idx, items)
	})

	t.Run("check index description: double tree", func(t *testing.T) {
		idx := getIndexByName(t, "double_tree", indexMap)
		checkSortable(t, ns, idx, items)
		require.Equal(t, generalCondsArray, idx.Conditions)
		checkGeneralConds(t, ns, idx, items)
	})

	t.Run("check index description: double store", func(t *testing.T) {
		idx := getIndexByName(t, "double_store", indexMap)
		checkSortable(t, ns, idx, items)
		require.Equal(t, generalCondsArray, idx.Conditions)
		checkGeneralConds(t, ns, idx, items)
	})

	t.Run("check index description: bool store", func(t *testing.T) {
		idx := getIndexByName(t, "bool_store", indexMap)
		checkSortable(t, ns, idx, items)
		require.Equal(t, generalCondsArray, idx.Conditions)
		checkGeneralConds(t, ns, idx, items)
	})

	t.Run("check index description: rtree", func(t *testing.T) {
		idx := getIndexByName(t, "rtree", indexMap)
		checkSortableRtree(t, ns, idx, items)
		require.Equal(t, rtreeCond, idx.Conditions)
		checkPointConds(t, ns, idx)
	})
}
