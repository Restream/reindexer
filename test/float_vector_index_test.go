package reindexer

import (
	"log"
	"math/rand"
	"os"
	"reflect"
	"testing"

	"github.com/restream/reindexer/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestItemHnswST struct {
	ID  int                                `reindex:"id,,pk"`
	Vec [kTestFloatVectorDimension]float32 `reindex:"vec,hnsw,metric=inner_product"`
}

type TestItemHnswMT struct {
	ID  int                                `reindex:"id,,pk"`
	Vec [kTestFloatVectorDimension]float32 `reindex:"vec,hnsw,m=16,ef_construction=200,start_size=100,metric=l2,multithreading=1"`
}

type TestItemVecBF struct {
	ID  int                                `reindex:"id,,pk"`
	Vec [kTestFloatVectorDimension]float32 `reindex:"vec,vec_bf,start_size=100,metric=l2"`
}

type TestItemIvf struct {
	ID  int                                `reindex:"id,,pk"`
	Vec [kTestFloatVectorDimension]float32 `reindex:"vec,ivf,centroids_count=80,metric=l2"`
}

type TestItemMultiIndexVec struct {
	ID   int       `reindex:"id,,pk"`
	Vec1 []float32 `json:"vec1"`
	Vec2 []float32 `json:"vec2"`
	Vec3 []float32 `json:"vec3"`
	Vec4 []float32 `json:"vec4"`
}

const (
	kHnswNsST        = "test_items_hnsw_st"
	kHnswNsMT        = "test_items_hnsw_mt"
	kVecBfNs         = "test_items_vec_bf"
	kIvfNs           = "test_items_ivf"
	kMultiIndexVecNs = "test_items_multi_index_vec"

	kMultiIndexVecDimension = kTestFloatVectorDimension / 5
	kMultiIndexMaxElems     = kTestBFloatVectorMaxElements / 5
)

func init() {
	tnamespaces[kHnswNsST] = TestItemHnswST{}
	tnamespaces[kHnswNsMT] = TestItemHnswMT{}
	tnamespaces[kVecBfNs] = TestItemVecBF{}
	tnamespaces[kIvfNs] = TestItemIvf{}
	tnamespaces[kMultiIndexVecNs] = TestItemMultiIndexVec{}
}

func removeSomeItems(t *testing.T, ns string, createItem testItemsCreator, maxElements int) {
	tx := newTestTx(DB, ns)
	for i := rand.Int() % (maxElements / 100); i < maxElements; i += rand.Int() % (maxElements / 100) {
		err := tx.Delete(createItem(i, 0))
		require.NoError(t, err)
	}
	tx.MustCommit()
}

func newTestItemHnswST(id int, pkgsCount int) interface{} {
	result := &TestItemHnswST{
		ID: mkID(id),
	}
	if rand.Int()%10 != 0 {
		vect := randVect(kTestFloatVectorDimension)
		for i := 0; i < kTestFloatVectorDimension; i++ {
			result.Vec[i] = vect[i]
		}
	}
	return result
}

func newTestItemHnswMT(id int, pkgsCount int) interface{} {
	result := &TestItemHnswMT{
		ID: mkID(id),
	}
	if rand.Int()%10 != 0 {
		vect := randVect(kTestFloatVectorDimension)
		for i := 0; i < kTestFloatVectorDimension; i++ {
			result.Vec[i] = vect[i]
		}
	}
	return result
}

func newTestItemVecBF(id int, pkgsCount int) interface{} {
	result := &TestItemVecBF{
		ID: mkID(id),
	}
	if rand.Int()%10 != 0 {
		vect := randVect(kTestFloatVectorDimension)
		for i := 0; i < kTestFloatVectorDimension; i++ {
			result.Vec[i] = vect[i]
		}
	}
	return result
}

func newTestItemMultiIndexVec(id int, pkgsCount int) interface{} {
	result := &TestItemMultiIndexVec{
		ID:   mkID(id),
		Vec1: make([]float32, kMultiIndexVecDimension),
		Vec2: make([]float32, kMultiIndexVecDimension),
		Vec3: make([]float32, kMultiIndexVecDimension),
		Vec4: make([]float32, kMultiIndexVecDimension),
	}
	if rand.Int()%10 != 0 {
		vect := randVect(kMultiIndexVecDimension)
		for i := 0; i < kMultiIndexVecDimension; i++ {
			result.Vec1[i] = vect[i]
		}
	}
	if rand.Int()%10 != 0 {
		vect := randVect(kMultiIndexVecDimension)
		for i := 0; i < kMultiIndexVecDimension; i++ {
			result.Vec2[i] = vect[i]
		}
	}
	if rand.Int()%10 != 0 {
		vect := randVect(kMultiIndexVecDimension)
		for i := 0; i < kMultiIndexVecDimension; i++ {
			result.Vec3[i] = vect[i]
		}
	}
	if rand.Int()%10 != 0 {
		vect := randVect(kMultiIndexVecDimension)
		for i := 0; i < kMultiIndexVecDimension; i++ {
			result.Vec4[i] = vect[i]
		}
	}
	return result
}

func newTestItemIvf(id int, pkgsCount int) interface{} {
	result := &TestItemIvf{
		ID: mkID(id),
	}
	if rand.Int()%10 != 0 {
		vect := randVect(kTestFloatVectorDimension)
		for i := 0; i < kTestFloatVectorDimension; i++ {
			result.Vec[i] = vect[i]
		}
	}
	return result
}

func TestHnswST(t *testing.T) {
	const kMaxElements = kTestHNSWFloatVectorMaxElements
	FillTestItemsWithFuncParts(kHnswNsST, 0, kMaxElements, kMaxElements/10, 0, newTestItemHnswST)
	removeSomeItems(t, kHnswNsST, newTestItemHnswST, kMaxElements)
	defer DB.DropIndex(kHnswNsST, "vec") // Deallocate index

	knnBaseSearchParams, err := reindexer.NewBaseKnnSearchParam(500)
	require.NoError(t, err)
	hnswSearchParams, err := reindexer.NewIndexHnswSearchParam(1000, knnBaseSearchParams)
	require.NoError(t, err)

	newTestQuery(DB, kHnswNsST).SelectAllFields().ExecAndVerify(t)

	query := newTestQuery(DB, kHnswNsST).WhereKnn("vec", randVect(kTestFloatVectorDimension), hnswSearchParams).SelectAllFields()
	it := query.Exec(t)
	require.NoError(t, it.Error())
	defer it.Close()
	for it.Next() {
		require.NoError(t, it.Error())
		returnedItem := it.Object().(*TestItemHnswST)
		pk := getPK(query.ns, reflect.Indirect(reflect.ValueOf(returnedItem)))
		insertedItem := query.ns.items[pk].(*TestItemHnswST)
		assert.Equal(t, returnedItem.Vec, insertedItem.Vec)
	}
	assert.NoError(t, it.Error())
	log.Println("HNSW_ST", it.Count())
}

func TestHnswMT(t *testing.T) {
	const kMaxElements = kTestHNSWFloatVectorMaxElements
	FillTestItemsWithFuncParts(kHnswNsMT, 0, kMaxElements, kMaxElements/10, 0, newTestItemHnswMT)
	removeSomeItems(t, kHnswNsMT, newTestItemHnswMT, kMaxElements)
	defer DB.DropIndex(kHnswNsMT, "vec") // Deallocate index

	knnBaseSearchParams, err := reindexer.NewBaseKnnSearchParam(500)
	require.NoError(t, err)
	hnswSearchParams, err := reindexer.NewIndexHnswSearchParam(1000, knnBaseSearchParams)
	require.NoError(t, err)

	newTestQuery(DB, kHnswNsMT).SelectAllFields().ExecAndVerify(t)

	query := newTestQuery(DB, kHnswNsMT).WhereKnn("vec", randVect(kTestFloatVectorDimension), hnswSearchParams).Select("Vec")
	it := query.Exec(t)
	require.NoError(t, it.Error())
	defer it.Close()
	for it.Next() {
		require.NoError(t, it.Error())
		returnedItem := it.Object().(*TestItemHnswMT)
		pk := getPK(query.ns, reflect.Indirect(reflect.ValueOf(returnedItem)))
		insertedItem := query.ns.items[pk].(*TestItemHnswMT)
		assert.Equal(t, returnedItem.Vec, insertedItem.Vec)
	}
	assert.NoError(t, it.Error())
	log.Println("HNSW_MT", it.Count())
}

func TestVecBF(t *testing.T) {
	const kMaxElements = kTestBFloatVectorMaxElements
	FillTestItemsWithFuncParts(kVecBfNs, 0, kMaxElements, kMaxElements/10, 0, newTestItemVecBF)
	removeSomeItems(t, kVecBfNs, newTestItemVecBF, kMaxElements)
	defer DB.DropIndex(kVecBfNs, "vec") // Deallocate index

	knnBaseSearchParams, err := reindexer.NewBaseKnnSearchParam(1000)
	require.NoError(t, err)
	bfSearchParams, err := reindexer.NewIndexBFSearchParam(knnBaseSearchParams)
	require.NoError(t, err)

	newTestQuery(DB, kVecBfNs).SelectAllFields().ExecAndVerify(t)

	query := newTestQuery(DB, kVecBfNs).WhereKnn("vec", randVect(kTestFloatVectorDimension), bfSearchParams)
	it := query.Exec(t)
	require.NoError(t, it.Error())
	defer it.Close()
	zero := [kTestFloatVectorDimension]float32{}
	for it.Next() {
		require.NoError(t, it.Error())
		returnedItem := it.Object().(*TestItemVecBF)
		assert.Equal(t, returnedItem.Vec, zero)
	}
	assert.NoError(t, it.Error())
	log.Println("BF", it.Count())
}

func TestIvf(t *testing.T) {
	if os.Getenv("REINDEXER_GH_CI_TSAN") != "" {
		t.Skip() // Skip this test on github CI(TSAN) due to TSAN false positives on OpenMP
	}

	const kMaxElements = kTestIVFFloatVectorMaxElements
	FillTestItemsWithFuncParts(kIvfNs, 0, kMaxElements, kMaxElements/10, 0, newTestItemIvf)
	removeSomeItems(t, kIvfNs, newTestItemIvf, kMaxElements)
	defer DB.DropIndex(kIvfNs, "vec") // Deallocate index

	knnBaseSearchParams, err := reindexer.NewBaseKnnSearchParam(1000)
	require.NoError(t, err)
	ivfSearchParams, err := reindexer.NewIndexIvfSearchParam(10, knnBaseSearchParams)
	require.NoError(t, err)

	newTestQuery(DB, kIvfNs).SelectAllFields().ExecAndVerify(t)

	it := DB.GetBaseQuery(kIvfNs).WhereKnn("vec", randVect(kTestFloatVectorDimension), ivfSearchParams).Exec()
	require.NoError(t, it.Error())
	defer it.Close()
	for it.Next() {
		require.NoError(t, it.Error())
		elem := it.Object().(*TestItemIvf)
		assert.GreaterOrEqual(t, elem.ID, 0)
	}
	assert.NoError(t, it.Error())
	log.Println("IVF", it.Count())
}

func TestAddKnnIndex(t *testing.T) {
	const kMaxElements = kMultiIndexMaxElems / 5
	currentSize := 0
	FillTestItemsWithFuncParts(kMultiIndexVecNs, currentSize, currentSize+kMaxElements, kMaxElements/10, 0, newTestItemMultiIndexVec)
	currentSize += kMaxElements
	removeSomeItems(t, kMultiIndexVecNs, newTestItemMultiIndexVec, currentSize)

	bfOpts := reindexer.FloatVectorIndexOpts{Metric: "l2", Dimension: kMultiIndexVecDimension, StartSize: kMultiIndexMaxElems}
	indexDef := reindexer.IndexDef{
		Name:      "vec1",
		JSONPaths: []string{"vec1"},
		IndexType: "vec_bf",
		FieldType: "float_vector",
		Config:    bfOpts,
	}
	err := DB.AddIndex(kMultiIndexVecNs, indexDef)
	require.NoError(t, err)
	defer DB.DropIndex(kMultiIndexVecNs, "vec1") // Deallocate index

	FillTestItemsWithFuncParts(kMultiIndexVecNs, currentSize, currentSize+kMaxElements, kMaxElements/10, 0, newTestItemMultiIndexVec)
	currentSize += kMaxElements
	removeSomeItems(t, kMultiIndexVecNs, newTestItemMultiIndexVec, currentSize)

	hnswSTOpts := reindexer.FloatVectorIndexOpts{
		Metric:             "inner_product",
		Dimension:          kMultiIndexVecDimension,
		M:                  32,
		EfConstruction:     100,
		StartSize:          kMultiIndexMaxElems,
		MultithreadingMode: 0,
	}
	indexDef = reindexer.IndexDef{
		Name:      "vec2",
		JSONPaths: []string{"vec2"},
		IndexType: "hnsw",
		FieldType: "float_vector",
		Config:    hnswSTOpts,
	}
	err = DB.AddIndex(kMultiIndexVecNs, indexDef)
	require.NoError(t, err)
	defer DB.DropIndex(kMultiIndexVecNs, "vec2") // Deallocate index

	FillTestItemsWithFuncParts(kMultiIndexVecNs, currentSize, currentSize+kMaxElements, kMaxElements/10, 0, newTestItemMultiIndexVec)
	currentSize += kMaxElements
	removeSomeItems(t, kMultiIndexVecNs, newTestItemMultiIndexVec, currentSize)

	hnswMTOpts := reindexer.FloatVectorIndexOpts{
		Metric:             "cosine",
		Dimension:          kMultiIndexVecDimension,
		M:                  8,
		EfConstruction:     300,
		StartSize:          kMultiIndexMaxElems,
		MultithreadingMode: 1,
	}
	indexDef = reindexer.IndexDef{
		Name:      "vec3",
		JSONPaths: []string{"vec3"},
		IndexType: "hnsw",
		FieldType: "float_vector",
		Config:    hnswMTOpts,
	}
	err = DB.AddIndex(kMultiIndexVecNs, indexDef)
	require.NoError(t, err)
	defer DB.DropIndex(kMultiIndexVecNs, "vec3") // Deallocate index

	FillTestItemsWithFuncParts(kMultiIndexVecNs, currentSize, currentSize+kMaxElements, kMaxElements/10, 0, newTestItemMultiIndexVec)
	currentSize += kMaxElements
	removeSomeItems(t, kMultiIndexVecNs, newTestItemMultiIndexVec, currentSize)

	ivfOpts := reindexer.FloatVectorIndexOpts{
		Metric:         "l2",
		Dimension:      kMultiIndexVecDimension,
		CentroidsCount: 32,
	}
	indexDef = reindexer.IndexDef{
		Name:      "vec4",
		JSONPaths: []string{"vec4"},
		IndexType: "ivf",
		FieldType: "float_vector",
		Config:    ivfOpts,
	}
	err = DB.AddIndex(kMultiIndexVecNs, indexDef)
	require.NoError(t, err)
	defer DB.DropIndex(kMultiIndexVecNs, "vec4") // Deallocate index

	FillTestItemsWithFuncParts(kMultiIndexVecNs, currentSize, currentSize+kMaxElements, kMaxElements/10, 0, newTestItemMultiIndexVec)
	currentSize += kMaxElements
	removeSomeItems(t, kMultiIndexVecNs, newTestItemMultiIndexVec, currentSize)
}
