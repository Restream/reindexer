package reindexer

import (
	"log"
	"math/rand"
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
	Vec [kTestFloatVectorDimension]float32 `reindex:"vec,vec_bf,start_size=100,metric=cosine"`
}

type TestItemIvf struct {
	ID  int                                `reindex:"id,,pk"`
	Vec [kTestFloatVectorDimension]float32 `reindex:"vec,ivf,centroids_count=80,metric=l2,radius=1e38"`
}

type TestItemMultiIndexVec struct {
	ID   int       `reindex:"id,,pk"`
	Vec1 []float32 `json:"vec1"`
	Vec2 []float32 `json:"vec2"`
	Vec3 []float32 `json:"vec3"`
	Vec4 []float32 `json:"vec4"`
}

const (
	kMultiIndexVecDimension = kTestFloatVectorDimension / 5
	kMultiIndexMaxElems     = kTestBFloatVectorMaxElements / 5
)

const (
	testHnswNsSTNs      = "test_items_hnsw_st"
	testHnswNsMTNs      = "test_items_hnsw_mt"
	testVecBfNs         = "test_items_vec_bf"
	testIvfNs           = "test_items_ivf"
	testMultiIndexVecNs = "test_items_multi_index_vec"
)

func init() {
	tnamespaces[testHnswNsSTNs] = TestItemHnswST{}
	tnamespaces[testHnswNsMTNs] = TestItemHnswMT{}
	tnamespaces[testVecBfNs] = TestItemVecBF{}
	tnamespaces[testIvfNs] = TestItemIvf{}
	tnamespaces[testMultiIndexVecNs] = TestItemMultiIndexVec{}
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

func removeSomeItems(t *testing.T, ns string, createItem testItemsCreator, maxElements int) {
	tx := newTestTx(DB, ns)
	for i := rand.Int() % (maxElements / 100); i < maxElements; i += rand.Int() % (maxElements / 100) {
		err := tx.Delete(createItem(i, 0))
		require.NoError(t, err)
	}
	tx.MustCommit()
}

type RadiusProcessingF func(it *reindexer.Iterator, comparator AssertComparatorF)
type AssertComparatorF func(t assert.TestingT, e1 interface{}, e2 interface{}, msgAndArgs ...interface{}) bool

func testWithRadiusWrapperImpl(t *testing.T, test func(RadiusProcessingF), knnBaseSearchParams *reindexer.BaseKnnSearchParam) {
	var rBeg, rEnd float32
	test(
		func(it *reindexer.Iterator, comparator AssertComparatorF) {
			if knnBaseSearchParams.Radius == nil {
				if rBeg == 0 {
					rBeg = it.Rank()
				}
				rEnd = it.Rank()
			} else {
				comparator(t, it.Rank(), *knnBaseSearchParams.Radius)
				knnBaseSearchParams.K = nil
			}
		})

	if knnBaseSearchParams.Radius == nil {
		knnBaseSearchParams.SetRadius((rBeg + rEnd) / 2)
	}
}

func testWithRadiusWrapper(t *testing.T, test func(RadiusProcessingF), knnBaseSearchParams *reindexer.BaseKnnSearchParam) {
	for range []string{"Only K", "K and Radius", "Only Radius"} {
		testWithRadiusWrapperImpl(t, test, knnBaseSearchParams)
	}
}

func TestHnswST(t *testing.T) {
	const ns = testHnswNsSTNs
	const kMaxElements = kTestHNSWFloatVectorMaxElements

	FillTestItemsWithFuncParts(ns, 0, kMaxElements, kMaxElements/10, 0, newTestItemHnswST)
	removeSomeItems(t, ns, newTestItemHnswST, kMaxElements)
	defer DB.DropIndex(ns, "vec") // Deallocate index

	hnswSearchParams, err := reindexer.NewIndexHnswSearchParam(1000, reindexer.BaseKnnSearchParam{}.SetK(500))
	require.NoError(t, err)

	newTestQuery(DB, ns).SelectAllFields().ExecAndVerify(t)

	vec := randVect(kTestFloatVectorDimension)
	test := func(radiusProcessing RadiusProcessingF) {
		query := newTestQuery(DB, ns).WhereKnn("vec", vec, hnswSearchParams).SelectAllFields()
		query.q.WithRank()
		it := query.Exec(t)
		require.NoError(t, it.Error())
		defer it.Close()
		for it.Next() {
			require.NoError(t, it.Error())
			returnedItem := it.Object().(*TestItemHnswST)
			pk := getPK(query.ns, reflect.Indirect(reflect.ValueOf(returnedItem)))
			insertedItem := query.ns.items[pk].(*TestItemHnswST)
			assert.Equal(t, returnedItem.Vec, insertedItem.Vec)

			radiusProcessing(it, assert.Greater)
		}
		assert.NoError(t, it.Error())
		log.Println("HNSW_ST", it.Count())
	}

	testWithRadiusWrapper(t, test, &hnswSearchParams.BaseKnnSearchParam)
}

func TestHnswMT(t *testing.T) {
	const ns = testHnswNsMTNs
	const kMaxElements = kTestHNSWFloatVectorMaxElements

	FillTestItemsWithFuncParts(ns, 0, kMaxElements, kMaxElements/10, 0, newTestItemHnswMT)
	removeSomeItems(t, ns, newTestItemHnswMT, kMaxElements)
	defer DB.DropIndex(ns, "vec") // Deallocate index

	hnswSearchParams, err := reindexer.NewIndexHnswSearchParam(1000, reindexer.BaseKnnSearchParam{}.SetK(500))
	require.NoError(t, err)

	newTestQuery(DB, ns).SelectAllFields().ExecAndVerify(t)

	vec := randVect(kTestFloatVectorDimension)
	test := func(radiusProcessing RadiusProcessingF) {
		query := newTestQuery(DB, ns).WhereKnn("vec", vec, hnswSearchParams).Select("Vec", "rank()")
		it := query.Exec(t)
		require.NoError(t, it.Error())
		defer it.Close()
		for it.Next() {
			require.NoError(t, it.Error())
			returnedItem := it.Object().(*TestItemHnswMT)
			pk := getPK(query.ns, reflect.Indirect(reflect.ValueOf(returnedItem)))
			insertedItem := query.ns.items[pk].(*TestItemHnswMT)
			assert.Equal(t, returnedItem.Vec, insertedItem.Vec)

			radiusProcessing(it, assert.Less)
		}
		assert.NoError(t, it.Error())
		log.Println("HNSW_MT", it.Count())
	}

	testWithRadiusWrapper(t, test, &hnswSearchParams.BaseKnnSearchParam)
}

func TestVecBF(t *testing.T) {
	const ns = testVecBfNs
	const kMaxElements = kTestBFloatVectorMaxElements

	FillTestItemsWithFuncParts(ns, 0, kMaxElements, kMaxElements/10, 0, newTestItemVecBF)
	removeSomeItems(t, ns, newTestItemVecBF, kMaxElements)
	defer DB.DropIndex(ns, "vec") // Deallocate index

	bfSearchParams, err := reindexer.NewIndexBFSearchParam(reindexer.BaseKnnSearchParam{}.SetK(1000))
	require.NoError(t, err)

	newTestQuery(DB, ns).SelectAllFields().ExecAndVerify(t)

	vec := randVect(kTestFloatVectorDimension)
	test := func(radiusProcessing RadiusProcessingF) {
		query := newTestQuery(DB, ns).WhereKnn("vec", vec, bfSearchParams)
		query.q.WithRank()
		it := query.Exec(t)
		require.NoError(t, it.Error())
		defer it.Close()
		zero := [kTestFloatVectorDimension]float32{}
		for it.Next() {
			require.NoError(t, it.Error())
			returnedItem := it.Object().(*TestItemVecBF)
			assert.Equal(t, returnedItem.Vec, zero)

			radiusProcessing(it, assert.Less)
		}
		assert.NoError(t, it.Error())
		log.Println("BF", it.Count())
	}

	testWithRadiusWrapper(t, test, &bfSearchParams.BaseKnnSearchParam)
}

func TestIvf(t *testing.T) {
	const ns = testIvfNs
	const kMaxElements = kTestIVFFloatVectorMaxElements

	FillTestItemsWithFuncParts(ns, 0, kMaxElements, kMaxElements/10, 0, newTestItemIvf)
	removeSomeItems(t, ns, newTestItemIvf, kMaxElements)
	defer DB.DropIndex(ns, "vec") // Deallocate index

	ivfSearchParams, err := reindexer.NewIndexIvfSearchParam(10, reindexer.BaseKnnSearchParam{}.SetK(1000))
	require.NoError(t, err)

	newTestQuery(DB, ns).SelectAllFields().ExecAndVerify(t)

	vec := randVect(kTestFloatVectorDimension)
	test := func(radiusProcessing RadiusProcessingF) {
		it := DB.GetBaseQuery(ns).WhereKnn("vec", vec, ivfSearchParams).WithRank().Exec()
		require.NoError(t, it.Error())
		defer it.Close()
		for it.Next() {
			require.NoError(t, it.Error())
			elem := it.Object().(*TestItemIvf)
			assert.GreaterOrEqual(t, elem.ID, 0)

			radiusProcessing(it, assert.Less)
		}
		assert.NoError(t, it.Error())
		log.Println("IVF", it.Count())
	}

	testWithRadiusWrapper(t, test, &ivfSearchParams.BaseKnnSearchParam)
	testWithRadiusWrapper(t, test, &ivfSearchParams.BaseKnnSearchParam)
}

func TestAddKnnIndex(t *testing.T) {
	const ns = testMultiIndexVecNs
	const kMaxElements = kMultiIndexMaxElems / 5

	currentSize := 0
	FillTestItemsWithFuncParts(ns, currentSize, currentSize+kMaxElements, kMaxElements/10, 0, newTestItemMultiIndexVec)
	currentSize += kMaxElements
	removeSomeItems(t, ns, newTestItemMultiIndexVec, currentSize)

	bfOpts := reindexer.FloatVectorIndexOpts{Metric: "l2", Dimension: kMultiIndexVecDimension, StartSize: kMultiIndexMaxElems}
	indexDef := reindexer.IndexDef{
		Name:      "vec1",
		JSONPaths: []string{"vec1"},
		IndexType: "vec_bf",
		FieldType: "float_vector",
		Config:    bfOpts,
	}
	err := DB.AddIndex(ns, indexDef)
	require.NoError(t, err)
	defer DB.DropIndex(ns, "vec1") // Deallocate index

	FillTestItemsWithFuncParts(ns, currentSize, currentSize+kMaxElements, kMaxElements/10, 0, newTestItemMultiIndexVec)
	currentSize += kMaxElements
	removeSomeItems(t, ns, newTestItemMultiIndexVec, currentSize)

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
	err = DB.AddIndex(ns, indexDef)
	require.NoError(t, err)
	defer DB.DropIndex(ns, "vec2") // Deallocate index

	FillTestItemsWithFuncParts(ns, currentSize, currentSize+kMaxElements, kMaxElements/10, 0, newTestItemMultiIndexVec)
	currentSize += kMaxElements
	removeSomeItems(t, ns, newTestItemMultiIndexVec, currentSize)

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
	err = DB.AddIndex(ns, indexDef)
	require.NoError(t, err)
	defer DB.DropIndex(ns, "vec3") // Deallocate index

	FillTestItemsWithFuncParts(ns, currentSize, currentSize+kMaxElements, kMaxElements/10, 0, newTestItemMultiIndexVec)
	currentSize += kMaxElements
	removeSomeItems(t, ns, newTestItemMultiIndexVec, currentSize)

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
	err = DB.AddIndex(ns, indexDef)
	require.NoError(t, err)
	defer DB.DropIndex(ns, "vec4") // Deallocate index

	FillTestItemsWithFuncParts(ns, currentSize, currentSize+kMaxElements, kMaxElements/10, 0, newTestItemMultiIndexVec)
	currentSize += kMaxElements
	removeSomeItems(t, ns, newTestItemMultiIndexVec, currentSize)
}
