package reindexer

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/restream/reindexer/v5"
	"github.com/restream/reindexer/v5/cjson"
)

type TestItemBench struct {
	Prices     []*TestJoinItem `reindex:"prices,,joined"`
	Pricesx    []*TestJoinItem `reindex:"pricesx,,joined"`
	ID         int32           `reindex:"id,,pk"`
	Genre      int64           `reindex:"genre,tree"`
	Year       int32           `reindex:"year,tree"`
	Packages   []int32         `reindex:"packages,hash"`
	Countries  []string        `reindex:"countries,tree"`
	Age        int32           `reindex:"age,hash"`
	PricesIDs  []int32         `reindex:"price_id"`
	LocationID string          `reindex:"location"`
	EndTime    int32           `reindex:"end_time,-"`
	StartTime  int32           `reindex:"start_time,tree"`
	Uuid       string          `reindex:"uuid,hash,uuid"`
	UuidStr    string          `reindex:"uuid_str,hash"`
}

type TestItemBenchKnn struct {
	ID   int       `reindex:"id,,pk"`
	Vect []float32 `reindex:"vect,-"`
}

type TestJoinCtx struct {
	allPrices []*TestJoinItem
}

const (
	testBenchItemsSimpleNs          = "test_bench_items_simple"
	testBenchItemsSimpleComplexPkNs = "test_bench_items_simple_cmplx_pk"
	testBenchItemsNs                = "test_bench_items"
	testBenchItemsInsertJsonNs      = "test_bench_items_insert_json"
	testBenchItemsInsertNs          = "test_bench_items_insert"
	benchKnnNsPrefix                = "bench_knn_"
)

func knnBenchNsName(indexType string, metric string) string {
	var builder strings.Builder
	builder.WriteString(benchKnnNsPrefix)
	builder.WriteString(indexType)
	builder.WriteString("_")
	builder.WriteString(metric)
	return builder.String()
}

func newKnnItem(id int) TestItemBenchKnn {
	return TestItemBenchKnn{
		ID:   id,
		Vect: randVect(kBenchFloatVectorDimension),
	}
}

func initKnnNs(indexType string, metric string) {
	ns := knnBenchNsName(indexType, metric)
	DBD.DropNamespace(ns)
	if err := DBD.OpenNamespace(ns, reindexer.DefaultNamespaceOptions(), TestItemBenchKnn{}); err != nil {
		panic(fmt.Sprintf("Namespace: %s, err: %s", ns, err.Error()))
	}
	fvIndexOpts := reindexer.FloatVectorIndexOpts{
		Metric: metric,
		Dimension: kBenchFloatVectorDimension,
	}
	if indexType == "hnsw" {
		fvIndexOpts.M = 16
		fvIndexOpts.EfConstruction = 200
		fvIndexOpts.MultithreadingMode = 1
		fvIndexOpts.StartSize = kBenchKnnNsSize
	} else if indexType == "ivf" {
		fvIndexOpts.CentroidsCount = kBenchKnnNsSize / 100
	} else {
		panic(fmt.Sprintf("Cannot define fv index type: %s", ns))
	}
	fvIndexDef := reindexer.IndexDef {
		Name: "vect",
		JSONPaths: []string{"Vect"},
		IndexType: indexType,
		FieldType: "float_vector",
		Config: fvIndexOpts,
	}
	if err := DBD.UpdateIndex(ns, fvIndexDef); err != nil {
		panic(fmt.Sprintf("Add index into namespace: %s, err: %s", ns, err.Error()))
	}
	for i := 0; i < kBenchKnnNsSize; {
		tx := DBD.MustBeginTx(ns)
		for j := 0; j < kBenchKnnTxSize && i < kBenchKnnNsSize; j++ {
			tx.Insert(newKnnItem(i))
			i++
		}
		if err := tx.Commit(); err != nil {
			panic(fmt.Sprintf("Fill namespace: %s, err: %s", ns, err.Error()))
		}
	}
}

func initKnn() {
	for _, indexType := range []string{"hnsw", "ivf"} {
		for _, metric := range []string{"l2", "cosine", "inner_product"} {
			initKnnNs(indexType, metric)
		}
	}
}

func init() {
	rand.Seed(*benchmarkSeed)

	for i := 0; i < 10; i++ {
		pkgs = append(pkgs, randInt32Arr(20, 10000, 10))
	}
	for i := 0; i < 20; i++ {
		priceIds = append(priceIds, randInt32Arr(10, 7000, 50))
	}

	cjenc := cjsonState.NewEncoder()

	buf := &bytes.Buffer{}
	gobenc := gob.NewEncoder(buf)

	for i := 0; i < 100000; i++ {
		testItemsSeed = append(testItemsSeed, newTestItem(i, 20).(*TestItem))

		json, _ := json.Marshal(newTestItem(i+200000, 20))
		testItemsJsonSeed = append(testItemsJsonSeed, json)

		ser := &cjson.Serializer{}
		cjenc.EncodeRaw(newTestItem(i, 20), ser)
		testItemsCJsonSeed = append(testItemsCJsonSeed, ser.Bytes())

		gobenc.Encode(newTestItem(i, 20))
		gobData := make([]byte, len(buf.Bytes()), len(buf.Bytes()))
		copy(gobData, buf.Bytes())

		testItemsGobSeed = append(testItemsGobSeed, gobData)
		buf.Reset()
	}

	tnamespaces[testBenchItemsSimpleNs] = TestItemSimple{}
	tnamespaces[testBenchItemsSimpleComplexPkNs] = TestItemCmplxPK{}
	tnamespaces[testBenchItemsNs] = TestItemBench{}
	tnamespaces[testBenchItemsInsertJsonNs] = TestItem{}
	tnamespaces[testBenchItemsInsertNs] = TestItem{}
}

var (
	pkgs     = make([][]int32, 0)
	priceIds = make([][]int32, 0)

	testItemsSeed      = make([]*TestItem, 0)
	testItemsJsonSeed  = make([][]byte, 0)
	testItemsCJsonSeed = make([][]byte, 0)
	testItemsGobSeed   = make([][]byte, 0)
)

var cjsonState = cjson.NewState()

var prepared = false

func (item *TestItemBench) Join(field string, subitems []interface{}, context interface{}) {
	testJoinCtx := context.(*TestJoinCtx)
	if testJoinCtx.allPrices == nil {
		testJoinCtx.allPrices = make([]*TestJoinItem, 0, 50)
	}

	switch field {
	case "prices":
		allPricesOffset := len(testJoinCtx.allPrices)
		for _, srcItem := range subitems {
			testJoinCtx.allPrices = append(testJoinCtx.allPrices, srcItem.(*TestJoinItem))
		}
		item.Prices = testJoinCtx.allPrices[allPricesOffset : allPricesOffset+len(subitems)]
	}
}

// DBD.Query(testBenchItemsNs)
// DBD.Query(testBenchItemsNs).WhereInt("year", reindexer.EQ, 2010).Limit(1).GetJson()
func newTestBenchItem(id int, pkgCount int) *TestItemBench {
	rand.Seed(*benchmarkSeed)
	startTime := rand.Int() % 50000

	return &TestItemBench{
		ID:         int32(id),
		Year:       int32(rand.Int()%50 + 2000),
		Genre:      int64(rand.Int() % 50),
		Age:        int32(rand.Int() % 5),
		Packages:   randInt32Arr(pkgCount, 10000, 50),
		PricesIDs:  priceIds[rand.Int()%len(priceIds)],
		LocationID: randLocation(),
		StartTime:  int32(startTime),
		EndTime:    int32(startTime + (rand.Int()%5)*1000),
		Uuid:       randUuid(),
		UuidStr:    randUuid(),
	}
}

func FillTestItemsBench(start int, count int, pkgsCount int) {

	wg := sync.WaitGroup{}
	seeder := func(start int, count int) {
		for i := 0; i < count; i++ {
			item := newTestBenchItem(mkID(start+i), pkgsCount)
			if err := DBD.Upsert(testBenchItemsNs, item); err != nil {
				panic(err)
			}
		}
		wg.Done()
	}
	for i := 0; i < *benchmarkSeedCPU; i++ {
		wg.Add(1)
		go seeder(start + i*count / *benchmarkSeedCPU, count / *benchmarkSeedCPU)
	}
	wg.Wait()
}

func BenchmarkPrepare(b *testing.B) {
	rand.Seed(*benchmarkSeed)
	if prepared {
		return
	}
	prepared = true
	DBD.SetLogger(nil)
	FillTestItemsBench(0, *benchmarkSeedCount, 10)
	FillTestJoinItems(7000, 500, "test_join_items")
	initKnn()
}

func BenchmarkSimpleInsert(b *testing.B) {
	rand.Seed(*benchmarkSeed)
	tx := DBD.MustBeginTx(testBenchItemsSimpleNs)
	for i := 0; i < b.N; i++ {
		if err := tx.Upsert(TestItemSimple{ID: mkID(i), Year: rand.Int()%1000 + 10, Name: randString(), Phone: randString()}); err != nil {
			panic(err)
		}
	}
	tx.MustCommit()
}

func BenchmarkSimpleUpdate(b *testing.B) {
	rand.Seed(*benchmarkSeed)
	tx := DBD.MustBeginTx(testBenchItemsSimpleNs)
	for i := 0; i < b.N; i++ {
		if err := tx.Upsert(TestItemSimple{ID: mkID(i), Year: rand.Int()%1000 + 10, Name: randString()}); err != nil {
			panic(err)
		}
	}
	tx.MustCommit()
}

func BenchmarkSimpleUpdateAsync(b *testing.B) {
	rand.Seed(*benchmarkSeed)
	tx := DBD.MustBeginTx(testBenchItemsSimpleNs)
	for i := 0; i < b.N; i++ {
		tx.UpsertAsync(TestItemSimple{ID: mkID(i), Year: rand.Int()%1000 + 10, Name: randString()},
			func(err error) {
				if err != nil {
					panic(err)
				}
			})
	}
	tx.MustCommit()
}

func BenchmarkSimpleCmplxPKUpsert(b *testing.B) {
	rand.Seed(*benchmarkSeed)
	tx := DBD.MustBeginTx(testBenchItemsSimpleComplexPkNs)

	for i := 0; i < b.N; i++ {
		if err := tx.Upsert(TestItemCmplxPK{ID: int32(i), Year: int32(rand.Int()%1000 + 10), Name: randString(), SubID: randString()}); err != nil {
			panic(err)
		}
	}
	tx.MustCommit()
}

func BenchmarkInsert(b *testing.B) {
	tx := DBD.MustBeginTx(testBenchItemsInsertNs)

	for i := 0; i < b.N; i++ {
		if err := tx.Upsert(testItemsSeed[i%len(testItemsSeed)]); err != nil {
			panic(err)
		}
	}
	tx.MustCommit()
}

func BenchmarkCJsonEncode(b *testing.B) {

	enc := cjsonState.NewEncoder()

	for i := 0; i < b.N; i++ {
		ser := cjson.NewPoolSerializer()
		enc.Encode(testItemsSeed[i%len(testItemsSeed)], ser)
		ser.Close()
	}
}

func BenchmarkCJsonDecode(b *testing.B) {

	dec := cjsonState.NewDecoder(TestItem{}, nil)
	defer dec.Finalize()
	for i := 0; i < b.N; i++ {
		ti := TestItem{}
		dec.Decode(testItemsCJsonSeed[i%len(testItemsCJsonSeed)], &ti)
	}
}

func BenchmarkGobEncode(b *testing.B) {
	// Just for the reference timings
	buf := &bytes.Buffer{}
	enc := gob.NewEncoder(buf)

	for i := 0; i < b.N; i++ {
		enc.Encode(testItemsSeed[i%len(testItemsSeed)])
		buf.Reset()
	}
}

func BenchmarkGobDecode(b *testing.B) {
	// Just for the reference timings
	buf := &bytes.Buffer{}
	dec := gob.NewDecoder(buf)
	buf.Write(testItemsGobSeed[0])
	for i := 0; i < b.N; i++ {
		ti := TestItem{}
		if err := dec.Decode(&ti); err != nil {
			panic(err)
		}
		buf.Reset()
		buf.Write(testItemsGobSeed[(i%(len(testItemsGobSeed)-1))+1])
	}
}

func BenchmarkJsonEncode(b *testing.B) {
	// Just for the reference timings
	for i := 0; i < b.N; i++ {
		ser := cjson.NewPoolSerializer()
		enc := json.NewEncoder(ser)
		enc.Encode(testItemsSeed[i%len(testItemsSeed)])
		ser.Close()
	}
}

func BenchmarkJsonDecode(b *testing.B) {
	// Just for the reference timings
	for i := 0; i < b.N; i++ {
		ti := TestItem{}
		json.Unmarshal(testItemsJsonSeed[i%len(testItemsJsonSeed)], &ti)
	}
}

func BenchmarkInsertJson(b *testing.B) {
	tx := DBD.MustBeginTx(testBenchItemsInsertJsonNs)

	for i := 0; i < b.N; i++ {
		if err := tx.UpsertJSON(testItemsJsonSeed[i%len(testItemsJsonSeed)]); err != nil {
			panic(err)
		}
	}
	tx.MustCommit()
}

func BenchmarkUpdate(b *testing.B) {
	tx := DBD.MustBeginTx(testBenchItemsInsertNs)

	for i := 0; i < b.N; i++ {
		if err := tx.Upsert(testItemsSeed[i%len(testItemsSeed)]); err != nil {
			panic(err)
		}
	}
	tx.MustCommit()
}

func BenchmarkDeleteAndUpdate(b *testing.B) {
	rand.Seed(*benchmarkSeed)
	tx := DBD.MustBeginTx(testBenchItemsInsertNs)
	for i := 0; i < b.N; i++ {
		tx.Delete(TestItem{ID: mkID(rand.Int() % b.N)})
		if err := tx.Upsert(testItemsSeed[i%len(testItemsSeed)]); err != nil {
			panic(err)
		}
	}
	tx.Commit()
}

func BenchmarkWarmup(b *testing.B) {
	for {
		if items, err := DBD.Query("#memstats").Where("name", reindexer.EQ, testBenchItemsNs).Exec().FetchAll(); err != nil {
			panic(err)
		} else if items[0].(*reindexer.NamespaceMemStat).OptimizationCompleted {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	for i := 0; i < len(pkgs)*3; i++ {
		DBD.Query(testBenchItemsNs).Limit(20).Sort("start_time", false).
			Where("packages", reindexer.SET, pkgs[i%len(pkgs)]).
			MustExec().Close()
		DBD.Query(testBenchItemsNs).Limit(20).Sort("year", false).
			Where("packages", reindexer.SET, pkgs[i%len(pkgs)]).
			MustExec().Close()
		DBD.Query(testBenchItemsNs).Limit(20).
			Where("year", reindexer.RANGE, []int{2010, 2016}).
			MustExec().Close()
	}
	for i := 0; i < len(priceIds)*3; i++ {
		DBD.Query("test_join_items").Limit(20).
			Where("id", reindexer.SET, priceIds[i%len(priceIds)]).
			MustExec().Close()
	}
}

func Benchmark4CondQuery(b *testing.B) {
	rand.Seed(*benchmarkSeed)
	for i := 0; i < b.N; i++ {
		q := DBD.Query(testBenchItemsNs).Limit(20).
			WhereInt("genre", reindexer.EQ, 5).
			WhereString("age", reindexer.EQ, "2").
			WhereInt("year", reindexer.RANGE, 2010, 2016).
			WhereInt32("packages", reindexer.SET, pkgs[rand.Int()%len(pkgs)]...)
		q.MustExec().FetchAll()
	}
}

func Benchmark4CondQueryTotal(b *testing.B) {
	rand.Seed(*benchmarkSeed)
	for i := 0; i < b.N; i++ {
		q := DBD.Query(testBenchItemsNs).Limit(20).ReqTotal().
			WhereInt("genre", reindexer.EQ, 5).
			WhereString("age", reindexer.EQ, "2").
			WhereInt("year", reindexer.RANGE, 2010, 2016).
			WhereInt32("packages", reindexer.SET, pkgs[rand.Int()%len(pkgs)]...)

		q.MustExec().FetchAll()
	}
}

func Benchmark4CondRangeQuery(b *testing.B) {
	rand.Seed(*benchmarkSeed)
	for i := 0; i < b.N; i++ {
		startTime := rand.Int() % 50000
		endTime := startTime + 10000
		q := DBD.Query(testBenchItemsNs).Limit(20).
			WhereInt("genre", reindexer.EQ, 5).
			WhereInt("year", reindexer.RANGE, 2010, 2016).
			WhereInt("start_time", reindexer.GT, startTime).
			WhereInt("end_time", reindexer.LT, endTime)
		q.MustExec().FetchAll()
	}
}

func Benchmark4CondRangeQueryTotal(b *testing.B) {
	rand.Seed(*benchmarkSeed)
	for i := 0; i < b.N; i++ {
		startTime := rand.Int() % 50000
		endTime := startTime + 10000
		q := DBD.Query(testBenchItemsNs).Limit(20).
			WhereInt("genre", reindexer.EQ, 5).
			WhereInt("year", reindexer.RANGE, 2010, 2016).
			WhereInt("start_time", reindexer.GT, startTime).
			WhereInt("end_time", reindexer.LT, endTime).ReqTotal()

		q.MustExec().FetchAll()
	}
}

func Benchmark3CondQuery(b *testing.B) {
	rand.Seed(*benchmarkSeed)
	for i := 0; i < b.N; i++ {
		q := DBD.Query(testBenchItemsNs).Limit(20).Sort("year", false).
			WhereInt("genre", reindexer.EQ, 5).
			WhereInt("year", reindexer.RANGE, 2010, 2016).
			WhereInt32("packages", reindexer.SET, pkgs[rand.Int()%len(pkgs)]...)
		q.MustExec().FetchAll()
	}
}

func Benchmark3CondQueryTotal(b *testing.B) {
	rand.Seed(*benchmarkSeed)
	for i := 0; i < b.N; i++ {
		q := DBD.Query(testBenchItemsNs).Limit(20).Sort("year", false).ReqTotal().
			WhereInt("genre", reindexer.EQ, 5).
			WhereInt("year", reindexer.RANGE, 2010, 2016).
			WhereInt32("packages", reindexer.SET, pkgs[rand.Int()%len(pkgs)]...)
		q.MustExec().FetchAll()
	}
}

func Benchmark3CondQueryKillIdsCache(b *testing.B) {
	rand.Seed(*benchmarkSeed)
	for i := 0; i < b.N; i++ {
		q := DBD.Query(testBenchItemsNs).Limit(20).Sort("year", false).
			WhereInt("genre", reindexer.EQ, 5).
			WhereInt("year", reindexer.RANGE, 2010, 2016).
			WhereInt("packages", reindexer.SET, randIntArr(20, 10000, 10)...) // Using random array for each request. Test for cache performance

		q.MustExec().FetchAll()
	}
}

func Benchmark3CondQueryRestoreIdsCache(b *testing.B) {
	rand.Seed(*benchmarkSeed)
	for i := 0; i < b.N; i++ {
		q := DBD.Query(testBenchItemsNs).Limit(20).Sort("year", false).
			WhereInt("genre", reindexer.EQ, 5).
			WhereInt("year", reindexer.RANGE, 2010, 2016).
			WhereInt32("packages", reindexer.SET, pkgs[rand.Int()%len(pkgs)]...) // Using subset of arrays for each request. Cache will restore
		q.MustExec().FetchAll()
	}
}

func Benchmark2CondQuery(b *testing.B) {
	for i := 0; i < b.N; i++ {
		q := DBD.Query(testBenchItemsNs).Limit(20).
			WhereInt("genre", reindexer.EQ, 5).
			WhereInt("year", reindexer.RANGE, 2010, 2016)
		q.MustExec().FetchAll()
	}
}

func Benchmark2CondQueryTotal(b *testing.B) {
	for i := 0; i < b.N; i++ {
		q := DBD.Query(testBenchItemsNs).Limit(20).Sort("year", false).ReqTotal().
			WhereInt("genre", reindexer.EQ, 5).
			WhereInt("year", reindexer.RANGE, 2010, 2016)
		q.MustExec().FetchAll()
	}
}

func BenchmarkSubQueryEq(b *testing.B) {
	for i := 0; i < b.N; i++ {
		prices := priceIds[rand.Int()%len(priceIds)]
		q := DBD.Query(testBenchItemsNs).Where("price_id", reindexer.EQ, DBD.Query("test_join_items").Select("id").WhereInt32("id", reindexer.EQ, prices[rand.Int()%len(prices)])).Limit(20)
		q.MustExec().FetchAll()
	}
}

func BenchmarkSubQuerySet(b *testing.B) {
	for i := 0; i < b.N; i++ {
		prices := priceIds[rand.Int()%len(priceIds)]
		rangeMin := prices[rand.Int()%len(prices)]
		q := DBD.Query(testBenchItemsNs).Where("price_id", reindexer.SET, DBD.Query("test_join_items").Select("id").WhereInt32("id", reindexer.RANGE, rangeMin, rangeMin+500)).Limit(20)
		q.MustExec().FetchAll()
	}
}

func BenchmarkSubQueryAggregate(b *testing.B) {
	for i := 0; i < b.N; i++ {
		prices := priceIds[rand.Int()%len(priceIds)]
		q := DBD.Query(testBenchItemsNs).Where("price_id", reindexer.LT, DBD.Query("test_join_items").AggregateAvg("id").WhereInt32("id", reindexer.SET, prices...).Limit(500)).Limit(20)
		q.MustExec().FetchAll()
	}
}

func Benchmark2CondQueryLeftJoin(b *testing.B) {
	ctx := &TestJoinCtx{}
	for i := 0; i < b.N; i++ {
		q2 := DBD.Query("test_join_items").WhereString("device", reindexer.EQ, "ottstb").WhereString("location", reindexer.SET, "mos", "dv", "sib")
		q := DBD.Query(testBenchItemsNs).Limit(20).Sort("year", false).
			WhereInt("genre", reindexer.EQ, 5).
			WhereInt("year", reindexer.RANGE, 2010, 2016).
			InnerJoin(q2, "prices").On("price_id", reindexer.SET, "id")
		ctx.allPrices = ctx.allPrices[:0]
		q.SetContext(ctx)
		q.MustExec().FetchAll()
	}
}

func Benchmark2CondQueryLeftJoinTotal(b *testing.B) {
	ctx := &TestJoinCtx{}
	for i := 0; i < b.N; i++ {
		q2 := DBD.Query("test_join_items").WhereString("device", reindexer.EQ, "ottstb").WhereString("location", reindexer.SET, "mos", "dv", "sib")
		q := DBD.Query(testBenchItemsNs).Limit(20).Sort("year", false).ReqTotal().
			WhereInt("genre", reindexer.EQ, 5).
			WhereInt("year", reindexer.RANGE, 2010, 2016).
			Join(q2, "prices").On("price_id", reindexer.SET, "id")
		ctx.allPrices = ctx.allPrices[:0]
		q.SetContext(ctx)
		q.MustExec().FetchAll()
	}
}

func Benchmark2CondQueryLeftJoinCachedTotal(b *testing.B) {
	ctx := &TestJoinCtx{}
	for i := 0; i < b.N; i++ {
		q2 := DBD.Query("test_join_items").WhereString("device", reindexer.EQ, "ottstb").WhereString("location", reindexer.SET, "mos", "dv", "sib")
		q := DBD.Query(testBenchItemsNs).Limit(20).Sort("year", false).CachedTotal().
			WhereInt("genre", reindexer.EQ, 5).
			WhereInt("year", reindexer.RANGE, 2010, 2016).
			Join(q2, "prices").On("price_id", reindexer.SET, "id")
		ctx.allPrices = ctx.allPrices[:0]
		q.SetContext(ctx)
		q.MustExec().FetchAll()
	}
}

func Benchmark2CondQueryInnerJoin(b *testing.B) {
	ctx := &TestJoinCtx{}
	for i := 0; i < b.N; i++ {

		q2 := DBD.Query("test_join_items").WhereString("device", reindexer.EQ, "ottstb").WhereString("location", reindexer.SET, "mos", "dv", "sib")
		q := DBD.Query(testBenchItemsNs).Limit(20).Sort("year", false).
			WhereInt("genre", reindexer.EQ, 5).
			WhereInt("year", reindexer.RANGE, 2010, 2016).
			InnerJoin(q2, "prices").On("price_id", reindexer.SET, "id")
		ctx.allPrices = ctx.allPrices[:0]
		q.SetContext(ctx)
		q.MustExec().FetchAll()
	}
}

func Benchmark2CondQueryInnerJoinCachedRandom(b *testing.B) {
	rand.Seed(*benchmarkSeed)
	ctx := &TestJoinCtx{}

	for i := 0; i < b.N; i++ {
		id_start := 7000 + rand.Int()%200
		id_end := id_start + rand.Int()%(7200-id_start)
		q2 := DBD.Query("test_join_items").WhereInt("id", reindexer.RANGE, id_start, id_end)
		q := DBD.Query(testBenchItemsNs).Limit(20).Sort("year", false).
			WhereInt("genre", reindexer.EQ, 5).
			WhereInt("year", reindexer.RANGE, 2010, 2016).
			InnerJoin(q2, "prices").On("price_id", reindexer.SET, "id")
		ctx.allPrices = ctx.allPrices[:0]
		q.SetContext(ctx)
		q.MustExec().FetchAll()
	}
}

func Benchmark2CondQueryInnerJoinCached(b *testing.B) {
	ctx := &TestJoinCtx{}
	for i := 0; i < b.N; i++ {

		q2 := DBD.Query("test_join_items").WhereInt("id", reindexer.RANGE, 7000, 7300)
		q := DBD.Query(testBenchItemsNs).Limit(20).Sort("year", false).
			WhereInt("genre", reindexer.EQ, 5).
			WhereInt("year", reindexer.RANGE, 2010, 2016).
			InnerJoin(q2, "prices").On("price_id", reindexer.SET, "id")
		ctx.allPrices = ctx.allPrices[:0]
		q.SetContext(ctx)
		q.MustExec().FetchAll()
	}
}

func Benchmark2CondQueryInnerJoinTotal(b *testing.B) {
	ctx := &TestJoinCtx{}
	for i := 0; i < b.N; i++ {

		q2 := DBD.Query("test_join_items").WhereString("device", reindexer.EQ, "ottstb").WhereString("location", reindexer.SET, "mos", "dv", "sib")
		q := DBD.Query(testBenchItemsNs).Limit(20).Sort("year", false).ReqTotal().
			WhereInt("genre", reindexer.EQ, 5).
			WhereInt("year", reindexer.RANGE, 2010, 2016).
			InnerJoin(q2, "prices").On("price_id", reindexer.SET, "id")
		ctx.allPrices = ctx.allPrices[:0]
		q.SetContext(ctx)
		q.MustExec().FetchAll()
	}
}

func Benchmark2CondQueryInnerJoinCachedTotal(b *testing.B) {
	ctx := &TestJoinCtx{}
	for i := 0; i < b.N; i++ {
		q2 := DBD.Query("test_join_items").WhereString("device", reindexer.EQ, "ottstb").WhereString("location", reindexer.SET, "mos", "dv", "sib")
		q := DBD.Query(testBenchItemsNs).Limit(20).Sort("year", false).CachedTotal().
			WhereInt("genre", reindexer.EQ, 5).
			WhereInt("year", reindexer.RANGE, 2010, 2016).
			InnerJoin(q2, "prices").On("price_id", reindexer.SET, "id")
		ctx.allPrices = ctx.allPrices[:0]
		q.SetContext(ctx)
		q.MustExec().FetchAll()
	}
}

// func Benchmark2CondQueryPseudoJoin(b *testing.B) {
// 	for i := 0; i < b.N; i++ {

// 		res, _ := DBD.Query(testBenchItemsNs).Limit(20).Sort("year", false).
// 			WhereInt("genre", EQ, 5).
// 			WhereInt("year", RANGE, 2010, 2016).
// 			MustExec().FetchAll()
// 		for _, item := range res {
// 			testItem := *item.(*TestItemBench)
// 			r2, _ := DBD.Query("test_join_items").WhereString("device", EQ, "ottstb").
// 				WhereString("location", SET, "mos", "dv", "sib").WhereInt("id", SET, testItem.PricesIDs...).MustExec().FetchAll()
// 			testItem.Prices = make([]*TestJoinItem, 0, len(r2))
// 			for _, subItem := range r2 {
// 				testItem.Prices = append(testItem.Prices, subItem.(*TestJoinItem))
// 			}
// 		}
// 	}
// }

func Benchmark1CondQuery(b *testing.B) {
	for i := 0; i < b.N; i++ {
		q := DBD.Query(testBenchItemsNs).Limit(20).WhereInt("year", reindexer.GT, 2020)
		q.MustExec().FetchAll()
	}
}

func BenchmarkUuid(b *testing.B) {
	for i := 0; i < b.N; i++ {
		q := DBD.Query(testBenchItemsNs).Limit(20).WhereUuid("uuid", reindexer.EQ, randUuid())
		q.MustExec().FetchAll()
	}
}

func BenchmarkUuidStr(b *testing.B) {
	for i := 0; i < b.N; i++ {
		q := DBD.Query(testBenchItemsNs).Limit(20).WhereString("uuid_str", reindexer.EQ, randUuid())
		q.MustExec().FetchAll()
	}
}

func Benchmark1CondQueryUnsafe(b *testing.B) {
	for i := 0; i < b.N; i++ {
		q := DBD.Query(testBenchItemsNs).Limit(20).WhereInt("year", reindexer.GT, 2020)
		it := q.MustExec().AllowUnsafe(true)
		for it.Next() {
			_ = it.Object()
		}
		it.Close()
	}
}

func Benchmark1CondQueryTotal(b *testing.B) {
	for i := 0; i < b.N; i++ {
		q := DBD.Query(testBenchItemsNs).Limit(20).WhereInt("year", reindexer.GT, 2020).ReqTotal()
		q.MustExec().FetchAll()
	}
}

func BenchmarkSimpleByIdQuery(b *testing.B) {
	rand.Seed(*benchmarkSeed)
	for i := 0; i < b.N; i++ {
		q := DBD.Query(testBenchItemsSimpleNs).WhereInt("id", reindexer.EQ, mkID(rand.Int()%50))
		q.Exec().FetchOne()
	}
}

func BenchmarkSimpleByIdUnsafeQuery(b *testing.B) {
	rand.Seed(*benchmarkSeed)
	for i := 0; i < b.N; i++ {
		q := DBD.Query(testBenchItemsSimpleNs).WhereInt("id", reindexer.EQ, mkID(rand.Int()%50))
		it := q.Exec().AllowUnsafe(true)
		it.FetchOne()
	}
}

func BenchmarkSimpleByIdJsonQuery(b *testing.B) {
	rand.Seed(*benchmarkSeed)
	for i := 0; i < b.N; i++ {
		q := DBD.Query(testBenchItemsSimpleNs).WhereInt("id", reindexer.EQ, mkID(rand.Int()%50))
		q.GetJson()
	}
}

func BenchmarkByIdQuery(b *testing.B) {
	rand.Seed(*benchmarkSeed)
	for i := 0; i < b.N; i++ {
		q := DBD.Query(testBenchItemsNs).WhereInt("id", reindexer.EQ, mkID(rand.Int()%50))
		q.Exec().FetchOne()
	}
}

func BenchmarkByIdComplexQuery(b *testing.B) {
	rand.Seed(*benchmarkSeed)
	for i := 0; i < b.N; i++ {
		q := DBD.Query("test_items_encdec").WhereInt("id", reindexer.EQ, mkID(rand.Int()%50))
		q.Exec().FetchOne()
	}
}

func BenchmarkByIdUnsafeQuery(b *testing.B) {
	rand.Seed(*benchmarkSeed)
	for i := 0; i < b.N; i++ {
		q := DBD.Query(testBenchItemsNs).WhereInt("id", reindexer.EQ, mkID(rand.Int()%50))
		q.Exec().AllowUnsafe(true).FetchOne()
	}
}

func BenchmarkByIdJsonQuery(b *testing.B) {
	rand.Seed(*benchmarkSeed)
	for i := 0; i < b.N; i++ {
		q := DBD.Query(testBenchItemsNs).WhereInt("id", reindexer.EQ, mkID(rand.Int()%50))
		q.GetJson()
	}
}

func BenchmarkFullScan(b *testing.B) {
	rand.Seed(*benchmarkSeed)
	for i := 0; i < b.N; i++ {
		q := DBD.Query(testBenchItemsNs).Limit(20).ReqTotal().
			WhereInt("end_time", reindexer.GT, rand.Int()%10000)
		q.MustExec().FetchAll()
	}
}

func BenchmarkSelectByPKAndUpdate(b *testing.B) {
	rand.Seed(*benchmarkSeed)
	for i := 0; i < b.N; i++ {
		FillTestItemsBench(i, 1, 10)
		DBD.Query(testBenchItemsNs).WhereInt("id", reindexer.EQ, mkID(rand.Int()%100000)).Limit(1).GetJson()
	}
}

func BenchmarkSelectByIdxAndUpdate(b *testing.B) {
	for i := 0; i < b.N; i++ {
		FillTestItemsBench(i, 1, 10)
		DBD.Query(testBenchItemsNs).WhereInt("year", reindexer.EQ, 2010).Limit(1).GetJson()
	}
}

func generateBenchKnnParams(indexType string) reindexer.KnnSearchParam {
	if indexType == "hnsw" {
		hnswSearchParams, err := reindexer.NewIndexHnswSearchParam(kBenchKnnK, reindexer.BaseKnnSearchParam{}.SetK(kBenchKnnK))
		if err != nil {
			panic(fmt.Sprintf("Cannot generate knn search params for index %s", indexType))
		}
		return hnswSearchParams
	} else if indexType == "ivf" {
		ivfSearchParams, err := reindexer.NewIndexIvfSearchParam(10, reindexer.BaseKnnSearchParam{}.SetK(kBenchKnnK))
		if err != nil {
			panic(fmt.Sprintf("Cannot generate knn search params for index %s", indexType))
		}
		return ivfSearchParams
	} else {
		panic(fmt.Sprintf("Unknown fv index type: %s", indexType))
	}
}

func benchmarkKnn(b *testing.B, indexType string, metric string) {
	ns := knnBenchNsName(indexType, metric)
	knnParams := generateBenchKnnParams(indexType)
	for i := 0; i < b.N; i++ {
		DBD.Query(ns).
			WhereKnn("vect", randVect(kBenchFloatVectorDimension), knnParams).
			MustExec().
			FetchAll()
	}
}

func BenchmarkKnnHnswIP(b *testing.B) {
	benchmarkKnn(b, "hnsw", "inner_product")
}

func BenchmarkKnnHnswCosine(b *testing.B) {
	benchmarkKnn(b, "hnsw", "cosine")
}

func BenchmarkKnnHnswL2(b *testing.B) {
	benchmarkKnn(b, "hnsw", "l2")
}

func BenchmarkKnnIvfIP(b *testing.B) {
	benchmarkKnn(b, "ivf", "inner_product")
}

func BenchmarkKnnIvfCosine(b *testing.B) {
	benchmarkKnn(b, "ivf", "cosine")
}

func BenchmarkKnnIvfL2(b *testing.B) {
	benchmarkKnn(b, "ivf", "l2")
}

func benchmarkKnnWithVectors(b *testing.B, indexType string, metric string) {
	ns := knnBenchNsName(indexType, metric)
	knnParams := generateBenchKnnParams(indexType)
	for i := 0; i < b.N; i++ {
		DBD.Query(ns).
			SelectAllFields().
			WhereKnn("vect", randVect(kBenchFloatVectorDimension), knnParams).
			MustExec().
			FetchAll()
	}
}

func BenchmarkKnnHnswIPWithVectors(b *testing.B) {
	benchmarkKnnWithVectors(b, "hnsw", "inner_product")
}

func BenchmarkKnnHnswCosineWithVectors(b *testing.B) {
	benchmarkKnnWithVectors(b, "hnsw", "cosine")
}

func BenchmarkKnnHnswL2WithVectors(b *testing.B) {
	benchmarkKnnWithVectors(b, "hnsw", "l2")
}

func BenchmarkKnnIvfIPWithVectors(b *testing.B) {
	benchmarkKnnWithVectors(b, "ivf", "inner_product")
}

func BenchmarkKnnIvfCosineWithVectors(b *testing.B) {
	benchmarkKnnWithVectors(b, "ivf", "cosine")
}

func BenchmarkKnnIvfL2WithVectors(b *testing.B) {
	benchmarkKnnWithVectors(b, "ivf", "l2")
}

func benchmarkFloatVectorInsert(b *testing.B, indexType string, metric string) {
	ns := knnBenchNsName(indexType, metric)
	for i := 0; i < b.N; i++ {
		_, err := DBD.Insert(ns, newKnnItem(i + kBenchKnnNsSize))
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkInsertHnswIP(b *testing.B) {
	benchmarkFloatVectorInsert(b, "hnsw", "inner_product")
}

func BenchmarkInsertHnswCosine(b *testing.B) {
	benchmarkFloatVectorInsert(b, "hnsw", "cosine")
}

func BenchmarkInsertHnswL2(b *testing.B) {
	benchmarkFloatVectorInsert(b, "hnsw", "l2")
}

func BenchmarkInsertIvfIP(b *testing.B) {
	benchmarkFloatVectorInsert(b, "ivf", "inner_product")
}

func BenchmarkInsertIvfCosine(b *testing.B) {
	benchmarkFloatVectorInsert(b, "ivf", "cosine")
}

func BenchmarkInsertIvfL2(b *testing.B) {
	benchmarkFloatVectorInsert(b, "ivf", "l2")
}
