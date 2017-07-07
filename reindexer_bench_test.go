package reindexer

import (
	"log"
	"math/rand"
	"testing"
)

type TestItemBench struct {
	Prices  []*TestJoinItem `reindex:"prices,,joined"`
	Pricesx []*TestJoinItem `reindex:"pricesx,,joined"`
	//	TestItemBase
	ID         int      `reindex:"id,,pk"`
	Genre      int64    `reindex:"genre,tree"`
	Year       int      `reindex:"year,tree"`
	Packages   []int    `reindex:"packages,hash"`
	Countries  []string `reindex:"countries,tree"`
	Age        int      `reindex:"age,hash"`
	PricesIDs  []int    `reindex:"price_id"`
	LocationID string   `reindex:"location"`
	EndTime    int      `reindex:"end_time,-"`
	StartTime  int      `reindex:"start_time,tree"`
}

type TestJoinCtx struct {
	allPrices []*TestJoinItem
}

func (item *TestItemBench) ClonePtrSlice(src []interface{}) []interface{} {
	objSlice := make([]TestItemBench, 0, len(src))
	for i := 0; i < len(src); i++ {
		objSlice = append(objSlice, *src[i].(*TestItemBench))
	}

	for i := 0; i < len(src); i++ {
		src[i] = &objSlice[i]
	}
	return src
}

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

var pkgs = make([][]int, 0)
var priceIds = make([][]int, 0)

func init() {
	for i := 0; i < 10; i++ {
		pkgs = append(pkgs, randIntArr(20, 10000, 10))
	}
	for i := 0; i < 20; i++ {
		priceIds = append(priceIds, randIntArr(10, 7000, 50))
	}
}

func BenchmarkPrepare(b *testing.B) {
	FillTestItemsBench(0, 500000, 10)
	// force commit and make sort orders
	DB.Query("test_items_bench").Where("year", EQ, 1).Sort("year", false).Limit(1).MustExec()

	for i := 0; i < len(pkgs)*3; i++ {
		DB.Query("test_items_bench").Limit(20).Sort("start_time", false).
			Where("packages", SET, pkgs[i%len(pkgs)]).
			MustExec()
		DB.Query("test_items_bench").Limit(20).Sort("year", false).
			Where("packages", SET, pkgs[i%len(pkgs)]).
			MustExec()
		DB.Query("test_items_bench").Limit(20).
			Where("year", RANGE, []int{2010, 2016}).
			MustExec()
	}
	for i := 0; i < len(priceIds)*3; i++ {
		DB.Query("test_join_items").Limit(20).
			Where("id", SET, priceIds[i%len(priceIds)]).
			MustExec()
	}
}

func BenchmarkSimpleInsert(b *testing.B) {
	tx := DB.MustBeginTx("test_items_simple")
	for i := 0; i < b.N; i++ {
		if err := tx.Upsert(&TestItemSimple{ID: i, Year: rand.Int()%1000 + 10, Name: randString()}); err != nil {
			panic(err)
		}
	}
	tx.MustCommit(nil)
}

func BenchmarkSimpleUpdate(b *testing.B) {
	tx := DB.MustBeginTx("test_items_simple")
	for i := 0; i < b.N; i++ {
		if err := tx.Upsert(&TestItemSimple{ID: i, Year: rand.Int()%1000 + 10, Name: randString()}); err != nil {
			panic(err)
		}
	}
	tx.MustCommit(nil)
}

func BenchmarkSimpleCmplxPKUpsert(b *testing.B) {
	tx := DB.MustBeginTx("test_items_simple_cmplx_pk")

	for i := 0; i < b.N; i++ {
		if err := tx.Upsert(&TestItemSimpleCmplxPK{ID: i, Year: rand.Int()%1000 + 10, Name: randString(), SubID: randString()}); err != nil {
			panic(err)
		}
	}
	tx.MustCommit(nil)
}

func BenchmarkInsert(b *testing.B) {
	FillTestItems(0, b.N, 20)
}

func BenchmarkUpdate(b *testing.B) {
	FillTestItems(0, b.N, 20)
}

func BenchmarkDeleteAndUpdate(b *testing.B) {
	tx := newTestTx(DB, "test_items")
	for i := 0; i < b.N; i++ {
		tx.Delete(TestItem{ID: mkID(rand.Int() % b.N)})
		FillTestItemsTx(i, 1, 20, tx)
	}
	tx.Commit(nil)
}

func Benchmark4CondQuery(b *testing.B) {

	for i := 0; i < b.N; i++ {
		q := DB.Query("test_items_bench").Limit(20).
			WhereInt("genre", EQ, 5).
			WhereString("age", EQ, "2").
			WhereInt("year", RANGE, 2010, 2016).
			WhereInt("packages", SET, pkgs[rand.Int()%len(pkgs)]...)
		q.MustExec().PtrSlice()
		q.Close()
	}
}

func Benchmark4CondQueryTotal(b *testing.B) {

	for i := 0; i < b.N; i++ {
		q := DB.Query("test_items_bench").Limit(20).ReqTotal().
			WhereInt("genre", EQ, 5).
			WhereString("age", EQ, "2").
			WhereInt("year", RANGE, 2010, 2016).
			WhereInt("packages", SET, pkgs[rand.Int()%len(pkgs)]...)

		q.MustExec().PtrSlice()
		q.Close()
	}
}

func Benchmark4CondRangeQuery(b *testing.B) {
	for i := 0; i < b.N; i++ {
		startTime := rand.Int() % 50000
		endTime := startTime + 10000
		q := DB.Query("test_items_bench").Limit(20).
			WhereInt("genre", EQ, 5).
			WhereInt("year", RANGE, 2010, 2016).
			WhereInt("start_time", GT, startTime).
			WhereInt("end_time", LT, endTime)
		q.MustExec().PtrSlice()
		q.Close()

	}
}

func Benchmark4CondRangeQueryTotal(b *testing.B) {
	for i := 0; i < b.N; i++ {
		startTime := rand.Int() % 50000
		endTime := startTime + 10000
		q := DB.Query("test_items_bench").Limit(20).
			WhereInt("genre", EQ, 5).
			WhereInt("year", RANGE, 2010, 2016).
			WhereInt("start_time", GT, startTime).
			WhereInt("end_time", LT, endTime).ReqTotal()

		q.MustExec().PtrSlice()
		q.Close()

	}
}

func Benchmark3CondQuery(b *testing.B) {

	for i := 0; i < b.N; i++ {
		q := DB.Query("test_items_bench").Limit(20).Sort("year", false).
			WhereInt("genre", EQ, 5).
			WhereInt("year", RANGE, 2010, 2016).
			WhereInt("packages", SET, pkgs[rand.Int()%len(pkgs)]...)
		q.MustExec().PtrSlice()
		q.Close()
	}
}

func Benchmark3CondQueryTotal(b *testing.B) {
	for i := 0; i < b.N; i++ {
		q := DB.Query("test_items_bench").Limit(20).Sort("year", false).ReqTotal().
			WhereInt("genre", EQ, 5).
			WhereInt("year", RANGE, 2010, 2016).
			WhereInt("packages", SET, pkgs[rand.Int()%len(pkgs)]...)
		q.MustExec().PtrSlice()
		q.Close()
	}
}

func Benchmark3CondQueryKillIdsCache(b *testing.B) {
	for i := 0; i < b.N; i++ {
		q := DB.Query("test_items_bench").Limit(20).Sort("year", false).
			WhereInt("genre", EQ, 5).
			WhereInt("year", RANGE, 2010, 2016).
			WhereInt("packages", SET, randIntArr(20, 10000, 10)...) // Using random array for each request. Test for cache performance

		q.MustExec().PtrSlice()
		q.Close()
	}
}

func Benchmark3CondQueryRestoreIdsCache(b *testing.B) {
	for i := 0; i < b.N; i++ {
		q := DB.Query("test_items_bench").Limit(20).Sort("year", false).
			WhereInt("genre", EQ, 5).
			WhereInt("year", RANGE, 2010, 2016).
			WhereInt("packages", SET, pkgs[rand.Int()%len(pkgs)]...) // Using subset of arrays for each request. Cache will restore
		q.MustExec().PtrSlice()
		q.Close()
	}
}

func Benchmark2CondQuery(b *testing.B) {
	for i := 0; i < b.N; i++ {
		q := DB.Query("test_items_bench").Limit(20).
			WhereInt("genre", EQ, 5).
			WhereInt("year", RANGE, 2010, 2016)
		q.MustExec().PtrSlice()
		q.Close()

	}
}

func Benchmark2CondQueryTotal(b *testing.B) {
	for i := 0; i < b.N; i++ {
		q := DB.Query("test_items_bench").Limit(20).Sort("year", false).ReqTotal().
			WhereInt("genre", EQ, 5).
			WhereInt("year", RANGE, 2010, 2016)
		q.MustExec().PtrSlice()
		q.Close()
	}
}

func Benchmark2CondQueryLeftJoin(b *testing.B) {
	ctx := &TestJoinCtx{}
	for i := 0; i < b.N; i++ {
		q2 := DB.Query("test_join_items").WhereString("device", EQ, "ottstb").WhereString("location", SET, "mos", "dv", "sib")
		q := DB.Query("test_items_bench").Limit(20).Sort("year", false).
			WhereInt("genre", EQ, 5).
			WhereInt("year", RANGE, 2010, 2016).
			InnerJoin(q2, "prices").On("price_id", SET, "id")
		ctx.allPrices = ctx.allPrices[:0]
		q.SetContext(ctx)
		q.MustExec().PtrSlice()
		q.Close()
	}
}

func Benchmark2CondQueryLeftJoinTotal(b *testing.B) {
	ctx := &TestJoinCtx{}
	for i := 0; i < b.N; i++ {

		q2 := DB.Query("test_join_items").WhereString("device", EQ, "ottstb").WhereString("location", SET, "mos", "dv", "sib")
		q := DB.Query("test_items_bench").Limit(20).Sort("year", false).ReqTotal().
			WhereInt("genre", EQ, 5).
			WhereInt("year", RANGE, 2010, 2016).
			Join(q2, "prices").On("price_id", SET, "id")
		ctx.allPrices = ctx.allPrices[:0]
		q.SetContext(ctx)
		q.MustExec().PtrSlice()
		q.Close()
	}
}

func Benchmark2CondQueryInnerJoin(b *testing.B) {
	ctx := &TestJoinCtx{}
	for i := 0; i < b.N; i++ {

		q2 := DB.Query("test_join_items").WhereString("device", EQ, "ottstb").WhereString("location", SET, "mos", "dv", "sib")
		q := DB.Query("test_items_bench").Limit(20).Sort("year", false).
			WhereInt("genre", EQ, 5).
			WhereInt("year", RANGE, 2010, 2016).
			InnerJoin(q2, "prices").On("price_id", SET, "id")
		ctx.allPrices = ctx.allPrices[:0]
		q.SetContext(ctx)
		q.MustExec().PtrSlice()
		q.Close()
	}
}

func Benchmark2CondQueryInnerJoinTotal(b *testing.B) {
	ctx := &TestJoinCtx{}
	for i := 0; i < b.N; i++ {

		q2 := DB.Query("test_join_items").WhereString("device", EQ, "ottstb").WhereString("location", SET, "mos", "dv", "sib")
		q := DB.Query("test_items_bench").Limit(20).Sort("year", false).ReqTotal().
			WhereInt("genre", EQ, 5).
			WhereInt("year", RANGE, 2010, 2016).
			InnerJoin(q2, "prices").On("price_id", SET, "id")
		ctx.allPrices = ctx.allPrices[:0]
		q.SetContext(ctx)
		q.MustExec().PtrSlice()
		q.Close()
	}
}

// func Benchmark2CondQueryPseudoJoin(b *testing.B) {
// 	for i := 0; i < b.N; i++ {

// 		res, _ := DB.Query("test_items_bench").Limit(20).Sort("year", false).
// 			WhereInt("genre", EQ, 5).
// 			WhereInt("year", RANGE, 2010, 2016).
// 			MustExec().PtrSlice()
// 		for _, item := range res {
// 			testItem := *item.(*TestItemBench)
// 			r2, _ := DB.Query("test_join_items").WhereString("device", EQ, "ottstb").
// 				WhereString("location", SET, "mos", "dv", "sib").WhereInt("id", SET, testItem.PricesIDs...).MustExec().PtrSlice()
// 			testItem.Prices = make([]*TestJoinItem, 0, len(r2))
// 			for _, subItem := range r2 {
// 				testItem.Prices = append(testItem.Prices, subItem.(*TestJoinItem))
// 			}
// 		}
// 	}
// }

func Benchmark1CondQuery(b *testing.B) {
	for i := 0; i < b.N; i++ {
		q := DB.Query("test_items_bench").Limit(20).WhereInt("year", GT, 2020)
		q.MustExec().PtrSlice()
		q.Close()
	}
}

func Benchmark1CondQueryTotal(b *testing.B) {
	for i := 0; i < b.N; i++ {
		q := DB.Query("test_items_bench").Limit(20).WhereInt("year", GT, 2020).ReqTotal()
		q.MustExec().PtrSlice()
		q.Close()
	}
}

func BenchmarkByIdQuery(b *testing.B) {
	for i := 0; i < b.N; i++ {
		q := DB.Query("test_items_bench").WhereInt("id", EQ, mkID(rand.Int()%50))
		q.Get()
	}
}

func BenchmarkFullScan(b *testing.B) {
	for i := 0; i < b.N; i++ {
		q := DB.Query("test_items_bench").Limit(20).ReqTotal().
			WhereInt("end_time", GT, rand.Int()%10000)
		q.MustExec().PtrSlice()
		q.Close()
	}
}

func FillTestItemsBench(start int, count int, pkgsCount int) {
	log.Printf("Filling %d items for bench", count)
	tx, _ := DB.BeginTx("test_items_bench")
	for i := 0; i < count; i++ {
		startTime := rand.Int() % 50000

		if err := tx.Upsert(&TestItemBench{
			ID:         mkID(start + i),
			Year:       rand.Int()%50 + 2000,
			Genre:      int64(rand.Int() % 50),
			Age:        rand.Int() % 5,
			Packages:   randIntArr(pkgsCount, 10000, 50),
			PricesIDs:  priceIds[rand.Int()%len(priceIds)],
			LocationID: randLocation(),
			StartTime:  startTime,
			EndTime:    startTime + (rand.Int()%5)*1000,
		}); err != nil {
			panic(err)
		}
	}
	tx.Commit(nil)
	FillTestJoinItems(7000, 500)
}
