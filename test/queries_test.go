package reindexer

import (
	"fmt"
	"log"
	"math/rand"
	"testing"

	"github.com/restream/reindexer"
	"github.com/restream/reindexer/dsl"
)

type TestItem struct {
	Prices      []*TestJoinItem `reindex:"prices,,joined"`
	Pricesx     []*TestJoinItem `reindex:"pricesx,,joined"`
	ID          int             `reindex:"id,-"`
	Genre       int64           `reindex:"genre,tree"`
	Year        int             `reindex:"year,tree"`
	Packages    []int           `reindex:"packages,hash"`
	Name        string          `reindex:"name,tree"`
	Countries   []string        `reindex:"countries,tree"`
	Age         int             `reindex:"age,hash"`
	Description string          `reindex:"description,fuzzytext"`
	Rate        float64         `reindex:"rate,tree"`
	IsDeleted   bool            `reindex:"isdeleted,-"`
	Actor       Actor           `reindex:"actor"`
	PricesIDs   []int           `reindex:"price_id"`
	LocationID  string          `reindex:"location"`
	EndTime     int             `reindex:"end_time,-"`
	StartTime   int             `reindex:"start_time,tree"`
	Tmp         string          `reindex:"tmp,-"`
	_           struct{}        `reindex:"id+tmp,,composite,pk"`
	_           struct{}        `reindex:"age+genre,,composite"`
	_           struct{}        `reindex:"location+rate,,composite"`
}

type TestItemSimple struct {
	ID    int    `reindex:"id,,pk"`
	Year  int    `reindex:"year,tree"`
	Name  string `reindex:"name"`
	Phone string
}

type TestItemSimpleCmplxPK struct {
	ID    int    `reindex:"id,,pk"`
	Year  int    `reindex:"year,tree"`
	Name  string `reindex:"name"`
	SubID string `reindex:"subid,,pk"`
}

func init() {
	tnamespaces["test_items_simple"] = TestItemSimple{}
	tnamespaces["test_items_simple_cmplx_pk"] = TestItemSimpleCmplxPK{}
	tnamespaces["test_items"] = TestItem{}
	tnamespaces["test_items_not"] = TestItemSimple{}
	tnamespaces["test_items_delete_query"] = TestItem{}
}

func FillTestItemsForNot() {
	tx := newTestTx(DB, "test_items_not")

	if err := tx.Upsert(&TestItemSimple{
		ID:   1,
		Year: 2001,
		Name: "blabla",
	}); err != nil {
		panic(err)
	}
	if err := tx.Upsert(&TestItemSimple{
		ID:   2,
		Year: 2002,
		Name: "sss",
	}); err != nil {
		panic(err)
	}
	tx.MustCommit(nil)

}

func newTestItem(id int, pkgsCount int) *TestItem {
	startTime := rand.Int() % 50000
	return &TestItem{
		ID:          mkID(id),
		Year:        rand.Int()%50 + 2000,
		Genre:       int64(rand.Int() % 50),
		Name:        randString(),
		Age:         rand.Int() % 5,
		Description: randString(),
		Packages:    randIntArr(pkgsCount, 10000, 50),
		Rate:        float64(rand.Int()%100) / 10,
		IsDeleted:   rand.Int()%2 != 0,
		PricesIDs:   randIntArr(10, 7000, 50),
		LocationID:  randLocation(),
		StartTime:   startTime,
		EndTime:     startTime + (rand.Int()%5)*1000,
		Actor: Actor{
			Name: randString(),
		},
	}
}

func FillTestItemsTx(start int, count int, pkgsCount int, tx *txTest) {
	for i := 0; i < count; i++ {
		if err := tx.Upsert(newTestItem(start+i, pkgsCount)); err != nil {
			panic(err)
		}
	}
}

func FillTestItems(ns string, start int, count int, pkgsCount int) {
	tx := newTestTx(DB, ns)
	FillTestItemsTx(start, count, pkgsCount, tx)
	tx.MustCommit(nil)
}

func TestQueries(t *testing.T) {

	log.Printf("Seeding test items")
	FillTestItems("test_items", 0, 2500, 20)
	FillTestItems("test_items", 2500, 2500, 0)
	FillTestItemsForNot()

	if err := DB.CloseNamespace("test_items"); err != nil {
		panic(err)
	}

	if err := DB.OpenNamespace("test_items", reindexer.DefaultNamespaceOptions(), TestItem{}); err != nil {
		panic(err)
	}

	CheckTestItemsJsonQueries()

	CheckAggregateQueries()

	CheckTestItemsQueries()
	CheckTestItemsSQLQueries()
	CheckTestItemsDSLQueries()

	// Delete test
	tx := newTestTx(DB, "test_items")
	for i := 0; i < 4000; i++ {
		if err := tx.Delete(TestItem{ID: mkID(i)}); err != nil {
			panic(err)
		}
	}
	// Check insert after delete
	FillTestItemsTx(0, 500, 0, tx)
	//Check second update
	FillTestItemsTx(0, 1000, 5, tx)

	for i := 0; i < 5000; i++ {
		tx.Delete(TestItem{ID: mkID(i)})
	}

	// Stress test delete & update & insert
	for i := 0; i < 5000; i++ {
		tx.Delete(TestItem{ID: mkID(rand.Int() % 500)})
		FillTestItemsTx(rand.Int()%500, 1, 0, tx)
		tx.Delete(TestItem{ID: mkID(rand.Int() % 500)})
		FillTestItemsTx(rand.Int()%500, 1, 10, tx)
		if (i % 1000) == 0 {
			tx.Commit(nil)
			tx = newTestTx(DB, "test_items")
		}
	}

	tx.Commit(nil)

	FillTestItems("test_items", 3000, 1000, 0)
	FillTestItems("test_items", 4000, 500, 20)
	CheckTestItemsQueries()
	CheckTestItemsSQLQueries()
	CheckTestItemsDSLQueries()

}

func CheckAggregateQueries() {

	limit := 100
	q := DB.Query("test_items").Where("genre", reindexer.EQ, 10).Limit(limit).Aggregate("year", reindexer.AggAvg).Aggregate("year", reindexer.AggSum)
	it := q.Exec()

	qcheck := DB.Query("test_items").Where("genre", reindexer.EQ, 10).Limit(limit)
	res, err := qcheck.Exec().FetchAll()
	if err != nil {
		panic(err)
	}
	var sum float64

	for _, it := range res {
		sum += float64(it.(*TestItem).Year)
	}
	if sum != it.GetAggreatedValue(1) {
		panic(fmt.Errorf("%f != %f", sum, it.GetAggreatedValue(1)))
	}
	if sum/float64(len(res)) != it.GetAggreatedValue(0) {
		panic(fmt.Errorf("%f != %f,len=%d", sum/float64(len(res)), it.GetAggreatedValue(0), len(res)))
	}
}

func CheckTestItemsJsonQueries() {
	json, _ := DB.Query("test_items").Select("ID", "Genre").Limit(3).ReqTotal("total_count").ExecToJson("test_items").FetchAll()
	//	fmt.Println(string(json))
	_ = json

	json2, _ := DB.Query("test_items").Select("ID", "Genre").Limit(3).ReqTotal("total_count").GetJson()
	//	fmt.Println(string(json2))
	_ = json2
	// TODO

}

func CheckTestItemsQueries() {
	sortIdxs := []string{"", "name", "year", "rate"}
	distinctIdxs := []string{"", "year", "rate"}

	for _, sortOrder := range []bool{true, false} {
		for _, sort := range sortIdxs {
			for _, distinct := range distinctIdxs {
				log.Printf("DISTINCT '%s' SORT '%s' DESC %v\n", distinct, sort, sortOrder)
				// Just take all items from namespace
				newTestQuery(DB, "test_items").Distinct(distinct).Sort(sort, sortOrder).Limit(1).ExecAndVerify()

				// Take items with single condition
				newTestQuery(DB, "test_items").Where("genre", reindexer.EQ, rand.Int()%50).Distinct(distinct).Sort(sort, sortOrder).ExecAndVerify()
				newTestQuery(DB, "test_items").Where("name", reindexer.EQ, randString()).Distinct(distinct).Sort(sort, sortOrder).ExecAndVerify()
				newTestQuery(DB, "test_items").Where("rate", reindexer.EQ, float32(rand.Int()%100)/10).Distinct(distinct).Sort(sort, sortOrder).ExecAndVerify()

				newTestQuery(DB, "test_items").Where("genre", reindexer.GT, rand.Int()%50).Distinct(distinct).Sort(sort, sortOrder).Debug(reindexer.TRACE).ExecAndVerify()
				newTestQuery(DB, "test_items").Where("name", reindexer.GT, randString()).Distinct(distinct).Sort(sort, sortOrder).ExecAndVerify()
				newTestQuery(DB, "test_items").Where("rate", reindexer.GT, float32(rand.Int()%100)/10).Distinct(distinct).Sort(sort, sortOrder).ExecAndVerify()

				newTestQuery(DB, "test_items").Where("genre", reindexer.LT, rand.Int()%50).Distinct(distinct).Sort(sort, sortOrder).ExecAndVerify()
				newTestQuery(DB, "test_items").Where("name", reindexer.LT, randString()).ExecAndVerify()
				newTestQuery(DB, "test_items").Where("rate", reindexer.LT, float32(rand.Int()%100)/10).Distinct(distinct).Sort(sort, sortOrder).ExecAndVerify()

				newTestQuery(DB, "test_items").Where("genre", reindexer.RANGE, []int{rand.Int() % 100, rand.Int() % 100}).Distinct(distinct).Sort(sort, sortOrder).ExecAndVerify()
				newTestQuery(DB, "test_items").Where("name", reindexer.RANGE, []string{randString(), randString()}).Distinct(distinct).Sort(sort, sortOrder).ExecAndVerify()
				newTestQuery(DB, "test_items").Where("rate", reindexer.RANGE, []float32{float32(rand.Int()%100) / 10, float32(rand.Int()%100) / 10}).Distinct(distinct).Sort(sort, sortOrder).ExecAndVerify()

				newTestQuery(DB, "test_items").Where("packages", reindexer.SET, randIntArr(10, 10000, 50)).Distinct(distinct).Sort(sort, sortOrder).ExecAndVerify()
				newTestQuery(DB, "test_items").Where("packages", reindexer.EMPTY, 0).Distinct(distinct).Sort(sort, sortOrder).ExecAndVerify()
				newTestQuery(DB, "test_items").Where("packages", reindexer.ANY, 0).Distinct(distinct).Sort(sort, sortOrder).ExecAndVerify()

				newTestQuery(DB, "test_items").Where("isdeleted", reindexer.EQ, true).Distinct(distinct).Sort(sort, sortOrder).ExecAndVerify()

				// Complex queires
				newTestQuery(DB, "test_items").Distinct(distinct).Sort(sort, sortOrder).ReqTotal().
					Where("genre", reindexer.EQ, 5). // composite index age+genre
					Where("age", reindexer.EQ, 3).
					Where("year", reindexer.GT, 2010).
					Where("packages", reindexer.SET, randIntArr(5, 10000, 50)).Debug(reindexer.TRACE).
					ExecAndVerify()

				newTestQuery(DB, "test_items").Distinct(distinct).Sort(sort, sortOrder).ReqTotal().
					Where("year", reindexer.GT, 2002).
					Where("genre", reindexer.EQ, 4). // composite index age+genre, and extra and + or conditions
					Where("age", reindexer.EQ, 3).
					Where("isdeleted", reindexer.EQ, true).Or().Where("year", reindexer.GT, 2001).
					Where("packages", reindexer.SET, randIntArr(5, 10000, 50)).Debug(reindexer.TRACE).
					ExecAndVerify()

				newTestQuery(DB, "test_items").Distinct(distinct).Sort(sort, sortOrder).ReqTotal().
					Where("age", reindexer.SET, []int{1, 2, 3, 4}).
					Where("id", reindexer.EQ, mkID(rand.Int()%5000)).
					Where("tmp", reindexer.EQ, ""). // composite pk with store index
					Where("isdeleted", reindexer.EQ, true).Or().Where("year", reindexer.GT, 2001).Debug(reindexer.TRACE).
					ExecAndVerify()

				newTestQuery(DB, "test_items").Distinct(distinct).Sort(sort, sortOrder).ReqTotal().
					Where("genre", reindexer.SET, []int{5, 1, 7}).
					Where("year", reindexer.LT, 2010).Or().Where("genre", reindexer.EQ, 3).
					Where("packages", reindexer.SET, randIntArr(5, 10000, 50)).Or().Where("packages", reindexer.EMPTY, 0).Debug(reindexer.TRACE).
					ExecAndVerify()

				newTestQuery(DB, "test_items").Distinct(distinct).Sort(sort, sortOrder).ReqTotal().
					Where("genre", reindexer.SET, []int{5, 1, 7}).
					Where("year", reindexer.LT, 2010).Or().Where("packages", reindexer.ANY, 0).
					Where("packages", reindexer.SET, randIntArr(5, 10000, 50)).Debug(reindexer.TRACE).
					ExecAndVerify()

				newTestQuery(DB, "test_items").Distinct(distinct).Sort(sort, sortOrder).ReqTotal().
					Where("genre", reindexer.EQ, 5).Or().Where("genre", reindexer.EQ, 6).
					Where("year", reindexer.RANGE, []int{2001, 2020}).
					Where("packages", reindexer.SET, randIntArr(5, 10000, 50)).
					ExecAndVerify()

				newTestQuery(DB, "test_items").Distinct(distinct).Sort(sort, sortOrder).ReqTotal().
					Where("actor.name", reindexer.EQ, randString()).
					ExecAndVerify()

				newTestQuery(DB, "test_items").Distinct(distinct).Sort(sort, sortOrder).ReqTotal().Debug(reindexer.TRACE).
					Not().Where("genre", reindexer.EQ, 5).
					Where("year", reindexer.RANGE, []int{2001, 2020}).
					Where("packages", reindexer.SET, randIntArr(5, 10000, 50)).
					ExecAndVerify()

				newTestQuery(DB, "test_items").Distinct(distinct).Sort(sort, sortOrder).ReqTotal().Debug(reindexer.TRACE).
					Where("genre", reindexer.EQ, 5).
					Not().Where("year", reindexer.RANGE, []int{2001, 2020}).
					Where("packages", reindexer.SET, randIntArr(5, 10000, 50)).
					ExecAndVerify()

				newTestQuery(DB, "test_items").Distinct(distinct).Sort(sort, sortOrder).ReqTotal().Debug(reindexer.TRACE).
					Not().Where("genre", reindexer.EQ, 10).
					ExecAndVerify()

				newTestQuery(DB, "test_items_not").ReqTotal().
					Where("name", reindexer.EQ, "blabla").
					ExecAndVerify()

				newTestQuery(DB, "test_items_not").ReqTotal().
					Where("year", reindexer.EQ, 2002).
					ExecAndVerify()

				newTestQuery(DB, "test_items_not").ReqTotal().
					Where("year", reindexer.EQ, 2002).
					Not().Where("name", reindexer.EQ, "blabla").
					ExecAndVerify()

				newTestQuery(DB, "test_items_not").ReqTotal().
					Where("name", reindexer.EQ, "blabla").
					Not().Where("year", reindexer.EQ, 2002).
					ExecAndVerify()

				newTestQuery(DB, "test_items_not").ReqTotal().
					Where("name", reindexer.EQ, "blabla").
					Not().Where("year", reindexer.EQ, 2001).
					ExecAndVerify()

				newTestQuery(DB, "test_items_not").ReqTotal().
					Where("year", reindexer.EQ, 2002).
					Not().Where("name", reindexer.EQ, "sss").
					ExecAndVerify()

				compositeValues := []interface{}{[]interface{}{rand.Int() % 10, int64(rand.Int() % 50)}}

				newTestQuery(DB, "test_items").Distinct(distinct).Sort(sort, sortOrder).ReqTotal().
					Where("age+genre", reindexer.EQ, compositeValues).
					ExecAndVerify()

				newTestQuery(DB, "test_items").Distinct(distinct).Sort(sort, sortOrder).ReqTotal().
					Where("age+genre", reindexer.LE, compositeValues).
					ExecAndVerify()

				newTestQuery(DB, "test_items").Distinct(distinct).Sort(sort, sortOrder).ReqTotal().
					Where("age+genre", reindexer.LT, compositeValues).
					ExecAndVerify()

				newTestQuery(DB, "test_items").Distinct(distinct).Sort(sort, sortOrder).ReqTotal().
					Where("age+genre", reindexer.GT, compositeValues).
					ExecAndVerify()

				newTestQuery(DB, "test_items").Distinct(distinct).Sort(sort, sortOrder).ReqTotal().
					Where("age+genre", reindexer.GE, compositeValues).
					ExecAndVerify()

				compositeValues = []interface{}{[]interface{}{randLocation(), float64(rand.Int()%100) / 10}}

				newTestQuery(DB, "test_items").Distinct(distinct).Sort(sort, sortOrder).ReqTotal().
					Where("location+rate", reindexer.EQ, compositeValues).
					ExecAndVerify()

				newTestQuery(DB, "test_items").Distinct(distinct).Sort(sort, sortOrder).ReqTotal().
					Where("location+rate", reindexer.GT, compositeValues).
					ExecAndVerify()

				newTestQuery(DB, "test_items").Distinct(distinct).Sort(sort, sortOrder).ReqTotal().
					Where("location+rate", reindexer.LT, compositeValues).
					ExecAndVerify()

				compositeValues = []interface{}{
					[]interface{}{randLocation(), float64(rand.Int()%100) / 10},
					[]interface{}{randLocation(), float64(rand.Int()%100) / 10},
				}
				newTestQuery(DB, "test_items").Distinct(distinct).Sort(sort, sortOrder).ReqTotal().
					Where("location+rate", reindexer.RANGE, compositeValues).
					ExecAndVerify()

				compositeValues = []interface{}{
					[]interface{}{rand.Int() % 10, int64(rand.Int() % 50)},
					[]interface{}{rand.Int() % 10, int64(rand.Int() % 50)},
					[]interface{}{rand.Int() % 10, int64(rand.Int() % 50)},
					[]interface{}{rand.Int() % 10, int64(rand.Int() % 50)},
					[]interface{}{rand.Int() % 10, int64(rand.Int() % 50)},
					[]interface{}{rand.Int() % 10, int64(rand.Int() % 50)},
					[]interface{}{rand.Int() % 10, int64(rand.Int() % 50)},
					[]interface{}{rand.Int() % 10, int64(rand.Int() % 50)},
				}
				newTestQuery(DB, "test_items").Distinct(distinct).Sort(sort, sortOrder).ReqTotal().
					Where("age+genre", reindexer.SET, compositeValues).
					ExecAndVerify()

			}
		}
	}
}

func CheckTestItemsSQLQueries() {
	if res, err := DB.ExecSQL("SELECT ID,Year,Genre FROM test_items WHERE year > '2016' AND genre IN ('1',2,'3') ORDER BY year DESC LIMIT 10000000 ; ").FetchAll(); err != nil {
		panic(err)
	} else {
		newTestQuery(DB, "test_items").Where("year", reindexer.GT, 2016).Where("genre", reindexer.SET, []int{1, 2, 3}).Sort("year", true).Verify(res, false)
	}

	if res, err := DB.ExecSQL("SELECT * FROM test_items WHERE year <= '2016' OR genre < 5 ORDER BY year ASC ").FetchAll(); err != nil {
		panic(err)
	} else {
		newTestQuery(DB, "test_items").Where("year", reindexer.LE, 2016).Or().Where("genre", reindexer.LT, 5).Sort("year", false).Verify(res, true)
	}

	if res, err := DB.ExecSQL("SELECT COUNT(*), * FROM test_items WHERE year >= '2016' OR rate = '1.1' OR year RANGE (2010,2014)").FetchAll(); err != nil {
		panic(err)
	} else {
		newTestQuery(DB, "test_items").Where("year", reindexer.GE, 2016).Or().Where("rate", reindexer.EQ, 1.1).Or().Where("year", reindexer.RANGE, []int{2010, 2014}).Verify(res, true)
	}
}

func CheckTestItemsDSLQueries() {
	d := dsl.DSL{
		Namespace: "test_items",
		Filters: []dsl.Filter{
			{
				Field: "year",
				Cond:  "GT",
				Value: "2016",
			},
			{
				Field: "genre",
				Cond:  "SET",
				Value: []string{"1", "2", "3"},
			},
			{
				Field: "packages",
				Cond:  "ANY",
				Value: 0,
			},
			{
				Field: "countries",
				Cond:  "EMPTY",
				Value: 0,
			},
			{
				Field: "isdeleted",
				Cond:  "EQ",
				Value: true,
			},
		},
		Sort: dsl.Sort{
			Field: "year",
			Desc:  true,
		},
	}

	if q, err := DB.QueryFrom(d); err != nil {
		panic(err)
	} else if res, err := q.Exec().FetchAll(); err != nil {
		panic(err)
	} else {
		newTestQuery(DB, "test_items").
			Where("year", reindexer.GT, 2016).
			Where("genre", reindexer.SET, []int{1, 2, 3}).
			Where("packages", reindexer.ANY, 0).
			Where("countries", reindexer.EMPTY, 0).
			Where("isdeleted", reindexer.EQ, true).
			Sort("year", true).
			Verify(res, true)
	}
}

func TestDeleteQuery(t *testing.T) {
	err := DB.Upsert("test_items_delete_query", newTestItem(1000, 5))

	if err != nil {
		panic(err)
	}

	count, err := DB.Query("test_items_delete_query").Where("id", reindexer.EQ, mkID(1000)).Delete()
	if err != nil {
		panic(err)
	}

	if count != 1 {
		panic(fmt.Errorf("Expected delete query return 1 item"))
	}

	_, ok := DB.Query("test_items_delete_query").Where("id", reindexer.EQ, mkID(1000)).Get()
	if ok {
		panic(fmt.Errorf("Item was found after delete query, but will be deleted"))
	}

}
