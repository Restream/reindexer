package reindexer

import (
	"log"
	"math/rand"
	"os"
	"testing"

	"reflect"

	"fmt"

	"encoding/json"

	_ "github.com/restream/reindexer/bindings/builtin"
	"github.com/restream/reindexer/dsl"
	// _ "github.com/restream/reindexer/pprof"
)

var DB *Reindexer

func TestMain(m *testing.M) {
	DB = NewReindex("builtin")

	os.RemoveAll("/tmp/reindex_test/")
	DB.EnableStorage("/tmp/reindex_test/")
	if err := DB.CreateIndex("test_items_simple", TestItemSimple{}); err != nil {
		panic(err)
	} else if err := DB.CreateIndex("test_items_simple_cmplx_pk", TestItemSimpleCmplxPK{}); err != nil {
		panic(err)
	} else if err := DB.CreateIndex("test_items", TestItem{}); err != nil {
		panic(err)
	} else if err := DB.CreateIndex("test_items_bench", TestItemBench{}); err != nil {
		panic(err)
	} else if err := DB.CreateIndex("test_join_items", TestJoinItem{}); err != nil {
		panic(err)
	} else if err := DB.CreateIndex("test_items_not", TestItemSimple{}); err != nil {
		panic(err)
	}

	// Create namespaces for tests
	testData["test_items_simple"] = make(map[string]interface{}, 100)
	testData["test_items_simple_cmplx_pk"] = make(map[string]interface{}, 100)
	testData["test_items"] = make(map[string]interface{}, 100)
	testData["test_join_items"] = make(map[string]interface{}, 100)
	testData["test_items_not"] = make(map[string]interface{}, 100)

	retCode := m.Run()
	os.Exit(retCode)
}

type Actor struct {
	Name string `reindex:"actors.name"`
}

type TestJoinItem struct {
	ID       int    `reindex:"id,,pk"`
	Name     string `reindex:"name,tree"`
	Location string `reindex:"location"`
	Device   string `reindex:"device"`
	Price    int
}

type TestItemBase struct {
}

type TestItem struct {
	Prices  []*TestJoinItem `reindex:"prices,,joined"`
	Pricesx []*TestJoinItem `reindex:"pricesx,,joined"`
	//	TestItemBase
	ID          int      `reindex:"id,,pk"`
	Genre       int64    `reindex:"genre,tree"`
	Year        int      `reindex:"year,tree"`
	Packages    []int    `reindex:"packages,hash"`
	Name        string   `reindex:"name,tree"`
	Countries   []string `reindex:"countries,tree"`
	Age         int      `reindex:"age,hash"`
	Description string   `reindex:"description,fulltext"`
	Rate        float64  `reindex:"rate,tree"`
	IsDeleted   bool     `reindex:"isdeleted,tree"`
	Actors      []Actor
	PricesIDs   []int    `reindex:"price_id"`
	LocationID  string   `reindex:"location"`
	EndTime     int      `reindex:"end_time,-"`
	StartTime   int      `reindex:"start_time,tree"`
	Tmp         string   `reindex:"tmp,-,pk"`
	_           struct{} `reindex:"id+tmp,,composite"`
	_           struct{} `reindex:"age+genre,,composite"`
}

func (item *TestItem) Clone(src interface{}) interface{} {
	newItem := *src.(*TestItem)
	return &newItem
}

func (item *TestItem) Join(field string, subitems []interface{}, context interface{}) {
	switch field {
	case "prices":
		if item.Prices == nil {
			item.Prices = make([]*TestJoinItem, 0, len(subitems))
		}
		for _, srcItem := range subitems {
			item.Prices = append(item.Prices, srcItem.(*TestJoinItem))
		}
	case "pricesx":
		if item.Pricesx == nil {
			item.Pricesx = make([]*TestJoinItem, 0, len(subitems))
		}
		for _, srcItem := range subitems {
			item.Pricesx = append(item.Pricesx, srcItem.(*TestJoinItem))
		}
	}
}

type TestItemSimple struct {
	ID   int    `reindex:"id,,pk"`
	Year int    `reindex:"year,tree"`
	Name string `reindex:"name"`
}

type TestItemSimpleCmplxPK struct {
	ID    int    `reindex:"id,,pk"`
	Year  int    `reindex:"year,tree"`
	Name  string `reindex:"name"`
	SubID string `reindex:"subid,,pk"`
}

func mkID(i int) int {
	return i*17 + 8000000
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
func FillTestItemsTx(start int, count int, pkgsCount int, tx *txTest) {
	for i := 0; i < count; i++ {
		startTime := rand.Int() % 50000
		if err := tx.Upsert(&TestItem{
			ID:          mkID(start + i),
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
		}); err != nil {
			panic(err)
		}
	}
}

func FillTestItems(start int, count int, pkgsCount int) {
	tx := newTestTx(DB, "test_items")
	FillTestItemsTx(start, count, pkgsCount, tx)
	tx.MustCommit(nil)
}

func FillTestJoinItems(start int, count int) {
	tx := newTestTx(DB, "test_join_items")

	for i := 0; i < count; i++ {
		if err := tx.Upsert(&TestJoinItem{
			ID:       i + start,
			Name:     "price_" + randString(),
			Location: randLocation(),
			Device:   randDevice(),
		}); err != nil {
			panic(err)
		}
	}
	tx.MustCommit(nil)
}

func CheckTestItemsJoinQueries(left, inner, or bool) {
	qj1 := DB.Query("test_join_items").Where("device", EQ, "ottstb") //.Sort("name", false)
	qj2 := DB.Query("test_join_items").Where("device", EQ, "android")
	qj3 := DB.Query("test_join_items").Where("device", EQ, "iphone")

	qjoin := DB.Query("test_items").Where("genre", EQ, 10).Limit(10).Debug(TRACE)

	if left {
		qjoin.LeftJoin(qj1, "prices").On("price_id", SET, "id")
	}
	if inner {
		qjoin.InnerJoin(qj2, "pricesx").On("location", EQ, "location").On("price_id", SET, "id")

		if or {
			qjoin.Or()
		}
		qjoin.InnerJoin(qj3, "pricesx").On("location", LT, "location").On("price_id", SET, "id")
	}

	rjoin, _ := qjoin.MustExec().PtrSlice()

	// for _, rr := range rjoin {
	// 	item := rr.(*TestItem)
	// 	log.Printf("%#v %d -> %#d,%#d\n", item.PricesIDs, item.LocationID, len(item.Pricesx), len(item.Prices))
	// }

	// Verify join results with manual join
	r1, _ := DB.Query("test_items").Where("genre", EQ, 10).CopyResults().MustExec().PtrSlice()
	rjcheck := make([]interface{}, 0, 1000)

	for _, iitem := range r1 {

		item := iitem.(*TestItem)
		if left {
			rj1, _ := DB.Query("test_join_items").
				Where("device", EQ, "ottstb").
				Where("id", SET, item.PricesIDs).
				//				Sort("name", false).
				MustExec().PtrSlice()

			if len(rj1) != 0 {
				item.Prices = make([]*TestJoinItem, 0, len(rj1))
				for _, rrj := range rj1 {
					item.Prices = append(item.Prices, rrj.(*TestJoinItem))
				}
			}
		}

		if inner {
			rj2 := DB.Query("test_join_items").
				Where("device", EQ, "android").
				Where("id", SET, item.PricesIDs).
				Where("location", EQ, item.LocationID).
				MustExec()

			rj3 := DB.Query("test_join_items").
				Where("device", EQ, "iphone").
				Where("id", SET, item.PricesIDs).
				Where("location", LT, item.LocationID).MustExec()

			if (or && (rj2.Len() != 0 || rj3.Len() != 0)) || (!or && (rj2.Len() != 0 && rj3.Len() != 0)) {
				item.Pricesx = make([]*TestJoinItem, 0)
				for rj2.Next() {
					item.Pricesx = append(item.Pricesx, rj2.Ptr().(*TestJoinItem))
				}
				for rj3.Next() {
					item.Pricesx = append(item.Pricesx, rj3.Ptr().(*TestJoinItem))
				}
			} else {
				continue
			}

		}
		rjcheck = append(rjcheck, item)

		if len(rjcheck) == 10 {
			break
		}
	}

	if len(rjcheck) != len(rjoin) {
		panic(fmt.Errorf("%d != %d", len(rjcheck), len(rjoin)))
	}
	for i := 0; i < len(rjcheck); i++ {
		i1 := rjcheck[i].(*TestItem)
		i2 := rjoin[i].(*TestItem)
		if !reflect.DeepEqual(i1, i2) {
			i1s, _ := json.Marshal(i1)
			i2s, _ := json.Marshal(i2)

			panic(fmt.Errorf("-----expect:\n%s\n-----got:\n%s", string(i1s), string(i2s)))

		}
	}

}

func CheckTestItemsQueries() {
	sortIdxs := []string{"", "name", "year", "rate"}
	distinctIdxs := []string{"", "year", "rate"}

	for _, sortOrder := range []bool{true, false} {
		for _, sort := range sortIdxs {
			for _, distinct := range distinctIdxs {
				log.Printf("DISTINCT '%s' SORT '%s' DESC %v\n", distinct, sort, sortOrder)
				// Just take all items from namespace
				newTestQuery(DB, "test_items").Distinct(distinct).Sort(sort, sortOrder).ExecAndVerify()

				// Take items with single condition
				newTestQuery(DB, "test_items").Where("genre", EQ, rand.Int()%50).Distinct(distinct).Sort(sort, sortOrder).ExecAndVerify()
				newTestQuery(DB, "test_items").Where("name", EQ, randString()).Distinct(distinct).Sort(sort, sortOrder).ExecAndVerify()
				newTestQuery(DB, "test_items").Where("rate", EQ, float32(rand.Int()%100)/10).Distinct(distinct).Sort(sort, sortOrder).ExecAndVerify()

				newTestQuery(DB, "test_items").Where("genre", GT, rand.Int()%50).Distinct(distinct).Sort(sort, sortOrder).Debug(TRACE).ExecAndVerify()
				newTestQuery(DB, "test_items").Where("name", GT, randString()).Distinct(distinct).Sort(sort, sortOrder).ExecAndVerify()
				newTestQuery(DB, "test_items").Where("rate", GT, float32(rand.Int()%100)/10).Distinct(distinct).Sort(sort, sortOrder).ExecAndVerify()

				newTestQuery(DB, "test_items").Where("genre", LT, rand.Int()%50).Distinct(distinct).Sort(sort, sortOrder).ExecAndVerify()
				newTestQuery(DB, "test_items").Where("name", LT, randString()).ExecAndVerify()
				newTestQuery(DB, "test_items").Where("rate", LT, float32(rand.Int()%100)/10).Distinct(distinct).Sort(sort, sortOrder).ExecAndVerify()

				newTestQuery(DB, "test_items").Where("genre", RANGE, []int{rand.Int() % 100, rand.Int() % 100}).Distinct(distinct).Sort(sort, sortOrder).ExecAndVerify()
				newTestQuery(DB, "test_items").Where("name", RANGE, []string{randString(), randString()}).Distinct(distinct).Sort(sort, sortOrder).ExecAndVerify()
				newTestQuery(DB, "test_items").Where("rate", RANGE, []float32{float32(rand.Int()%100) / 10, float32(rand.Int()%100) / 10}).Distinct(distinct).Sort(sort, sortOrder).ExecAndVerify()

				newTestQuery(DB, "test_items").Where("packages", SET, randIntArr(10, 10000, 50)).Distinct(distinct).Sort(sort, sortOrder).ExecAndVerify()
				newTestQuery(DB, "test_items").Where("packages", EMPTY, 0).Distinct(distinct).Sort(sort, sortOrder).ExecAndVerify()
				newTestQuery(DB, "test_items").Where("packages", ANY, 0).Distinct(distinct).Sort(sort, sortOrder).ExecAndVerify()

				newTestQuery(DB, "test_items").Where("isdeleted", EQ, true).Distinct(distinct).Sort(sort, sortOrder).ExecAndVerify()

				// Complex queires
				newTestQuery(DB, "test_items").Distinct(distinct).Sort(sort, sortOrder).ReqTotal().
					Where("genre", EQ, 5). // composite index age+genre
					Where("age", EQ, 3).
					Where("year", GT, 2010).
					Where("packages", SET, randIntArr(5, 10000, 50)).Debug(TRACE).
					ExecAndVerify()

				newTestQuery(DB, "test_items").Distinct(distinct).Sort(sort, sortOrder).ReqTotal().
					Where("year", GT, 2002).
					Where("genre", EQ, 4). // composite index age+genre, and extra and + or conditions
					Where("age", EQ, 3).
					Where("isdeleted", EQ, true).Or().Where("year", GT, 2001).
					Where("packages", SET, randIntArr(5, 10000, 50)).Debug(TRACE).
					ExecAndVerify()

				newTestQuery(DB, "test_items").Distinct(distinct).Sort(sort, sortOrder).ReqTotal().
					Where("age", SET, []int{1, 2, 3, 4}).
					Where("id", EQ, mkID(rand.Int()%5000)).
					Where("tmp", EQ, ""). // composite pk with store index
					Where("isdeleted", EQ, true).Or().Where("year", GT, 2001).Debug(TRACE).
					ExecAndVerify()

				newTestQuery(DB, "test_items").Distinct(distinct).Sort(sort, sortOrder).ReqTotal().
					Where("genre", SET, []int{5, 1, 7}).
					Where("year", LT, 2010).Or().Where("genre", EQ, 3).
					Where("packages", SET, randIntArr(5, 10000, 50)).Or().Where("packages", EMPTY, 0).Debug(TRACE).
					ExecAndVerify()

				newTestQuery(DB, "test_items").Distinct(distinct).Sort(sort, sortOrder).ReqTotal().
					Where("genre", SET, []int{5, 1, 7}).
					Where("year", LT, 2010).Or().Where("packages", ANY, 0).
					Where("packages", SET, randIntArr(5, 10000, 50)).Debug(TRACE).
					ExecAndVerify()

				newTestQuery(DB, "test_items").Distinct(distinct).Sort(sort, sortOrder).ReqTotal().
					Where("genre", EQ, 5).Or().Where("genre", EQ, 6).
					Where("year", RANGE, []int{2001, 2020}).
					Where("packages", SET, randIntArr(5, 10000, 50)).
					ExecAndVerify()

				newTestQuery(DB, "test_items").Distinct(distinct).Sort(sort, sortOrder).ReqTotal().Debug(TRACE).
					Not().Where("genre", EQ, 5).
					Where("year", RANGE, []int{2001, 2020}).
					Where("packages", SET, randIntArr(5, 10000, 50)).
					ExecAndVerify()

				newTestQuery(DB, "test_items").Distinct(distinct).Sort(sort, sortOrder).ReqTotal().Debug(TRACE).
					Where("genre", EQ, 5).
					Not().Where("year", RANGE, []int{2001, 2020}).
					Where("packages", SET, randIntArr(5, 10000, 50)).
					ExecAndVerify()

				newTestQuery(DB, "test_items").Distinct(distinct).Sort(sort, sortOrder).ReqTotal().Debug(TRACE).
					Not().Where("genre", EQ, 10).
					ExecAndVerify()

				newTestQuery(DB, "test_items_not").ReqTotal().
					Where("name", EQ, "blabla").
					ExecAndVerify()

				newTestQuery(DB, "test_items_not").ReqTotal().
					Where("year", EQ, 2002).
					ExecAndVerify()

				newTestQuery(DB, "test_items_not").ReqTotal().
					Where("year", EQ, 2002).
					Not().Where("name", EQ, "blabla").
					ExecAndVerify()

				newTestQuery(DB, "test_items_not").ReqTotal().
					Where("name", EQ, "blabla").
					Not().Where("year", EQ, 2002).
					ExecAndVerify()

				newTestQuery(DB, "test_items_not").ReqTotal().
					Where("name", EQ, "blabla").
					Not().Where("year", EQ, 2001).
					ExecAndVerify()

				newTestQuery(DB, "test_items_not").ReqTotal().
					Where("year", EQ, 2002).
					Not().Where("name", EQ, "sss").
					ExecAndVerify()

			}
		}
	}
}

func CheckTestItemsSQLQueries() {
	if res, err := DB.Select("SELECT * FROM test_items WHERE year > '2016' AND genre IN ('1',2,'3') ORDER BY year DESC LIMIT 10000000").PtrSlice(); err != nil {
		panic(err)
	} else {
		newTestQuery(DB, "test_items").Where("year", GT, 2016).Where("genre", SET, []int{1, 2, 3}).Sort("year", true).Verify(res)
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
	} else if res, err := q.Exec().PtrSlice(); err != nil {
		panic(err)
	} else {
		newTestQuery(DB, "test_items").
			Where("year", GT, 2016).
			Where("genre", SET, []int{1, 2, 3}).
			Where("packages", ANY, 0).
			Where("countries", EMPTY, 0).
			Where("isdeleted", EQ, true).
			Sort("year", true).
			Verify(res)
	}
}

// TODO: add more tests, add query results checking
// 3. Test full text search
// 8. Test limit and offset

func TestReindex(t *testing.T) {
	if testing.Verbose() {
		DB.SetLogger(&TestLogger{})
	}
	FillTestItems(0, 2500, 20)
	FillTestItems(2500, 2500, 0)
	FillTestItemsForNot()
	FillTestJoinItems(7000, 500)
	CheckTestItemsJoinQueries(true, false, false)
	CheckTestItemsJoinQueries(false, true, true)
	CheckTestItemsJoinQueries(true, true, true)
	CheckTestItemsJoinQueries(false, true, false)
	CheckTestItemsJoinQueries(true, true, false)
	CheckTestItemsQueries()
	CheckTestItemsSQLQueries()
	CheckTestItemsDSLQueries()
	CheckTestDescribeQuery()

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
	for i := 0; i < 20000; i++ {
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

	FillTestItems(3000, 1000, 0)
	FillTestItems(4000, 500, 20)
	CheckTestItemsQueries()
	CheckTestItemsSQLQueries()
	CheckTestItemsDSLQueries()
	CheckTestDescribeQuery()
	DB.SetLogger(nil)
}

type TestLogger struct {
}

func (TestLogger) Printf(level int, format string, msg ...interface{}) {
	//	if level <= INFO {
	log.Printf(format, msg...)
	//	}
}

func randString() string {
	return adjectives[rand.Int()%len(adjectives)] + "_" + names[rand.Int()%len(names)]
}

func randDevice() string {
	return devices[rand.Int()%len(devices)]
}

func randLocation() string {
	return locations[rand.Int()%len(locations)]
}

func randIntArr(cnt int, start int, rng int) (arr []int) {
	arr = make([]int, 0, cnt)
	for j := 0; j < cnt; j++ {
		arr = append(arr, start+rand.Int()%rng)
	}
	return arr
}

var (
	adjectives = [...]string{"able", "above", "absolute", "balanced", "becoming", "beloved", "calm", "capable", "capital", "destined", "devoted", "direct", "enabled", "enabling", "endless", "factual", "fair", "faithful", "grand", "grateful", "great", "humane", "humble", "humorous", "ideal", "immense", "immortal", "joint", "just", "keen", "key", "kind", "logical", "loved", "loving", "mint", "model", "modern", "nice", "noble", "normal", "one", "open", "optimal", "polite", "popular", "positive", "quality", "quick", "quiet", "rapid", "rare", "rational", "sacred", "safe", "saved", "tight", "together", "tolerant", "unbiased", "uncommon", "unified", "valid", "valued", "vast", "wealthy", "welcome"}

	names = [...]string{"ox", "ant", "ape", "asp", "bat", "bee", "boa", "bug", "cat", "cod", "cow", "cub", "doe", "dog", "eel", "eft", "elf", "elk", "emu", "ewe", "fly", "fox", "gar", "gnu", "hen", "hog", "imp", "jay", "kid", "kit", "koi", "lab", "man", "owl", "pig", "pug", "pup", "ram", "rat", "ray", "yak", "bass", "bear", "bird", "boar", "buck", "bull", "calf", "chow", "clam", "colt", "crab", "crow", "dane", "deer", "dodo", "dory", "dove", "drum", "duck", "fawn", "fish", "flea", "foal", "fowl", "frog", "gnat", "goat", "grub", "gull", "hare", "hawk", "ibex", "joey", "kite", "kiwi", "lamb", "lark", "lion", "loon", "lynx", "mako", "mink", "mite", "mole", "moth", "mule", "mutt", "newt", "orca", "oryx", "pika", "pony", "puma", "seal", "shad", "slug", "sole", "stag", "stud", "swan", "tahr", "teal", "tick", "toad", "tuna", "wasp", "wolf", "worm", "wren", "yeti"}

	devices   = [...]string{"iphone", "android", "smarttv", "stb", "ottstb"}
	locations = [...]string{"mos", "ct", "dv", "sth", "vlg", "sib", "ural"}
)
