package reindexer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/restream/reindexer/v5"
	"github.com/restream/reindexer/v5/dsl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type sortDistinctOptions struct {
	SortIndexes     []string
	DistinctIndexes [][]string
	TestComposite   bool
}

type IndexesTestCase struct {
	Name      string
	Namespace string
	Options   sortDistinctOptions
	Item      interface{}
}

type StrictTestNest struct {
	Name string
	Age  int
}

// TestItem common test case
type TestItem struct {
	Prices        []*TestJoinItem `reindex:"prices,,joined"`
	Pricesx       []*TestJoinItem `reindex:"pricesx,,joined"`
	ID            int             `reindex:"id,-"`
	Genre         int64           `reindex:"genre,tree"`
	Year          int             `reindex:"year,tree"`
	Packages      []int           `reindex:"packages,hash"`
	Name          string          `reindex:"name,tree,is_no_column"`
	Countries     []string        `reindex:"countries,tree"`
	Age           int             `reindex:"age,hash,is_no_column"`
	AgeLimit      int64           `json:"age_limit" reindex:"age_limit,hash,sparse"`
	CompanyName   string          `json:"company_name" reindex:"company_name,hash,sparse"`
	Address       string          `json:"address"`
	PostalCode    int             `json:"postal_code"`
	EmptyInt      int             `json:"empty_int,omitempty"`
	Description   string          `reindex:"description,fuzzytext"`
	Rate          float64         `reindex:"rate,tree"`
	ExchangeRate  float64         `json:"exchange_rate"`
	PollutionRate float32         `json:"pollution_rate"`
	IsDeleted     bool            `reindex:"isdeleted,-"`
	Actor         Actor           `reindex:"actor,-,is_no_column"`
	PricesIDs     []int           `reindex:"price_id"`
	LocationID    string          `reindex:"location"`
	EndTime       int             `reindex:"end_time,-"`
	StartTime     int             `reindex:"start_time,tree"`
	Tmp           string          `reindex:"tmp,-,is_no_column"`
	Nested        StrictTestNest  `reindex:"-" json:"nested"`
	Uuid          string          `reindex:"uuid,hash,uuid" json:"uuid"`
	UuidStore     string          `reindex:"uuid_store,-,uuid" json:"uuid_store"`
	UuidArray     []string        `reindex:"uuid_array,hash,uuid" json:"uuid_array"`
	_             struct{}        `reindex:"id+tmp,,composite,pk"`
	_             struct{}        `reindex:"age+genre,,composite"`
	_             struct{}        `reindex:"location+rate,,composite"`
	_             struct{}        `reindex:"rate+age,,composite"`
	_             struct{}        `reindex:"uuid+age,,composite"`
}

// TestItemIDOnly test case for non-indexed fields
type TestItemIDOnly struct {
	Prices        []*TestJoinItem `reindex:"prices,,joined"`
	Pricesx       []*TestJoinItem `reindex:"pricesx,,joined"`
	ID            int             `reindex:"id,-"`
	Genre         int64           `json:"genre"`
	Year          int             `json:"year"`
	Packages      []int           `json:"packages"`
	Name          string          `json:"name"`
	Countries     []string        `json:"countries"`
	Age           int             `json:"age"`
	AgeLimit      int64           `json:"age_limit"`    // reindex:"age_limit,hash,sparse"`
	CompanyName   string          `json:"company_name"` // reindex:"company_name,hash,sparse"`
	Address       string          `json:"address"`
	PostalCode    int             `json:"postal_code"`
	Description   string          `json:"description"`
	Rate          float64         `json:"rate"`
	ExchangeRate  float64         `json:"exchange_rate"`
	PollutionRate float32         `json:"pollution_rate"`
	IsDeleted     bool            `json:"isdeleted"`
	Actor         Actor           `reindex:"actor"`
	PricesIDs     int             `json:"price_id"`
	LocationID    string          `json:"location"`
	EndTime       int             `json:"end_time"`
	StartTime     int             `json:"start_time"`
	Tmp           string          `reindex:"tmp,-"`
	Uuid          string          `json:"uuid"`
	UuidStore     string          `json:"uuid_store"`
	UuidArray     []string        `json:"uuid_array"`
	_             struct{}        `reindex:"id+tmp,,composite,pk"`
}

// TestItemWithSparse test case for sparse indexes
type TestItemWithSparse struct {
	Prices        []*TestJoinItem `reindex:"prices,,joined"`
	Pricesx       []*TestJoinItem `reindex:"pricesx,,joined"`
	ID            int             `reindex:"id,-"`
	Genre         int64           `reindex:"genre,tree"`
	Year          int             `reindex:"year,tree,sparse"`
	Packages      []int           `reindex:"packages,hash,sparse"`
	Name          string          `reindex:"name,tree,sparse"`
	Countries     []string        `reindex:"countries,tree,sparse"`
	Age           int             `reindex:"age,hash,is_no_column"`
	AgeLimit      int64           `json:"age_limit" reindex:"age_limit,hash,sparse"`
	CompanyName   string          `json:"company_name" reindex:"company_name,hash,sparse"`
	Address       string          `json:"address"`
	PostalCode    int             `json:"postal_code"`
	Description   string          `reindex:"description,fuzzytext"`
	Rate          float64         `reindex:"rate,tree"`
	ExchangeRate  float64         `json:"exchange_rate"`
	PollutionRate float32         `json:"pollution_rate"`
	IsDeleted     bool            `reindex:"isdeleted,-"`
	Actor         Actor           `reindex:"actor,,is_no_column"`
	PricesIDs     []int           `reindex:"price_id,,sparse"`
	LocationID    string          `reindex:"location"`
	EndTime       int             `reindex:"end_time,-"`
	StartTime     int             `reindex:"start_time,tree"`
	Tmp           string          `reindex:"tmp,-,is_no_column"`
	Uuid          string          `reindex:"uuid,hash,uuid" json:"uuid"`
	UuidStore     string          `reindex:"uuid_store,-,uuid" json:"uuid_store"`
	UuidArray     []string        `reindex:"uuid_array,hash,uuid" json:"uuid_array"`
	_             struct{}        `reindex:"id+tmp,,composite,pk"`
	_             struct{}        `reindex:"age+genre,,composite"`
	_             struct{}        `reindex:"location+rate,,composite"`
	_             struct{}        `reindex:"uuid+age,,composite"`
}

type TestItemSimple struct {
	ID    int    `reindex:"id,,pk"`
	Year  int    `reindex:"year,tree"`
	Name  string `reindex:"name,,is_no_column"`
	Phone string
}

type TestItemGeom struct {
	ID                  int             `reindex:"id,,pk"`
	PointRTreeLinear    reindexer.Point `reindex:"point_rtree_linear,rtree,linear"`
	PointRTreeQuadratic reindexer.Point `reindex:"point_rtree_quadratic,rtree,quadratic"`
	PointRTreeGreene    reindexer.Point `reindex:"point_rtree_greene,rtree,greene"`
	PointRTreeRStar     reindexer.Point `reindex:"point_rtree_rstar,rtree,rstar"`
	PointNonIndex       reindexer.Point `json:"point_non_index"`
}

type TestItemGeomSimple struct {
	ID                  int             `reindex:"id,,pk"`
	PointRTreeLinear    reindexer.Point `reindex:"point_rtree_linear,rtree,linear"`
	PointRTreeQuadratic reindexer.Point `reindex:"point_rtree_quadratic,rtree,quadratic"`
}

type TestItemCustom struct {
	Actor             Actor `reindex:"actor"`
	Genre             int64
	Name              string `reindex:"name"`
	CustomUniqueField int
	ID                int `reindex:"id,,pk"`
	Year              int `reindex:"year,tree"`
}

type TestItemCmplxPK struct {
	ID    int32    `reindex:"id,-"`
	Year  int32    `reindex:"year,tree"`
	Name  string   `reindex:"name"`
	SubID string   `reindex:"subid,-"`
	_     struct{} `reindex:"id+subid,,composite,pk"`
}

type objectType struct {
	Name string `reindex:"name"`
	Age  int    `reindex:"age"`
	Rate int    `reindex:"rate"`
	Hash int64  `reindex:"hash"`
}

type TestItemObjectArray struct {
	ID        int          `reindex:"id,,pk"`
	Code      int64        `reindex:"code"`
	IsEnabled bool         `reindex:"is_enabled"`
	Desc      string       `reindex:"desc"`
	Objects   []objectType `reindex:"objects"`
	MainObj   objectType   `reindex:"main_obj"`
	Size      int          `reindex:"size"`
	Hash      int64        `reindex:"hash"`
	Salt      int          `reindex:"salt"`
	IsUpdated bool         `reindex:"is_updated"`
}

type TestItemNestedPK struct {
	PrimaryID int      `reindex:"primary_id"`
	Nested    TestItem `json:"nested"`
}

type TestItemEqualPosition struct {
	ID         string                        `reindex:"id,,pk"`
	Name       string                        `reindex:"name"`
	SecondName string                        `reindex:"second_name"`
	TestFlag   bool                          `reindex:"test_flag"`
	ItemsArray []*TestArrayItemEqualPosition `reindex:"items_array,-"`
	ValueArray []int                         `reindex:"value_array,-"`
	_          struct{}                      `reindex:"name+second_name=searching,text,composite"`
}

type TestArrayItemEqualPosition struct {
	SpaceId string `reindex:"space_id"`
	Value   int    `reindex:"value"`
}

type testItemsCreator func(int, int) interface{}

type CompositeFacetItem struct {
	CompanyName string
	Rate        float64
}

type CompositeFacetResultItem struct {
	CompanyName string
	Rate        float64
	Count       int
}

type CompositeFacetResult []CompositeFacetResultItem

type FakeTestItem TestItem

const (
	testItemsNs             = "test_items"
	testItemsWalNs          = "test_items_wal"
	testItemsQrIdleNs       = "test_items_qr_idle"
	testItemsAggsFetchingNs = "test_items_aggs_fetching"
	testItemsCancelNs       = "test_items_cancel"
	testItemsIdOnlyNs       = "test_items_id_only"
	testItemsWithSparseNs   = "test_items_with_sparse"
	testItemsGeomNs         = "test_items_geom"
	testItemsStDistanceNs   = "test_items_st_distance"
	testItemsNotNs          = "test_items_not"

	testItemsDeleteQueryNs = "test_items_delete_query"
	testItemsUpdateQueryNs = "test_items_update_query"

	testItemDeleteNs       = "test_item_delete"
	testItemsObjectArrayNs = "test_items_object_array"
	testItemNestedPkNs     = "test_item_nested_pk"

	testItemsEqualPositionNs = "test_items_eqaul_position"
	testItemsStrictNs        = "test_items_strict"
	testItemsStrictJoinedNs  = "test_items_strict_joined"

	testItemsExplainNs = "test_items_explain"
)

func init() {
	tnamespaces[testItemsNs] = TestItem{}
	tnamespaces[testItemsWalNs] = TestItem{}
	tnamespaces[testItemsQrIdleNs] = TestItemSimple{}
	tnamespaces[testItemsAggsFetchingNs] = TestItemSimple{}
	tnamespaces[testItemsCancelNs] = TestItem{}
	tnamespaces[testItemsIdOnlyNs] = TestItemIDOnly{}
	tnamespaces[testItemsWithSparseNs] = TestItemWithSparse{}
	tnamespaces[testItemsGeomNs] = TestItemGeom{}
	tnamespaces[testItemsStDistanceNs] = TestItemGeomSimple{}
	tnamespaces[testItemsNotNs] = TestItemSimple{}

	tnamespaces[testItemsDeleteQueryNs] = TestItem{}
	tnamespaces[testItemsUpdateQueryNs] = TestItem{}

	tnamespaces[testItemDeleteNs] = TestItem{}
	tnamespaces[testItemsObjectArrayNs] = TestItemObjectArray{}
	tnamespaces[testItemNestedPkNs] = TestItemNestedPK{}

	tnamespaces[testItemsEqualPositionNs] = TestItemEqualPosition{}
	tnamespaces[testItemsStrictNs] = TestItem{}
	tnamespaces[testItemsStrictJoinedNs] = TestJoinItem{}

	tnamespaces[testItemsExplainNs] = TestItemSimple{}
}

var testCaseWithCommonIndexes = IndexesTestCase{
	Name:      "TEST WITH COMMON INDEXES",
	Namespace: testItemsNs,
	Options: sortDistinctOptions{
		SortIndexes:     []string{"", "NAME", "YEAR", "RATE", "RATE + (GENRE - 40) * ISDELETED"},
		DistinctIndexes: [][]string{[]string{}, []string{"YEAR"}, []string{"NAME"}, []string{"YEAR", "NAME"}},
		TestComposite:   true,
	},
	Item: TestItem{},
}

var testCaseWithIDOnlyIndexes = IndexesTestCase{
	Name:      "TEST WITH ID ONLY INDEX",
	Namespace: testItemsIdOnlyNs,
	Options: sortDistinctOptions{
		SortIndexes:     []string{"", "name", "year", "rate", "rate + (genre - 40) * isdeleted"},
		DistinctIndexes: [][]string{[]string{}, []string{"year"}, []string{"name"}, []string{"year", "name"}},
		TestComposite:   false,
	},
	Item: TestItemIDOnly{},
}

var testCaseWithSparseIndexes = IndexesTestCase{
	Name:      "TEST WITH SPARSE INDEXES",
	Namespace: testItemsWithSparseNs,
	Options: sortDistinctOptions{
		SortIndexes:     []string{"", "NAME", "YEAR", "RATE", "-ID + (END_TIME - START_TIME) / 100"},
		DistinctIndexes: [][]string{[]string{}, []string{"YEAR"}, []string{"NAME"}, []string{"YEAR", "NAME"}},
		TestComposite:   false,
	},
	Item: TestItemWithSparse{},
}

func (f CompositeFacetResult) Len() int {
	return len(f)
}

func (f CompositeFacetResult) Swap(i, j int) {
	f[i], f[j] = f[j], f[i]
}

func (f CompositeFacetResult) Less(i, j int) bool {
	if f[i].Count == f[j].Count {
		if f[i].CompanyName == f[j].CompanyName {
			return f[i].Rate < f[j].Rate
		} else {
			return f[i].CompanyName > f[j].CompanyName
		}
	}
	return f[i].Count < f[j].Count
}

func makeLikePattern(s string) string {
	runes := make([]rune, len(s))
	i := 0
	for _, rune := range s {
		if rand.Int()%4 == 0 {
			runes[i] = '_'
		} else {
			runes[i] = rune
		}
		i++
	}
	var result string
	if rand.Int()%4 == 0 {
		result += "%"
	}
	current := 0
	next := rand.Int() % (len(s) + 1)
	last := next
	for current < len(s) {
		if current < next {
			result += string(runes[current:next])
			last = next
			current = rand.Int()%(len(s)-last+1) + last
		}
		next = rand.Int()%(len(s)-current+1) + current
		if current > last || rand.Int()%4 == 0 {
			result += "%"
		}
	}
	if rand.Int()%4 == 0 {
		result += "%"
	}
	return result
}

func newTestItem(id int, pkgsCount int) interface{} {
	startTime := rand.Int() % 50000
	return &TestItem{
		ID:            mkID(id),
		Year:          rand.Int()%50 + 2000,
		Genre:         int64(rand.Int() % 50),
		Name:          randString(),
		Age:           rand.Int() % 5,
		AgeLimit:      int64(rand.Int()%10 + 40),
		CompanyName:   randString(),
		Address:       randString(),
		PostalCode:    randPostalCode(),
		Description:   randString(),
		Packages:      randIntArr(10, 10000, 50),
		Rate:          float64(rand.Int()%100) / 10,
		ExchangeRate:  rand.Float64(),
		PollutionRate: rand.Float32(),
		IsDeleted:     rand.Int()%2 != 0,
		PricesIDs:     randIntArr(10, 7000, 50),
		LocationID:    randLocation(),
		StartTime:     startTime,
		EndTime:       startTime + (rand.Int()%5)*1000,
		Actor: Actor{
			Name: randString(),
		},
		Uuid:      randUuid(),
		UuidStore: randUuid(),
		UuidArray: randUuidArray(rand.Int() % 20),
	}
}

func newTestItemSimple(id int, pkgsCount int) interface{} {
	return &TestItemSimple{
		ID:   mkID(id),
		Year: rand.Int()%50 + 2000,
		Name: randString(),
	}
}

func newTestItemGeom(id int, pkgsCount int) interface{} {
	return &TestItemGeom{
		ID:                  mkID(id),
		PointRTreeLinear:    randPoint(),
		PointRTreeQuadratic: randPoint(),
		PointRTreeGreene:    randPoint(),
		PointRTreeRStar:     randPoint(),
		PointNonIndex:       randPoint(),
	}
}

func newTestItemGeomSimple(id int, pkgsCount int) interface{} {
	return &TestItemGeomSimple{
		ID:                  mkID(id),
		PointRTreeLinear:    randPoint(),
		PointRTreeQuadratic: randPoint(),
	}
}

func newTestItemIDOnly(id int, pkgsCount int) interface{} {
	startTime := rand.Int() % 50000
	return &TestItemIDOnly{
		ID:            mkID(id),
		Year:          rand.Int()%50 + 2000,
		Genre:         int64(rand.Int() % 50),
		Name:          randString(),
		Age:           rand.Int() % 5,
		AgeLimit:      int64(rand.Int()%10 + 40),
		CompanyName:   randString(),
		Address:       randString(),
		PostalCode:    randPostalCode(),
		Description:   randString(),
		Packages:      randIntArr(pkgsCount, 10000, 50),
		Rate:          float64(rand.Int()%100) / 10,
		ExchangeRate:  rand.Float64(),
		PollutionRate: rand.Float32(),
		IsDeleted:     rand.Int()%2 != 0,
		PricesIDs:     rand.Int() % 100, //randIntArr(10, 7000, 50),
		LocationID:    randLocation(),
		StartTime:     startTime,
		EndTime:       startTime + (rand.Int()%5)*1000,
		Actor: Actor{
			Name: randString(),
		},
		Uuid:      randUuid(),
		UuidStore: randUuid(),
		UuidArray: randUuidArray(rand.Int() % 20),
	}
}

func newTestItemWithSparse(id int, pkgsCount int) interface{} {
	startTime := rand.Int() % 50000
	return &TestItemWithSparse{
		ID:            mkID(id),
		Year:          rand.Int()%50 + 2000,
		Genre:         int64(rand.Int() % 50),
		Name:          randString(),
		Age:           rand.Int() % 5,
		AgeLimit:      int64(rand.Int()%10 + 40),
		CompanyName:   randString(),
		Address:       randString(),
		PostalCode:    randPostalCode(),
		Description:   randString(),
		Packages:      randIntArr(pkgsCount, 10000, 50),
		Rate:          float64(rand.Int()%100) / 10,
		ExchangeRate:  rand.Float64(),
		PollutionRate: rand.Float32(),
		IsDeleted:     rand.Int()%2 != 0,
		PricesIDs:     randIntArr(10, 7000, 50),
		LocationID:    randLocation(),
		StartTime:     startTime,
		EndTime:       startTime + (rand.Int()%5)*1000,
		Actor: Actor{
			Name: randString(),
		},
		Uuid:      randUuid(),
		UuidStore: randUuid(),
		UuidArray: randUuidArray(rand.Int() % 20),
	}
}

func newTestItemNestedPK(id int, pkgsCount int) *TestItemNestedPK {
	return &TestItemNestedPK{
		PrimaryID: mkID(id),
		Nested:    *newTestItem(id+10, pkgsCount).(*TestItem),
	}
}

func newTestItemObjectArray(id int, arrSize int) *TestItemObjectArray {
	arr := make([]objectType, 0, arrSize)

	for i := 0; i < arrSize; i++ {
		arr = append(arr, objectType{
			Name: randString(),
			Age:  rand.Int() % 50,
			Rate: rand.Int() % 10,
			Hash: rand.Int63() % 1000000 >> 1,
		})
	}

	return &TestItemObjectArray{
		ID:        id,
		Code:      rand.Int63() % 10000 >> 1,
		IsEnabled: (rand.Int() % 2) == 0,
		Desc:      randString(),
		Objects:   arr,
		MainObj: objectType{
			Name: randString(),
			Age:  rand.Int() % 50,
			Rate: rand.Int() % 10,
			Hash: rand.Int63() % 1000000 >> 1,
		},
		Size:      rand.Int() % 200000,
		Hash:      rand.Int63() % 1000000 >> 1,
		Salt:      rand.Int() % 100000,
		IsUpdated: (rand.Int() % 2) == 0,
	}
}

func newTestItemEqualPosition(id int, arrSize int) *TestItemEqualPosition {
	av := make([]*TestArrayItemEqualPosition, id%arrSize)
	av2 := make([]int, id%arrSize)
	for j := range av {
		av[j] = &TestArrayItemEqualPosition{
			SpaceId: "space_" + strconv.Itoa(j),
			Value:   id % 2,
		}
		av2[j] = (id + j) % 2
	}
	return &TestItemEqualPosition{
		ID:         strconv.Itoa(id),
		Name:       "Name_" + strconv.Itoa(id),
		SecondName: "Second_name_" + strconv.Itoa(id),
		TestFlag:   id%4 > 2,
		ItemsArray: av,
		ValueArray: av2,
	}
}

func FillTestItemsForNot() {
	tx := newTestTx(DB, testItemsNotNs)

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
	tx.MustCommit()

}

func FillTestItemsTx(start int, count int, pkgsCount int, tx *txTest) {
	for i := 0; i < count; i++ {
		testItem := newTestItem(start+i, pkgsCount)
		if err := tx.Upsert(testItem); err != nil {
			panic(err)
		}
	}
}

func FillTestItems(ns string, start int, count int, pkgsCount int) {
	tx := newTestTx(DB, ns)
	FillTestItemsTx(start, count, pkgsCount, tx)
	tx.MustCommit()
}

func FillTestItemsTxWithFunc(start, count, pkgsCount int, tx *txTest, fn testItemsCreator) {
	for i := 0; i < count; i++ {
		testItem := fn(start+i, pkgsCount)
		if err := tx.Upsert(testItem); err != nil {
			panic(err)
		}
	}
}

func FillTestItemsWithFunc(ns string, start int, count int, pkgsCount int, fn testItemsCreator) {
	tx := newTestTx(DB, ns)
	FillTestItemsTxWithFunc(start, count, pkgsCount, tx, fn)
	tx.MustCommit()
}

func FillTestItemsWithFuncParts(ns string, start int, count int, partSize int, pkgsCount int, fn testItemsCreator) {
	last := count + start
	for i := start; i < last; i += partSize {
		tx := newTestTx(DB, ns)
		curPartSize := partSize
		if i+curPartSize > last {
			curPartSize = last - i
		}
		FillTestItemsTxWithFunc(i, curPartSize, pkgsCount, tx, fn)
		tx.MustCommit()
	}
}

func callQueriesSequence(t *testing.T, namespace string, distinct []string, sort string, desc, testComposite bool) {
	// Take items with single condition
	newTestQuery(DB, namespace).Where("genre", reindexer.EQ, rand.Int()%50).Distinct(distinct).Sort(sort, desc).ExecAndVerify(t)
	newTestQuery(DB, namespace).Where("name", reindexer.EQ, randString()).Distinct(distinct).Sort(sort, desc).ExecAndVerify(t)
	newTestQuery(DB, namespace).Where("rate", reindexer.EQ, float32(rand.Int()%100)/10).Distinct(distinct).Sort(sort, desc).ExecAndVerify(t)

	newTestQuery(DB, namespace).Where("age_limit", reindexer.EQ, int64((rand.Int()%10)+40)).Distinct(distinct).Sort(sort, desc).ExecAndVerify(t)
	newTestQuery(DB, namespace).Where("postal_code", reindexer.EQ, randPostalCode()).Distinct(distinct).Sort(sort, desc).ExecAndVerify(t)
	newTestQuery(DB, namespace).Where("company_name", reindexer.EQ, randString()).Distinct(distinct).Sort(sort, desc).ExecAndVerify(t)
	newTestQuery(DB, namespace).Where("address", reindexer.EQ, randString()).Distinct(distinct).Sort(sort, desc).ExecAndVerify(t)
	newTestQuery(DB, namespace).Where("exchange_rate", reindexer.EQ, rand.Float64()).Distinct(distinct).Sort(sort, desc).ExecAndVerify(t)
	newTestQuery(DB, namespace).Where("pollution_rate", reindexer.EQ, rand.Float32()).Distinct(distinct).Sort(sort, desc).ExecAndVerify(t)

	newTestQuery(DB, namespace).Where("genre", reindexer.GT, rand.Int()%50).Distinct(distinct).Sort(sort, desc).Debug(reindexer.TRACE).ExecAndVerify(t)
	newTestQuery(DB, namespace).Where("name", reindexer.GT, randString()).Distinct(distinct).Offset(21).Limit(50).Sort(sort, desc).ExecAndVerify(t)
	newTestQuery(DB, namespace).Where("rate", reindexer.GT, float32(rand.Int()%100)/10).Distinct(distinct).Sort(sort, desc).ExecAndVerify(t)
	newTestQuery(DB, namespace).Where("age_limit", reindexer.GT, int64(40)).Distinct(distinct).Sort(sort, desc).ExecAndVerify(t)
	newTestQuery(DB, namespace).Where("postal_code", reindexer.GT, randPostalCode()).Distinct(distinct).Sort(sort, desc).ExecAndVerify(t)
	newTestQuery(DB, namespace).Where("company_name", reindexer.GT, randString()).Distinct(distinct).Sort(sort, desc).ExecAndVerify(t)
	newTestQuery(DB, namespace).Where("address", reindexer.GT, randString()).Distinct(distinct).Sort(sort, desc).ExecAndVerify(t)
	newTestQuery(DB, namespace).Where("exchange_rate", reindexer.GT, rand.Float64()).Distinct(distinct).Sort(sort, desc).ExecAndVerify(t)
	newTestQuery(DB, namespace).Where("pollution_rate", reindexer.GT, rand.Float32()).Distinct(distinct).Sort(sort, desc).ExecAndVerify(t)

	newTestQuery(DB, namespace).Where("genre", reindexer.LT, rand.Int()%50).Distinct(distinct).Sort(sort, desc).Limit(100).ExecAndVerify(t)
	newTestQuery(DB, namespace).Where("name", reindexer.LT, randString()).Offset(10).Limit(200).ExecAndVerify(t)
	newTestQuery(DB, namespace).Where("rate", reindexer.LT, float32(rand.Int()%100)/10).Distinct(distinct).Sort(sort, desc).ExecAndVerify(t)
	newTestQuery(DB, namespace).Where("age_limit", reindexer.LT, int64(50)).Distinct(distinct).Sort(sort, desc).ExecAndVerify(t)
	newTestQuery(DB, namespace).Where("postal_code", reindexer.LT, randPostalCode()).Distinct(distinct).Sort(sort, desc).ExecAndVerify(t)
	newTestQuery(DB, namespace).Where("company_name", reindexer.LT, randString()).ExecAndVerify(t)
	newTestQuery(DB, namespace).Where("address", reindexer.LT, randString()).ExecAndVerify(t)
	newTestQuery(DB, namespace).Where("exchange_rate", reindexer.LT, rand.Float64()).Distinct(distinct).Sort(sort, desc).ExecAndVerify(t)
	newTestQuery(DB, namespace).Where("pollution_rate", reindexer.LT, rand.Float32()).Distinct(distinct).Sort(sort, desc).Limit(500).ExecAndVerify(t)

	newTestQuery(DB, namespace).Where("genre", reindexer.RANGE, []int{rand.Int() % 100, rand.Int() % 100}).Distinct(distinct).Sort(sort, desc).ExecAndVerify(t)
	newTestQuery(DB, namespace).Where("name", reindexer.RANGE, []string{randString(), randString()}).Distinct(distinct).Sort(sort, desc).ExecAndVerify(t)
	newTestQuery(DB, namespace).Where("rate", reindexer.RANGE, []float32{float32(rand.Int()%100) / 10, float32(rand.Int()%100) / 10}).Distinct(distinct).Sort(sort, desc).ExecAndVerify(t)
	newTestQuery(DB, namespace).Where("age_limit", reindexer.RANGE, []int64{40, 50}).ExecAndVerify(t)
	newTestQuery(DB, namespace).Where("company_name", reindexer.RANGE, []string{randString(), randString()}).Distinct(distinct).Sort(sort, desc).ExecAndVerify(t)

	newTestQuery(DB, namespace).Where("name", reindexer.LIKE, makeLikePattern(randString())).ExecAndVerify(t)

	newTestQuery(DB, namespace).Where("packages", reindexer.SET, randIntArr(10, 10000, 50)).Distinct(distinct).Sort(sort, desc).ExecAndVerify(t)
	newTestQuery(DB, namespace).Where("packages", reindexer.EMPTY, nil).Distinct(distinct).Sort(sort, desc).ExecAndVerify(t)
	newTestQuery(DB, namespace).Where("packages", reindexer.ANY, nil).Distinct(distinct).Sort(sort, desc).ExecAndVerify(t)

	newTestQuery(DB, namespace).Where("isdeleted", reindexer.EQ, true).Distinct(distinct).Sort(sort, desc).ExecAndVerify(t)

	newTestQuery(DB, namespace).Where("name", reindexer.EQ, randString()).Distinct(distinct).Sort("address", false).Sort(sort, desc).ExecAndVerify(t)
	newTestQuery(DB, namespace).Where("name", reindexer.EQ, randString()).Distinct(distinct).Sort("postal_code", true).Sort(sort, desc).ExecAndVerify(t)
	newTestQuery(DB, namespace).Where("name", reindexer.EQ, randString()).Distinct(distinct).Sort("age_limit", false).Sort(sort, desc).ExecAndVerify(t)
	newTestQuery(DB, namespace).Where("packages", reindexer.GE, 5).Where("price_id", reindexer.GE, 100).EqualPosition("packages", "price_id").ExecAndVerify(t)
	newTestQuery(DB, namespace).
		Where("packages", reindexer.GE, 5).
		Where("price_id", reindexer.GE, 100).
		EqualPosition("packages", "price_id").
		Where("isdeleted", reindexer.EQ, true).
		Or().
		Where("year", reindexer.GT, 2001).
		ExecAndVerify(t)

	newTestQuery(DB, namespace).Where("name", reindexer.EQ, randString()).Distinct(distinct).
		Sort("year", true).
		Sort("name", false).
		Sort("genre", true).
		Sort("age", false).
		Sort("age_limit", true).
		ExecAndVerify(t)

	newTestQuery(DB, namespace).Where("name", reindexer.EQ, randString()).Distinct(distinct).
		Sort("year", true).
		Sort("name", false).
		Sort("genre", true).
		Sort("age", false).
		Sort("age_limit", true).
		Offset(4).
		Limit(44).
		ExecAndVerify(t)

	// Complex queires
	newTestQuery(DB, namespace).Distinct(distinct).Sort(sort, desc).ReqTotal().
		Where("packages", reindexer.SET, randIntArr(5, 10000, 50)).Debug(reindexer.TRACE).
		Where("genre", reindexer.EQ, 5). // composite index age+genre
		Where("age", reindexer.EQ, 3).
		Where("year", reindexer.GT, 2010).
		Where("age_limit", reindexer.LE, int64(50)).
		ExecAndVerify(t)

	newTestQuery(DB, namespace).Distinct(distinct).Sort(sort, desc).ReqTotal().
		Where("year", reindexer.GT, 2002).
		Where("genre", reindexer.EQ, 4). // composite index age+genre, and extra and + or conditions
		Where("age", reindexer.EQ, 3).
		Where("age_limit", reindexer.GE, int64(40)).
		Where("isdeleted", reindexer.EQ, true).Or().Where("year", reindexer.GT, 2001).
		Where("packages", reindexer.SET, randIntArr(5, 10000, 50)).Debug(reindexer.TRACE).
		ExecAndVerify(t)

	newTestQuery(DB, namespace).Distinct(distinct).Sort(sort, desc).ReqTotal().
		Where("age", reindexer.SET, []int{1, 2, 3, 4}).
		Where("id", reindexer.EQ, mkID(rand.Int()%5000)).
		Where("tmp", reindexer.EQ, ""). // composite pk with store index
		Where("isdeleted", reindexer.EQ, true).Or().Where("year", reindexer.GT, 2001).Debug(reindexer.TRACE).
		ExecAndVerify(t)

	newTestQuery(DB, namespace).Distinct(distinct).Sort(sort, desc).ReqTotal().
		Where("genre", reindexer.SET, []int{5, 1, 7}).
		Where("year", reindexer.LT, 2010).Or().Where("genre", reindexer.EQ, 3).
		Where("packages", reindexer.SET, randIntArr(5, 10000, 50)).Or().Where("packages", reindexer.EMPTY, nil).Debug(reindexer.TRACE).
		ExecAndVerify(t)

	newTestQuery(DB, namespace).Distinct(distinct).Sort(sort, desc).ReqTotal().
		Where("genre", reindexer.SET, []int{5, 1, 7}).
		Where("year", reindexer.LT, 2010).Or().Where("packages", reindexer.ANY, nil).
		Where("packages", reindexer.SET, randIntArr(5, 10000, 50)).Debug(reindexer.TRACE).
		ExecAndVerify(t)

	newTestQuery(DB, namespace).Distinct(distinct).Sort(sort, desc).ReqTotal().
		Where("genre", reindexer.EQ, 5).Or().Where("genre", reindexer.EQ, 6).
		Where("year", reindexer.RANGE, []int{2001, 2020}).
		Where("packages", reindexer.SET, randIntArr(5, 10000, 50)).
		ExecAndVerify(t)

	newTestQuery(DB, namespace).Distinct(distinct).Sort(sort, desc).ReqTotal().
		Where("year", reindexer.RANGE, []int{2001, 2020}).
		Where("packages", reindexer.SET, randIntArr(5, 10000, 50)).
		Where("packages", reindexer.SET, randIntArr(5, 10000, 50)).
		ExecAndVerify(t)

	newTestQuery(DB, namespace).Distinct(distinct).Sort(sort, desc).ReqTotal().
		Where("actor.name", reindexer.EQ, randString()).
		ExecAndVerify(t)

	newTestQuery(DB, namespace).Distinct(distinct).Sort(sort, desc).ReqTotal().Debug(reindexer.TRACE).
		Not().Where("genre", reindexer.EQ, 5).
		Where("year", reindexer.RANGE, []int{2001, 2020}).
		Where("age_limit", reindexer.RANGE, []int64{40, 50}).
		Where("packages", reindexer.SET, randIntArr(5, 10000, 50)).
		ExecAndVerify(t)

	newTestQuery(DB, namespace).Distinct(distinct).Sort(sort, desc).ReqTotal().Debug(reindexer.TRACE).
		Where("genre", reindexer.EQ, 5).
		Not().Where("year", reindexer.RANGE, []int{2001, 2020}).
		Where("age_limit", reindexer.RANGE, []int64{40, 50}).
		Where("packages", reindexer.SET, randIntArr(5, 10000, 50)).
		ExecAndVerify(t)

	newTestQuery(DB, namespace).Distinct(distinct).Sort(sort, desc).ReqTotal().Debug(reindexer.TRACE).
		OpenBracket().
		Where("age_limit", reindexer.RANGE, []int64{40, 45}).
		CloseBracket().
		ExecAndVerify(t)

	newTestQuery(DB, namespace).Distinct(distinct).Sort(sort, desc).ReqTotal().Debug(reindexer.TRACE).
		OpenBracket().
		Where("age_limit", reindexer.RANGE, []int64{40, 45}).
		Where("genre", reindexer.EQ, 5).
		CloseBracket().
		ExecAndVerify(t)

	newTestQuery(DB, namespace).Distinct(distinct).Sort(sort, desc).ReqTotal().Debug(reindexer.TRACE).
		OpenBracket().
		Where("age_limit", reindexer.RANGE, []int64{40, 45}).
		Or().Where("genre", reindexer.EQ, 5).
		CloseBracket().
		ExecAndVerify(t)

	newTestQuery(DB, namespace).Distinct(distinct).Sort(sort, desc).ReqTotal().Debug(reindexer.TRACE).
		Not().OpenBracket().
		Where("age_limit", reindexer.RANGE, []int64{40, 45}).
		CloseBracket().
		ExecAndVerify(t)

	newTestQuery(DB, namespace).Distinct(distinct).Sort(sort, desc).ReqTotal().Debug(reindexer.TRACE).
		Where("genre", reindexer.EQ, 5).
		OpenBracket().
		Where("age_limit", reindexer.RANGE, []int64{40, 45}).
		Or().Where("packages", reindexer.SET, randIntArr(5, 10000, 50)).
		CloseBracket().
		ExecAndVerify(t)

	newTestQuery(DB, namespace).Distinct(distinct).Sort(sort, desc).ReqTotal().Debug(reindexer.TRACE).
		Where("genre", reindexer.EQ, 5).
		OpenBracket().
		Where("age_limit", reindexer.RANGE, []int64{40, 45}).
		Where("packages", reindexer.SET, randIntArr(5, 10000, 50)).
		CloseBracket().
		ExecAndVerify(t)

	newTestQuery(DB, namespace).Distinct(distinct).Sort(sort, desc).ReqTotal().Debug(reindexer.TRACE).
		Where("genre", reindexer.EQ, 5).
		Or().OpenBracket().
		Where("age_limit", reindexer.RANGE, []int64{40, 45}).
		Or().Where("packages", reindexer.SET, randIntArr(5, 10000, 50)).
		CloseBracket().
		ExecAndVerify(t)

	newTestQuery(DB, namespace).Distinct(distinct).Sort(sort, desc).ReqTotal().Debug(reindexer.TRACE).
		Where("genre", reindexer.EQ, 5).
		Or().OpenBracket().
		Where("age_limit", reindexer.RANGE, []int64{40, 45}).
		Where("packages", reindexer.SET, randIntArr(5, 10000, 50)).
		CloseBracket().
		ExecAndVerify(t)

	newTestQuery(DB, namespace).Distinct(distinct).Sort(sort, desc).ReqTotal().Debug(reindexer.TRACE).
		Where("genre", reindexer.EQ, 5).
		Not().Where("year", reindexer.RANGE, []int{2001, 2010}).
		OpenBracket().
		Where("age_limit", reindexer.RANGE, []int64{40, 45}).
		Or().Where("packages", reindexer.SET, randIntArr(5, 10000, 50)).
		CloseBracket().
		ExecAndVerify(t)

	newTestQuery(DB, namespace).Distinct(distinct).Sort(sort, desc).ReqTotal().Debug(reindexer.TRACE).
		Where("genre", reindexer.EQ, 5).
		Or().OpenBracket().
		Where("genre", reindexer.EQ, 4).
		Where("packages", reindexer.SET, randIntArr(5, 10000, 50)).
		CloseBracket().
		Not().Where("year", reindexer.RANGE, []int{2001, 2010}).
		OpenBracket().
		Where("age_limit", reindexer.RANGE, []int64{40, 45}).
		Or().Where("packages", reindexer.SET, randIntArr(5, 10000, 50)).
		Not().OpenBracket().
		OpenBracket().
		OpenBracket().
		Where("age", reindexer.SET, []int{1, 2, 3, 4}).
		Or().Where("id", reindexer.EQ, mkID(rand.Int()%5000)).
		Not().Where("tmp", reindexer.EQ, ""). // composite pk with store index
		CloseBracket().
		Or().OpenBracket().
		Where("tmp", reindexer.EQ, ""). // composite pk with store index
		Not().Where("isdeleted", reindexer.EQ, true).
		Or().Where("year", reindexer.GT, 2001).
		CloseBracket().
		CloseBracket().
		CloseBracket().
		CloseBracket().
		ExecAndVerify(t)

	newTestQuery(DB, namespace).Distinct(distinct).Sort(sort, desc).ReqTotal().Debug(reindexer.TRACE).
		Not().Where("genre", reindexer.EQ, 10).
		ExecAndVerify(t)

	newTestQuery(DB, namespace).Where("end_time", reindexer.GT, 10000).Not().Where("genre", reindexer.EQ, 10).Distinct(distinct).Sort(sort, desc).ExecAndVerify(t)
	newTestQuery(DB, namespace).WhereBetweenFields("end_time", reindexer.GT, "start_time").Distinct(distinct).Sort(sort, desc).ExecAndVerify(t)
	newTestQuery(DB, namespace).
		WhereBetweenFields("name", reindexer.EQ, "actor.name").
		Or().
		OpenBracket().
		WhereBetweenFields("exchange_rate", reindexer.GT, "pollution_rate").
		WhereBetweenFields("year", reindexer.LE, "price_id").
		WhereBetweenFields("packages", reindexer.GE, "exchange_rate").
		Or().
		WhereBetweenFields("packages", reindexer.GE, "price_id").
		CloseBracket().
		Distinct(distinct).
		Sort(sort, desc).
		ExecAndVerify(t)

	newTestQuery(DB, namespace).Distinct(distinct).Sort(sort, desc).ReqTotal().Debug(reindexer.TRACE).
		Not().Where("uuid", reindexer.EQ, randUuid()).
		ExecAndVerify(t)

	newTestQuery(DB, namespace).Distinct(distinct).Sort(sort, desc).ReqTotal().Debug(reindexer.TRACE).
		WhereUuid("uuid", reindexer.LT, randUuid()).
		ExecAndVerify(t)

	newTestQuery(DB, namespace).Distinct(distinct).Sort(sort, desc).Debug(reindexer.TRACE).
		WhereUuid("uuid", reindexer.SET, randUuidArray(rand.Int()%10)...).
		ExecAndVerify(t)

	newTestQuery(DB, namespace).Distinct(distinct).Sort(sort, desc).ReqTotal().Debug(reindexer.TRACE).
		Not().Where("uuid_store", reindexer.EQ, randUuid()).
		ExecAndVerify(t)

	newTestQuery(DB, namespace).Distinct(distinct).Sort(sort, desc).Debug(reindexer.TRACE).
		WhereUuid("uuid_store", reindexer.LT, randUuid()).
		ExecAndVerify(t)

	newTestQuery(DB, namespace).Distinct(distinct).Sort(sort, desc).ReqTotal().Debug(reindexer.TRACE).
		WhereUuid("uuid_store", reindexer.SET, randUuidArray(rand.Int()%10)...).
		ExecAndVerify(t)

	newTestQuery(DB, namespace).Distinct(distinct).Sort(sort, desc).ReqTotal().Debug(reindexer.TRACE).
		WhereUuid("uuid_array", reindexer.SET, randUuidArray(rand.Int()%10)...).
		ExecAndVerify(t)

	newTestQuery(DB, namespace).Distinct(distinct).Sort(sort, desc).ReqTotal().Debug(reindexer.TRACE).
		Not().Where("uuid_array", reindexer.SET, randUuidArray(rand.Int()%10)).
		ExecAndVerify(t)

	newTestQuery(DB, namespace).Distinct(distinct).Sort(sort, desc).ReqTotal().
		WhereQuery(t, newTestQuery(DB, namespace).Where("id", reindexer.EQ, mkID(rand.Int()%5000)),
			reindexer.ANY, nil).
		ExecAndVerify(t)

	newTestQuery(DB, namespace).Distinct(distinct).Sort(sort, desc).ReqTotal().
		WhereQuery(t, newTestQuery(DB, namespace).Select("id").Where("id", reindexer.GT, mkID(rand.Int()%5000)).Limit(10), reindexer.LT, mkID(rand.Int()%5000)).
		ExecAndVerify(t)

	newTestQuery(DB, namespace).Distinct(distinct).Sort(sort, desc).ReqTotal().
		WhereQuery(t, newTestQuery(DB, namespace).Where("id", reindexer.GT, mkID(rand.Int()%5000)).AggregateAvg("id"), reindexer.LT, mkID(rand.Int()%5000)).
		ExecAndVerify(t)

	newTestQuery(DB, namespace).Distinct(distinct).Sort(sort, desc).ReqTotal().
		Where("id", reindexer.SET, newTestQuery(DB, namespace).Select("id").Where("id", reindexer.GT, mkID(rand.Int()%5000)).Sort("id", false).Limit(10)).
		ExecAndVerify(t)

	newTestQuery(DB, namespace).Distinct(distinct).Sort(sort, desc).ReqTotal().
		Where("id", reindexer.LE, newTestQuery(DB, namespace).AggregateAvg("id").Where("id", reindexer.LT, mkID(rand.Int()%5000+5))).
		ExecAndVerify(t)

	if !testComposite {
		return
	}
	compositeValues := []interface{}{[]interface{}{rand.Int() % 10, int64(rand.Int() % 50)}}

	newTestQuery(DB, namespace).Distinct(distinct).Sort(sort, desc).ReqTotal().
		Where("age+genre", reindexer.EQ, compositeValues).
		ExecAndVerify(t)

	newTestQuery(DB, namespace).Distinct(distinct).Sort(sort, desc).ReqTotal().
		Where("age+genre", reindexer.LE, compositeValues).
		ExecAndVerify(t)

	newTestQuery(DB, namespace).Distinct(distinct).Sort(sort, desc).ReqTotal().
		Where("age+genre", reindexer.LT, compositeValues).
		ExecAndVerify(t)

	newTestQuery(DB, namespace).Distinct(distinct).Sort(sort, desc).ReqTotal().
		Where("age+genre", reindexer.GT, compositeValues).
		ExecAndVerify(t)

	newTestQuery(DB, namespace).Distinct(distinct).Sort(sort, desc).ReqTotal().
		Where("age+genre", reindexer.GE, compositeValues).
		ExecAndVerify(t)

	compositeValues = []interface{}{[]interface{}{randLocation(), float64(rand.Int()%100) / 10}}

	newTestQuery(DB, namespace).Distinct(distinct).Sort(sort, desc).ReqTotal().
		Where("location+rate", reindexer.EQ, compositeValues).
		ExecAndVerify(t)

	newTestQuery(DB, namespace).Distinct(distinct).Sort(sort, desc).ReqTotal().
		Where("location+rate", reindexer.GT, compositeValues).
		ExecAndVerify(t)

	newTestQuery(DB, namespace).Distinct(distinct).Sort(sort, desc).ReqTotal().
		Where("location+rate", reindexer.LT, compositeValues).
		ExecAndVerify(t)

	compositeValues = []interface{}{
		[]interface{}{randLocation(), float64(rand.Int()%100) / 10},
		[]interface{}{randLocation(), float64(rand.Int()%100) / 10},
	}
	newTestQuery(DB, namespace).Distinct(distinct).Sort(sort, desc).ReqTotal().
		Where("location+rate", reindexer.RANGE, compositeValues).
		ExecAndVerify(t)

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
	newTestQuery(DB, namespace).Distinct(distinct).Sort(sort, desc).ReqTotal().
		Where("age+genre", reindexer.SET, compositeValues).
		ExecAndVerify(t)

	newTestQuery(DB, namespace).Distinct(distinct).Sort(sort, desc).ReqTotal().
		WhereBetweenFields("age+genre", reindexer.GT, "rate+age").
		Or().
		WhereBetweenFields("age+genre", reindexer.LT, "rate+age").
		ExecAndVerify(t)

	compositeValues = []interface{}{
		[]interface{}{randUuid(), rand.Int() % 10},
		[]interface{}{randUuid(), rand.Int() % 10},
	}

	newTestQuery(DB, namespace).Distinct(distinct).Sort(sort, desc).ReqTotal().
		Where("uuid+age", reindexer.EQ, compositeValues).
		ExecAndVerify(t)
}

func CheckTestItemsQueries(t *testing.T, testCase IndexesTestCase) {
	log.Println(testCase.Name)
	desc := []bool{true, false}[rand.Intn(2)]
	sort := testCase.Options.SortIndexes[rand.Intn(len(testCase.Options.SortIndexes))]
	distinct := testCase.Options.DistinctIndexes[rand.Intn(len(testCase.Options.DistinctIndexes))]
	log.Printf("\tDISTINCT '%s' SORT '%s' DESC %v\n", distinct, sort, desc)
	// Just take all items from namespace
	newTestQuery(DB, testCase.Namespace).Distinct(distinct).Sort(sort, desc).Limit(1).ExecAndVerify(t)
	callQueriesSequence(t, testCase.Namespace, distinct, sort, desc, testCase.Options.TestComposite)
}

func CheckAggregateQueries(t *testing.T) {
	facetLimit := 100
	facetOffset := 10
	ctx, cancel := context.WithCancel(context.Background())
	q := DB.Query(testItemsNs)
	q.AggregateAvg("year")
	q.AggregateSum("YEAR")
	q.AggregateFacet("age")
	q.AggregateFacet("name")
	q.AggregateMin("age")
	q.AggregateMax("age")
	q.AggregateFacet("company_name", "rate").Limit(facetLimit).Offset(facetOffset).Sort("count", false).Sort("company_name", true).Sort("rate", false)
	q.AggregateFacet("packages")
	it := q.ExecCtx(t, ctx)
	cancel()
	require.NoError(t, it.Error())
	defer it.Close()

	qcheck := DB.Query(testItemsNs)
	res, err := qcheck.Exec(t).FetchAll()
	require.NoError(t, err)

	aggregations := it.AggResults()
	require.Len(t, aggregations, 8)

	sum := 0.0
	ageFacet := make(map[int]int, 0)
	nameFacet := make(map[string]int, 0)
	packagesFacet := make(map[int]int, 0)
	compositeFacet := make(map[CompositeFacetItem]int, 0)
	ageMin, ageMax := 100000000, -10000000

	for _, it := range res {
		testItem := it.(*TestItem)
		sum += float64(testItem.Year)
		ageFacet[testItem.Age]++
		nameFacet[testItem.Name]++
		for _, pack := range testItem.Packages {
			packagesFacet[pack]++
		}
		compositeFacet[CompositeFacetItem{testItem.CompanyName, testItem.Rate}]++
		if testItem.Age > ageMax {
			ageMax = testItem.Age
		}
		if testItem.Age < ageMin {
			ageMin = testItem.Age
		}
	}

	var compositeFacetResult CompositeFacetResult
	for k, v := range compositeFacet {
		compositeFacetResult = append(compositeFacetResult, CompositeFacetResultItem{k.CompanyName, k.Rate, v})
	}
	sort.Sort(compositeFacetResult)
	compositeFacetResult = compositeFacetResult[min(facetOffset, len(compositeFacetResult)):min(facetOffset+facetLimit, len(compositeFacetResult))]

	require.Len(t, aggregations[1].Fields, 1)
	assert.Equal(t, "YEAR", aggregations[1].Fields[0])
	assert.Equal(t, sum, *aggregations[1].Value)
	assert.Equal(t, sum/float64(len(res)), *aggregations[0].Value)

	require.Len(t, aggregations[2].Fields, 1)
	assert.Equal(t, aggregations[2].Fields[0], "age")
	require.Equal(t, len(aggregations[2].Facets), len(ageFacet))
	for _, facet := range aggregations[2].Facets {
		require.Len(t, facet.Values, 1)
		intVal, _ := strconv.Atoi(facet.Values[0])
		count, ok := ageFacet[intVal]
		require.True(t, ok)
		assert.Equal(t, count, facet.Count)
	}

	require.Len(t, aggregations[3].Fields, 1)
	assert.Equal(t, aggregations[3].Fields[0], "name")
	for _, facet := range aggregations[3].Facets {
		require.Len(t, facet.Values, 1)
		count, ok := nameFacet[facet.Values[0]]
		require.True(t, ok)
		assert.Equal(t, count, facet.Count)
	}
	assert.Equal(t, ageMin, int(*aggregations[4].Value))
	assert.Equal(t, ageMax, int(*aggregations[5].Value))
	require.Len(t, aggregations[6].Fields, 2)
	assert.Equal(t, aggregations[6].Fields[0], "company_name")
	assert.Equal(t, aggregations[6].Fields[1], "rate")
	require.Equal(t, len(compositeFacetResult), len(aggregations[6].Facets))
	for i := 0; i < len(compositeFacetResult); i++ {
		require.Len(t, aggregations[6].Facets[i].Values, 2)
		rate, err := strconv.ParseFloat(aggregations[6].Facets[i].Values[1], 64)
		require.NoError(t, err)
		assert.Equal(t, compositeFacetResult[i].CompanyName, aggregations[6].Facets[i].Values[0])
		assert.Equal(t, compositeFacetResult[i].Rate, rate)
		assert.Equal(t, compositeFacetResult[i].Count, aggregations[6].Facets[i].Count)
	}
	require.Len(t, aggregations[7].Fields, 1)
	assert.Equal(t, aggregations[7].Fields[0], "packages")
	for _, facet := range aggregations[7].Facets {
		require.Len(t, facet.Values, 1)
		value, err := strconv.Atoi(facet.Values[0])
		require.NoError(t, err)
		count, ok := packagesFacet[value]
		require.True(t, ok)
		assert.Equal(t, count, facet.Count)
	}
}

func CheckAggByNonExistFieldQuery(t *testing.T) {
	nonExistenField := "NonExistenField"

	q := DB.Query(testItemsNs)
	q.AggregateSum(nonExistenField)
	q.q.Strict(reindexer.QueryStrictModeNone)
	it := q.Exec(t)
	require.NoError(t, it.Error())
	defer it.Close()

	aggregations := it.AggResults()

	assert.Equal(t, len(aggregations), 1)
	assert.Empty(t, aggregations[0].Value)
}

func CheckTestItemsJsonQueries() {
	ctx, cancel := context.WithCancel(context.Background())
	json, _ := DB.Query(testItemsNs).Select("ID", "Genre").Limit(3).ReqTotal("total_count").ExecToJson(testItemsNs).FetchAll()
	//	fmt.Println(string(json))
	_ = json

	json2, _ := DB.Query(testItemsNs).Select("ID", "Genre").Limit(3).ReqTotal("total_count").GetJsonCtx(ctx)
	//	fmt.Println(string(json2))
	cancel()
	_ = json2
	// TODO

}

func CheckTestItemsGeomQueries(t *testing.T) {
	// Checks that DWithin works and verifies the result
	newTestQuery(DB, testItemsGeomNs).DWithin("point_non_index", randPoint(), randFloat(0, 2)).ExecAndVerify(t)
	newTestQuery(DB, testItemsGeomNs).DWithin("point_rtree_linear", randPoint(), randFloat(0, 2)).ExecAndVerify(t)
	newTestQuery(DB, testItemsGeomNs).DWithin("point_rtree_quadratic", randPoint(), randFloat(0, 2)).ExecAndVerify(t)
	newTestQuery(DB, testItemsGeomNs).DWithin("point_rtree_greene", randPoint(), randFloat(0, 2)).ExecAndVerify(t)
	newTestQuery(DB, testItemsGeomNs).DWithin("point_rtree_rstar", randPoint(), randFloat(0, 2)).ExecAndVerify(t)
}

func CheckTestItemsSQLQueries(t *testing.T) {
	aggResults := []reindexer.AggregationResult{}
	companyName := randString()
	if res, err := DB.ExecSQL("SELECT ID, company_name FROM test_ITEMS WHERE COMPANY_name > '" + companyName + "' ORDER BY YEAr DESC LIMIT 10000000 ; ").FetchAll(); err != nil {
		panic(err)
	} else {
		newTestQuery(DB, testItemsNs).Where("company_name", reindexer.GT, companyName).Sort("year", true).Verify(t, res, aggResults, false)
	}

	if res, err := DB.ExecSQL("SELECT ID, postal_code FROM test_items WHERE postal_code > 121355 ORDER BY year DESC LIMIT 10000000 ; ").FetchAll(); err != nil {
		panic(err)
	} else {
		newTestQuery(DB, testItemsNs).Where("postal_code", reindexer.GT, 121355).Sort("year", true).Verify(t, res, aggResults, false)
	}

	if res, err := DB.ExecSQL("SELECT ID,Year,Genre,age_limit FROM test_items WHERE YEAR > '2016' AND genre IN ('1',2,'3') and age_limit IN(40,'50',42,'47') ORDER BY year DESC LIMIT 10000000 ; ").FetchAll(); err != nil {
		panic(err)
	} else {
		newTestQuery(DB, testItemsNs).Where("year", reindexer.GT, 2016).Where("GENRE", reindexer.SET, []int{1, 2, 3}).Where("age_limit", reindexer.SET, []int64{40, 50, 42, 47}).Sort("YEAR", true).Verify(t, res, aggResults, false)
	}

	if res, err := DB.ExecSQL("SELECT * FROM test_items WHERE YEAR <= '2016' OR genre < 5 or AGE_LIMIT >= 40 ORDER BY YEAR ASC ").FetchAll(); err != nil {
		panic(err)
	} else {
		newTestQuery(DB, testItemsNs).Where("year", reindexer.LE, 2016).Or().Where("genre", reindexer.LT, 5).Or().Where("age_limit", reindexer.GE, int64(40)).Sort("year", false).Verify(t, res, aggResults, true)
	}

	if res, err := DB.ExecSQL("SELECT count(*), * FROM test_items WHERE year >= '2016' OR rate = '1.1' OR year RANGE (2010,2014) or AGE_LIMIT <= 50").FetchAll(); err != nil {
		panic(err)
	} else {
		newTestQuery(DB, testItemsNs).Where("year", reindexer.GE, 2016).Or().Where("RATE", reindexer.EQ, 1.1).Or().Where("YEAR", reindexer.RANGE, []int{2010, 2014}).Or().Where("age_limit", reindexer.LE, int64(50)).Verify(t, res, aggResults, true)
	}

	likePattern := makeLikePattern(randString())
	if res, err := DB.ExecSQL("SELECT count(*), * FROM test_items WHERE year >= '2016' OR rate = '1.1' OR company_name LIKE '" + likePattern + "' or AGE_LIMIT <= 50").FetchAll(); err != nil {
		panic(err)
	} else {
		newTestQuery(DB, testItemsNs).Where("year", reindexer.GE, 2016).Or().Where("RATE", reindexer.EQ, 1.1).Or().Where("company_name", reindexer.LIKE, likePattern).Or().Where("age_limit", reindexer.LE, int64(50)).Verify(t, res, aggResults, true)
	}

	if res, err := DB.ExecSQL("SELECT ID,'Actor.Name' FROM test_items WHERE \"actor.name\" > 'bde'  LIMIT 10000000").FetchAll(); err != nil {
		panic(err)
	} else {
		newTestQuery(DB, testItemsNs).Where("actor.name", reindexer.GT, []string{"bde"}).Verify(t, res, aggResults, false)
	}

	if res, err := DB.ExecSQL("SELECT count(*), * FROM test_items WHERE year >= '2016' OR (rate = '1.1' OR company_name LIKE '" + likePattern + "') and AGE_LIMIT <= 50").FetchAll(); err != nil {
		panic(err)
	} else {
		newTestQuery(DB, testItemsNs).Where("year", reindexer.GE, 2016).Or().OpenBracket().Where("RATE", reindexer.EQ, 1.1).Or().Where("company_name", reindexer.LIKE, likePattern).CloseBracket().Where("age_limit", reindexer.LE, int64(50)).Verify(t, res, aggResults, true)
	}
}

func CheckTestItemsDSLQueries(t *testing.T) {
	likePattern := makeLikePattern(randString())
	d := dsl.DSL{
		Namespace: testItemsNs,
		Filters: []dsl.Filter{
			{
				Field: "YEAR",
				Cond:  "GT",
				Value: "2016",
			},
			{
				Field: "GENRE",
				Cond:  "SET",
				Value: []string{"1", "2", "3"},
			},
			{
				Field: "PACKAGES",
				Cond:  "ANY",
				Value: nil,
			},
			{
				Field: "countries",
				Cond:  "EMPTY",
				Value: nil,
			},
			{
				Field: "isdeleted",
				Cond:  "EQ",
				Value: true,
			},
			{
				Field: "company_name",
				Cond:  "LIKE",
				Value: likePattern,
			},
		},
		Sort: dsl.Sort{
			Field: "YEAR",
			Desc:  true,
		},
	}

	if q, err := DB.QueryFrom(d); err != nil {
		panic(err)
	} else if res, err := q.Exec().FetchAll(); err != nil {
		panic(err)
	} else {
		newTestQuery(DB, testItemsNs).
			Where("year", reindexer.GT, 2016).
			Where("genre", reindexer.SET, []int{1, 2, 3}).
			Where("packages", reindexer.ANY, nil).
			Where("countries", reindexer.EMPTY, nil).
			Where("isdeleted", reindexer.EQ, true).
			Where("company_name", reindexer.LIKE, likePattern).
			Sort("year", true).
			Verify(t, res, []reindexer.AggregationResult{}, true)
	}
}

func CheckNotQueries(t *testing.T) {
	newTestQuery(DB, testItemsNotNs).ReqTotal().
		Where("NAME", reindexer.EQ, "blabla").
		ExecAndVerify(t)

	newTestQuery(DB, testItemsNotNs).ReqTotal().
		Where("year", reindexer.EQ, 2002).
		ExecAndVerify(t)

	newTestQuery(DB, testItemsNotNs).ReqTotal().
		Where("YEAR", reindexer.EQ, 2002).
		Not().Where("name", reindexer.EQ, "blabla").
		ExecAndVerify(t)

	newTestQuery(DB, testItemsNotNs).ReqTotal().
		Where("name", reindexer.EQ, "blabla").
		Not().Where("year", reindexer.EQ, 2002).
		ExecAndVerify(t)

	newTestQuery(DB, testItemsNotNs).ReqTotal().
		Where("name", reindexer.EQ, "blabla").
		Not().Where("year", reindexer.EQ, 2001).
		ExecAndVerify(t)

	newTestQuery(DB, testItemsNotNs).ReqTotal().
		Where("year", reindexer.EQ, 2002).
		Not().Where("name", reindexer.EQ, "sss").
		ExecAndVerify(t)

}

func checkExplainSubqueries(t *testing.T, res []reindexer.ExplainSubQuery, expected []expectedExplainSubQuery) {
	require.Equal(t, len(expected), len(res))
	for i := 0; i < len(expected); i++ {
		assert.Equal(t, expected[i].Namespace, res[i].Namespace)
		assert.Equal(t, expected[i].Field, res[i].Field)
		assert.Equal(t, expected[i].Keys, res[i].Keys)
		checkExplain(t, res[i].Explain.Selectors, expected[i].Selectors, "")
	}
}

func TestQueries(t *testing.T) {
	t.Run("Common indexed queries", func(t *testing.T) {
		t.Parallel()

		FillTestItemsWithFunc(testItemsNs, 0, 2500, 20, newTestItem)
		FillTestItemsWithFunc(testItemsNs, 2500, 2500, 0, newTestItem)
		FillTestItemsWithFunc(testItemsGeomNs, 0, 2500, 0, newTestItemGeom)
		CheckTestItemsGeomQueries(t)

		FillTestItemsForNot()
		CheckNotQueries(t)

		require.NoError(t, DB.CloseNamespace(testItemsNs))
		require.NoError(t, DB.OpenNamespace(testItemsNs, reindexer.DefaultNamespaceOptions(), TestItem{}))

		CheckTestItemsJsonQueries()

		CheckAggregateQueries(t)
		CheckAggByNonExistFieldQuery(t)

		CheckTestItemsQueries(t, testCaseWithCommonIndexes)
		CheckTestItemsSQLQueries(t)
		CheckTestItemsDSLQueries(t)

		// Delete test
		tx := newTestTx(DB, testItemsNs)
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
				tx.Commit()
				tx = newTestTx(DB, testItemsNs)
			}
		}

		tx.Commit()

		FillTestItems(testItemsNs, 3000, 1000, 0)
		FillTestItems(testItemsNs, 4000, 500, 20)
		FillTestItemsWithFunc(testItemsGeomNs, 2500, 5000, 0, newTestItemGeom)
		CheckTestItemsGeomQueries(t)
		CheckTestItemsQueries(t, testCaseWithCommonIndexes)
		CheckTestItemsSQLQueries(t)
		CheckTestItemsDSLQueries(t)
	})

	t.Run("Non Indexed queries", func(t *testing.T) {
		t.Parallel()

		FillTestItemsWithFunc(testItemsIdOnlyNs, 0, 500, 20, newTestItemIDOnly)
		FillTestItemsWithFunc(testItemsIdOnlyNs, 500, 500, 0, newTestItemIDOnly)

		require.NoError(t, DB.CloseNamespace(testItemsIdOnlyNs))
		require.NoError(t, DB.OpenNamespace(testItemsIdOnlyNs, reindexer.DefaultNamespaceOptions(), TestItemIDOnly{}))

		CheckTestItemsQueries(t, testCaseWithIDOnlyIndexes)
	})

	t.Run("Sparse indexed queries", func(t *testing.T) {
		t.Parallel()

		FillTestItemsWithFunc(testItemsWithSparseNs, 0, 2500, 20, newTestItemWithSparse)
		FillTestItemsWithFunc(testItemsWithSparseNs, 2500, 2500, 0, newTestItemWithSparse)

		require.NoError(t, DB.CloseNamespace(testItemsWithSparseNs))
		require.NoError(t, DB.OpenNamespace(testItemsWithSparseNs, reindexer.DefaultNamespaceOptions(), TestItemWithSparse{}))

		CheckTestItemsQueries(t, testCaseWithSparseIndexes)
	})

}

func TestSTDistanceWrappers(t *testing.T) {
	t.Parallel()

	const ns = testItemsStDistanceNs
	const (
		field1 = "point_rtree_linear"
		field2 = "point_rtree_quadratic"
	)
	FillTestItemsWithFunc(ns, 0, 10000, 0, newTestItemGeomSimple)

	t.Run("ST_Distance between field and point", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			searchPoint := randPoint()
			distance := randFloat(0, 2)
			sortPoint := randPoint()
			it1, err := DBD.Query(ns).DWithin(field1, searchPoint, distance).SortStPointDistance(field1, sortPoint, false).
				ExecToJson().FetchAll()
			require.NoError(t, err)
			it2, err := DBD.Query(ns).DWithin(field1, searchPoint, distance).Sort(fmt.Sprintf("ST_Distance(%s, ST_GeomFromText('point(%s %s)'))",
				field1, strconv.FormatFloat(sortPoint[0], 'f', -1, 64), strconv.FormatFloat(sortPoint[1], 'f', -1, 64)), false).
				ExecToJson().FetchAll()
			require.NoError(t, err)
			require.Equal(t, string(it1), string(it2))
		}
	})

	t.Run("ST_Distance between fields", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			searchPoint := randPoint()
			distance := randFloat(0, 2)
			it1, err := DBD.Query(ns).DWithin(field1, searchPoint, distance).SortStFieldDistance(field1, field2, true).ExecToJson().FetchAll()
			require.NoError(t, err)
			it2, err := DBD.Query(ns).DWithin(field1, searchPoint, distance).Sort(fmt.Sprintf("ST_Distance(%s, %s)", field1, field2), true).ExecToJson().FetchAll()
			require.NoError(t, err)
			require.Equal(t, string(it1), string(it2))
		}
	})
}

func TestWALQueries(t *testing.T) {
	t.Parallel()

	const ns = testItemsWalNs
	FillTestItemsWithFunc(ns, 0, 2500, 20, newTestItem)
	validateJson := func(t *testing.T, jsonIt *reindexer.JSONIterator) {
		defer jsonIt.Close()
		assert.NoError(t, jsonIt.Error())
		assert.Greater(t, jsonIt.Count(), 0)
		for jsonIt.Next() {
			dict := map[string]interface{}{}
			err := json.Unmarshal(jsonIt.JSON(), &dict)
			assert.NoError(t, err)
			_, hasLSN := dict["lsn"]
			assert.True(t, hasLSN, "JSON: %s", string(jsonIt.JSON()))
			_, hasItem := dict["item"]
			_, hasType := dict["type"]
			assert.True(t, hasItem || hasType, "JSON: %s", string(jsonIt.JSON()))
		}
	}

	t.Run("JSON WAL query with GT", func(t *testing.T) {
		lsn := reindexer.LsnT{Counter: 1, ServerId: DB.leaderServerID}
		jsonIt := DBD.Query(ns).Where("#lsn", reindexer.GT, reindexer.CreateInt64FromLSN(lsn)).ExecToJson()
		validateJson(t, jsonIt)
	})

	t.Run("JSON WAL query with ANY", func(t *testing.T) {
		jsonIt := DBD.Query(ns).Where("#lsn", reindexer.ANY, nil).ExecToJson()
		validateJson(t, jsonIt)
	})

	t.Run("CJSON WAL query with GT (expecting error)", func(t *testing.T) {
		lsn := reindexer.LsnT{Counter: 1, ServerId: DB.leaderServerID}
		it := DBD.Query(ns).Where("#lsn", reindexer.GT, reindexer.CreateInt64FromLSN(lsn)).Exec()
		assert.Error(t, it.Error())
	})

	t.Run("CJSON WAL query with ANY (expecting error)", func(t *testing.T) {
		it := DBD.Query(ns).Where("#lsn", reindexer.ANY, nil).Exec()
		assert.Error(t, it.Error())
	})
}

func TestCanceledSelectQuery(t *testing.T) {
	if !strings.HasPrefix(DB.dsn, "builtin://") {
		return
	}

	FillTestItemsWithFunc(testItemsCancelNs, 0, 1000, 80, newTestItem)
	FillTestItemsWithFunc(testItemsCancelNs, 1000, 1000, 0, newTestItem)

	likePattern := makeLikePattern(randString())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := DB.WithContext(ctx).ExecSQL("SELECT count(*), * FROM test_items_cancel WHERE year >= '2016' OR (rate = '1.1' OR company_name LIKE '" + likePattern + "') and AGE_LIMIT <= 50").FetchAll()
	if err != context.Canceled {
		panic(fmt.Errorf("Canceled select request was executed"))
	}

	it := newTestQuery(DB, testItemsCancelNs).Where("year", reindexer.GE, 2016).Or().OpenBracket().Where("RATE", reindexer.EQ, 1.1).Or().Where("company_name", reindexer.LIKE, likePattern).CloseBracket().Where("age_limit", reindexer.LE, int64(50)).ExecCtx(t, ctx)
	defer it.Close()
	if err != context.Canceled {
		panic(fmt.Errorf("Canceled select request was executed"))
	}
}

func TestDeleteQuery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	err := DB.UpsertCtx(ctx, testItemsDeleteQueryNs, newTestItem(1000, 5))
	cancel()
	if err != nil {
		panic(err)
	}

	ctx, cancel = context.WithCancel(context.Background())
	count, err := DB.Query(testItemsDeleteQueryNs).Where("id", reindexer.EQ, mkID(1000)).DeleteCtx(ctx)
	cancel()
	if err != nil {
		panic(err)
	}

	if count != 1 {
		panic(fmt.Errorf("Expected delete query return 1 item"))
	}

	_, cancel = context.WithCancel(context.Background())
	_, ok := DB.Query(testItemsDeleteQueryNs).Where("id", reindexer.EQ, mkID(1000)).Get()
	cancel()
	if ok {
		panic(fmt.Errorf("Item was found after delete query, but will be deleted"))
	}

}
func TestCanceledDeleteQuery(t *testing.T) {
	if !strings.HasPrefix(DB.dsn, "builtin://") {
		return
	}

	err := DB.Upsert(testItemsDeleteQueryNs, newTestItem(1000, 5))
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = DB.Query(testItemsDeleteQueryNs).Where("id", reindexer.EQ, mkID(1000)).DeleteCtx(ctx)
	if err != context.Canceled {
		panic(fmt.Errorf("Canceled delete request was executed"))
	}

	_, ok := DB.Query(testItemsDeleteQueryNs).Where("id", reindexer.EQ, mkID(1000)).Get()
	if !ok {
		panic(fmt.Errorf("Item was deleted after canceled delete query"))
	}
}

func TestUpdateQuery(t *testing.T) {
	err := DB.Upsert(testItemsUpdateQueryNs, newTestItem(1000, 5))

	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	it := DB.Query(testItemsUpdateQueryNs).Where("id", reindexer.EQ, mkID(1000)).
		Set("name", "hello").
		Set("empty_int", 1).
		Set("postal_code", 10).UpdateCtx(ctx)
	cancel()
	defer it.Close()
	if it.Error() != nil {
		panic(it.Error())
	}

	if it.Count() != 1 {
		panic(fmt.Errorf("Expected update query return 1 item"))
	}

	ctx, cancel = context.WithCancel(context.Background())
	f, ok := DB.Query(testItemsUpdateQueryNs).Where("id", reindexer.EQ, mkID(1000)).GetCtx(ctx)
	cancel()
	if !ok {
		panic(fmt.Errorf("Item was not found after update query"))
	}
	if f.(*TestItem).EmptyInt != 1 {
		panic(fmt.Errorf("Item have wrong value %d, should %d", f.(*TestItem).EmptyInt, 1))
	}
	if f.(*TestItem).PostalCode != 10 {
		panic(fmt.Errorf("Item have wrong value %d, should %d", f.(*TestItem).PostalCode, 10))
	}
	if f.(*TestItem).Name != "hello" {
		panic(fmt.Errorf("Item have wrong value %s, should %s", f.(*TestItem).Name, "hello"))
	}

}
func TestCanceledUpdateQuery(t *testing.T) {
	if !strings.HasPrefix(DB.dsn, "builtin://") {
		return
	}

	item := newTestItem(1000, 5)
	err := DB.Upsert(testItemsUpdateQueryNs, item)

	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = DB.UpsertCtx(ctx, testItemsUpdateQueryNs, item)
	if err != context.Canceled {
		panic(fmt.Errorf("Canceled upsert request was executed"))
	}

	it := DB.Query(testItemsUpdateQueryNs).Where("id", reindexer.EQ, mkID(1000)).
		Set("name", "hello").
		Set("empty_int", 1).
		Set("postal_code", 10).UpdateCtx(ctx)
	defer it.Close()
	if it.Error() != context.Canceled {
		panic(fmt.Errorf("Canceled update request was executed"))
	}

	f, ok := DB.Query(testItemsUpdateQueryNs).Where("id", reindexer.EQ, mkID(1000)).Get()
	if !ok {
		panic(fmt.Errorf("Item was not found after update query"))
	}
	if f.(*TestItem).EmptyInt != item.(*TestItem).EmptyInt {
		panic(fmt.Errorf("Item have wrong value %d, should %d", f.(*TestItem).EmptyInt, item.(*TestItem).EmptyInt))
	}
	if f.(*TestItem).PostalCode != item.(*TestItem).PostalCode {
		panic(fmt.Errorf("Item have wrong value %d, should %d", f.(*TestItem).PostalCode, item.(*TestItem).PostalCode))
	}
	if f.(*TestItem).Name != item.(*TestItem).Name {
		panic(fmt.Errorf("Item have wrong value %s, should %s", f.(*TestItem).Name, item.(*TestItem).Name))
	}
}

func TestDeleteByPK(t *testing.T) {
	nsOpts := reindexer.DefaultNamespaceOptions()

	t.Run("delete by pk with regular item", func(t *testing.T) {
		const ns = testItemDeleteNs
		for i := 1; i <= 30; i++ {
			assert.NoError(t, DB.Upsert(ns, newTestItem(i, rand.Int()%20)))
		}

		assert.NoError(t, DB.MustBeginTx(ns).Commit())
		assert.NoError(t, DB.CloseNamespace(ns))
		assert.NoError(t, DB.OpenNamespace(ns, nsOpts, TestItem{}))

		for i := 1; i <= 30; i++ {
			// specially create a complete item
			assert.NoError(t, DB.Delete(ns, newTestItem(i, rand.Int()%20)))

			// check deletion result
			if item, found := DB.Query(ns).WhereInt("id", reindexer.EQ, mkID(i)).Get(); found {
				t.Fatalf("Item has not been deleted. < %+v > ", item)
			}
		}
	})

	t.Run("delete by pk with item object array", func(t *testing.T) {
		const ns = testItemsObjectArrayNs
		for i := 1; i <= 30; i++ {
			assert.NoError(t, DB.Upsert(ns, newTestItemObjectArray(i, rand.Int()%10)))
		}

		assert.NoError(t, DB.MustBeginTx(ns).Commit())
		assert.NoError(t, DB.CloseNamespace(ns))
		assert.NoError(t, DB.OpenNamespace(ns, nsOpts, TestItemObjectArray{}))

		for i := 1; i <= 30; i++ {
			// specially create a complete item
			assert.NoError(t, DB.Delete(ns, newTestItemObjectArray(i, rand.Int()%10)))

			// check deletion result
			if item, found := DB.Query(ns).WhereInt("id", reindexer.EQ, i).Get(); found {
				t.Fatalf("Item has not been deleted. < %+v > ", item)
			}
		}
	})

	t.Run("delete by pk with item nested pk", func(t *testing.T) {
		const ns = testItemNestedPkNs
		for i := 1; i <= 30; i++ {
			assert.NoError(t, DB.Upsert(ns, newTestItemNestedPK(i, rand.Int()%20)))
		}

		assert.NoError(t, DB.MustBeginTx(ns).Commit())
		assert.NoError(t, DB.CloseNamespace(ns))
		assert.NoError(t, DB.OpenNamespace(ns, nsOpts, TestItemNestedPK{}))

		for i := 1; i <= 30; i++ {
			// specially create a complete item
			assert.NoError(t, DB.Delete(ns, newTestItemNestedPK(i, rand.Int()%20)))

			// check deletion result
			if item, found := DB.Query(ns).WhereInt("id", reindexer.EQ, mkID(i)).Get(); found {
				t.Fatalf("Item has not been deleted. < %+v > ", item)
			}
		}
	})
}

func TestEqualPosition(t *testing.T) {
	const ns = testItemsEqualPositionNs

	tx := newTestTx(DB, ns)
	for i := 0; i < 20; i++ {
		tx.Upsert(newTestItemEqualPosition(i, 3))
	}
	tx.MustCommit()

	t.Run("simple equal position", func(t *testing.T) {
		expectedIds := map[string]bool{
			"2":  true,
			"4":  true,
			"8":  true,
			"10": true,
			"14": true,
			"16": true,
		}
		it := newTestQuery(DB, ns).
			Match("searching", "name Name*").
			Where("items_array.space_id", reindexer.EQ, "space_0").
			Where("items_array.value", reindexer.EQ, 0).
			EqualPosition("items_array.space_id", "items_array.value").
			MustExec(t)
		defer it.Close()
		assert.NoError(t, it.Error())
		assert.Equal(t, len(expectedIds), it.Count())
		for it.Next() {
			assert.True(t, expectedIds[it.Object().(*TestItemEqualPosition).ID])
		}
	})

	t.Run("equal position with additional conditions", func(t *testing.T) {
		expectedIds := map[string]bool{
			"5":  true,
			"17": true,
		}
		it := newTestQuery(DB, ns).
			Match("searching", "name Name*").
			Where("items_array.space_id", reindexer.EQ, "space_1").
			Where("items_array.value", reindexer.EQ, 1).
			WhereBool("test_flag", reindexer.EQ, false).
			EqualPosition("items_array.space_id", "items_array.value").
			MustExec(t)
		defer it.Close()
		assert.NoError(t, it.Error())
		assert.Equal(t, len(expectedIds), it.Count())
		for it.Next() {
			assert.True(t, expectedIds[it.Object().(*TestItemEqualPosition).ID])
		}
	})

	t.Run("equal position in brackets", func(t *testing.T) {
		expectedIds := map[string]bool{
			"2":  true,
			"3":  true,
			"4":  true,
			"7":  true,
			"8":  true,
			"10": true,
			"11": true,
			"14": true,
			"15": true,
			"16": true,
			"19": true,
		}
		it := newTestQuery(DB, ns).
			OpenBracket().
			Where("items_array.space_id", reindexer.EQ, "space_0").
			Where("items_array.value", reindexer.EQ, 0).
			Where("value_array", reindexer.EQ, 0).
			EqualPosition("items_array.space_id", "items_array.value", "value_array").
			CloseBracket().
			Or().
			WhereBool("test_flag", reindexer.EQ, true).
			MustExec(t)
		defer it.Close()
		assert.NoError(t, it.Error())
		assert.Equal(t, len(expectedIds), it.Count())
		for it.Next() {
			fmt.Println(it.Object().(*TestItemEqualPosition).ID, expectedIds[it.Object().(*TestItemEqualPosition).ID])
			assert.True(t, expectedIds[it.Object().(*TestItemEqualPosition).ID])
		}
	})
	t.Run("equal position array mark", func(t *testing.T) {
		expectedIds := map[string]bool{
			"1": true,
		}
		it := newTestQuery(DB, ns).
			Where("ID", reindexer.EQ, 1).
			Where("items_array.space_id", reindexer.EQ, "space_0").
			Where("items_array.value", reindexer.EQ, 1).
			EqualPosition("ItemsArray[#].SpaceId", "ItemsArray[#].Value").
			MustExec(t)
		defer it.Close()
		assert.NoError(t, it.Error())
		assert.Equal(t, len(expectedIds), it.Count())
		for it.Next() {
			fmt.Println(it.Object().(*TestItemEqualPosition).ID, expectedIds[it.Object().(*TestItemEqualPosition).ID])
			assert.True(t, expectedIds[it.Object().(*TestItemEqualPosition).ID])
		}
	})

}

func TestStrictMode(t *testing.T) {
	const (
		ns1 = testItemsStrictNs
		ns2 = testItemsStrictJoinedNs
	)

	const nonExistentField = "NonExistentField"

	t.Run("Strict filtering/sort by folded fields (empty namespace)", func(t *testing.T) {
		{
			itNames := DBD.Query(ns1).Strict(reindexer.QueryStrictModeNames).Where("nested.Name", reindexer.ANY, nil).Sort("nested.Name", false).MustExec()
			assert.Equal(t, itNames.Count(), 0)
			itNames.Close()
			itNone := DBD.Query(ns1).Strict(reindexer.QueryStrictModeNone).Where("nested.Name", reindexer.ANY, nil).Sort("nested.Name", false).MustExec()
			assert.Equal(t, itNone.Count(), 0)
			itNone.Close()
			itIndexes := DBD.Query(ns1).Strict(reindexer.QueryStrictModeIndexes).Where("nested.Name", reindexer.ANY, nil).Sort("nested.Name", false).Exec()
			assert.Error(t, itIndexes.Error())
			itIndexes.Close()
		}
	})

	tx := newTestTx(DB, ns1)
	itemsCount := 500
	for i := 0; i < itemsCount; i++ {
		item := newTestItem(i, rand.Int()%4)
		var itemJSON []byte
		var err error
		testItem, _ := item.(*TestItem)
		testItem.Year = i
		itemJSON, err = json.Marshal(struct {
			FakeTestItem
			RealNewField int `json:"real_new_field"`
		}{
			FakeTestItem: FakeTestItem(*testItem),
			RealNewField: i % 3,
		})
		assert.NoError(t, err)
		err = tx.tx.UpsertJSON(itemJSON)
		assert.NoError(t, err, "json: %s", itemJSON)
	}
	tx.MustCommit()

	FillTestJoinItems(0, 100, ns2)

	t.Run("Strict sort check", func(t *testing.T) {
		yearVal := rand.Int()%250 + 50
		itNames := DBD.Query(ns1).Strict(reindexer.QueryStrictModeNames).Distinct("age").Where("year", reindexer.GE, yearVal).Sort("year", false).ExecToJson()
		itIndexes := DBD.Query(ns1).Strict(reindexer.QueryStrictModeIndexes).Distinct("age").Where("year", reindexer.GE, yearVal).Sort("year", false).ExecToJson()
		itNone := DBD.Query(ns1).Strict(reindexer.QueryStrictModeNone).Distinct("age").Where("year", reindexer.GE, yearVal).Sort("year", false).ExecToJson()

		itNames1 := DBD.Query(ns1).Distinct("age").Where("year", reindexer.GE, yearVal).Sort("real_new_field", false).Sort("year", false).
			Strict(reindexer.QueryStrictModeNone).
			ExecToJson()
		assert.Equal(t, itNames.Count(), itNames1.Count())
		itNone1 := DBD.Query(ns1).Distinct("age").Strict(reindexer.QueryStrictModeNames).Where("year", reindexer.GE, yearVal).Sort("real_new_field", false).Sort("year", false).
			ExecToJson()
		assert.Equal(t, itNone.Count(), itNone1.Count())
		itIndexes1 := DBD.Query(ns1).Strict(reindexer.QueryStrictModeIndexes).Where("year", reindexer.GE, yearVal).Sort("real_new_field", false).Sort("year", false).
			Exec()
		assert.Error(t, itIndexes1.Error())

		itNames2 := DBD.Query(ns1).Strict(reindexer.QueryStrictModeNames).Where("year", reindexer.GE, yearVal).Sort("unknown_field", false).Sort("year", false).
			ExecToJson()
		assert.Error(t, itNames2.Error())
		itNone2 := DBD.Query(ns1).Distinct("age").Strict(reindexer.QueryStrictModeNone).Where("year", reindexer.GE, yearVal).Sort("unknown_field", false).Sort("year", false).
			ExecToJson()
		itIndexes2 := DBD.Query(ns1).Strict(reindexer.QueryStrictModeIndexes).Where("year", reindexer.GE, yearVal).Sort("unknown_field", false).Sort("year", false).
			Exec()
		assert.Error(t, itIndexes2.Error())

		for itNone.Next() {
			assert.True(t, itNames.Next())
			assert.True(t, itIndexes.Next())
			assert.True(t, itNone2.Next())
			assert.Equal(t, itNames.JSON(), itNone.JSON())
			assert.Equal(t, itIndexes.JSON(), itNone.JSON())
			assert.Equal(t, itNone2.JSON(), itNone.JSON())
		}
		itNone.Close()
		itNone1.Close()
		itNone2.Close()
		itNames.Close()
		itIndexes.Close()
		itIndexes1.Close()
		itIndexes2.Close()
	})

	t.Run("Strict filtering with non-index field", func(t *testing.T) {
		itNames := DBD.Query(ns1).Where("real_new_field", reindexer.EQ, 0).Sort("year", true).
			Sort("name", false).Strict(reindexer.QueryStrictModeNames).MustExec()
		itNone := DBD.Query(ns1).Where("real_new_field", reindexer.EQ, 0).Sort("year", true).
			Sort("name", false).Strict(reindexer.QueryStrictModeNone).MustExec()
		assert.Equal(t, itNames.Count(), itNone.Count())
		itIndexes := DBD.Query(ns1).Where("real_new_field", reindexer.EQ, 0).Sort("year", true).
			Sort("name", false).Strict(reindexer.QueryStrictModeIndexes).Exec()
		assert.Error(t, itIndexes.Error())

		itNames1 := DBD.Query(ns1).Distinct("real_new_field").Sort("year", true).
			Sort("name", false).Strict(reindexer.QueryStrictModeNames).MustExec()
		itNone1 := DBD.Query(ns1).Distinct("real_new_field").Sort("year", true).
			Sort("name", false).Strict(reindexer.QueryStrictModeNone).MustExec()
		assert.Equal(t, itNames1.Count(), itNone1.Count())
		itIndexes1 := DBD.Query(ns1).Distinct("real_new_field").Sort("year", true).
			Sort("name", false).Strict(reindexer.QueryStrictModeIndexes).Exec()
		assert.Error(t, itIndexes1.Error())

		itNone.Close()
		itNone1.Close()
		itNames.Close()
		itIndexes.Close()
		itIndexes1.Close()
		itNames1.Close()
	})

	t.Run("Strict filtering with non-existing field", func(t *testing.T) {
		{
			itNames := DBD.Query(ns1).Where("unknown_field", reindexer.EQ, true).Sort("year", true).
				Sort("name", false).Strict(reindexer.QueryStrictModeNames).Exec()
			assert.Error(t, itNames.Error())
			itNames.Close()
			itNone := DBD.Query(ns1).Where("unknown_field", reindexer.EQ, true).Sort("year", true).
				Sort("name", false).Strict(reindexer.QueryStrictModeNone).MustExec()
			assert.Equal(t, itNone.Count(), 0)
			itNone.Close()
			itIndexes := DBD.Query(ns1).Where("unknown_field", reindexer.EQ, true).Sort("year", true).
				Sort("name", false).Strict(reindexer.QueryStrictModeIndexes).Exec()
			assert.Error(t, itIndexes.Error())

			itNames1 := DBD.Query(ns1).Distinct("unknown_field").Sort("year", true).
				Sort("name", false).Strict(reindexer.QueryStrictModeNames).Exec()
			assert.Error(t, itNames1.Error())
			itNames1.Close()
			itNone1 := DBD.Query(ns1).Distinct("unknown_field").Sort("year", true).
				Sort("name", false).Strict(reindexer.QueryStrictModeNone).MustExec()
			assert.Equal(t, itNone1.Count(), 0)
			itNone1.Close()
			itIndexes1 := DBD.Query(ns1).Distinct("unknown_field").Sort("year", true).
				Sort("name", false).Strict(reindexer.QueryStrictModeIndexes).Exec()
			assert.Error(t, itIndexes1.Error())
			itIndexes1.Close()

			itNone3 := DBD.Query(ns1).Where("unknown_field", reindexer.EMPTY, nil).Sort("year", true).
				Sort("name", false).Strict(reindexer.QueryStrictModeNone).MustExec()
			assert.Equal(t, itNone3.Count(), itemsCount)
			itNone3.Close()
		}

		{
			itNames := DBD.Query(ns1).Where("unknown_field", reindexer.EMPTY, nil).Sort("year", true).
				Sort("name", false).Strict(reindexer.QueryStrictModeNames).Exec()
			assert.Error(t, itNames.Error())
			itNames.Close()
			itNone := DBD.Query(ns1).Where("unknown_field", reindexer.EMPTY, nil).Sort("year", true).
				Sort("name", false).Strict(reindexer.QueryStrictModeNone).MustExec()
			itNone.Close()
			itAll := DBD.Query(ns1).Sort("year", true).
				Sort("name", false).Strict(reindexer.QueryStrictModeNone).MustExec()
			assert.Equal(t, itNone.Count(), itAll.Count())
			itAll.Close()
			itIndexes := DBD.Query(ns1).Where("unknown_field", reindexer.EMPTY, nil).Sort("year", true).
				Sort("name", false).Strict(reindexer.QueryStrictModeIndexes).Exec()
			assert.Error(t, itIndexes.Error())
			itIndexes.Close()
		}
	})

	t.Run("Strict filtering/sort by joined fields", func(t *testing.T) {
		{
			priceVal := rand.Int()%500 + 50
			itNames := DBD.Query(ns1).Strict(reindexer.QueryStrictModeNames).InnerJoin(DBD.Query(ns2), "prices").On("year", reindexer.EQ, "id").Where("price", reindexer.LE, priceVal).Sort("price", false).MustExec()
			itNone := DBD.Query(ns1).Strict(reindexer.QueryStrictModeNone).InnerJoin(DBD.Query(ns2), "prices").On("year", reindexer.EQ, "id").Where("price", reindexer.LE, priceVal).Sort("price", false).MustExec()
			assert.Equal(t, itNames.Count(), itNone.Count())
			itIndexes := DBD.Query(ns1).Strict(reindexer.QueryStrictModeIndexes).InnerJoin(DBD.Query(ns2), "prices").On("year", reindexer.EQ, "id").Where("price", reindexer.LE, priceVal).Sort("price", false).Exec()
			assert.Error(t, itIndexes.Error())
			itNames.Close()
			itNone.Close()
			itIndexes.Close()
		}
		{
			itNames := DBD.Query(ns1).Strict(reindexer.QueryStrictModeNames).Sort(ns2+".price", false).InnerJoin(DBD.Query(ns2), "prices").On("year", reindexer.EQ, "id").MustExec()
			itNone := DBD.Query(ns1).Strict(reindexer.QueryStrictModeNone).Sort(ns2+".price", false).InnerJoin(DBD.Query(ns2), "prices").On("year", reindexer.EQ, "id").MustExec()
			assert.Equal(t, itNames.Count(), itNone.Count())
			itIndexes := DBD.Query(ns1).Strict(reindexer.QueryStrictModeIndexes).Sort(ns2+".price", false).InnerJoin(DBD.Query(ns2), "prices").On("year", reindexer.EQ, "id").Exec()
			assert.Error(t, itIndexes.Error())
			itNames.Close()
			itNone.Close()
			itIndexes.Close()

		}
		{
			itNames := DBD.Query(ns1).Strict(reindexer.QueryStrictModeNames).Sort(ns2+".amount", false).InnerJoin(DBD.Query(ns2), "prices").On("year", reindexer.EQ, "id").MustExec()
			itNone := DBD.Query(ns1).Strict(reindexer.QueryStrictModeNone).Sort(ns2+".amount", false).InnerJoin(DBD.Query(ns2), "prices").On("year", reindexer.EQ, "id").MustExec()
			assert.Equal(t, itNames.Count(), itNone.Count())
			itIndexes := DBD.Query(ns1).Strict(reindexer.QueryStrictModeIndexes).Sort(ns2+".amount", false).InnerJoin(DBD.Query(ns2), "prices").On("year", reindexer.EQ, "id").MustExec()
			assert.Equal(t, itIndexes.Count(), itNone.Count())
			itNames.Close()
			itNone.Close()
			itIndexes.Close()

		}
	})

	t.Run("Aggregate no error with non-existent field and strict_mode=none", func(t *testing.T) {
		q := DB.Query(testItemsNs)
		q.q.Strict(reindexer.QueryStrictModeNone)

		q.AggregateSum(nonExistentField)
		it := q.Exec(t)
		require.NoError(t, it.Error())
		defer it.Close()

		aggregations := it.AggResults()
		assert.Equal(t, len(aggregations), 1)
		assert.Empty(t, aggregations[0].Value)
	})

	t.Run("Aggregate error with non-existent field and strict_mode=indexes", func(t *testing.T) {
		q := DB.Query(testItemsNs)
		q.q.Strict(reindexer.QueryStrictModeIndexes)

		q.AggregateSum(nonExistentField)
		it := q.Exec(t)
		require.Error(t, it.Error())
		defer it.Close()

		err_msg := "Current query strict mode allows aggregate index fields only. There are no indexes with name 'NonExistentField' in namespace 'test_items'"
		assert.ErrorContains(t, it.Error(), err_msg)
	})

	t.Run("Aggregate error with non-existent field and strict_mode=names", func(t *testing.T) {
		q := DB.Query(testItemsNs)
		q.q.Strict(reindexer.QueryStrictModeNames)

		q.AggregateSum(nonExistentField)
		it := q.Exec(t)
		require.Error(t, it.Error())
		defer it.Close()

		err_msg := "Current query strict mode allows aggregate existing fields only. There are no fields with name 'NonExistentField' in namespace 'test_items'"
		assert.ErrorContains(t, it.Error(), err_msg)
	})
}

func TestAggregationsFetching(t *testing.T) {
	// Validate, that distinct results will remain valid after query results fetching.
	// Actual aggregation values will be sent for initial 'select' only, but must be available at any point of iterator's lifetime.

	const ns = testItemsAggsFetchingNs
	const nsSize = 50
	FillTestItemsWithFunc(ns, 0, nsSize, 0, newTestItemSimple)

	it := DBD.Query(ns).FetchCount(nsSize / 5).Distinct("id").ReqTotal().Explain().MustExec()
	defer it.Close()
	assert.NoError(t, it.Error())
	assert.Equal(t, nsSize, it.Count())
	aggs := it.AggResults()
	assert.Equal(t, 2, len(aggs))
	assert.Equal(t, "distinct", aggs[0].Type)
	assert.Equal(t, "count", aggs[1].Type)
	explain, err := it.GetExplainResults()
	assert.NoError(t, err)
	for it.Next() {
		assert.NoError(t, it.Error())
		assert.Equal(t, aggs, it.AggResults())
		exp, err := it.GetExplainResults()
		assert.NoError(t, err)
		assert.Equal(t, explain, exp)
	}
}

func TestQrIdleTimeout(t *testing.T) {
	if len(DB.slaveList) > 0 {
		t.Skip()
	}
	if !strings.HasPrefix(*dsn, "cproto") {
		t.Skip()
	}
	t.Parallel()

	const ns = testItemsQrIdleNs
	const nsSize = 600
	FillTestItemsWithFunc(ns, 0, nsSize, 0, newTestItemSimple)

	t.Run("check if qr wil be correctly reused after connections drop", func(t *testing.T) {
		db, err := reindexer.NewReindex(*dsn, reindexer.WithConnPoolSize(1), reindexer.WithDedicatedServerThreads())
		require.NoError(t, err)
		db.SetLogger(testLogger)
		err = db.RegisterNamespace(ns, reindexer.DefaultNamespaceOptions(), TestItemSimple{})
		require.NoError(t, err)
		const qrCount = 32
		const fetchCount = 1
		qrs := make([]*reindexer.Iterator, 0, 2*qrCount)

		for i := 0; i < qrCount; i++ {
			it := db.Query(ns).FetchCount(fetchCount).Limit(fetchCount * 2).Exec()
			assert.NoError(t, it.Error())
			qrs = append(qrs, it)
		}

		// Await qrs expiration
		time.Sleep(time.Second * 25)

		// Drop connection without QRs close
		db.Close()
		db, err = reindexer.NewReindex(*dsn, reindexer.WithConnPoolSize(1))
		require.NoError(t, err)
		err = db.RegisterNamespace(ns, reindexer.DefaultNamespaceOptions(), TestItemSimple{})
		require.NoError(t, err)

		// Create 2x more QRs, than were previously created
		for i := qrCount; i < 2*qrCount; i++ {
			it := db.Query(ns).FetchCount(fetchCount).Limit(fetchCount * 2).Exec()
			assert.NoError(t, it.Error())
			qrs = append(qrs, it)
		}

		// Get data
		for i := qrCount; i < 2*qrCount; i++ {
			_, err = qrs[i].FetchAll()
			assert.NoError(t, err, "i = %d", i)
		}
	})

	t.Run("concurrent query results timeouts", func(t *testing.T) {
		db, err := reindexer.NewReindex(*dsn, reindexer.WithConnPoolSize(16), reindexer.WithDedicatedServerThreads())
		require.NoError(t, err)
		defer db.Close()
		db.SetLogger(testLogger)
		err = db.RegisterNamespace(ns, reindexer.DefaultNamespaceOptions(), TestItemSimple{})
		require.NoError(t, err)
		const fillingRoutines = 5
		const readingRoutines = 4
		const qrInEachGoroutine = 440
		const qrCount = fillingRoutines * qrInEachGoroutine
		const fetchCount = 1
		qrs := make([]*reindexer.Iterator, 0, qrCount+1)
		var mtx sync.Mutex
		var wg sync.WaitGroup
		wg.Add(fillingRoutines)
		for i := 0; i < fillingRoutines; i++ {
			go func() {
				defer wg.Done()
				for j := 0; j < qrInEachGoroutine; j++ {
					it := db.Query(ns).FetchCount(fetchCount).Limit(fetchCount * 2).Exec()
					assert.NoError(t, it.Error())
					mtx.Lock()
					qrs = append(qrs, it)
					mtx.Unlock()
				}
			}()
		}
		wg.Wait()

		wg.Add(readingRoutines)
		var stop int32 = 0
		for i := 0; i < readingRoutines; i++ {
			go func(mult int) {
				defer wg.Done()
				counter := 0
				for s := atomic.LoadInt32(&stop); s == 0; s = atomic.LoadInt32(&stop) {
					counter++
					it := db.Query(ns).FetchCount(fetchCount).Limit(fetchCount * mult).Exec()
					assert.NoError(t, it.Error())
					for it.Next() {
						assert.NoError(t, it.Error())
						time.Sleep(1 * time.Millisecond)
					}
					time.Sleep(10 * time.Millisecond)
				}
			}(i + 5)
		}

		const iterations = 40
		const limit = iterations * (fetchCount + 2)
		require.Less(t, limit, nsSize)
		it := db.Query(ns).FetchCount(fetchCount).Limit(limit).Exec()
		assert.NoError(t, it.Error())
		qrs = append(qrs, it)

		for i := 0; i < iterations; i++ {
			for j := 0; j < fetchCount+1; j++ {
				qrs[qrCount].Next()
				assert.NoError(t, qrs[qrCount].Error())
			}
			time.Sleep(time.Second * 1)
		}
		_, err = qrs[qrCount].FetchAll()
		assert.NoError(t, err)

		if testLogger != nil {
			testLogger.Printf(reindexer.ERROR, "----- Expecting a lot of query results timeout errors after this line -----")
		}
		for i := 1; i < qrCount; i++ {
			_, err = qrs[i].FetchAll()
			assert.Error(t, err, "i = %d", i)
		}

		atomic.AddInt32(&stop, 1)
		wg.Wait()
		if testLogger != nil {
			testLogger.Printf(reindexer.ERROR, "----- No more query results timeout errors after this line -----")
		}
	})

	t.Run("check if timed out query results will be reused after client's qr buffer overflow", func(t *testing.T) {
		db, err := reindexer.NewReindex(*dsn, reindexer.WithConnPoolSize(1), reindexer.WithDedicatedServerThreads())
		require.NoError(t, err)
		defer db.Close()
		db.SetLogger(testLogger)
		err = db.RegisterNamespace(ns, reindexer.DefaultNamespaceOptions(), TestItemSimple{})
		require.NoError(t, err)
		const qrCount = 256
		const fetchCount = 1
		qrs := make([]*reindexer.Iterator, 0, 2*qrCount)

		for i := 0; i < qrCount; i++ {
			it := db.Query(ns).FetchCount(fetchCount).Limit(fetchCount * 2).Exec()
			assert.NoError(t, it.Error())
			qrs = append(qrs, it)
		}

		time.Sleep(time.Second * 30)

		for i := 0; i < qrCount; i++ {
			it := db.Query(ns).FetchCount(fetchCount).Limit(fetchCount * 2).Exec()
			assert.NoError(t, it.Error())
			qrs = append(qrs, it)
		}

		if testLogger != nil {
			testLogger.Printf(reindexer.ERROR, "----- Expecting connection drop and a lot of query results EOF errors after this line -----")
		}

		// Actual overflow. Connect must be dropped
		it := db.Query(ns).FetchCount(fetchCount).Limit(fetchCount * 2).Exec()
		assert.Error(t, it.Error())

		// Old Query results must be invalidated
		for i := 0; i < 2*qrCount; i++ {
			_, err = qrs[i].FetchAll()
			assert.Error(t, err, "i = %d", i)
		}

		if testLogger != nil {
			testLogger.Printf(reindexer.ERROR, "----- No more query results EOF errors after this line -----")
		}

		// Trying to create new QRs after connection drop
		for i := 0; i < qrCount; i++ {
			it := db.Query(ns).FetchCount(fetchCount).Limit(fetchCount * 2).Exec()
			assert.NoError(t, it.Error())
			qrs[i] = it
		}
		for i := 0; i < qrCount; i++ {
			_, err = qrs[i].FetchAll()
			assert.NoError(t, err, "i = %d", i)
		}
	})
}

func TestQueryExplain(t *testing.T) {
	t.Parallel()

	const ns = testItemsExplainNs

	tx := newTestTx(DB, ns)
	for i := 0; i < 5; i++ {
		tx.Upsert(TestItemSimple{ID: i, Year: i, Name: randString()})
	}
	tx.MustCommit()

	t.Run("Subquery explain check (WhereQuery)", func(t *testing.T) {
		q := DB.Query(ns).Explain().
			WhereQuery(t, DB.Query(ns).Select("id").Where("year", reindexer.EQ, 1), reindexer.GE, 0)
		it := q.MustExec(t)
		defer it.Close()
		explainRes, err := it.GetExplainResults()
		require.NoError(t, err)
		require.NotNil(t, explainRes)

		printExplainRes(explainRes)
		checkExplain(t, explainRes.Selectors, []expectedExplain{
			{
				Field:       "-scan",
				Method:      "scan",
				Keys:        0,
				Comparators: 0,
				Matched:     5,
			},
		}, "")
		checkExplainSubqueries(t, explainRes.SubQueriesExplains, []expectedExplainSubQuery{
			{
				Namespace: ns,
				Selectors: []expectedExplain{
					{
						Field:       "year",
						FieldType:   "indexed",
						Method:      "index",
						Keys:        1,
						Comparators: 0,
						Matched:     1,
					},
					{
						Field:       "id",
						FieldType:   "indexed",
						Method:      "scan",
						Keys:        0,
						Comparators: 1,
						Matched:     1,
					},
				},
			},
		})
	})

	t.Run("Subquery explain check (Where)", func(t *testing.T) {
		q := DB.Query(ns).Explain().
			Where("id", reindexer.EQ, DB.Query(ns).Select("id").Where("year", reindexer.EQ, 3))
		it := q.MustExec(t)
		defer it.Close()
		explainRes, err := it.GetExplainResults()
		require.NoError(t, err)
		require.NotNil(t, explainRes)

		printExplainRes(explainRes)
		checkExplain(t, explainRes.Selectors, []expectedExplain{
			{
				Field:       "id",
				FieldType:   "indexed",
				Method:      "index",
				Keys:        1,
				Comparators: 0,
				Matched:     1,
			},
		}, "")
		checkExplainSubqueries(t, explainRes.SubQueriesExplains, []expectedExplainSubQuery{
			{
				Namespace: ns,
				Field:     "id",
				Selectors: []expectedExplain{
					{
						Field:       "year",
						FieldType:   "indexed",
						Method:      "index",
						Keys:        1,
						Comparators: 0,
						Matched:     1,
					},
				},
			},
		})
	})

	t.Run("Subquery explain check (Where + WhereQuery)", func(t *testing.T) {
		q := DB.Query(ns).Explain().
			Where("id", reindexer.SET, DB.Query(ns).Select("id").Where("year", reindexer.SET, []int{1, 2})).
			WhereQuery(t, DB.Query(ns).Select("id").Where("year", reindexer.EQ, 5), reindexer.LE, 10)
		it := q.MustExec(t)
		defer it.Close()
		explainRes, err := it.GetExplainResults()
		require.NoError(t, err)
		require.NotNil(t, explainRes)

		printExplainRes(explainRes)
		checkExplain(t, explainRes.Selectors, []expectedExplain{
			{
				Field:       "always_false",
				Method:      "index",
				Keys:        1,
				Comparators: 0,
				Matched:     0,
			},
		}, "")
		checkExplainSubqueries(t, explainRes.SubQueriesExplains, []expectedExplainSubQuery{
			{
				Namespace: ns,
				Field:     "id",
				Selectors: []expectedExplain{
					{
						Field:       "-scan",
						Method:      "scan",
						Keys:        0,
						Comparators: 0,
						Matched:     5,
					},
					{
						Field:       "year",
						FieldType:   "indexed",
						Method:      "scan",
						Keys:        0,
						Comparators: 1,
						Matched:     2,
					},
				},
			},
			{
				Namespace: ns,
				Selectors: []expectedExplain{
					{
						Field:       "year",
						FieldType:   "indexed",
						Method:      "index",
						Keys:        0,
						Comparators: 0,
						Matched:     0,
					},
					{
						Field:       "id",
						FieldType:   "indexed",
						Method:      "scan",
						Keys:        0,
						Comparators: 1,
						Matched:     0,
					},
				},
			},
		})
	})
}
