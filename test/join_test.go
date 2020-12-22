package reindexer

import (
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"

	"github.com/restream/reindexer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	tnamespaces["test_items_for_join"] = TestItem{}
	tnamespaces["test_join_items"] = TestJoinItem{}
}

type TestJoinItem struct {
	ID       int    `reindex:"id,,pk"`
	Name     string `reindex:"name,tree"`
	Location string `reindex:"location"`
	Device   string `reindex:"device"`
	Amount   int    `reindex:"amount,tree"`
	Price    int    `json:"price"`
}

func (item *TestItem) Join(field string, subitems []interface{}, context interface{}) {
	switch strings.ToLower(field) {
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

func TestJoin(t *testing.T) {
	FillTestItems("test_items_for_join", 0, 10000, 20)
	FillTestJoinItems(7000, 500, "test_join_items")
	for _, left := range []bool{true, false} {
		for _, inner := range []bool{true, false} {
			if inner {
				for _, whereOrJoin := range []bool{true, false} {
					for _, orInner := range []bool{true, false} {
						CheckTestItemsJoinQueries(t, left, inner, whereOrJoin, orInner)
					}
				}
			} else {
				CheckTestItemsJoinQueries(t, left, false, false, false)
			}
		}
	}
	CheckJoinsAsWhereCondition(t)
}

func FillTestJoinItems(start int, count int, ns string) {
	tx := newTestTx(DB, ns)

	for i := 0; i < count; i++ {
		if err := tx.Upsert(&TestJoinItem{
			ID:       i + start,
			Name:     "price_" + randString(),
			Location: randLocation(),
			Device:   randDevice(),
			Amount:   rand.Int() % 10,
			Price:    rand.Int() % 1000,
		}); err != nil {
			panic(err)
		}
	}
	tx.MustCommit()
}

type byID []*TestJoinItem

func (s byID) Len() int {
	return len(s)
}
func (s byID) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s byID) Less(i, j int) bool {
	return s[i].ID < s[j].ID
}

func CheckJoinsAsWhereCondition(t *testing.T) {
	qj1 := DB.Query("test_join_items").Where("DEVICE", reindexer.EQ, "ottstb").Sort("NAME", true)
	qj2 := DB.Query("test_join_items").Where("DEVICE", reindexer.EQ, "android").Where("AMOUNT", reindexer.GT, 2)
	qj3 := DB.Query("test_join_items").Where("DEVICE", reindexer.EQ, "iphone")

	qjoin := DB.Query("test_join_items").Where("GENRE", reindexer.GE, 1).Limit(100).Debug(reindexer.TRACE)
	qjoin.InnerJoin(qj1, "PRICES").On("PRICE_ID", reindexer.SET, "ID")
	qjoin.Or().Where("DEVICE", reindexer.EQ, "android")
	qjoin.Or().InnerJoin(qj2, "PRICESX").On("LOCATION", reindexer.EQ, "LOCATION").On("PRICE_ID", reindexer.SET, "ID")
	qjoin.Or().InnerJoin(qj3, "PRICESX").On("LOCATION", reindexer.LT, "LOCATION").Or().On("PRICE_ID", reindexer.SET, "ID")
	qjoin.Or().Where("DEVICE", reindexer.EQ, "iphone")

	rjcheck := make([]interface{}, 0, 1000)

	jr, _ := DB.Query("test_join_items").Where("GENRE", reindexer.GE, 1).Limit(100).MustExec().FetchAll()
	for _, iitem := range jr {
		item := iitem.(*TestItem)
		rj1, _ := DB.Query("test_join_items").
			Where("DEVICE", reindexer.EQ, "ottstb").
			Where("ID", reindexer.SET, item.PricesIDs).
			Sort("NAME", true).
			MustExec().FetchAll()

		if len(rj1) != 0 {
			item.Prices = make([]*TestJoinItem, 0, len(rj1))
			for _, rrj := range rj1 {
				item.Prices = append(item.Prices, rrj.(*TestJoinItem))
			}
		} else {
			jitem := iitem.(*TestJoinItem)
			if jitem.Device != "android" {
				rj2 := DB.Query("test_join_items").
					Where("DEVICE", reindexer.EQ, "android").
					Where("AMOUNT", reindexer.GT, 2).
					Where("ID", reindexer.SET, item.PricesIDs).
					Where("LOCATION", reindexer.EQ, item.LocationID).
					Sort("NAME", true).
					Limit(30).
					MustExec()

				rj3 := DB.Query("test_join_items").
					Where("DEVICE", reindexer.EQ, "iphone").
					Where("ID", reindexer.SET, item.PricesIDs).Or().
					Where("LOCATION", reindexer.LT, item.LocationID).
					MustExec()

				item.Pricesx = make([]*TestJoinItem, 0)
				for rj2.Next() {
					item.Pricesx = append(item.Pricesx, rj2.Object().(*TestJoinItem))
				}
				for rj3.Next() {
					item.Pricesx = append(item.Pricesx, rj3.Object().(*TestJoinItem))
				}
				rj2.Close()
				rj3.Close()
			}
		}
		rjcheck = append(rjcheck, item)
		if len(rjcheck) == 100 {
			break
		}
	}

	rjoin, _ := qjoin.MustExec().FetchAll()
	assert.Equal(t, len(rjcheck), len(rjoin))
	for i := 0; i < len(rjcheck); i++ {
		i1 := rjcheck[i].(*TestItem)
		i2 := rjoin[i].(*TestItem)
		sort.Sort(byID(i1.Pricesx))
		sort.Sort(byID(i2.Pricesx))
		sort.Sort(byID(i1.Prices))
		sort.Sort(byID(i2.Prices))
		assert.Equal(t, i1, i2)
	}
}

func appendJoined(item *TestItem, jr1 *reindexer.Iterator, jr2 *reindexer.Iterator) {
	item.Pricesx = make([]*TestJoinItem, 0)
	for jr1.Next() {
		item.Pricesx = append(item.Pricesx, jr1.Object().(*TestJoinItem))
	}
	for jr2.Next() {
		item.Pricesx = append(item.Pricesx, jr2.Object().(*TestJoinItem))
	}
	jr1.Close()
	jr2.Close()
}

const (
	ageMin = 1
	ageMax = 3
)

type addCondition func()

func shuffle(n int, swap func(i, j int)) {
	if n < 0 {
		panic("invalid argument to Shuffle")
	}

	i := n - 1
	for ; i > 1<<31-1-1; i-- {
		j := int(rand.Int63n(int64(i + 1)))
		swap(i, j)
	}
	for ; i > 0; i-- {
		j := int(rand.Int31n(int32(i + 1)))
		swap(i, j)
	}
}

func permutateOr(q *queryTest, orConditions []addCondition) {
	if len(orConditions) == 0 {
		return
	}
	if len(orConditions) == 1 {
		panic(fmt.Errorf("Or cannot connect just 1 condition"))
	}
	shuffle(len(orConditions), func(i, j int) {
		orConditions[i], orConditions[j] = orConditions[j], orConditions[i]
	})
	orConditions[0]()
	for i := 1; i < len(orConditions); i++ {
		q.Or()
		orConditions[i]()
	}
}

func permutate(q *queryTest, andConditions []addCondition, orConditions []addCondition) {
	var indexes []int
	for i := 0; i <= len(andConditions); i++ {
		indexes = append(indexes, i)
	}
	shuffle(len(indexes), func(i, j int) {
		indexes[i], indexes[j] = indexes[j], indexes[i]
	})
	for i := range indexes {
		if i == len(andConditions) {
			permutateOr(q, orConditions)
		} else {
			andConditions[i]()
		}
	}
}

func CheckTestItemsJoinQueries(t *testing.T, left, inner, whereOrJoin bool, orInner bool) {
	qj1 := DB.Query("test_join_items").Where("DEVICE", reindexer.EQ, "ottstb").Sort("NAME", true)
	qj2 := DB.Query("test_join_items").Where("DEVICE", reindexer.EQ, "android").Where("AMOUNT", reindexer.GT, 2)
	qj3 := DB.Query("test_join_items").Where("DEVICE", reindexer.EQ, "iphone")

	qjoin := DB.Query("test_items_for_join").Limit(10).Debug(reindexer.TRACE)

	var andConditions []addCondition
	var orConditions []addCondition

	andConditions = append(andConditions, func() {
		qjoin.Where("GENRE", reindexer.EQ, 10)
	})

	if left {
		andConditions = append(andConditions, func() {
			qjoin.LeftJoin(qj1, "PRICES").On("PRICE_ID", reindexer.SET, "ID")
		})
	}
	if inner {
		firstInner := func() {
			qjoin.InnerJoin(qj2, "PRICESX").On("LOCATION", reindexer.EQ, "LOCATION").On("PRICE_ID", reindexer.SET, "ID")
		}
		if whereOrJoin || orInner {
			orConditions = append(orConditions, firstInner)
		} else {
			andConditions = append(andConditions, firstInner)
		}
		secondInner := func() {
			qjoin.InnerJoin(qj3, "PRICESX").On("LOCATION", reindexer.LT, "LOCATION").Or().On("PRICE_ID", reindexer.SET, "ID")
		}
		if orInner {
			orConditions = append(orConditions, secondInner)
		} else {
			andConditions = append(andConditions, secondInner)
		}
		if whereOrJoin {
			orConditions = append(orConditions, func() {
				qjoin.Where("AGE", reindexer.RANGE, []int{ageMin, ageMax})
			})
		}
	}
	permutate(qjoin, andConditions, orConditions)

	rjoin, _ := qjoin.MustExec().FetchAll()

	// for _, rr := range rjoin {
	// 	item := rr.(*TestItem)
	// 	log.Printf("%#v %d -> %#d,%#d\n", item.PricesIDs, item.LocationID, len(item.Pricesx), len(item.Prices))
	// }

	// Verify join results with manual join
	r1, _ := DB.Query("test_items_for_join").Where("genre", reindexer.EQ, 10).MustExec().FetchAll()
	rjcheck := make([]interface{}, 0, 1000)

	for _, iitem := range r1 {

		item := iitem.(*TestItem)
		if left {
			rj1, _ := DB.Query("test_join_items").
				Where("DEVICE", reindexer.EQ, "ottstb").
				Where("ID", reindexer.SET, item.PricesIDs).
				Sort("NAME", true).
				MustExec().FetchAll()

			if len(rj1) != 0 {
				item.Prices = make([]*TestJoinItem, 0, len(rj1))
				for _, rrj := range rj1 {
					item.Prices = append(item.Prices, rrj.(*TestJoinItem))
				}
			}
		}

		if inner {
			rj2 := DB.Query("test_join_items").
				Where("DEVICE", reindexer.EQ, "android").
				Where("AMOUNT", reindexer.GT, 2).
				Where("ID", reindexer.SET, item.PricesIDs).
				Where("LOCATION", reindexer.EQ, item.LocationID).
				Sort("NAME", true).
				Limit(30).
				MustExec()

			rj3 := DB.Query("test_join_items").
				Where("DEVICE", reindexer.EQ, "iphone").
				Where("ID", reindexer.SET, item.PricesIDs).Or().
				Where("LOCATION", reindexer.LT, item.LocationID).
				MustExec()

			if whereOrJoin && orInner {
				if rj2.Count() != 0 || rj3.Count() != 0 {
					appendJoined(item, rj2, rj3)
				} else {
					rj2.Close()
					rj3.Close()
					if item.Age < ageMin || item.Age > ageMax {
						continue
					}
				}
			} else if whereOrJoin && !orInner {
				if rj3.Count() != 0 && (rj2.Count() != 0 || (item.Age >= ageMin && item.Age <= ageMax)) {
					appendJoined(item, rj2, rj3)
				} else {
					rj2.Close()
					rj3.Close()
					continue
				}
			} else {
				if (orInner && (rj2.Count() != 0 || rj3.Count() != 0)) || (!orInner && (rj2.Count() != 0 && rj3.Count() != 0)) {
					appendJoined(item, rj2, rj3)
				} else {
					rj2.Close()
					rj3.Close()
					continue
				}
			}
		}
		rjcheck = append(rjcheck, item)

		if len(rjcheck) == 10 {
			break
		}
	}

	assert.Equal(t, len(rjcheck), len(rjoin))
	for i := 0; i < len(rjcheck); i++ {
		i1 := rjcheck[i].(*TestItem)
		i2 := rjoin[i].(*TestItem)
		sort.Sort(byID(i1.Pricesx))
		sort.Sort(byID(i2.Pricesx))
		sort.Sort(byID(i1.Prices))
		sort.Sort(byID(i2.Prices))
		assert.Equal(t, i1, i2)
	}
}

func TestJoinQueryResultsOnIterator(t *testing.T) {
	qjoin := DB.Query("test_items_for_join").Where("GENRE", reindexer.EQ, 10).Limit(10).Debug(reindexer.TRACE)
	qj1 := DB.Query("test_join_items").Where("DEVICE", reindexer.EQ, "ottstb").Sort("name", false)
	qj2 := DB.Query("test_join_items").Where("DEVICE", reindexer.EQ, "android")
	qjoin.LeftJoin(qj1, "PRICES").On("PRICE_ID", reindexer.SET, "ID").
		InnerJoin(qj2, "PRICESX").On("LOCATION", reindexer.EQ, "LOCATION").On("PRICE_ID", reindexer.SET, "Id")

	var handlerSubitems []interface{}

	qjoin.JoinHandler("PRICES", func(field string, item interface{}, subitems []interface{}) (isContinue bool) {

		assert.True(t, strings.EqualFold(field, "prices"), "field expected: '%v'; actual: '%v'", "prices", field)
		assert.NotNil(t, item, "item in handler is nil")

		handlerSubitems = subitems
		return true
	})

	iter := qjoin.MustExec()
	defer iter.Close()

	for iter.Next() {
		item := iter.Object().(*TestItem)
		joinResultsPrices, err := iter.JoinedObjects("PRICES")
		assert.NoError(t, err)
		joinResultsPricesx, err := iter.JoinedObjects("PRICESX")
		assert.NoError(t, err)

		for i := range item.Prices {
			assert.EqualValues(t, item.Prices[i], joinResultsPrices[i])
			assert.EqualValues(t, item.Prices[i], handlerSubitems[i])
		}
		for i := range item.Pricesx {
			assert.EqualValues(t, item.Pricesx[i], joinResultsPricesx[i])
		}
	}
}

type explainNs struct {
	Id                int          `reindex:"id,,pk"`
	Data              int          `reindex:"data"`
	InnerJoinedData   []*explainNs `reindex:"inner_joined,,joined"`
	OrInnerJoinedData []*explainNs `reindex:"or_inner_joined,,joined"`
	LeftJoinedData    []*explainNs `reindex:"left_joined,,joined"`
}

func initNsForExplain(t *testing.T, ns string, count int) {
	DB.DropNamespace(ns)
	err := DB.OpenNamespace(ns, reindexer.DefaultNamespaceOptions(), explainNs{})
	assert.NoError(t, err)
	tx := newTestTx(DB, ns)
	for i := 0; i < count; i++ {
		testItem := explainNs{i, i, nil, nil, nil}
		err = tx.Upsert(testItem)
		assert.NoError(t, err)
	}
	tx.MustCommit()
}

type expectedExplain struct {
	Field       string
	Method      string
	Keys        int
	Comparators int
	Matched     int
	Preselect   []expectedExplain
	JoinSelect  []expectedExplain
}

func checkExplain(t *testing.T, res *reindexer.ExplainResults, expected []expectedExplain, fieldName string) {
	require.Equal(t, len(expected), len(res.Selectors))
	for i := 0; i < len(expected); i++ {
		assert.Equalf(t, expected[i].Field, res.Selectors[i].Field, fieldName+expected[i].Field)
		assert.Equalf(t, expected[i].Method, res.Selectors[i].Method, fieldName+expected[i].Field)
		assert.Equalf(t, expected[i].Matched, res.Selectors[i].Matched, fieldName+expected[i].Field)
		assert.Equalf(t, expected[i].Keys, res.Selectors[i].Keys, fieldName+expected[i].Field)
		assert.Equalf(t, expected[i].Comparators, res.Selectors[i].Comparators, fieldName+expected[i].Field)
		if len(expected[i].Preselect) == 0 {
			assert.Nil(t, res.Selectors[i].ExplainPreselect, fieldName+expected[i].Field)
		} else {
			checkExplain(t, res.Selectors[i].ExplainPreselect, expected[i].Preselect, fieldName+expected[i].Field+" -> ")
		}
		if len(expected[i].JoinSelect) == 0 {
			assert.Nil(t, res.Selectors[i].ExplainSelect, fieldName+expected[i].Field)
		} else {
			checkExplain(t, res.Selectors[i].ExplainSelect, expected[i].JoinSelect, fieldName+expected[i].Field+" -> ")
		}
	}
}

func TestExplainJoin(t *testing.T) {
	nsMain := "test_explain_main"
	nsJoined := "test_explain_joined"
	initNsForExplain(t, nsMain, 5)
	initNsForExplain(t, nsJoined, 20)

	qjoin1 := DB.Query(nsJoined).Where("data", reindexer.GT, 0)
	qjoin2 := DB.Query(nsJoined).Where("data", reindexer.SET, []int{1, 2, 4})
	qjoin3 := DB.Query(nsJoined).Where("data", reindexer.EQ, 1)
	q := DB.Query(nsMain).Explain()
	q.InnerJoin(qjoin1, "inner_joined").On("id", reindexer.EQ, "id")
	q.Or().Where("id", reindexer.EQ, 1)
	q.Or().InnerJoin(qjoin2, "or_inner_joined").On("id", reindexer.EQ, "id")
	q.Not().Where("data", reindexer.EQ, 4)
	q.LeftJoin(qjoin3, "left_joined").On("id", reindexer.EQ, "id")

	iter := q.MustExec()
	defer iter.Close()
	explainRes, err := iter.GetExplainResults()
	assert.NoError(t, err)
	assert.NotNil(t, explainRes)
	checkExplain(t, explainRes, []expectedExplain{
		{
			Field:       "",
			Method:      "scan",
			Keys:        0,
			Comparators: 0,
			Matched:     5,
			Preselect:   nil,
			JoinSelect:  nil,
		},
		{
			Field:       "not data",
			Method:      "index",
			Keys:        1,
			Comparators: 0,
			Matched:     1,
			Preselect:   nil,
			JoinSelect:  nil,
		},
		{
			Field:       "inner_join test_explain_joined",
			Method:      "no_preselect",
			Keys:        1,
			Comparators: 0,
			Matched:     3,
			Preselect:   nil,
			JoinSelect: []expectedExplain{
				{
					Field:       "id",
					Method:      "index",
					Keys:        1,
					Comparators: 0,
					Matched:     1,
					Preselect:   nil,
					JoinSelect:  nil,
				},
				{
					Field:       "data",
					Method:      "scan",
					Keys:        0,
					Comparators: 1,
					Matched:     0,
					Preselect:   nil,
					JoinSelect:  nil,
				},
			},
		},
		{
			Field:       " or id",
			Method:      "index",
			Keys:        1,
			Comparators: 0,
			Matched:     1,
			Preselect:   nil,
			JoinSelect:  nil,
		},
		{
			Field:       "or_inner_join test_explain_joined",
			Method:      "preselected_values",
			Keys:        3,
			Comparators: 0,
			Matched:     2,
			Preselect: []expectedExplain{
				{
					Field:       "data",
					Method:      "index",
					Keys:        3,
					Comparators: 0,
					Matched:     3,
					Preselect:   nil,
					JoinSelect:  nil,
				},
			},
			JoinSelect: nil,
		},
		{
			Field:       "left_join test_explain_joined",
			Method:      "preselected_values",
			Keys:        1,
			Comparators: 0,
			Matched:     1,
			Preselect: []expectedExplain{
				{
					Field:       "data",
					Method:      "index",
					Keys:        1,
					Comparators: 0,
					Matched:     1,
					Preselect:   nil,
					JoinSelect:  nil,
				},
			},
			JoinSelect: nil,
		},
	}, "")
}
