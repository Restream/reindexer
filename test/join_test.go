package reindexer

import (
	"fmt"
	"log"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/restream/reindexer/v5"
	"github.com/restream/reindexer/v5/bindings/builtinserver/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestJoinItem struct {
	ID        int      `reindex:"id,,pk"`
	Name      string   `reindex:"name,tree"`
	Location  string   `reindex:"location"`
	Device    string   `reindex:"device"`
	Amount    int      `reindex:"amount,tree"`
	Price     int      `json:"price"`
	Uuid      string   `reindex:"uuid,hash,uuid" json:"uuid"`
	UuidArray []string `reindex:"uuid_array,hash,uuid" json:"uuid_array"`
}

type TestItemWithJoinedField struct {
	Id          int             `reindex:"id,hash,pk"`
	JoinedField []*TestJoinItem `reindex:"j,,joined"`
}

type explainNs struct {
	Id                int          `reindex:"id,,pk"`
	Data              int          `reindex:"data"`
	InnerJoinedData   []*explainNs `reindex:"inner_joined,,joined"`
	OrInnerJoinedData []*explainNs `reindex:"or_inner_joined,,joined"`
	LeftJoinedData    []*explainNs `reindex:"left_joined,,joined"`
}

type expectedExplain struct {
	Field       string
	FieldType   string
	Method      string
	Description string
	Keys        int
	Comparators int
	Matched     int
	Preselect   []expectedExplain
	JoinSelect  []expectedExplain
	Selectors   []expectedExplain
}

type expectedExplainConditionInjection struct {
	InitialCondition   string
	AggType            string
	Succeed            bool
	Reason             string
	NewCondition       string
	ValuesCount        int
	ConditionSelectors []expectedExplain
}

type expectedExplainJoinOnInjections struct {
	RightNsName       string
	JoinOnCondition   string
	Succeed           bool
	Reason            string
	Type              string
	InjectedCondition string
	Conditions        []expectedExplainConditionInjection
}

type expectedExplainSubQuery struct {
	Namespace string
	Keys      int
	Field     string
	Selectors []expectedExplain
}

type strictJoinHandlerNs struct {
	Id         int                    `reindex:"id,,pk"`
	Data       int                    `reindex:"data"`
	JoinedData []*strictJoinHandlerNs `reindex:"joined_data,,joined"`
}

type addCondition func()

type byID []*TestJoinItem

const (
	ageMin = 1
	ageMax = 3
)

const (
	testItemsForJoinNs = "test_items_for_join"
	testJoinItemsNs    = "test_join_items"
	testJoinedFieldNs  = "test_joined_field"

	testItemsForModifyJoinNs = "test_items_for_modify_join"
	testModifyJoinItemsNs    = "test_modify_join_items"

	testExplainMainNs   = "test_explain_main"
	testExplainJoinedNs = "test_explain_joined"

	// should not be in init()
	testStrictJoinHandlersMainNs   = "strict_join_handlers_main"
	testStrictJoinHandlersJoinedNs = "strict_join_handlers_joined"
)

func init() {
	tnamespaces[testItemsForJoinNs] = TestItem{}
	tnamespaces[testJoinItemsNs] = TestJoinItem{}
	tnamespaces[testJoinedFieldNs] = TestItemWithJoinedField{}

	tnamespaces[testItemsForModifyJoinNs] = TestItem{}
	tnamespaces[testModifyJoinItemsNs] = TestJoinItem{}

	tnamespaces[testExplainMainNs] = explainNs{}
	tnamespaces[testExplainJoinedNs] = explainNs{}
}

func (s byID) Len() int {
	return len(s)
}

func (s byID) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s byID) Less(i, j int) bool {
	return s[i].ID < s[j].ID
}

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

func initNsForStrictJoinHandlers(t *testing.T, db *reindexer.Reindexer, ns string, count int) {
	db.DropNamespace(ns)
	err := db.OpenNamespace(ns, reindexer.DefaultNamespaceOptions().NoStorage(), strictJoinHandlerNs{})
	assert.NoError(t, err)

	tx := db.MustBeginTx(ns)
	for i := 0; i < count; i++ {
		err = tx.Upsert(strictJoinHandlerNs{i, i, nil})
		assert.NoError(t, err)
	}
	tx.MustCommit()
}

func FillTestJoinItems(start int, count int, ns string) {
	tx := newTestTx(DB, ns)

	for i := 0; i < count; i++ {
		if err := tx.Upsert(&TestJoinItem{
			ID:        i + start,
			Name:      "price_" + randString(),
			Location:  randLocation(),
			Device:    randDevice(),
			Amount:    rand.Int() % 10,
			Price:     rand.Int() % 1000,
			Uuid:      randUuid(),
			UuidArray: randUuidArray(rand.Int() % 20),
		}); err != nil {
			panic(err)
		}
	}
	tx.MustCommit()
}

func fillExplainNs(t *testing.T, ns string, count int) {
	tx := newTestTx(DB, ns)
	for i := 0; i < count; i++ {
		testItem := explainNs{i, i, nil, nil, nil}
		err := tx.Upsert(testItem)
		assert.NoError(t, err)
	}
	tx.MustCommit()
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

func CheckTestItemsJoinQueries(t *testing.T, left, inner, whereOrJoin bool, orInner bool) {
	qj1 := DB.Query(testJoinItemsNs).Where("DEVICE", reindexer.EQ, "ottstb").Sort("NAME", true)
	qj2 := DB.Query(testJoinItemsNs).Where("DEVICE", reindexer.EQ, "android").Where("AMOUNT", reindexer.GT, 2)
	qj3 := DB.Query(testJoinItemsNs).Where("DEVICE", reindexer.EQ, "iphone")

	qjoin := DB.Query(testItemsForJoinNs).Limit(10).Debug(reindexer.TRACE)

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

	rjoin, err := qjoin.MustExec(t).FetchAll()
	require.NoError(t, err)

	// Verify join results with manual join
	r1, err := DB.Query(testItemsForJoinNs).Where("genre", reindexer.EQ, 10).MustExec(t).FetchAll()
	require.NoError(t, err)
	rjcheck := make([]interface{}, 0, 1000)

	for _, iitem := range r1 {

		item := iitem.(*TestItem)
		if left {
			rj1, err := DB.Query(testJoinItemsNs).
				Where("DEVICE", reindexer.EQ, "ottstb").
				Where("ID", reindexer.SET, item.PricesIDs).
				Sort("NAME", true).
				MustExec(t).FetchAll()
			require.NoError(t, err)
			if len(rj1) != 0 {
				item.Prices = make([]*TestJoinItem, 0, len(rj1))
				for _, rrj := range rj1 {
					item.Prices = append(item.Prices, rrj.(*TestJoinItem))
				}
			}
		}

		if inner {
			rj2 := DB.Query(testJoinItemsNs).
				Where("DEVICE", reindexer.EQ, "android").
				Where("AMOUNT", reindexer.GT, 2).
				Where("ID", reindexer.SET, item.PricesIDs).
				Where("LOCATION", reindexer.EQ, item.LocationID).
				Sort("NAME", true).
				Limit(30).
				MustExec(t)

			rj3 := DB.Query(testJoinItemsNs).
				Where("DEVICE", reindexer.EQ, "iphone").
				Where("ID", reindexer.SET, item.PricesIDs).Or().
				Where("LOCATION", reindexer.GT, item.LocationID).
				MustExec(t)

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

	require.Equal(t, len(rjcheck), len(rjoin))
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

func checkExplain(t *testing.T, res []reindexer.ExplainSelector, expected []expectedExplain, fieldName string) {
	require.Equal(t, len(expected), len(res))
	for i := 0; i < len(expected); i++ {
		if len(expected[i].Selectors) != 0 {
			assert.Equalf(t, expected[i].Field, res[i].Field, fieldName+expected[i].Field)
			require.Equal(t, len(expected[i].Selectors), len(res[i].Selectors), fieldName+expected[i].Field)
			checkExplain(t, res[i].Selectors, expected[i].Selectors, fieldName+expected[i].Field+"(")
		} else {
			assert.Equalf(t, len(res[i].Selectors), 0, fieldName+expected[i].Field)
			assert.Equalf(t, expected[i].Field, res[i].Field, fieldName+expected[i].Field)
			assert.Equalf(t, expected[i].FieldType, res[i].FieldType, fieldName+expected[i].Field)
			assert.Equalf(t, expected[i].Method, res[i].Method, fieldName+expected[i].Field)
			assert.Equalf(t, expected[i].Matched, res[i].Matched, fieldName+expected[i].Field)
			assert.Equalf(t, expected[i].Keys, res[i].Keys, fieldName+expected[i].Field)
			assert.Equalf(t, expected[i].Comparators, res[i].Comparators, fieldName+expected[i].Field)
			assert.Equalf(t, expected[i].Description, res[i].Description, fieldName+expected[i].Field)
			if len(expected[i].Preselect) == 0 {
				assert.Nil(t, res[i].ExplainPreselect, fieldName+expected[i].Field)
			} else {
				checkExplain(t, res[i].ExplainPreselect.Selectors, expected[i].Preselect, fieldName+expected[i].Field+" -> ")
			}
			if len(expected[i].JoinSelect) == 0 {
				assert.Nil(t, res[i].ExplainSelect, fieldName+expected[i].Field)
			} else {
				require.NotNil(t, res[i].ExplainSelect, fieldName+expected[i].Field)
				checkExplain(t, res[i].ExplainSelect.Selectors, expected[i].JoinSelect, fieldName+expected[i].Field+" -> ")
			}
		}
	}
}

func checkExplainConditionInjection(t *testing.T, resConditions []reindexer.ExplainConditionInjection, expectedConditions []expectedExplainConditionInjection) {
	for i := 0; i < len(expectedConditions); i++ {
		assert.Equal(t, expectedConditions[i].InitialCondition, resConditions[i].InitialCondition)
		assert.Equal(t, expectedConditions[i].AggType, resConditions[i].AggType)
		assert.Equal(t, expectedConditions[i].Succeed, resConditions[i].Succeed)
		assert.Equal(t, expectedConditions[i].Reason, resConditions[i].Reason)
		assert.Equal(t, expectedConditions[i].NewCondition, resConditions[i].NewCondition)
		assert.Equal(t, expectedConditions[i].ValuesCount, resConditions[i].ValuesCount)
		if len(expectedConditions[i].ConditionSelectors) == 0 {
			assert.Nil(t, resConditions[i].Explain)
		} else {
			require.NotNil(t, resConditions[i].Explain)
			checkExplain(t, resConditions[i].Explain.Selectors, expectedConditions[i].ConditionSelectors, "")
		}
	}
}

func checkExplainJoinOnInjections(t *testing.T, res []reindexer.ExplainJoinOnInjections, expected []expectedExplainJoinOnInjections) {
	require.Equal(t, len(expected), len(res))
	for i := 0; i < len(expected); i++ {
		assert.Equal(t, expected[i].RightNsName, res[i].RightNsName)
		assert.Equal(t, expected[i].JoinOnCondition, res[i].JoinOnCondition)
		assert.Equal(t, expected[i].Succeed, res[i].Succeed)
		assert.Equal(t, expected[i].Reason, res[i].Reason)
		assert.Equal(t, expected[i].Type, res[i].Type)
		assert.Equal(t, expected[i].InjectedCondition, res[i].InjectedCondition)
		if len(expected[i].Conditions) == 0 {
			assert.Nil(t, res[i].Conditions)
		} else {
			checkExplainConditionInjection(t, res[i].Conditions, expected[i].Conditions)
		}
	}
}

func getAffectedItems(t *testing.T, q *reindexer.Query, ns *testNamespace) (affected []interface{}, unaffected []interface{}) {
	affected, err := q.Exec().FetchAll()
	require.NoError(t, err)
	affectedPKs := make([][]interface{}, 0, len(affected))
	for _, it := range affected {
		affectedPKs = append(affectedPKs, getPKComposite(ns, reflect.Indirect(reflect.ValueOf(it))))
	}

	unaffected, err = DB.Query(q.Namespace).
		Not().
		Where(ns.pkIdxName, reindexer.SET, affectedPKs).
		Sort(ns.pkIdxName, false).
		Exec(t).FetchAll()
	require.NoError(t, err)
	return
}

func selectByPK(t *testing.T, items []interface{}, nsName string, ns *testNamespace) []interface{} {
	ids := make([][]interface{}, 0, len(items))
	for _, it := range items {
		pks := getPKComposite(ns, reflect.Indirect(reflect.ValueOf(it)))
		ids = append(ids, pks)
	}
	res, err := DB.Query(nsName).
		Where(ns.pkIdxName, reindexer.SET, ids).
		Sort(ns.pkIdxName, false).
		Exec(t).FetchAll()
	require.NoError(t, err)
	return res
}

func TestJoinModifyQueries(t *testing.T) {
	FillTestItems(testItemsForModifyJoinNs, 0, 5000, 20)
	FillTestJoinItems(7000, 500, testModifyJoinItemsNs)

	validateUpdateQuery := func(q *queryTest, addSetEntries func(*queryTest), transform func(*TestItem)) {
		q.Sort(q.ns.pkIdxName, false)
		qCopy1 := q.q.MakeCopy(DBD)
		affected, unaffected := getAffectedItems(t, qCopy1, q.ns)
		addSetEntries(q)
		DB.SetSyncRequired()
		updRes, err := q.Update().FetchAll()
		require.NoError(t, err)
		require.Equal(t, len(updRes), len(affected))
		require.Equal(t, unaffected, selectByPK(t, unaffected, q.namespace, q.ns))
		selRes := selectByPK(t, affected, q.namespace, q.ns)
		require.Equal(t, selRes, updRes)

		for _, oldIt := range affected {
			item := oldIt.(*TestItem)
			item.Prices = nil
			item.Pricesx = nil
			transform(oldIt.(*TestItem))
		}
		require.Equal(t, selRes, affected)

		log.Printf("Update with joins: affected rows: %v, unaffected rows: %v\n", len(affected), len(unaffected))
	}

	t.Run("update queries with not inner join", func(t *testing.T) {
		q := DB.Query(testItemsForModifyJoinNs).
			Where("GENRE", reindexer.GE, 1).
			Limit(10)
		q.Not().OpenBracket()
		q.InnerJoin(DB.Query(
			testModifyJoinItemsNs,
		).Where("DEVICE", reindexer.EQ, "android").
			Limit(0), "some random name").
			On("PRICE_ID", reindexer.SET, "ID")
		q.CloseBracket().Debug(reindexer.TRACE)

		newName := randString()
		newYear := rand.Int()

		validateUpdateQuery(q,
			func(qt *queryTest) { qt.Set("name", newName).Set("year", newYear) },
			func(item *TestItem) {
				item.Year, item.Name = newYear, newName
			})
	})

	t.Run("update queries with not inner join II: without brackets", func(t *testing.T) {
		q := DB.Query(testItemsForModifyJoinNs).
			Where("GENRE", reindexer.GE, 1).
			Limit(10)
		q.Not()
		q.InnerJoin(DB.Query(
			testModifyJoinItemsNs,
		).Where("DEVICE", reindexer.EQ, "android").
			Limit(0), "some random name").
			On("PRICE_ID", reindexer.SET, "ID")
		q.Debug(reindexer.TRACE)

		newName := randString()
		newYear := rand.Int()

		validateUpdateQuery(q,
			func(qt *queryTest) { qt.Set("name", newName).Set("year", newYear) },
			func(item *TestItem) {
				item.Year, item.Name = newYear, newName
			})
	})

	t.Run("update queries with self not inner join", func(t *testing.T) {
		q := DB.Query(testItemsForModifyJoinNs).
			Where("GENRE", reindexer.GE, 2).
			Limit(20)
		q.Not().OpenBracket()
		q.InnerJoin(DB.Query(
			testItemsForModifyJoinNs,
		).Where("GENRE", reindexer.EQ, 2),
			"some random name").
			On("ID", reindexer.SET, "ID")
		q.CloseBracket().Debug(reindexer.TRACE)

		newName := randString()
		newYear := rand.Int()

		validateUpdateQuery(q,
			func(qt *queryTest) { qt.Set("company_name", newName).Set("year", newYear) },
			func(item *TestItem) {
				item.Year, item.CompanyName = newYear, newName
			})
	})

	t.Run("update queries with self not inner join II: without brackets", func(t *testing.T) {
		q := DB.Query(testItemsForModifyJoinNs).
			Where("GENRE", reindexer.GE, 2).
			Limit(20)
		q.Not()
		q.InnerJoin(DB.Query(
			testItemsForModifyJoinNs,
		).Where("GENRE", reindexer.EQ, 2),
			"some random name").
			On("ID", reindexer.SET, "ID")
		q.Debug(reindexer.TRACE)

		newName := randString()
		newYear := rand.Int()

		validateUpdateQuery(q,
			func(qt *queryTest) { qt.Set("company_name", newName).Set("year", newYear) },
			func(item *TestItem) {
				item.Year, item.CompanyName = newYear, newName
			})
	})

	t.Run("update queries with multiple inner joins", func(t *testing.T) {
		qj1 := DB.Query(testModifyJoinItemsNs).Where("DEVICE", reindexer.EQ, "ottstb").Sort("NAME", true)
		qj2 := DB.Query(testModifyJoinItemsNs).Where("DEVICE", reindexer.EQ, "android").Where("AMOUNT", reindexer.GT, 2)
		qj3 := DB.Query(testModifyJoinItemsNs).Where("DEVICE", reindexer.EQ, "iphone")

		q := DB.Query(testItemsForModifyJoinNs).Limit(11).Debug(reindexer.TRACE)
		q.InnerJoin(qj1, "PRICES").On("PRICE_ID", reindexer.SET, "ID")
		q.InnerJoin(qj2, "PRICESX").On("LOCATION", reindexer.EQ, "LOCATION").Or().On("PRICE_ID", reindexer.SET, "ID")
		q.InnerJoin(qj3, "PRICESX").On("LOCATION", reindexer.LT, "LOCATION").Or().On("PRICE_ID", reindexer.SET, "ID")

		newName := randString()
		newYear := rand.Int()

		validateUpdateQuery(q,
			func(qt *queryTest) { qt.Set("company_name", newName).Set("year", newYear) },
			func(item *TestItem) {
				item.Year, item.CompanyName = newYear, newName
			})

	})

	// Delete queries
	validateDeleteQuery := func(q *queryTest) {
		q.Sort(q.ns.pkIdxName, false)
		qCopy1 := q.q.MakeCopy(DBD)
		affected, unaffected := getAffectedItems(t, qCopy1, q.ns)
		DB.SetSyncRequired()
		delCnt, err := q.Delete()
		require.NoError(t, err)
		require.Equal(t, delCnt, len(affected))
		require.Equal(t, unaffected, selectByPK(t, unaffected, q.namespace, q.ns))
		selRes := selectByPK(t, affected, q.namespace, q.ns)
		require.Equal(t, len(selRes), 0)

		log.Printf("Delete with join: affected rows: %v, unaffected rows: %v\n", len(affected), len(unaffected))
	}

	t.Run("delete queries with not inner join", func(t *testing.T) {
		q := DB.Query(testItemsForModifyJoinNs).
			Where("GENRE", reindexer.GE, 1).
			Limit(10)
		q.Not().OpenBracket()
		q.InnerJoin(DB.Query(
			testModifyJoinItemsNs,
		).Where("DEVICE", reindexer.EQ, "android").
			Limit(0), "some random name").
			On("PRICE_ID", reindexer.SET, "ID")
		q.CloseBracket().Debug(reindexer.TRACE)

		validateDeleteQuery(q)
	})

	t.Run("delete queries with not inner join II: without brackets", func(t *testing.T) {
		q := DB.Query(testItemsForModifyJoinNs).
			Where("GENRE", reindexer.GE, 1).
			Limit(10)
		q.Not()
		q.InnerJoin(DB.Query(
			testModifyJoinItemsNs,
		).Where("DEVICE", reindexer.EQ, "android").
			Limit(0), "some random name").
			On("PRICE_ID", reindexer.SET, "ID")
		q.Debug(reindexer.TRACE)

		validateDeleteQuery(q)
	})

	t.Run("delete queries with self not inner join", func(t *testing.T) {
		q := DB.Query(testItemsForModifyJoinNs).
			Where("GENRE", reindexer.GE, 2).
			Limit(20)
		q.Not().OpenBracket()
		q.InnerJoin(DB.Query(
			testItemsForModifyJoinNs,
		).Where("GENRE", reindexer.EQ, 2),
			"some random name").
			On("ID", reindexer.SET, "ID")
		q.CloseBracket().Debug(reindexer.TRACE)

		validateDeleteQuery(q)
	})

	t.Run("delete queries with self not inner join II: without brackets", func(t *testing.T) {
		q := DB.Query(testItemsForModifyJoinNs).
			Where("GENRE", reindexer.GE, 2).
			Limit(20)
		q.Not()
		q.InnerJoin(DB.Query(
			testItemsForModifyJoinNs,
		).Where("GENRE", reindexer.EQ, 2),
			"some random name").
			On("ID", reindexer.SET, "ID")
		q.Debug(reindexer.TRACE)

		validateDeleteQuery(q)
	})

	t.Run("delete queries with multiple inner joins", func(t *testing.T) {
		qj1 := DB.Query(testModifyJoinItemsNs).Where("DEVICE", reindexer.EQ, "ottstb").Sort("NAME", true)
		qj2 := DB.Query(testModifyJoinItemsNs).Where("DEVICE", reindexer.EQ, "android").Where("AMOUNT", reindexer.GT, 2)
		qj3 := DB.Query(testModifyJoinItemsNs).Where("DEVICE", reindexer.EQ, "iphone")

		q := DB.Query(testItemsForModifyJoinNs).Limit(11).Debug(reindexer.TRACE)
		q.InnerJoin(qj1, "PRICES").On("PRICE_ID", reindexer.SET, "ID")
		q.InnerJoin(qj2, "PRICESX").On("LOCATION", reindexer.EQ, "LOCATION").Or().On("PRICE_ID", reindexer.SET, "ID")
		q.InnerJoin(qj3, "PRICESX").On("LOCATION", reindexer.LT, "LOCATION").Or().On("PRICE_ID", reindexer.SET, "ID")

		validateDeleteQuery(q)
	})
}

func TestJoin(t *testing.T) {
	FillTestItems(testItemsForJoinNs, 0, 10000, 20)
	FillTestJoinItems(7000, 500, testJoinItemsNs)

	t.Run("check join queries", func(t *testing.T) {
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
	})

	t.Run("check join as where condition", func(t *testing.T) {
		qj1 := DB.Query(testJoinItemsNs).Where("DEVICE", reindexer.EQ, "ottstb").Sort("NAME", true)
		qj2 := DB.Query(testJoinItemsNs).Where("DEVICE", reindexer.EQ, "android").Where("AMOUNT", reindexer.GT, 2).Sort("NAME", true).Limit(30)
		qj3 := DB.Query(testJoinItemsNs).Where("DEVICE", reindexer.EQ, "iphone").Sort("NAME", true).Limit(30)

		qjoin := DB.Query(testItemsForJoinNs).Where("GENRE", reindexer.GE, 1).Limit(100).Debug(reindexer.TRACE)
		qjoin.InnerJoin(qj1, "PRICES").On("PRICE_ID", reindexer.SET, "ID")
		qjoin.Or().InnerJoin(qj2, "PRICESX").On("LOCATION", reindexer.EQ, "LOCATION").On("PRICE_ID", reindexer.SET, "ID")
		qjoin.Or().InnerJoin(qj3, "PRICESX").On("LOCATION", reindexer.LT, "LOCATION").Or().On("PRICE_ID", reindexer.SET, "ID")

		rjcheck := make([]interface{}, 0, 100)

		jr, err := DB.Query(testItemsForJoinNs).Where("GENRE", reindexer.GE, 1).MustExec(t).FetchAll()
		require.NoError(t, err)
		for _, iitem := range jr {
			item := iitem.(*TestItem)
			rj1, err := DB.Query(testJoinItemsNs).
				Where("DEVICE", reindexer.EQ, "ottstb").
				Where("ID", reindexer.SET, item.PricesIDs).
				Sort("NAME", true).
				MustExec(t).FetchAll()
			require.NoError(t, err)

			found := false
			if len(rj1) != 0 {
				item.Prices = make([]*TestJoinItem, 0, len(rj1))
				for _, rrj := range rj1 {
					item.Prices = append(item.Prices, rrj.(*TestJoinItem))
				}
				found = true
			}

			rj2, err := DB.Query(testJoinItemsNs).
				Where("DEVICE", reindexer.EQ, "android").
				Where("AMOUNT", reindexer.GT, 2).
				Where("ID", reindexer.SET, item.PricesIDs).
				Where("LOCATION", reindexer.EQ, item.LocationID).
				Sort("NAME", true).
				Limit(30).
				MustExec(t).FetchAll()
			require.NoError(t, err)
			if len(rj2) != 0 {
				item.Pricesx = make([]*TestJoinItem, 0, len(rj2))
				for _, rrj := range rj2 {
					item.Pricesx = append(item.Pricesx, rrj.(*TestJoinItem))
				}
				found = true
			}

			rj3, err := DB.Query(testJoinItemsNs).
				Where("DEVICE", reindexer.EQ, "iphone").
				Where("ID", reindexer.SET, item.PricesIDs).Or().
				Where("LOCATION", reindexer.GT, item.LocationID).
				Sort("NAME", true).
				Limit(30).
				MustExec(t).FetchAll()
			require.NoError(t, err)
			if len(rj3) != 0 {
				if item.Pricesx == nil {
					item.Pricesx = make([]*TestJoinItem, 0, len(rj3))
				}
				for _, rrj := range rj3 {
					item.Pricesx = append(item.Pricesx, rrj.(*TestJoinItem))
				}
				found = true
			}

			if found {
				rjcheck = append(rjcheck, item)
				if len(rjcheck) == 100 {
					break
				}
			}
		}

		rjoin, err := qjoin.MustExec(t).FetchAll()
		require.NoError(t, err)
		require.Equal(t, len(rjcheck), len(rjoin))
		for i := 0; i < len(rjcheck); i++ {
			i1 := rjcheck[i].(*TestItem)
			i2 := rjoin[i].(*TestItem)
			sort.Sort(byID(i1.Pricesx))
			sort.Sort(byID(i2.Pricesx))
			sort.Sort(byID(i1.Prices))
			sort.Sort(byID(i2.Prices))
			assert.Equal(t, i1, i2)
		}
	})

	t.Run("check join by uuid", func(t *testing.T) {
		jr, err := DB.Query(testItemsForJoinNs).
			InnerJoin(DB.Query(testJoinItemsNs), "PRICES").On("uuid", reindexer.LT, "uuid").
			Limit(100).MustExec(t).FetchAll()
		require.NoError(t, err)
		for _, iitem := range jr {
			item := iitem.(*TestItem)
			for _, joinedItem := range item.Prices {
				require.Less(t, item.Uuid, joinedItem.Uuid)
			}
		}
	})
}

func TestJoinQueryResultsOnIterator(t *testing.T) {
	qjoin := DB.Query(testItemsForJoinNs).Where("GENRE", reindexer.EQ, 10).Limit(10).Debug(reindexer.TRACE)
	qj1 := DB.Query(testJoinItemsNs).Where("DEVICE", reindexer.EQ, "ottstb").Sort("name", false)
	qj2 := DB.Query(testJoinItemsNs).Where("DEVICE", reindexer.EQ, "android")
	qjoin.LeftJoin(qj1, "PRICES").On("PRICE_ID", reindexer.SET, "ID").
		InnerJoin(qj2, "PRICESX").On("LOCATION", reindexer.EQ, "LOCATION").On("PRICE_ID", reindexer.SET, "Id")

	var handlerSubitems []interface{}

	qjoin.JoinHandler("PRICES", func(field string, item interface{}, subitems []interface{}) (isContinue bool) {

		assert.True(t, strings.EqualFold(field, "prices"), "field expected: '%v'; actual: '%v'", "prices", field)
		assert.NotNil(t, item, "item in handler is nil")

		handlerSubitems = subitems
		return true
	})

	iter := qjoin.MustExec(t)
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

func TestExplainJoin(t *testing.T) {
	const (
		nsMain   = testExplainMainNs
		nsJoined = testExplainJoinedNs
	)
	fillExplainNs(t, nsMain, 5)
	fillExplainNs(t, nsJoined, 20)

	qjoin1 := DB.Query(nsJoined).Where("data", reindexer.SET, []int{0, 2, 4})
	qjoin2 := DB.Query(nsJoined).Where("data", reindexer.SET, []int{1, 2, 4})
	qjoin3 := DB.Query(nsJoined).Where("data", reindexer.EQ, 1)
	q := DB.Query(nsMain).Explain()
	q.InnerJoin(qjoin1, "inner_joined").On("id", reindexer.EQ, "id")
	q.Or().Where("id", reindexer.EQ, 1)
	q.Or().InnerJoin(qjoin2, "or_inner_joined").On("id", reindexer.EQ, "id")
	q.Not().Where("data", reindexer.EQ, 4)
	q.LeftJoin(qjoin3, "left_joined").On("id", reindexer.EQ, "id")

	iter := q.MustExec(t)
	defer iter.Close()
	explainRes, err := iter.GetExplainResults()
	assert.NoError(t, err)
	assert.NotNil(t, explainRes)
	checkExplain(t, explainRes.Selectors, []expectedExplain{
		{
			Field:       "-scan",
			Method:      "scan",
			Keys:        0,
			Comparators: 0,
			Matched:     5,
		},
		{
			Field:       "not data",
			FieldType:   "indexed",
			Method:      "index",
			Keys:        1,
			Comparators: 0,
			Matched:     4,
		},
		{
			Field: "(id and inner_join test_explain_joined)",
			Selectors: []expectedExplain{
				{
					Field:       "id",
					FieldType:   "indexed",
					Method:      "scan",
					Keys:        0,
					Comparators: 1,
					Matched:     2,
				},
				{
					Field:       "inner_join test_explain_joined",
					Method:      "preselected_values",
					Keys:        3,
					Description: "using preselected_values, because the namespace's max iterations count is very small of 3",
					Comparators: 0,
					Matched:     2,
					Preselect: []expectedExplain{
						{
							Field:       "data",
							FieldType:   "indexed",
							Method:      "index",
							Keys:        3,
							Comparators: 0,
							Matched:     3,
						},
					},
					JoinSelect: nil,
				},
			},
		},
		{
			Field: "or (id and inner_join test_explain_joined)",
			Selectors: []expectedExplain{
				{
					Field:       "id",
					FieldType:   "indexed",
					Method:      "scan",
					Keys:        0,
					Comparators: 1,
					Matched:     2,
				},
				{
					Field:       "inner_join test_explain_joined",
					Method:      "preselected_values",
					Keys:        3,
					Description: "using preselected_values, because the namespace's max iterations count is very small of 3",
					Comparators: 0,
					Matched:     2,
					Preselect: []expectedExplain{
						{
							Field:       "data",
							FieldType:   "indexed",
							Method:      "index",
							Keys:        3,
							Comparators: 0,
							Matched:     3,
						},
					},
					JoinSelect: nil,
				},
			},
		},
		{
			Field:       "or id",
			FieldType:   "indexed",
			Method:      "index",
			Keys:        1,
			Comparators: 0,
			Matched:     0,
		},
		{
			Field:       "left_join test_explain_joined",
			Method:      "preselected_values",
			Keys:        1,
			Description: "using preselected_values, because the namespace's max iterations count is very small of 1",
			Comparators: 0,
			Matched:     1,
			Preselect: []expectedExplain{
				{
					Field:       "data",
					FieldType:   "indexed",
					Method:      "index",
					Keys:        1,
					Comparators: 0,
					Matched:     1,
				},
			},
			JoinSelect: nil,
		},
	}, "")
	checkExplainJoinOnInjections(t, explainRes.OnConditionsInjections, []expectedExplainJoinOnInjections{
		{
			RightNsName:       "test_explain_joined",
			JoinOnCondition:   "INNER JOIN ON (test_explain_joined.id = id)",
			Succeed:           true,
			Type:              "by_value",
			InjectedCondition: "(id IN (...) )",
			Conditions: []expectedExplainConditionInjection{
				{
					InitialCondition: "test_explain_joined.id = id",
					Succeed:          true,
					NewCondition:     "id IN (...)",
					ValuesCount:      3,
				},
			},
		},
		{
			RightNsName:       "test_explain_joined",
			JoinOnCondition:   "OR INNER JOIN ON (test_explain_joined.id = id)",
			Succeed:           true,
			Type:              "by_value",
			InjectedCondition: "(id IN (...) )",
			Conditions: []expectedExplainConditionInjection{
				{
					InitialCondition: "test_explain_joined.id = id",
					Succeed:          true,
					NewCondition:     "id IN (...)",
					ValuesCount:      3,
				},
			},
		},
	})
}

func TestStrictJoinHandlers(t *testing.T) {
	if len(DB.slaveList) > 0 {
		t.Skip()
	}
	t.Parallel()

	const (
		nsMain   = testStrictJoinHandlersMainNs
		nsJoined = testStrictJoinHandlersJoinedNs
	)

	cfg := config.DefaultServerConfig()
	cfg.Net.HTTPAddr = "0:17173"
	cfg.Net.RPCAddr = "0:17174"
	cfg.Storage.Path = ""

	db, err := reindexer.NewReindex("builtinserver://xxx", reindexer.WithServerConfig(time.Second*100, cfg), reindexer.WithStrictJoinHandlers())
	require.NoError(t, err)
	defer db.Close()

	initNsForStrictJoinHandlers(t, db, nsMain, 5)
	initNsForStrictJoinHandlers(t, db, nsJoined, 20)

	t.Run("expecting error without join handler", func(t *testing.T) {
		qjoin := db.Query(nsJoined).Where("data", reindexer.GT, 0)
		_, err := db.Query(nsMain).
			InnerJoin(qjoin, "joined_data").
			On("id", reindexer.EQ, "id").
			Exec().FetchAll()
		require.ErrorContains(t, err, "join handler is missing.")
	})

	t.Run("expecting error with join handler returning 'true'", func(t *testing.T) {
		mainQ := db.Query(nsMain)
		qjoin := db.Query(nsJoined).Where("data", reindexer.GT, 0)
		mainQ.InnerJoin(qjoin, "joined_data").On("id", reindexer.EQ, "id")
		_, err := mainQ.
			JoinHandler("joined_data", func(field string, item interface{}, subitems []interface{}) bool { return true }).
			Exec().FetchAll()
		require.ErrorContains(t, err, "join handler was found, but returned 'true' and the field was handled via reflection.")
	})

	t.Run("expecting success with join handler returning 'false'", func(t *testing.T) {
		mainQ := db.Query(nsMain)
		qjoin := db.Query(nsJoined).Where("data", reindexer.GT, 0)
		mainQ.InnerJoin(qjoin, "joined_data").On("id", reindexer.EQ, "id")
		_, err := mainQ.
			JoinHandler("joined_data", func(field string, item interface{}, subitems []interface{}) bool { return false }).
			Exec().FetchAll()
		require.NoError(t, err)
	})

	t.Run("expecting success with join handler returning 'false' set via joined query", func(t *testing.T) {
		qjoin := db.Query(nsJoined).Where("data", reindexer.GT, 0)
		_, err := db.Query(nsMain).
			InnerJoin(qjoin, "joined_data").
			On("id", reindexer.EQ, "id").
			JoinHandler("joined_data", func(field string, item interface{}, subitems []interface{}) bool { return false }).
			Exec().FetchAll()
		require.NoError(t, err)
	})

	t.Run("expecting error with join handler set before the actual join", func(t *testing.T) {
		qjoin := db.Query(nsJoined).Where("data", reindexer.GT, 0)
		_, err := db.Query(nsMain).
			JoinHandler("joined_data", func(field string, item interface{}, subitems []interface{}) bool { return false }).
			InnerJoin(qjoin, "joined_data").
			On("id", reindexer.EQ, "id").
			Exec().FetchAll()
		require.ErrorContains(t, err, "join handler is missing.")
	})
}

func TestJoinedFieldUpsert(t *testing.T) {
	t.Parallel()

	const ns = testJoinedFieldNs
	item := TestItemWithJoinedField{
		Id: rand.Intn(100),
	}
	err := DB.Upsert(ns, item)
	require.NoError(t, err)

	j, err := DB.Query(ns).ExecToJson().FetchAll()
	require.NoError(t, err)
	require.Greater(t, len(j), 0)
	require.NotContains(t, string(j), "JoinedField")
}
