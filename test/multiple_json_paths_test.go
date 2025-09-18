package reindexer

import (
	"math"
	"math/rand"
	"reflect"
	"sort"
	"strconv"
	"testing"

	"github.com/restream/reindexer/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestItemAppendable struct {
	ID     int `json:"id" reindex:"id,,pk"`
	Field1 int `json:"field1,omitempty" reindex:"idx,,appendable"`
	Field2 int `json:"field2,omitempty" reindex:"idx,,appendable"`
}

type TestArrItemAppendable struct {
	ID        int   `json:"id" reindex:"id,,pk"`
	ArrField1 []int `json:"arrfield1,omitempty" reindex:"arridx,,appendable"`
	ArrField2 []int `json:"arrfield2,omitempty" reindex:"arridx,,appendable"`
}

type TestJoinItemAppendable struct {
	ID             int                   `json:"id" reindex:"id,,pk"`
	Field1         int                   `json:"field1,omitempty" reindex:"idx,,appendable"`
	Field2         int                   `json:"field2,omitempty" reindex:"idx,,appendable"`
	TestItemJoined []*TestItemAppendable `reindex:"test_joined,,joined"`
}

type TestJoinArrItemAppendable struct {
	ID                int                      `json:"id" reindex:"id,,pk"`
	ArrField1         []int                    `json:"arrfield1,omitempty" reindex:"arridx,,appendable"`
	ArrField2         []int                    `json:"arrfield2,omitempty" reindex:"arridx,,appendable"`
	TestArrItemJoined []*TestArrItemAppendable `reindex:"test_arr_joined,,joined"`
}

type TestItemNestedAppendable struct {
	ID          int                         `json:"id" reindex:"id,,pk"`
	NField      int                         `json:"nfield,omitempty" reindex:"idx,,appendable"`
	TestNested1 *TestItemNestedAppendableN1 `json:"test_nested_1,omitempty"`
	TestNested2 *TestItemNestedAppendableN2 `json:"test_nested_2,omitempty"`
}

type TestItemNestedAppendableN1 struct {
	ID     int `json:"id"`
	NField int `json:"nfield,omitempty" reindex:"idx,,appendable"`
}

type TestItemNestedAppendableN2 struct {
	ID     int `json:"id"`
	NField int `json:"nfield,omitempty" reindex:"idx,,appendable"`
}

type TestItemNestedArrAppendable struct {
	ID          int                            `json:"id" reindex:"id,,pk"`
	NFieldArr   []int                          `json:"nfield_arr,omitempty" reindex:"arridx,,appendable"`
	TestNested1 *TestItemNestedArrAppendableN1 `json:"test_nested_1,omitempty"`
	TestNested2 *TestItemNestedArrAppendableN2 `json:"test_nested_2,omitempty"`
}

type TestItemNestedArrAppendableN1 struct {
	ID        int   `json:"id"`
	NFieldArr []int `json:"nfield_arr,omitempty" reindex:"arridx,,appendable"`
}

type TestItemNestedArrAppendableN2 struct {
	ID        int   `json:"id"`
	NFieldArr []int `json:"nfield_arr,omitempty" reindex:"arridx,,appendable"`
}

type aggValuesStruct struct {
	Min float64 `json:"min"`
	Max float64 `json:"max"`
	Sum float64 `json:"sum"`
	Avg float64 `json:"avg"`
}

const (
	TestSelectWithMultipleJsonPathsNs    = "test_select_with_multiple_json_paths"
	TestSelectArrWithMultipleJsonPathsNs = "test_select_arr_with_multiple_json_paths"

	TestJoinWithMultipleJsonPathsNs      = "test_join_with_multiple_json_paths"
	TestJoinedWithMultipleJsonPathsNs    = "test_joined_with_multiple_json_paths"
	TestJoinArrWithMultipleJsonPathsNs   = "test_join_arr_with_multiple_json_paths"
	TestJoinedArrWithMultipleJsonPathsNs = "test_joined_arr_with_multiple_json_paths"

	TestAggregWithMultipleJsonPathsNs    = "test_aggreg_with_multiple_json_paths"
	TestAggregArrWithMultipleJsonPathsNs = "test_aggreg_arr_with_multiple_json_paths"

	TestNestedWithMultipleJsonPathsNs    = "test_nested_with_multiple_json_paths"
	TestNestedArrWithMultipleJsonPathsNs = "test_nested_arr_with_multiple_json_paths"
)

func init() {
	tnamespaces[TestSelectWithMultipleJsonPathsNs] = TestItemAppendable{}
	tnamespaces[TestSelectArrWithMultipleJsonPathsNs] = TestArrItemAppendable{}

	tnamespaces[TestJoinWithMultipleJsonPathsNs] = TestJoinItemAppendable{}
	tnamespaces[TestJoinedWithMultipleJsonPathsNs] = TestItemAppendable{}
	tnamespaces[TestJoinArrWithMultipleJsonPathsNs] = TestJoinArrItemAppendable{}
	tnamespaces[TestJoinedArrWithMultipleJsonPathsNs] = TestArrItemAppendable{}

	tnamespaces[TestAggregWithMultipleJsonPathsNs] = TestItemAppendable{}
	tnamespaces[TestAggregArrWithMultipleJsonPathsNs] = TestArrItemAppendable{}

	tnamespaces[TestNestedWithMultipleJsonPathsNs] = TestItemNestedAppendable{}
	tnamespaces[TestNestedArrWithMultipleJsonPathsNs] = TestItemNestedArrAppendable{}
}

func addValuesFromArrToMap(m map[string]int, arrField []int) {
	for _, arr := range arrField {
		m[strconv.Itoa(arr)] += 1
	}
}

func TestSelectWithMultipleJsonPaths(t *testing.T) {
	t.Parallel()

	const ns = TestSelectWithMultipleJsonPathsNs
	const ns2 = TestSelectArrWithMultipleJsonPathsNs

	testItem1 := TestItemAppendable{ID: 1, Field1: 10}
	testItem2 := TestItemAppendable{ID: 2, Field2: 50}
	testItem3 := TestItemAppendable{ID: 3, Field1: 30}
	testItem4 := TestItemAppendable{ID: 4, Field2: 30}
	for _, item := range []TestItemAppendable{testItem1, testItem2, testItem3, testItem4} {
		err := DB.Upsert(ns, item)
		require.NoError(t, err)
	}

	t.Run("test select with index multiple json paths", func(t *testing.T) {
		it1 := DBD.Query(ns).Where("idx", reindexer.EQ, testItem1.Field1).MustExec()
		checkResultItem(t, it1, &testItem1)

		it2 := DBD.Query(ns).Where("idx", reindexer.EQ, testItem2.Field2).MustExec()
		checkResultItem(t, it2, &testItem2)

		items, err := DBD.Query(ns).
			Where("idx", reindexer.EQ, testItem3.Field1).MustExec().FetchAll()
		require.NoError(t, err)
		expectedItems := []TestItemAppendable{testItem3, testItem4}
		for i, v := range items {
			require.EqualValues(t, &expectedItems[i], v.(*TestItemAppendable))
		}
	})

	testArrItem1 := TestArrItemAppendable{ID: 5, ArrField1: []int{50, 51}}
	testArrItem2 := TestArrItemAppendable{ID: 6, ArrField2: []int{40, 41}}
	testArrItem3 := TestArrItemAppendable{ID: 7, ArrField1: []int{70, 71}, ArrField2: []int{72, 73}}
	for _, item := range []TestArrItemAppendable{testArrItem1, testArrItem2, testArrItem3} {
		err := DB.Upsert(ns2, item)
		require.NoError(t, err)
	}

	t.Run("test select with array index multiple json paths", func(t *testing.T) {
		it1 := DBD.Query(ns2).Where("arridx", reindexer.EQ, testArrItem1.ArrField1[0]).MustExec()
		checkResultItem(t, it1, &testArrItem1)

		it2 := DBD.Query(ns2).Where("arridx", reindexer.EQ, testArrItem2.ArrField2[0]).MustExec()
		checkResultItem(t, it2, &testArrItem2)

		it3 := DBD.Query(ns2).Where("arridx", reindexer.EQ, testArrItem3.ArrField1[0]).MustExec()
		checkResultItem(t, it3, &testArrItem3)

		it4 := DBD.Query(ns2).Where("arridx", reindexer.EQ, testArrItem3.ArrField2[0]).MustExec()
		checkResultItem(t, it4, &testArrItem3)
	})

	t.Run("can sort with appendable tag", func(t *testing.T) {
		checkItems := func(t *testing.T, items []interface{}, expectedItems []interface{}) {
			require.Equal(t, len(items), len(expectedItems))
			for i := 0; i < len(items); i++ {
				assert.True(t, reflect.DeepEqual(items[i], expectedItems[i]), "%v\ndoesn't equal to\n%v", items[i], expectedItems[i])
			}
		}

		items, err := DBD.Query(ns).Sort("idx", false).Exec().FetchAll()
		require.NoError(t, err)
		checkItems(t, items, []interface{}{&testItem1, &testItem3, &testItem4, &testItem2})

		items, err = DBD.Query(ns).Sort("idx", true).Exec().FetchAll()
		require.NoError(t, err)
		checkItems(t, items, []interface{}{&testItem2, &testItem4, &testItem3, &testItem1})

		items, err = DBD.Query(ns2).Sort("arridx", false).Exec().FetchAll()
		require.NoError(t, err)
		checkItems(t, items, []interface{}{&testArrItem2, &testArrItem1, &testArrItem3})

		items, err = DBD.Query(ns2).Sort("arridx", true).Exec().FetchAll()
		require.NoError(t, err)
		checkItems(t, items, []interface{}{&testArrItem3, &testArrItem1, &testArrItem2})
	})
}

func TestJoinWithMultipleJsonPaths(t *testing.T) {
	t.Parallel()

	const ns = TestJoinWithMultipleJsonPathsNs
	const nsj = TestJoinedWithMultipleJsonPathsNs

	testItem11 := TestJoinItemAppendable{ID: 1, Field1: 10}
	testItem12 := TestJoinItemAppendable{ID: 2, Field2: 20}
	testItem13 := TestJoinItemAppendable{ID: 3, Field1: 30}
	for _, item := range []TestJoinItemAppendable{testItem11, testItem12, testItem13} {
		err := DB.Upsert(ns, item)
		require.NoError(t, err)
	}

	testItem21 := TestItemAppendable{ID: 1, Field1: 10}
	testItem22 := TestItemAppendable{ID: 2, Field2: 20}
	testItem23 := TestItemAppendable{ID: 3, Field2: 30}
	for _, item := range []TestItemAppendable{testItem21, testItem22, testItem23} {
		err := DB.Upsert(nsj, item)
		require.NoError(t, err)
	}

	testItem11.TestItemJoined = []*TestItemAppendable{&testItem21}
	testItem12.TestItemJoined = []*TestItemAppendable{&testItem22}
	testItem13.TestItemJoined = []*TestItemAppendable{&testItem23}

	expectedItems1 := []TestJoinItemAppendable{testItem11, testItem12, testItem13}

	t.Run("test inner join with index multiple json paths", func(t *testing.T) {
		qjoin := DBD.Query(nsj)
		items, err := DBD.Query(ns).InnerJoin(qjoin, "test_joined").
			On("idx", reindexer.EQ, "idx").MustExec().FetchAll()
		require.NoError(t, err)

		for i, v := range items {
			require.EqualValues(t, &expectedItems1[i], v.(*TestJoinItemAppendable))
		}
	})

	t.Run("test left join with index multiple json paths", func(t *testing.T) {
		qjoin := DBD.Query(nsj)
		items, err := DBD.Query(ns).LeftJoin(qjoin, "test_joined").
			On("idx", reindexer.EQ, "idx").MustExec().FetchAll()
		require.NoError(t, err)

		for i, v := range items {
			require.EqualValues(t, &expectedItems1[i], v.(*TestJoinItemAppendable))
		}
	})

	const ns2 = TestJoinArrWithMultipleJsonPathsNs
	const ns2j = TestJoinedArrWithMultipleJsonPathsNs

	testArrItem11 := TestJoinArrItemAppendable{ID: 1, ArrField1: []int{10, 11}}
	testArrItem12 := TestJoinArrItemAppendable{ID: 2, ArrField2: []int{20, 21, 22}}
	testArrItem13 := TestJoinArrItemAppendable{ID: 3, ArrField1: []int{30, 31, 32}, ArrField2: []int{32, 33}}
	for _, item := range []TestJoinArrItemAppendable{testArrItem11, testArrItem12, testArrItem13} {
		err := DB.Upsert(ns2, item)
		require.NoError(t, err)
	}

	testArrItem21 := TestArrItemAppendable{ID: 1, ArrField1: []int{10, 11, 12}}
	testArrItem22 := TestArrItemAppendable{ID: 2, ArrField2: []int{20, 21}}
	testArrItem23 := TestArrItemAppendable{ID: 3, ArrField2: []int{30, 31}, ArrField1: []int{32, 33, 34}}
	for _, item := range []TestArrItemAppendable{testArrItem21, testArrItem22, testArrItem23} {
		err := DB.Upsert(ns2j, item)
		require.NoError(t, err)
	}

	testArrItem11.TestArrItemJoined = []*TestArrItemAppendable{&testArrItem21}
	testArrItem12.TestArrItemJoined = []*TestArrItemAppendable{&testArrItem22}
	testArrItem13.TestArrItemJoined = []*TestArrItemAppendable{&testArrItem23}

	expectedItems2 := []TestJoinArrItemAppendable{testArrItem11, testArrItem12, testArrItem13}

	t.Run("test inner join with array index multiple json paths", func(t *testing.T) {
		qjoin := DBD.Query(ns2j)
		items, err := DBD.Query(ns2).InnerJoin(qjoin, "test_arr_joined").
			On("arridx", reindexer.EQ, "arridx").MustExec().FetchAll()
		require.NoError(t, err)

		for i, v := range items {
			require.EqualValues(t, &expectedItems2[i], v.(*TestJoinArrItemAppendable))
		}
	})

	t.Run("test left join with array index multiple json paths", func(t *testing.T) {
		qjoin := DBD.Query(ns2j)
		items, err := DBD.Query(ns2).LeftJoin(qjoin, "test_arr_joined").
			On("arridx", reindexer.EQ, "arridx").MustExec().FetchAll()
		require.NoError(t, err)

		for i, v := range items {
			require.EqualValues(t, &expectedItems2[i], v.(*TestJoinArrItemAppendable))
		}
	})
}

func TestAggregationsWithMultipleJsonPaths(t *testing.T) {
	t.Parallel()

	const ns = TestAggregWithMultipleJsonPathsNs
	const ns2 = TestAggregArrWithMultipleJsonPathsNs

	start, end := 0, 10
	minValue, maxRandValue := 1, 20

	aggValues := aggValuesStruct{
		Min: float64(math.MaxFloat64), Max: float64(0), Sum: 0, Avg: 0,
	}

	fieldsSet := make(map[string]int)
	for i := start; i < end; i++ {
		item1 := TestItemAppendable{ID: i, Field1: minValue + rand.Intn(maxRandValue)}
		err := DB.Upsert(ns, item1)
		require.NoError(t, err)
		value1 := strconv.Itoa(item1.Field1)
		fieldsSet[value1] += 1
		item2 := TestItemAppendable{ID: i + 10, Field2: minValue + rand.Intn(maxRandValue)}
		err = DB.Upsert(ns, item2)
		require.NoError(t, err)
		value2 := strconv.Itoa(item2.Field2)
		fieldsSet[value2] += 1

		val1, val2 := float64(item1.Field1), float64(item2.Field2)
		if aggValues.Min > val1 || aggValues.Min > val2 {
			aggValues.Min = float64(min(item1.Field1, item2.Field2))
		}
		if aggValues.Max < val1 || aggValues.Max < val2 {
			aggValues.Max = float64(max(item1.Field1, item2.Field2))
		}
		aggValues.Sum += float64(item1.Field1 + item2.Field2)
	}
	aggValues.Avg = aggValues.Sum / float64(end*2)

	fieldValues := []string{}
	for k := range fieldsSet {
		fieldValues = append(fieldValues, k)
	}
	sort.Strings(fieldValues)

	t.Run("test aggreations with index multiple json paths", func(t *testing.T) {
		q := DBD.Query(ns)
		q.AggregateMin("idx")
		q.AggregateMax("idx")
		q.AggregateSum("idx")
		q.AggregateAvg("idx")

		it := q.MustExec()
		require.NoError(t, it.Error())
		defer it.Close()

		aggResults := it.AggResults()
		require.Equal(t, 4, len(aggResults))
		require.Equal(t, aggValues.Min, *aggResults[0].Value)
		require.Equal(t, aggValues.Max, *aggResults[1].Value)
		require.Equal(t, aggValues.Sum, *aggResults[2].Value)
		require.Equal(t, aggValues.Avg, *aggResults[3].Value)
	})

	t.Run("test distinct with index multiple json paths", func(t *testing.T) {
		it := DBD.Query(ns).Distinct("idx").MustExec()
		require.NoError(t, it.Error())
		defer it.Close()
		require.Equal(t, len(fieldValues), it.Count())

		aggResults := it.AggResults()
		require.Equal(t, 1, len(aggResults))
		var distincts []string
		for i := 0; i < len(aggResults[0].Distincts); i++ {
			distincts = append(distincts, aggResults[0].Distincts[i][0])
		}
		sort.Strings(distincts)
		require.Equal(t, fieldValues, distincts)
	})

	t.Run("test facet with index multiple json paths", func(t *testing.T) {
		q := DBD.Query(ns)
		q.AggregateFacet("idx")
		it := q.MustExec()
		require.NoError(t, it.Error())
		defer it.Close()
		require.Equal(t, 0, it.Count())

		aggResults := it.AggResults()
		require.Equal(t, 1, len(aggResults))
		for _, facet := range aggResults[0].Facets {
			require.Equal(t, fieldsSet[facet.Values[0]], facet.Count)
		}
	})

	aggArrValues := aggValuesStruct{
		Min: float64(math.MaxFloat64), Max: float64(0), Sum: 0, Avg: 0,
	}

	intValues := []int{}
	fieldsArrSet := make(map[string]int)
	for i := start; i < end; i++ {
		item1 := TestArrItemAppendable{ID: i, ArrField1: randIntArr(2, minValue, maxRandValue)}
		err := DB.Upsert(ns2, item1)
		require.NoError(t, err)
		addValuesFromArrToMap(fieldsArrSet, item1.ArrField1)
		item2 := TestArrItemAppendable{ID: i + 10, ArrField2: randIntArr(2, minValue, maxRandValue)}
		err = DB.Upsert(ns2, item2)
		require.NoError(t, err)
		addValuesFromArrToMap(fieldsArrSet, item2.ArrField2)
		item3 := TestArrItemAppendable{ID: i + 20, ArrField1: randIntArr(2, minValue, maxRandValue),
			ArrField2: randIntArr(2, 0, minValue)}
		err = DB.Upsert(ns2, item3)
		require.NoError(t, err)
		addValuesFromArrToMap(fieldsArrSet, item3.ArrField1)
		addValuesFromArrToMap(fieldsArrSet, item3.ArrField2)

		arrFields := [][]int{item1.ArrField1, item2.ArrField2, item3.ArrField1, item3.ArrField2}
		for _, intArrValue := range arrFields {
			for _, intValue := range intArrValue {
				intValues = append(intValues, intValue)
				aggArrValues.Sum += float64(intValue)
			}
		}
	}
	sort.Ints(intValues)

	aggArrValues.Min = float64(intValues[0])
	aggArrValues.Max = float64(intValues[len(intValues)-1])
	aggArrValues.Avg = aggArrValues.Sum / float64(len(intValues))

	fieldValues2 := []string{}
	for k := range fieldsArrSet {
		fieldValues2 = append(fieldValues2, k)
	}
	sort.Strings(fieldValues2)

	t.Run("test aggreations with array index multiple json paths", func(t *testing.T) {
		q := DBD.Query(ns2)
		q.AggregateMin("arridx")
		q.AggregateMax("arridx")
		q.AggregateSum("arridx")
		q.AggregateAvg("arridx")

		it := q.MustExec()
		require.NoError(t, it.Error())
		defer it.Close()
		aggResults := it.AggResults()
		require.Equal(t, 4, len(aggResults))
		require.Equal(t, aggArrValues.Min, *aggResults[0].Value)
		require.Equal(t, aggArrValues.Max, *aggResults[1].Value)
		require.Equal(t, aggArrValues.Sum, *aggResults[2].Value)
		require.Equal(t, aggArrValues.Avg, *aggResults[3].Value)
	})

	t.Run("test distinct with array index multiple json paths", func(t *testing.T) {
		it := DBD.Query(ns2).Distinct("arridx").MustExec()
		require.NoError(t, it.Error())
		defer it.Close()
		// require.Equal(t, end*3, it.Count()) TODO: 1526

		aggResults := it.AggResults()
		require.Equal(t, 1, len(aggResults))
		var distincts []string
		for i := 0; i < len(aggResults[0].Distincts); i++ {
			distincts = append(distincts, aggResults[0].Distincts[i][0])
		}
		sort.Strings(distincts)
		require.Equal(t, fieldValues2, distincts)
	})

	t.Run("test facet with array index multiple json paths", func(t *testing.T) {
		q := DBD.Query(ns2)
		q.AggregateFacet("arridx")
		it := q.MustExec()
		require.NoError(t, it.Error())
		defer it.Close()
		require.Equal(t, 0, it.Count())

		aggResults := it.AggResults()
		require.Equal(t, 1, len(aggResults))
		for _, facet := range aggResults[0].Facets {
			require.Equal(t, fieldsArrSet[facet.Values[0]], facet.Count)
		}
	})
}

func TestNestedWithMultipleJsonPaths(t *testing.T) {
	t.Parallel()

	const ns = TestNestedWithMultipleJsonPathsNs
	const ns2 = TestNestedArrWithMultipleJsonPathsNs

	testItem1 := TestItemNestedAppendable{ID: 1, NField: 10,
		TestNested1: &TestItemNestedAppendableN1{ID: 1, NField: 30},
		TestNested2: &TestItemNestedAppendableN2{ID: 1, NField: 10}}
	testItem2 := TestItemNestedAppendable{ID: 2, NField: 20,
		TestNested1: &TestItemNestedAppendableN1{ID: 2, NField: 21},
		TestNested2: &TestItemNestedAppendableN2{ID: 2, NField: 30}}
	testItem3 := TestItemNestedAppendable{ID: 3, NField: 30,
		TestNested1: &TestItemNestedAppendableN1{ID: 3, NField: 31},
		TestNested2: &TestItemNestedAppendableN2{ID: 3, NField: 30}}

	for _, item := range []TestItemNestedAppendable{testItem1, testItem2, testItem3} {
		err := DB.Upsert(ns, item)
		require.NoError(t, err)
	}

	t.Run("test select with nested index multiple json paths", func(t *testing.T) {
		it1 := DBD.Query(ns).Where("idx", reindexer.EQ, testItem1.NField).MustExec()
		checkResultItem(t, it1, &testItem1)

		it2 := DBD.Query(ns).Where("idx", reindexer.EQ, testItem2.TestNested1.NField).MustExec()
		checkResultItem(t, it2, &testItem2)

		items, err := DBD.Query(ns).
			Where("idx", reindexer.EQ, testItem3.NField).MustExec().FetchAll()
		require.NoError(t, err)
		expectedItems := []TestItemNestedAppendable{testItem1, testItem2, testItem3}
		for i, v := range items {
			require.EqualValues(t, &expectedItems[i], v.(*TestItemNestedAppendable))
		}
	})

	testArrItem1 := TestItemNestedArrAppendable{ID: 1, NFieldArr: []int{10, 11},
		TestNested1: &TestItemNestedArrAppendableN1{ID: 1, NFieldArr: []int{30, 11}},
		TestNested2: &TestItemNestedArrAppendableN2{ID: 1, NFieldArr: []int{10, 11}}}
	testArrItem2 := TestItemNestedArrAppendable{ID: 2, NFieldArr: []int{20, 11},
		TestNested1: &TestItemNestedArrAppendableN1{ID: 1, NFieldArr: []int{21, 11}},
		TestNested2: &TestItemNestedArrAppendableN2{ID: 1, NFieldArr: []int{31, 30}}}
	testArrItem3 := TestItemNestedArrAppendable{ID: 3, NFieldArr: []int{30, 11},
		TestNested1: &TestItemNestedArrAppendableN1{ID: 1, NFieldArr: []int{31, 11}},
		TestNested2: &TestItemNestedArrAppendableN2{ID: 1, NFieldArr: []int{30, 11}}}

	for _, item := range []TestItemNestedArrAppendable{testArrItem1, testArrItem2, testArrItem3} {
		err := DB.Upsert(ns2, item)
		require.NoError(t, err)
	}

	t.Run("test select with nested array index multiple json paths", func(t *testing.T) {
		it1 := DBD.Query(ns2).Where("arridx", reindexer.EQ, testArrItem1.NFieldArr[0]).MustExec()
		checkResultItem(t, it1, &testArrItem1)

		it2 := DBD.Query(ns2).Where("arridx", reindexer.EQ, testArrItem2.TestNested1.NFieldArr[0]).MustExec()
		checkResultItem(t, it2, &testArrItem2)

		items, err := DBD.Query(ns2).
			Where("arridx", reindexer.EQ, testArrItem3.NFieldArr[0]).MustExec().FetchAll()
		require.NoError(t, err)
		expectedItems := []TestItemNestedArrAppendable{testArrItem1, testArrItem2, testArrItem3}
		for i, v := range items {
			require.EqualValues(t, &expectedItems[i], v.(*TestItemNestedArrAppendable))
		}
	})

}
