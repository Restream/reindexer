package reindexer

import (
	"math/rand"
	"sort"

	"testing"

	"github.com/restream/reindexer/v5"
	"github.com/stretchr/testify/require"
)

type TestSubQItem struct {
	ID       int    `reindex:"id,,pk"`
	Value    string `reindex:"value,tree"`
	Array    []int  `reindex:"array,hash"`
	IntValue int    `reindex:"intvalue,hash"`
}

type TestSubQItemMain struct {
	ID    int    `reindex:"id,,pk"`
	Value string `reindex:"value,tree"`
}

const (
	testSubQMainNs = "test_main_subq_modify"
	testSubQNs     = "test_subq_modify"
)

func init() {
	tnamespaces[testSubQMainNs] = TestSubQItemMain{}
	tnamespaces[testSubQNs] = TestSubQItem{}
}

func FillTestSubQItem(count int, arrayMaxVal int) {
	tx := newTestTx(DB, testSubQNs)

	for i := 0; i < count; i++ {
		if err := tx.Upsert(&TestSubQItem{
			ID:       i,
			Value:    "20" + randString(),
			Array:    randIntArr(10, 0, arrayMaxVal),
			IntValue: rand.Int() % arrayMaxVal,
		}); err != nil {
			panic(err)
		}
	}
	tx.MustCommit()
}

func FillTestSubQItemMain(count int) {
	tx := newTestTx(DB, testSubQMainNs)
	for i := 0; i < count; i++ {
		if err := tx.Upsert(&TestSubQItemMain{
			ID:    i,
			Value: "10" + randString(),
		}); err != nil {
			panic(err)
		}
	}
	tx.MustCommit()
}

func uniqueValAndSort(k []int) []int {
	keymap := make(map[int]bool)
	for _, v := range k {
		keymap[v] = true
	}

	keys := make([]int, 0, len(keymap))
	for k := range keymap {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	return keys
}

func selectArrayFromSubQNs(sudNsId int, t *testing.T) []int {
	qSelect := DB.Query(testSubQNs).Select("Array").Where("id", reindexer.EQ, sudNsId)
	selRes, errSel := qSelect.Exec(t).FetchAll()
	require.NoError(t, errSel)
	require.Equal(t, len(selRes), 1)
	item := selRes[0].(*TestSubQItem)
	return item.Array
}

func selectValueFromSubQNs(subNsId int, t *testing.T) []int {
	qSelect := DB.Query(testSubQNs).Select("IntValue").Where("id", reindexer.GT, subNsId)
	selRes, errSel := qSelect.Exec(t).FetchAll()
	require.NoError(t, errSel)
	var val []int
	for _, element := range selRes {
		item := element.(*TestSubQItem)
		val = append(val, item.IntValue)
	}
	return val
}

func checkQuery(qUpdate *queryTest, keys []int, changedValue string, t *testing.T) {
	updRes, errU := qUpdate.Update().FetchAll()
	require.NoError(t, errU)

	var updId []int
	for _, element := range updRes {
		itemMain := element.(*TestSubQItemMain)
		updId = append(updId, itemMain.ID)
		require.Equal(t, itemMain.Value, changedValue)
	}
	sort.Ints(updId)
	require.Equal(t, keys, updId)

	selQ := DB.Query(testSubQMainNs)
	selQ.Where("Value", reindexer.EQ, changedValue)
	selRes, errS := selQ.Exec(t).FetchAll()
	require.NoError(t, errS)
	var selId []int
	for _, element := range selRes {
		itemMain := element.(*TestSubQItemMain)
		selId = append(selId, itemMain.ID)
	}
	sort.Ints(selId)
	require.Equal(t, keys, selId)

}

func checkQueryOneNs(qUpdate *queryTest, keys []int, changedValue string, t *testing.T) {
	updRes, errU := qUpdate.Update().FetchAll()
	require.NoError(t, errU)

	var updId []int
	for _, element := range updRes {
		item := element.(*TestSubQItem)
		updId = append(updId, item.ID)
		require.Equal(t, item.Value, changedValue)
	}
	sort.Ints(updId)
	require.Equal(t, keys, updId)

	selQ := DB.Query(testSubQNs)
	selQ.Where("Value", reindexer.EQ, changedValue)
	selRes, errS := selQ.Exec(t).FetchAll()
	require.NoError(t, errS)
	var selId []int
	for _, element := range selRes {
		item := element.(*TestSubQItem)
		selId = append(selId, item.ID)
	}
	sort.Ints(selId)
	require.Equal(t, keys, selId)
}

func TestSuqueryModifyQueries(t *testing.T) {
	subQItemsCount := 100
	subQItemsMainCount := 20
	changedValue := "changed"
	changedValue2 := "changed2"
	changedValue3 := "changed3"
	FillTestSubQItem(subQItemsCount, subQItemsMainCount)
	FillTestSubQItemMain(subQItemsMainCount)

	t.Run("update queries with subQuery", func(t *testing.T) {
		subNsIdArray := rand.Int() % subQItemsCount
		keys := selectArrayFromSubQNs(subNsIdArray, t)
		keys = uniqueValAndSort(keys)
		qUpdate := DB.Query(testSubQMainNs)
		qUpdate.Where("ID", reindexer.EQ, DB.Query(testSubQNs).Select("Array").Where("id", reindexer.EQ, subNsIdArray))
		qUpdate.Set("Value", changedValue)
		checkQuery(qUpdate, keys, changedValue, t)
	})

	t.Run("update queries with two subQuery", func(t *testing.T) {
		subNsIdArray := rand.Int() % subQItemsCount
		keys := selectArrayFromSubQNs(subNsIdArray, t)

		subNsIdValue := rand.Int() % subQItemsCount
		keys2 := selectValueFromSubQNs(subNsIdValue, t)
		keys = append(keys, keys2...)
		keys = uniqueValAndSort(keys)

		q1 := DB.Query(testSubQNs).Select("Array").Where("id", reindexer.EQ, subNsIdArray)
		q2 := DB.Query(testSubQNs).Select("IntValue").Where("id", reindexer.GT, subNsIdValue)
		qUpdate := DB.Query(testSubQMainNs)
		qUpdate.Where("ID", reindexer.EQ, q1).Or().Where("ID", reindexer.EQ, q2)
		qUpdate.Set("Value", changedValue2)
		checkQuery(qUpdate, keys, changedValue2, t)
	})

	t.Run("update queries same ns subQuery", func(t *testing.T) {
		subNsIdArray := rand.Int() % subQItemsCount
		keys := selectArrayFromSubQNs(subNsIdArray, t)
		keys = uniqueValAndSort(keys)
		qUpdate := DB.Query(testSubQNs)
		qUpdate.Where("ID", reindexer.EQ, DB.Query(testSubQNs).Select("Array").Where("id", reindexer.EQ, subNsIdArray))
		qUpdate.Set("Value", changedValue3)
		checkQueryOneNs(qUpdate, keys, changedValue3, t)
	})

	t.Run("delete queries with subQuery", func(t *testing.T) {
		subNsIdArray := rand.Int() % subQItemsCount
		keys := selectArrayFromSubQNs(subNsIdArray, t)
		keys = uniqueValAndSort(keys)

		keymap := make(map[int]bool)
		for i := 0; i < subQItemsMainCount; i += 1 {
			keymap[i] = true
		}
		for _, v := range keys {
			delete(keymap, v)
		}
		qDelete := DB.Query(testSubQMainNs)
		qDelete.Where("ID", reindexer.EQ, DB.Query(testSubQNs).Select("Array").Where("id", reindexer.EQ, subNsIdArray))
		delCount, err := qDelete.Delete()
		require.NoError(t, err)
		require.Equal(t, len(keys), delCount)

		selQ := DB.Query(testSubQMainNs)
		selRes, errS := selQ.Exec(t).FetchAll()
		require.NoError(t, errS)
		require.Equal(t, len(selRes), len(keymap))
		for _, element := range selRes {
			itemMain := element.(*TestSubQItemMain)
			_, ok := keymap[itemMain.ID]
			require.Equal(t, true, ok)
		}
	})

	t.Run("delete queries same ns subQuery", func(t *testing.T) {
		subNsIdArray := rand.Int() % subQItemsCount
		keys := selectArrayFromSubQNs(subNsIdArray, t)
		keys = uniqueValAndSort(keys)
		keymap := make(map[int]bool)
		for i := 0; i < subQItemsCount; i += 1 {
			keymap[i] = true
		}
		for _, v := range keys {
			delete(keymap, v)
		}

		qDelete := DB.Query(testSubQNs)
		qDelete.Where("ID", reindexer.EQ, DB.Query(testSubQNs).Select("Array").Where("id", reindexer.EQ, subNsIdArray))
		delCount, err := qDelete.Delete()
		require.NoError(t, err)
		require.Equal(t, len(keys), delCount)

		selQ := DB.Query(testSubQNs)
		selRes, errS := selQ.Exec(t).FetchAll()
		require.NoError(t, errS)
		require.Equal(t, len(selRes), len(keymap))

		for _, element := range selRes {
			itemMain := element.(*TestSubQItem)
			_, ok := keymap[itemMain.ID]
			require.Equal(t, true, ok)
		}

	})

}
