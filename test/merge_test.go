package reindexer

import (
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/restream/reindexer/v5"
	"github.com/stretchr/testify/assert"
)

type TestFullTextItem1 struct {
	ID     int                `reindex:"id,,pk"`
	Name   string             `reindex:"name,text"`
	JoinID int                `reindex:"join_id"`
	Joined []*MergeJoinedItem `reindex:"joined,,joined"`
}

type TestFullTextItem2 struct {
	ID     int                `reindex:"id,,pk"`
	Name   string             `reindex:"name,text"`
	JoinID int                `reindex:"join_id"`
	Joined []*MergeJoinedItem `reindex:"joined,,joined"`
}

type TestFullTextMergedItem struct {
	ID          int                `reindex:"id,,pk"`
	Description string             `reindex:"description,text"`
	Location    string             `reindex:"location,text"`
	Rate        int                `reindex:"rate"`
	JoinID      int                `reindex:"join_id"`
	Joined      []*MergeJoinedItem `reindex:"joined,,joined"`
}

type MergeJoinedItem struct {
	ID   int    `reindex:"id,,pk"`
	Name string `reindex:"name,text"`
}

type SortFullText struct {
	ID   int
	Rank float32
}

type ByProc []SortFullText

const (
	testFullTextItemNs1      = "test_full_text_simple_item"
	testFullTextItemNs2      = "test_full_text_item"
	testFullTextMergedItemNs = "test_full_text_merged_item"
	testMergeJoinItemNs1     = "merge_join_item1"
	testMergeJoinItemNs2     = "merge_join_item2"
)

func init() {
	tnamespaces[testFullTextItemNs1] = TestFullTextItem1{}
	tnamespaces[testFullTextItemNs2] = TestFullTextItem2{}
	tnamespaces[testFullTextMergedItemNs] = TestFullTextMergedItem{}
	tnamespaces[testMergeJoinItemNs1] = MergeJoinedItem{}
	tnamespaces[testMergeJoinItemNs2] = MergeJoinedItem{}
}

func (a ByProc) Len() int {
	return len(a)
}

func (a ByProc) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func (a ByProc) Less(i, j int) bool {
	return a[i].Rank > a[j].Rank
}

func FillTestFullTextItems1Tx(count int, tx *txTest) {
	for i := 0; i < count; i++ {
		if err := tx.Upsert(&TestFullTextItem1{
			ID:     mkID(i),
			Name:   randLangString(),
			JoinID: 7000 + rand.Int()%500,
		}); err != nil {
			panic(err)
		}
	}
}

func FillTestFullTextItems2Tx(count int, tx *txTest) {
	for i := 0; i < count; i++ {
		if err := tx.Upsert(&TestFullTextItem2{
			ID:     mkID(i),
			Name:   randLangString(),
			JoinID: 7000 + rand.Int()%500,
		}); err != nil {
			panic(err)
		}
	}
}

func FillTestFullTextMergedItemsTx(count int, tx *txTest) {
	for i := 0; i < count; i++ {
		if err := tx.Upsert(&TestFullTextMergedItem{
			ID:          mkID(i),
			Description: randLangString(),
			Location:    randLangString(),
			Rate:        rand.Int(),
			JoinID:      7000 + rand.Int()%500,
		}); err != nil {
			panic(err)
		}
	}

}

func FillMergeJoinItemTx(count int, tx *txTest) {
	for i := 0; i < count; i++ {
		if err := tx.Upsert(&MergeJoinedItem{
			ID:   7000 + i,
			Name: randString(),
		}); err != nil {
			panic(err)
		}
	}
}

func FillTestFullTextItems(count int) {
	tx := newTestTx(DB, testFullTextItemNs1)
	FillTestFullTextItems1Tx(count, tx)
	tx.MustCommit()

	tx = newTestTx(DB, testFullTextMergedItemNs)
	FillTestFullTextMergedItemsTx(count, tx)
	tx.MustCommit()

	tx = newTestTx(DB, testFullTextItemNs2)
	FillTestFullTextItems2Tx(count, tx)
	tx.MustCommit()

	tx = newTestTx(DB, testMergeJoinItemNs1)
	FillMergeJoinItemTx(250, tx)
	tx.MustCommit()

	tx = newTestTx(DB, testMergeJoinItemNs2)
	FillMergeJoinItemTx(250, tx)
	tx.MustCommit()
}

func UpdateFtIndex(t *testing.T, nsName string, idxName string, jsonPath string, enablePreselect bool) {
	conf := reindexer.DefaultFtFastConfig()
	conf.EnablePreselectBeforeFt = enablePreselect
	err := DB.UpdateIndex(nsName, reindexer.IndexDef{Name: idxName, JSONPaths: []string{jsonPath}, IndexType: "text", FieldType: "string", Config: conf})
	assert.NoError(t, err)
}

func UpdateFtIndexes(t *testing.T, enablePreselect bool) {
	UpdateFtIndex(t, testFullTextItemNs1, "name", "Name", enablePreselect)
	UpdateFtIndex(t, testFullTextMergedItemNs, "description", "Description", enablePreselect)
	UpdateFtIndex(t, testFullTextMergedItemNs, "location", "Location", enablePreselect)
	UpdateFtIndex(t, testFullTextItemNs2, "name", "Name", enablePreselect)
	UpdateFtIndex(t, testMergeJoinItemNs1, "name", "Name", enablePreselect)
	UpdateFtIndex(t, testMergeJoinItemNs2, "name", "Name", enablePreselect)
}

func CreateSort(t *testing.T, result []interface{}, rankList []float32) (res ByProc) {
	assert.Equal(t, len(result), len(rankList), "Percent Count from query is wrong")

	for i, item := range result {
		id := 0

		switch item.(type) {
		case *TestFullTextItem1:
			id = item.(*TestFullTextItem1).ID
		case *TestFullTextMergedItem:
			id = item.(*TestFullTextMergedItem).ID
		case *TestFullTextItem2:
			id = item.(*TestFullTextItem2).ID
		default:
			assert.Fail(t, "[%d] Unknown type after merge: %T", i, item)
		}
		res = append(res, SortFullText{id, rankList[i]})
	}
	return res
}

func CheckTestItemsMergeQueries(t *testing.T) {

	first := randLangString()
	second := randLangString()
	third := randLangString()

	q1 := DB.Query(testFullTextItemNs1).Where("name", reindexer.EQ, first).
		Join(DB.Query(testMergeJoinItemNs1), "joined").
		On("join_id", reindexer.EQ, "id")
	qq1 := DB.Query(testFullTextItemNs1).Where("name", reindexer.EQ, first)
	q2 := DB.Query(testFullTextMergedItemNs).Where("description", reindexer.EQ, second).
		InnerJoin(DB.Query(testMergeJoinItemNs2), "joined").
		On("join_id", reindexer.EQ, "id")
	q3 := DB.Query(testFullTextItemNs2).Where("name", reindexer.EQ, third)
	qm := q1.Merge(q2).Merge(q3).Debug(reindexer.TRACE)

	merge, q1Procs, _ := qm.MustExec(t).FetchAllWithRank()

	lmerge, _ := qq1.Limit(2).MustExec(&testing.T{}).FetchAll()
	assert.LessOrEqual(t, len(lmerge), 2, "LIMIT NOT WORKING")

	//TEST SIMPLE SORT

	usorted := CreateSort(t, merge, q1Procs)
	sorted := make([]SortFullText, len(usorted))
	copy(sorted, usorted)
	sort.Sort(ByProc(sorted))
	//After Sort result with same proc can be not in same order (smart order in c++) that why no use reflect.DeepEqual
	for i := 0; i < len(merge); i++ {
		assert.Equal(t, usorted[i].Rank, sorted[i].Rank, "Merge sort in go is not equal to C sort simple")
	}
	qs1 := DB.Query(testFullTextItemNs1).Where("name", reindexer.EQ, strings.ToUpper(first)).
		Join(DB.Query(testMergeJoinItemNs1), "joined").
		On("join_id", reindexer.EQ, "id")
	qs2 := DB.Query(testFullTextMergedItemNs).Where("description", reindexer.EQ, strings.ToUpper(second)).
		InnerJoin(DB.Query(testMergeJoinItemNs2), "joined").
		On("join_id", reindexer.EQ, "id")
	qs3 := DB.Query(testFullTextItemNs2).Where("name", reindexer.EQ, strings.ToUpper(third))
	r1, rr1, e1 := qs1.MustExec(t).FetchAllWithRank()
	assert.NoError(t, e1)
	r2, rr2, e2 := qs2.MustExec(t).FetchAllWithRank()
	assert.NoError(t, e2)
	r3, rr3, e3 := qs3.MustExec(t).FetchAllWithRank()
	assert.NoError(t, e3)
	//TEST LEN
	assert.Equal(t, len(r1)+len(r2)+len(r3), len(merge), "(%d+%d+%d) (%p, %p, %p)", len(r1), len(r2), len(r3), qs1, qs2, qs3)
	assert.NotEqual(t, len(merge), 0, "Full text didn't return any result - something bad happened")

	var items []interface{}
	check := make(map[string]interface{})

	for _, item := range r1 {
		items = append(items, item)
		check[strconv.Itoa(item.(*TestFullTextItem1).ID)+testFullTextItemNs1] = item
	}
	for _, item := range r2 {
		items = append(items, item)
		check[strconv.Itoa(item.(*TestFullTextMergedItem).ID)+testFullTextMergedItemNs] = item
	}
	for _, item := range r3 {
		items = append(items, item)
		check[strconv.Itoa(item.(*TestFullTextItem2).ID)+testFullTextItemNs2] = item
	}

	//TEST MERGE IS WORKING AND WORKING WITHOUT CACHE
	for _, item := range merge {
		switch item.(type) {
		case *TestFullTextItem1:
			key := strconv.Itoa(item.(*TestFullTextItem1).ID) + testFullTextItemNs1
			sitem, ok := check[key]
			assert.True(t, ok, "Item %s not fond in simple check", key)
			assert.Equal(t, item, sitem, "Item not same as from cache")
			delete(check, key)
		case *TestFullTextMergedItem:
			key := strconv.Itoa(item.(*TestFullTextMergedItem).ID) + testFullTextMergedItemNs
			sitem, ok := check[key]
			assert.True(t, ok, "Item %s not fond in simple check", key)
			assert.Equal(t, item, sitem, "Item not same as from cache")
			delete(check, key)
		case *TestFullTextItem2:
			key := strconv.Itoa(item.(*TestFullTextItem2).ID) + testFullTextItemNs2
			sitem, ok := check[key]
			assert.True(t, ok, "Item %s not fond in simple check", key)
			assert.Equal(t, item, sitem, "Item not same as from cache")
			delete(check, key)
		default:
			assert.Fail(t, "Unknown type after merge ")
		}
	}

	assert.Equal(t, len(check), 0, "Not all data in merge")

	//TEST  SORT
	sortedNew := CreateSort(t, r1, rr1)
	sortedNew = append(sortedNew, CreateSort(t, r2, rr2)...)

	sortedNew = append(sortedNew, CreateSort(t, r3, rr3)...)

	sort.Sort(ByProc(sortedNew))

	//In second read result can not directly same
	for i := 0; i < len(usorted); i++ {
		assert.Equal(t, usorted[i].Rank, sortedNew[i].Rank, "Merge sort in go is not equal to C sort simple")
	}
}

func TestMerge(t *testing.T) {
	UpdateFtIndexes(t, false)
	FillTestFullTextItems(5000)
	CheckTestItemsMergeQueries(t)
	UpdateFtIndexes(t, true)
	CheckTestItemsMergeQueries(t)
}
