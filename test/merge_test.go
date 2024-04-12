package reindexer

import (
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/restream/reindexer/v3"
	"github.com/stretchr/testify/assert"
)

type TestFullTextSimpleItem struct {
	ID     int                 `reindex:"id,,pk"`
	Name   string              `reindex:"name,text"`
	JoinID int                 `reindex:"join_id"`
	Joined []*MergeJoinedItem1 `reindex:"joined,,joined"`
}
type TestFullTextMergedItem struct {
	ID          int                 `reindex:"id,,pk"`
	Description string              `reindex:"description,text"`
	Location    string              `reindex:"location,text"`
	Rate        int                 `reindex:"rate"`
	JoinID      int                 `reindex:"join_id"`
	Joined      []*MergeJoinedItem2 `reindex:"joined,,joined"`
}
type TestFullTextItem struct {
	ID     int                 `reindex:"id,,pk"`
	Name   string              `reindex:"name,text"`
	JoinID int                 `reindex:"join_id"`
	Joined []*MergeJoinedItem2 `reindex:"joined,,joined"`
}

type MergeJoinedItem1 struct {
	ID   int    `reindex:"id,,pk"`
	Name string `reindex:"name,text"`
}
type MergeJoinedItem2 struct {
	ID   int    `reindex:"id,,pk"`
	Name string `reindex:"name,text"`
}

func init() {
	tnamespaces["test_full_text_simple_item"] = TestFullTextSimpleItem{}
	tnamespaces["test_full_text_merged_item"] = TestFullTextMergedItem{}
	tnamespaces["test_full_text_item"] = TestFullTextItem{}
	tnamespaces["merge_join_item1"] = MergeJoinedItem1{}
	tnamespaces["merge_join_item2"] = MergeJoinedItem2{}

}

func FillFullTextSimpleItemsTx(count int, tx *txTest) {
	for i := 0; i < count; i++ {
		if err := tx.Upsert(&TestFullTextSimpleItem{
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
func FillTestFullTextItemsTx(count int, tx *txTest) {
	for i := 0; i < count; i++ {
		if err := tx.Upsert(&TestFullTextItem{
			ID:     mkID(i),
			Name:   randLangString(),
			JoinID: 7000 + rand.Int()%500,
		}); err != nil {
			panic(err)
		}
	}
}

func FillMergeJoinItem1Tx(count int, tx *txTest) {
	for i := 0; i < count; i++ {
		if err := tx.Upsert(&MergeJoinedItem1{
			ID:   7000 + i,
			Name: randString(),
		}); err != nil {
			panic(err)
		}
	}
}

func FillMergeJoinItem2Tx(count int, tx *txTest) {
	for i := 0; i < count; i++ {
		if err := tx.Upsert(&MergeJoinedItem2{
			ID:   7000 + i,
			Name: randString(),
		}); err != nil {
			panic(err)
		}
	}
}

func FillTestFullTextItems(count int) {
	tx := newTestTx(DB, "test_full_text_simple_item")
	FillFullTextSimpleItemsTx(count, tx)
	tx.MustCommit()
	tx = newTestTx(DB, "test_full_text_merged_item")
	FillTestFullTextMergedItemsTx(count, tx)
	tx.MustCommit()

	tx = newTestTx(DB, "test_full_text_item")
	FillTestFullTextItemsTx(count, tx)
	tx.MustCommit()

	tx = newTestTx(DB, "merge_join_item1")
	FillMergeJoinItem1Tx(250, tx)
	tx.MustCommit()

	tx = newTestTx(DB, "merge_join_item2")
	FillMergeJoinItem2Tx(250, tx)
	tx.MustCommit()

}

func UpdateFtIndex(t *testing.T, nsName string, idxName string, jsonPath string, enablePreselect bool) {
	conf := reindexer.DefaultFtFastConfig()
	conf.EnablePreselectBeforeFt = enablePreselect
	err := DB.UpdateIndex(nsName, reindexer.IndexDef{Name: idxName, JSONPaths: []string{jsonPath}, IndexType: "text", FieldType: "string", Config: conf})
	assert.NoError(t, err)
}

func UpdateFtIndexes(t *testing.T, enablePreselect bool) {
	UpdateFtIndex(t, "test_full_text_simple_item", "name", "Name", enablePreselect)
	UpdateFtIndex(t, "test_full_text_merged_item", "description", "Description", enablePreselect)
	UpdateFtIndex(t, "test_full_text_merged_item", "location", "Location", enablePreselect)
	UpdateFtIndex(t, "test_full_text_item", "name", "Name", enablePreselect)
	UpdateFtIndex(t, "merge_join_item1", "name", "Name", enablePreselect)
	UpdateFtIndex(t, "merge_join_item2", "name", "Name", enablePreselect)
}

func TestMerge(t *testing.T) {
	UpdateFtIndexes(t, false)
	FillTestFullTextItems(5000)
	CheckTestItemsMergeQueries(t)
	UpdateFtIndexes(t, true)
	CheckTestItemsMergeQueries(t)
}

type SortFullText struct {
	ID   int
	Proc int
}
type ByProc []SortFullText

func (a ByProc) Len() int      { return len(a) }
func (a ByProc) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a ByProc) Less(i, j int) bool {
	return a[i].Proc > a[j].Proc
}

func CreateSort(t *testing.T, result []interface{}, procList []int) (res ByProc) {
	assert.Equal(t, len(result), len(procList), "Procent Count form query is wrong")

	for i, item := range result {
		id := 0

		switch item.(type) {
		case *TestFullTextSimpleItem:
			id = item.(*TestFullTextSimpleItem).ID
		case *TestFullTextMergedItem:
			id = item.(*TestFullTextMergedItem).ID
		case *TestFullTextItem:
			id = item.(*TestFullTextItem).ID
		default:
			assert.Fail(t, "[%d] Unknown type after merge: %T", i, item)
		}
		res = append(res, SortFullText{id, procList[i]})
	}
	return res
}

func CheckTestItemsMergeQueries(t *testing.T) {

	first := randLangString()
	second := randLangString()
	third := randLangString()

	q1 := DB.Query("test_full_text_simple_item").Where("name", reindexer.EQ, first).
		Join(DB.Query("merge_join_item1"), "joined").
		On("join_id", reindexer.EQ, "id")
	qq1 := DB.Query("test_full_text_simple_item").Where("name", reindexer.EQ, first)
	q2 := DB.Query("test_full_text_merged_item").Where("description", reindexer.EQ, second).
		InnerJoin(DB.Query("merge_join_item2"), "joined").
		On("join_id", reindexer.EQ, "id")
	q3 := DB.Query("test_full_text_item").Where("name", reindexer.EQ, third)
	qm := q1.Merge(q2).Merge(q3).Debug(reindexer.TRACE)

	merge, q1Procs, _ := qm.MustExec(t).FetchAllWithRank()

	lmerge, _ := qq1.Limit(2).MustExec(&testing.T{}).FetchAll()
	assert.LessOrEqual(t, len(lmerge), 2, "LIMIT NOT WORKING")

	//TEST SIMPLE SORT

	usorted := CreateSort(t, merge, q1Procs)
	sorted := make([]SortFullText, len(usorted))
	copy(sorted, usorted)
	sort.Sort(ByProc(sorted))
	//After Sort result with same proc can be not in same order (smart oreder in c++) that why no use reflect.DeepEqual
	for i := 0; i < len(merge); i++ {
		assert.Equal(t, usorted[i].Proc, sorted[i].Proc, "Merge sort in go not equual to c sort simple")
	}
	qs1 := DB.Query("test_full_text_simple_item").Where("name", reindexer.EQ, strings.ToUpper(first)).
		Join(DB.Query("merge_join_item1"), "joined").
		On("join_id", reindexer.EQ, "id")
	qs2 := DB.Query("test_full_text_merged_item").Where("description", reindexer.EQ, strings.ToUpper(second)).
		InnerJoin(DB.Query("merge_join_item2"), "joined").
		On("join_id", reindexer.EQ, "id")
	qs3 := DB.Query("test_full_text_item").Where("name", reindexer.EQ, strings.ToUpper(third))
	r1, rr1, e1 := qs1.MustExec(t).FetchAllWithRank()
	assert.NoError(t, e1)
	r2, rr2, e2 := qs2.MustExec(t).FetchAllWithRank()
	assert.NoError(t, e2)
	r3, rr3, e3 := qs3.MustExec(t).FetchAllWithRank()
	assert.NoError(t, e3)
	//TEST LEN
	assert.Equal(t, len(r1)+len(r2)+len(r3), len(merge), "(%d+%d+%d) (%p, %p, %p)", len(r1), len(r2), len(r3), qs1, qs2, qs3)
	assert.NotEqual(t, len(merge), 0, "Full text dosen't return any result - something bad happend")

	var items []interface{}
	check := make(map[string]interface{})

	for _, item := range r1 {
		items = append(items, item)
		check[strconv.Itoa(item.(*TestFullTextSimpleItem).ID)+"test_full_text_simple_item"] = item
	}
	for _, item := range r2 {
		items = append(items, item)
		check[strconv.Itoa(item.(*TestFullTextMergedItem).ID)+"test_full_text_merged_item"] = item
	}
	for _, item := range r3 {
		items = append(items, item)
		check[strconv.Itoa(item.(*TestFullTextItem).ID)+"test_full_text_item"] = item
	}

	//TEST MERGE IS WORKING AND WORKING WITHOUT CACHE
	for _, item := range merge {
		switch item.(type) {
		case *TestFullTextSimpleItem:
			key := strconv.Itoa(item.(*TestFullTextSimpleItem).ID) + "test_full_text_simple_item"
			sitem, ok := check[key]
			assert.True(t, ok, "Item %s not fond in simple check", key)
			assert.Equal(t, item, sitem, "Item not same as from cache")
			delete(check, key)
		case *TestFullTextMergedItem:
			key := strconv.Itoa(item.(*TestFullTextMergedItem).ID) + "test_full_text_merged_item"
			sitem, ok := check[key]
			assert.True(t, ok, "Item %s not fond in simple check", key)
			assert.Equal(t, item, sitem, "Item not same as from cache")
			delete(check, key)
		case *TestFullTextItem:
			key := strconv.Itoa(item.(*TestFullTextItem).ID) + "test_full_text_item"
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
		assert.Equal(t, usorted[i].Proc, sortedNew[i].Proc, "Merge sort in go not equual to c sort simple")
	}
}
