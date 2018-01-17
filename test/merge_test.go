package reindexer

import (
	"log"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/restream/reindexer"

	"reflect"

	"fmt"
)

type TestFullTextSimpleItem struct {
	ID   int    `reindex:"id,,pk"`
	Name string `reindex:"name,text"`
}
type TestFullTextMergedItem struct {
	ID          int    `reindex:"id,,pk"`
	Description string `reindex:"description,text"`
	Location    string `reindex:"location,text"`
	Rate        int    `reindex:"rate"`
}
type TestFullTextItem struct {
	ID   int    `reindex:"id,,pk"`
	Name string `reindex:"name,text"`
}

func init() {
	tnamespaces["test_full_text_simple_item"] = TestFullTextSimpleItem{}
	tnamespaces["test_full_text_merged_item"] = TestFullTextMergedItem{}
	tnamespaces["test_full_text_item"] = TestFullTextItem{}
}

func FillFullTextSimpleItemsTx(count int, tx *txTest) {
	for i := 0; i < count; i++ {
		if err := tx.Upsert(&TestFullTextSimpleItem{
			ID:   mkID(i),
			Name: randLangString(),
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
		}); err != nil {
			panic(err)
		}
	}
}
func FillTestFullTextItemsTx(count int, tx *txTest) {
	for i := 0; i < count; i++ {
		if err := tx.Upsert(&TestFullTextItem{
			ID:   mkID(i),
			Name: randLangString(),
		}); err != nil {
			panic(err)
		}
	}
}
func FillTestFullTextItems(count int) {
	tx := newTestTx(DB, "test_full_text_simple_item")
	FillFullTextSimpleItemsTx(count, tx)
	tx.MustCommit(nil)
	tx = newTestTx(DB, "test_full_text_merged_item")
	FillTestFullTextMergedItemsTx(count, tx)
	tx.MustCommit(nil)
	tx = newTestTx(DB, "test_full_text_item")
	FillTestFullTextItemsTx(count, tx)
	tx.MustCommit(nil)
}

func TestMerge(t *testing.T) {
	FillTestFullTextItems(5000)
	CheckTestItemsMergeQueries()
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

func CreateSort(result []interface{}, procList []int) (res ByProc) {
	if len(result) != len(procList) {
		panic(fmt.Errorf("Procent Count form query is wrong, got %d expects %d", len(result), len(procList)))
	}
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
			panic(fmt.Errorf("[%d] Unknown type after merge: %T", i, item))
		}
		res = append(res, SortFullText{id, procList[i]})
	}
	return res
}

func CheckTestItemsMergeQueries() {
	log.Printf("DO MERGE TESTS")

	first := randLangString()
	second := randLangString()
	third := randLangString()

	q1 := DB.Query("test_full_text_simple_item").Where("name", reindexer.EQ, first).Sort("name", false)
	qq1 := DB.Query("test_full_text_simple_item").Where("name", reindexer.EQ, first).Sort("name", false)
	q2 := DB.Query("test_full_text_merged_item").Where("description", reindexer.EQ, second)
	q3 := DB.Query("test_full_text_item").Where("name", reindexer.EQ, third)
	merge, q1Procs, _ := q1.Merge(q2).Merge(q3).MustExec().FetchAllWithRank()

	lmerge, _ := qq1.Limit(2).MustExec().FetchAll()
	if len(lmerge) > 2 {
		panic(fmt.Errorf("LIMIT NOT WORKING"))
	}
	//TEST SIMPLE SORT

	usorted := CreateSort(merge, q1Procs)
	sorted := make([]SortFullText, len(usorted))
	copy(sorted, usorted)
	sort.Sort(ByProc(sorted))
	//After Sort result with same proc can be not in same order (smart oreder in c++) that why no use reflect.DeepEqual
	for i := 0; i < len(merge); i++ {
		if usorted[i].Proc != sorted[i].Proc {
			panic(fmt.Errorf("Merge sort in go not equual to c sort simple"))
		}
	}
	qs1 := DB.Query("test_full_text_simple_item").Where("name", reindexer.EQ, strings.ToUpper(first))
	qs2 := DB.Query("test_full_text_merged_item").Where("description", reindexer.EQ, strings.ToUpper(second))
	qs3 := DB.Query("test_full_text_item").Where("name", reindexer.EQ, strings.ToUpper(third))
	r1, rr1, e1 := qs1.MustExec().FetchAllWithRank()
	r2, rr2, e2 := qs2.MustExec().FetchAllWithRank()
	r3, rr3, e3 := qs3.MustExec().FetchAllWithRank()
	if e1 != nil || e2 != nil || e3 != nil {
		panic(fmt.Errorf("query error:[1:%v;\t2:%v;\t3:%v]", e1, e2, e3))
	}
	//TEST LEN
	if len(r1)+len(r2)+len(r3) != len(merge) {
		panic(fmt.Errorf("%d != %d (%d+%d+%d) (%p, %p, %p)", len(r1)+len(r2)+len(r3), len(merge), len(r1), len(r2), len(r3), qs1, qs2, qs3))
	}
	if len(merge) == 0 {
		panic(fmt.Errorf("Full text dosen't return any result - somthing bad happend"))
	}

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
			if sitem, ok := check[key]; ok {
				if !reflect.DeepEqual(item, sitem) {
					panic(fmt.Errorf("Item %v not same as from cahe %v", item, sitem))
				}
				delete(check, key)

			} else {
				panic(fmt.Errorf("Item %s not fond in simple check", key))
			}
		case *TestFullTextMergedItem:
			key := strconv.Itoa(item.(*TestFullTextMergedItem).ID) + "test_full_text_merged_item"
			if sitem, ok := check[key]; ok {
				if !reflect.DeepEqual(item, sitem) {
					panic(fmt.Errorf("Item %v not same as from cahe %v", item, sitem))
				}
				delete(check, key)
			} else {
				panic(fmt.Errorf("Item %s not fond in simple check", key))
			}
		case *TestFullTextItem:
			key := strconv.Itoa(item.(*TestFullTextItem).ID) + "test_full_text_item"
			if sitem, ok := check[key]; ok {
				if !reflect.DeepEqual(item, sitem) {
					panic(fmt.Errorf("Item %v not same as from cahe %v", item, sitem))
				}
				delete(check, key)
			} else {
				panic(fmt.Errorf("Item %s not fond in simple check", key))
			}
		default:
			panic(fmt.Errorf("Unknown type after merge "))
		}
	}
	if len(check) != 0 {
		panic(fmt.Errorf("Not all data in merge"))
	}
	//TEST  SORT
	sortedNew := CreateSort(r1, rr1)
	sortedNew = append(sortedNew, CreateSort(r2, rr2)...)

	sortedNew = append(sortedNew, CreateSort(r3, rr3)...)

	sort.Sort(ByProc(sortedNew))

	//In second read result can not directly same
	for i := 0; i < len(usorted); i++ {
		if usorted[i].Proc != sortedNew[i].Proc {
			panic(fmt.Errorf("Merge sort in go not equual to c sort"))
		}
	}
}
