package reindexer

import (
	"fmt"
	"log"
	"testing"

	"github.com/restream/reindexer"
)

const testNs string = "test_items_force_sort_order"

type TestForceSortOrderItem struct {
	ID    int    `reindex:"id,,pk"`
	Year  int    `reindex:"year,tree"`
	Name  string `reindex:"name"`
	Phone string `reindex:"phone"`
}

var forceSortOrderData = []*TestForceSortOrderItem{
	{1, 2007, "item1", "111111"},
	{2, 2000, "item2", "222222"},
	{7, 2003, "item7", "333333"},
	{3, 2001, "item3", "444444"},
	{6, 2006, "item6", "111111"},
	{5, 2005, "item5", "222222"},
	{8, 2004, "item8", "333333"},
	{4, 2002, "item4", "444444"},
}

func init() {
	tnamespaces[testNs] = TestForceSortOrderItem{}
}

type SortOrderValues struct {
	q      *queryTest
	limit  int
	offset int
}

func newSortOrderValues(query *queryTest) *SortOrderValues {
	q := newTestQuery(query.db, query.namespace)
	q.Distinct(query.distinctIndex)
	q.Sort(query.sortIndex, query.sortDesc, query.sortValues...)
	if query.reqTotalCount {
		q.ReqTotal()
	}
	q.nextOp = query.nextOp

	for _, entry := range query.entries {
		q.Where(entry.index, entry.condition, entry.ikeys)
	}

	return &SortOrderValues{
		q:      q,
		limit:  query.limitItems,
		offset: query.startOffset,
	}
}

func (so *SortOrderValues) GetVerifyItems() []interface{} {
	items, err := so.q.Exec().FetchAll()
	if err != nil {
		panic(err)
	}

	begin, end := 0, 0
	if so.offset >= len(items)-1 {
		return nil
	}

	begin = so.offset

	if so.limit == 0 || begin+so.limit > len(items) {
		end = len(items)
	} else {
		end = begin + so.limit
	}
	return items[begin:end]
}

func execAndVerifyForceSortOrderQuery(query *queryTest) {
	log.Println("\t", query.toString())

	items, err := query.Exec().FetchAll()
	if err != nil {
		panic(err)
	}

	sortOrderValues := newSortOrderValues(query)
	checkItems := sortOrderValues.GetVerifyItems()
	sortIdx := query.ns.fieldsIdx[query.sortIndex]

	if len(items) == len(checkItems) {
		for i := 0; i < len(items); i++ {
			// log.Println(" --- ", items[i].(*TestForceSortOrderItem), " == ", checkItems[i].(*TestForceSortOrderItem))
			v1 := getValues(items[i], sortIdx)
			v2 := getValues(checkItems[i], sortIdx)
			if len(v1) != 1 || len(v2) != 1 {
				log.Fatalf("Found len(values) != 1 on sort index %s in item %+v", query.sortIndex, items[i])
			}

			if compareValues(v1[0], v2[0]) != 0 {
				log.Fatalf("Sort error on index %s,desc=%v ... expected: %v ... real: %v .... ", query.sortIndex, query.sortDesc, v2[0], v1[0])
			}
		}
	}
}

func FillTestItemsForceSortOrder() {
	tx := newTestTx(DB, testNs)
	for _, item := range forceSortOrderData {
		if cnt, err := tx.Insert(item); err != nil {
			panic(err)
		} else if cnt == 0 {
			panic(fmt.Errorf("Could not insert item: %+v", *item))
		}
	}
	tx.MustCommit(nil)
}

func CheckTestItemsForceSorted() {
	log.Println("Check force sort order ...")
	execAndVerifyForceSortOrderQuery(newTestQuery(DB, testNs).Sort("id", false, 7, 8, 6, 5))
	execAndVerifyForceSortOrderQuery(newTestQuery(DB, testNs).Sort("id", false, 8, 7, 6, 5).Limit(3))
	execAndVerifyForceSortOrderQuery(newTestQuery(DB, testNs).Sort("id", false, 8, 7, 6, 5).Limit(3).Offset(2))
	execAndVerifyForceSortOrderQuery(newTestQuery(DB, testNs).Sort("id", false, 8, 7, 6, 5).Limit(3).Offset(9))
	execAndVerifyForceSortOrderQuery(newTestQuery(DB, testNs).Sort("year", false, 2007, 2003, 2005, 2002).Where("id", reindexer.GT, 2))
	execAndVerifyForceSortOrderQuery(newTestQuery(DB, testNs).Sort("name", false, "item3", "item5", "item6", "item8"))
	execAndVerifyForceSortOrderQuery(newTestQuery(DB, testNs).Sort("phone", false, "444444", "111111", "333333", "222222"))
	execAndVerifyForceSortOrderQuery(newTestQuery(DB, testNs).Sort("id", false, 11, 3, 16, 2, 15, 1))
	execAndVerifyForceSortOrderQuery(newTestQuery(DB, testNs).Sort("id", false, 18, 17, 16, 15))
	execAndVerifyForceSortOrderQuery(newTestQuery(DB, testNs).Where("phone", reindexer.SET, []string{"111111", "222222"}).Sort("id", true, 1, 6, 2, 5).Offset(1).Limit(3))
}

func TestForceSortOrder(b *testing.T) {
	FillTestItemsForceSortOrder()
	CheckTestItemsForceSorted()
}
