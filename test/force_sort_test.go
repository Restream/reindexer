package reindexer

import (
	"fmt"
	"log"
	"testing"

	"git.itv.restr.im/itv-backend/reindexer"
)

const testNs string = "test_items_force_sort_order"

type TestForceSortOrderItem struct {
	ID      int      `reindex:"id,,pk"`
	Year    int      `reindex:"year,tree"`
	Name    string   `reindex:"name"`
	Phone   string   `reindex:"phone"`
	IdPhone struct{} `reindex:"id+phone,,composite"`
}

var forceSortOrderData = []*TestForceSortOrderItem{
	{ID: 1, Year: 2007, Name: "item1", Phone: "111111"},
	{ID: 2, Year: 2000, Name: "item2", Phone: "222222"},
	{ID: 7, Year: 2003, Name: "item7", Phone: "333333"},
	{ID: 3, Year: 2001, Name: "item3", Phone: "444444"},
	{ID: 6, Year: 2006, Name: "item6", Phone: "111111"},
	{ID: 5, Year: 2005, Name: "item5", Phone: "222222"},
	{ID: 8, Year: 2004, Name: "item8", Phone: "333333"},
	{ID: 4, Year: 2002, Name: "item4", Phone: "444444"},
}

func init() {
	tnamespaces[testNs] = TestForceSortOrderItem{}
}

type SortOrderValues struct {
	q      *queryTest
	limit  int
	offset int
}

func copyWholeTree(query *queryTest, tree *queryTestEntryTree) {
	for _, d := range tree.data {
		query.nextOp = d.op
		if d.dataType == leaf {
			entry := d.data.(*queryTestEntry)
			query.Where(entry.index, entry.condition, entry.ikeys)
		} else {
			query.OpenBracket()
			copyWholeTree(query, tree)
			query.CloseBracket()
		}
	}
}

func newSortOrderValues(query *queryTest) *SortOrderValues {
	q := newTestQuery(query.db, query.namespace)
	q.Distinct(query.distinctIndex)
	for i := 0; i < len(query.sortIndex); i++ {
		q.Sort(query.sortIndex[i], query.sortDesc, query.sortValues[query.sortIndex[i]]...)
	}
	if query.reqTotalCount {
		q.ReqTotal()
	}
	q.nextOp = query.nextOp
	copyWholeTree(q, &query.entries)

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

	defer query.close()

	items, err := query.ManualClose().Exec().FetchAll()
	if err != nil {
		panic(err)
	}

	sortOrderValues := newSortOrderValues(query)
	checkItems := sortOrderValues.GetVerifyItems()
	sortIdx, _ := query.ns.getField(query.sortIndex[0])

	ret := ""
	if len(items) == len(checkItems) {
		for i := 0; i < len(items); i++ {
			// log.Println(" --- ", items[i].(*TestForceSortOrderItem), " == ", checkItems[i].(*TestForceSortOrderItem))
			v1 := getValues(items[i], sortIdx)
			ret += fmt.Sprintf("%v ", v1[0].Interface())

			v2 := getValues(checkItems[i], sortIdx)
			if len(v1) != len(v2) {
				log.Fatalf("Found len(values) != len(sort) on sort index %s in item %+v", query.sortIndex, items[i])
			}

			if compareValues(v1[0], v2[0]) != 0 {
				log.Fatalf("Sort error on index %s,desc=%v ... expected: %v ... real: %v .... ", query.sortIndex, query.sortDesc, v2[0], v1[0])
			}
		}
	}
	log.Printf("\t %s -> %s", query.toString(), ret)
}

func FillTestItemsForceSortOrder() {
	tx := newTestTx(DB, testNs)
	for _, item := range forceSortOrderData {
		if err := tx.Insert(item); err != nil {
			panic(err)
		}
	}
	cnt := tx.MustCommit()
	if cnt != len(forceSortOrderData) {
		panic(fmt.Errorf("Could not commit forceSortOrderData"))
	}
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

	execAndVerifyForceSortOrderQuery(newTestQuery(DB, testNs).Sort("id+phone", false,
		[]interface{}{7, "333333"},
		[]interface{}{4, "444444"},
		[]interface{}{5, "222222"},
	))

}

func TestForceSortOrder(b *testing.T) {
	FillTestItemsForceSortOrder()
	CheckTestItemsForceSorted()
}
