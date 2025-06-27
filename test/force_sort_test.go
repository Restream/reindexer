package reindexer

import (
	"testing"

	"github.com/restream/reindexer/v5"
	"github.com/stretchr/testify/assert"
)

type TestForceSortOrderItem struct {
	ID      int      `reindex:"id,,pk"`
	Year    int      `reindex:"year,tree"`
	Name    string   `reindex:"name"`
	Phone   string   `reindex:"phone"`
	IdPhone struct{} `reindex:"id+phone,,composite"`
}

type SortOrderValues struct {
	q      *queryTest
	limit  int
	offset int
}

const testForceSortNs = "test_items_force_sort_order"

func init() {
	tnamespaces[testForceSortNs] = TestForceSortOrderItem{}
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

func copyWholeTree(query *queryTest, tree *queryTestEntryTree) {
	for _, d := range tree.data {
		query.nextOp = d.op
		switch d.dataType {
		case oneFieldEntry:
			entry := d.data.(*queryTestEntry)
			query.Where(entry.index, entry.condition, entry.ikeys)
		case twoFieldsEntry:
			entry := d.data.(*queryBetweenFieldsTestEntry)
			query.WhereBetweenFields(entry.firstField, entry.condition, entry.secondField)
		case bracket:
			query.OpenBracket()
			copyWholeTree(query, tree)
			query.CloseBracket()
		}
	}
}

func newSortOrderValues(query *queryTest) *SortOrderValues {
	q := newTestQuery(query.db, query.namespace)
	q.Distinct(query.distinctIndexes)
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

func (so *SortOrderValues) GetVerifyItems(t *testing.T) []interface{} {
	items, err := so.q.Exec(t).FetchAll()
	assert.NoError(t, err)

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

func execAndVerifyForceSortOrderQuery(t *testing.T, query *queryTest) {

	defer query.close()

	items, err := query.ManualClose().Exec(t).FetchAll()
	assert.NoError(t, err)

	sortOrderValues := newSortOrderValues(query)
	checkItems := sortOrderValues.GetVerifyItems(t)
	sortIdx, _ := query.ns.getField(query.sortIndex[0])

	// if len(items) == len(checkItems) {
	for i := 0; i < len(items); i++ {
		v1 := getValues(items[i], sortIdx)

		v2 := getValues(checkItems[i], sortIdx)
		assert.Equal(t, len(v1), len(v2), "Found len(values) != len(sort) on sort index %s in item %+v", query.sortIndex, items[i])

		assert.Equal(t, compareValues(t, v1[0], v2[0]), 0,
			"Sort error on index %s,desc=%v ... expected: %v ... real: %v .... ", query.sortIndex, query.sortDesc, v2[0], v1[0])
	}
	// }
}

func TestForceSortOrder(t *testing.T) {
	const ns = testForceSortNs
	tx := newTestTx(DB, ns)
	for _, item := range forceSortOrderData {
		assert.NoError(t, tx.Insert(item))
	}
	assert.Equal(t, tx.MustCommit(), len(forceSortOrderData), "Could not commit forceSortOrderData")

	execAndVerifyForceSortOrderQuery(t, newTestQuery(DB, ns).Sort("id", false, 7, 8, 6, 5))
	execAndVerifyForceSortOrderQuery(t, newTestQuery(DB, ns).Sort("id", false, 8, 7, 6, 5).Limit(3))
	execAndVerifyForceSortOrderQuery(t, newTestQuery(DB, ns).Sort("id", false, 8, 7, 6, 5).Limit(3).Offset(2))
	execAndVerifyForceSortOrderQuery(t, newTestQuery(DB, ns).Sort("id", false, 8, 7, 6, 5).Limit(3).Offset(9))
	execAndVerifyForceSortOrderQuery(t, newTestQuery(DB, ns).Sort("year", false, 2007, 2003, 2005, 2002).Where("id", reindexer.GT, 2))
	execAndVerifyForceSortOrderQuery(t, newTestQuery(DB, ns).Sort("name", false, "item3", "item5", "item6", "item8"))
	execAndVerifyForceSortOrderQuery(t, newTestQuery(DB, ns).Sort("phone", false, "444444", "111111", "333333", "222222"))
	execAndVerifyForceSortOrderQuery(t, newTestQuery(DB, ns).Sort("id", false, 11, 3, 16, 2, 15, 1))
	execAndVerifyForceSortOrderQuery(t, newTestQuery(DB, ns).Sort("id", false, 18, 17, 16, 15))
	execAndVerifyForceSortOrderQuery(t, newTestQuery(DB, ns).Sort("id", false, 18, 17, 16, 15).Sort("year", false))
	execAndVerifyForceSortOrderQuery(t, newTestQuery(DB, ns).Where("phone", reindexer.SET, []string{"111111", "222222"}).Sort("id", true, 1, 6, 2, 5).Offset(1).Limit(3))

	execAndVerifyForceSortOrderQuery(t, newTestQuery(DB, ns).Sort("id+phone", false,
		[]interface{}{7, "333333"},
		[]interface{}{4, "444444"},
		[]interface{}{5, "222222"},
	))

	it := newTestQuery(DB, ns).Sort("id", false, 7, 8, 6, 5).Sort("year", false, 2007, 2003, 2005, 2002).Exec(t)
	assert.Error(t, it.Error())
	it = newTestQuery(DB, ns).Sort("id", false).Sort("year", false, 2007, 2003, 2005, 2002).Exec(t)
	assert.Error(t, it.Error())
}
