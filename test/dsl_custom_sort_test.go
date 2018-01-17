package reindexer

import (
	"fmt"
	"log"
	"reflect"
	"testing"
)

type SortOrderChecker interface {
	Check(item reflect.Value) bool
}

func newSortOrderChecker(values []interface{}) SortOrderChecker {
	sortMap := map[string]struct{}{}
	for _, value := range values {
		key := fmt.Sprintf("%v", reflect.ValueOf(value))
		sortMap[key] = struct{}{}
	}
	return &sortOrder{data: values, ptr: -1, m: sortMap}
}

type sortOrder struct {
	data []interface{}
	ptr  int
	m    map[string]struct{}
}

func (so *sortOrder) Check(item reflect.Value) bool {
	if !so.End() && so.ptr < 0 {
		so.Next()
	}

	if so.End() {
		return !so.Exist(item)
	}

	if compareValues(item, reflect.ValueOf(so.Ptr())) == 0 {
		return true
	} else {
		expected := so.expectedValue()
		defer so.Next()
		if expected != nil && compareValues(item, reflect.ValueOf(so.expectedValue())) == 0 {
			return true
		} else {
			return !so.Exist(item)
		}
	}

	return false
}

func (so *sortOrder) End() bool {
	return len(so.data) == 0 || so.ptr >= len(so.data)
}

func (so *sortOrder) Exist(item interface{}) bool {
	key := fmt.Sprintf("%v", item)
	_, ok := so.m[key]
	return ok
}

func (so *sortOrder) expectedValue() interface{} {
	ptr := so.ptr + 1
	if ptr < len(so.data) {
		return so.data[ptr]
	}
	return nil
}

func (so *sortOrder) Next() bool {
	so.ptr++
	return so.ptr < len(so.data)
}

func (so *sortOrder) Ptr() interface{} {
	if so.ptr < 0 {
		panic("You should call sortOrder.Next()")
	}

	if so.ptr >= len(so.data) {
		panic("Ptr() out of bounds")
	}

	return so.data[so.ptr]
}

const testNs string = "test_items_dsl_sort_order"

type TestDSLSortOrderItem struct {
	ID    int    `reindex:"id,,pk"`
	Year  int    `reindex:"year,tree"`
	Name  string `reindex:"name"`
	Phone string `reindex:"phone"`
}

var dslSortOrderData = []*TestDSLSortOrderItem{
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
	tnamespaces[testNs] = TestDSLSortOrderItem{}
}

func FillTestItemsForDSLCustomSort() {
	tx := newTestTx(DB, testNs)

	for _, item := range dslSortOrderData {
		if cnt, err := tx.Insert(item); err != nil {
			panic(err)
		} else if cnt == 0 {
			panic(fmt.Errorf("Could not insert item: %+v", *item))
		}
	}

	tx.MustCommit(nil)
}

func CheckTestItemsForDSLCustomSort() {
	log.Println("CHECK DSL SORT ORDER .....")

	// check by 'id'
	log.Println("\tCHECK BY id ...")
	newTestQuery(DB, testNs).Sort("id", false, 8, 7, 6, 5).ExecAndVerify()

	// check by 'year'
	log.Println("\tCHECK BY year ...")
	newTestQuery(DB, testNs).Sort("year", false, 2007, 2003, 2005, 2002).ExecAndVerify()

	// check by 'name'
	log.Println("\tCHECK BY name(string) ...")
	newTestQuery(DB, testNs).Sort("name", false, "item3", "item5", "item6", "item8").ExecAndVerify()

	// check by 'phone' and duplicated elements
	log.Println("\tCHECK BY phone(string) AND DUPLICATED VALUES IN RESULTS ...")
	newTestQuery(DB, testNs).Sort("phone", false, "444444", "111111", "333333", "222222").ExecAndVerify()

	// check by 'id' and SOME unused values in sort order
	log.Println("\tCHECK BY id AND SOME UNUSED VALUES IN SORT ORDER ...")
	newTestQuery(DB, testNs).Sort("id", false, 11, 3, 16, 2, 15, 1).ExecAndVerify()

	// check by 'id' and ALL unused values in sort order
	log.Println("\tCHECK BY id AND ALL UNUSED VALUES IN SORT ORDER ...")
	newTestQuery(DB, testNs).Sort("id", false, 18, 17, 16, 15).ExecAndVerify()

}

func TestDSLSortOrder(b *testing.T) {
	FillTestItemsForDSLCustomSort()
	CheckTestItemsForDSLCustomSort()
}
