package reindexer

import (
	"fmt"
	"log"
	"testing"
)

func init() {
	tnamespaces["test_items_insert_update"] = TestItemSimple{}
}

var checkInsertUpdateExistsData = []*TestItemSimple{
	{1, 2007, "item1", "123456"},
	{2, 2000, "item2", "123456"},
	{7, 2003, "item7", "123456"},
	{3, 2001, "item3", "123456"},
	{6, 2006, "item6", "123456"},
	{5, 2005, "item5", "123456"},
	{8, 2004, "item8", "123456"},
	{4, 2002, "item4", "123456"},
}

var checkInsertUpdateNonExistsData = []*TestItemSimple{
	{9, 1999, "item9", "123456"},
	{15, 2017, "item15", "123456"},
	{11, 2013, "item11", "123456"},
	{13, 2011, "item13", "123456"},
	{10, 1996, "item10", "123456"},
	{12, 1995, "item12", "123456"},
	{14, 2014, "item14", "123456"},
	{16, 2012, "item16", "123456"},
}

func TestUpdate(b *testing.T) {
	FillTestItemsForInsertUpdate()
	CheckTestItemsInsertUpdate()
}

func FillTestItemsForInsertUpdate() {
	tx := newTestTx(DB, "test_items_insert_update")

	for _, item := range checkInsertUpdateExistsData {
		if err := tx.Insert(item); err != nil {
			panic(err)
		}
	}

	cnt := tx.MustCommit(nil)
	if cnt != len(checkInsertUpdateExistsData) {
		panic(fmt.Errorf("Could not commit testSortModeDataCustomSource"))
	}
}

func CheckTestItemsInsertUpdate() {
	actionMap := map[string]func(string, interface{}, ...string) (int, error){
		"INSERT": DB.Insert,
		"UPDATE": DB.Update,
	}

	updateNonExistsData := func(data []*TestItemSimple) {
		for _, item := range data {
			item.ID = item.ID + 100
		}
	}

	existsMap := map[string][]*TestItemSimple{
		"EXISTING":     checkInsertUpdateExistsData,
		"NON EXISTING": checkInsertUpdateNonExistsData,
	}

	for actionName, doAction := range actionMap {
		for exists, dataset := range existsMap {
			log.Printf("DO '%s' %s ITEMS", actionName, exists)
			for _, item := range dataset {
				cnt, err := doAction("test_items_insert_update", item)

				if err != nil {
					panic(err)
				}

				act := actionName + " " + exists

				switch act {
				case "INSERT EXISTING":
					if cnt != 0 {
						panic(fmt.Errorf("Expected affected items count = 0, but got %d\n Item: %+v", cnt, item))
					}

				case "INSERT NON EXISTING":
					if cnt != 1 {
						panic(fmt.Errorf("Expected affected items count = 1, but got %d\n Item: %+v", cnt, item))
					}
					// need to update data before 'UPDATE NON EXISTING'
					updateNonExistsData(existsMap["NON EXISTING"])

				case "UPDATE EXISTING":
					if cnt != 1 {
						panic(fmt.Errorf("Expected affected items count = 1, but got %d\n Item: %+v", cnt, item))
					}
					// need to update data before 'UPDATE NON EXISTING'
					updateNonExistsData(existsMap["NON EXISTING"])

				case "UPDATE NON EXISTING":
					if cnt != 0 {
						panic(fmt.Errorf("Expected affected items count = 0, but got %d\n Item: %+v", cnt, item))
					}

				}
			}
		}
	}
}
