package reindexer

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"testing"

	"github.com/restream/reindexer"
)

const fieldsUpdateNs = "test_items_fields_update"

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

type testInnerObject struct {
	First  int    `reindex:"first" json:"first"`
	Second string `reindex:"second" json:"second"`
	Extra  string `json:"extra,omitempty"`
}

type testItemObject struct {
	Name   string            `reindex:"name" json:"name"`
	Age    int               `reindex:"age" json:"age"`
	Year   int               `reindex:"year" json:"year"`
	Price  int64             `reindex:"price" json:"price"`
	Main   testInnerObject   `reindex:"main" json:"main"`
	Nested []testInnerObject `reindex:"nested" json:"nested"`
	Bonus  int               `json:"bonus,omitempty"`
}

type TestItemComplexObject struct {
	ID        int              `reindex:"id,,pk" json:"id"`
	Code      int64            `reindex:"code" json:"code"`
	IsEnabled bool             `reindex:"is_enabled" json:"is_enabled"`
	Desc      string           `reindex:"desc" json:"desc"`
	MainObj   testItemObject   `reindex:"main_obj" json:"main_obj"`
	Size      int              `json:"size"`
	Employees []string         `reindex:"employees" json:"employees"`
	Animals   []string         `json:"animals"`
	Languages []string         `json:"languages"`
	Numbers   []int            `json:"numbers"`
	Objects   []testItemObject `json:"objects"`
	Optional  string           `json:"optional,omitempty"`
}

func newTestItemComplexObject(id int) *TestItemComplexObject {
	innerObjectsCnt := rand.Int()%20 + 1
	innerObjects := make([]testInnerObject, 0, innerObjectsCnt)
	for i := 0; i < innerObjectsCnt; i++ {
		innerObjects = append(innerObjects, testInnerObject{
			First:  rand.Int() % 1000,
			Second: randString(),
		})
	}

	nestedItemObjectCnt := rand.Int()%10 + 1
	nestedItemObject := make([]testItemObject, 0, nestedItemObjectCnt)
	for i := 0; i < nestedItemObjectCnt; i++ {
		nestedItemObject = append(nestedItemObject, testItemObject{
			Name:   randString(),
			Age:    rand.Int()%60 + 10,
			Year:   rand.Int() % 2019,
			Price:  rand.Int63() % 100000,
			Main:   testInnerObject{First: rand.Int() % 1000, Second: randString()},
			Nested: innerObjects,
		})
	}

	nestedArraySize := rand.Int()%10 + 1
	employees := make([]string, 0, nestedArraySize)
	animals := make([]string, 0, nestedArraySize)
	languages := make([]string, 0, nestedArraySize)
	numbers := make([]int, 0, nestedArraySize)
	for i := 0; i < nestedArraySize; i++ {
		employees = append(employees, randString())
		animals = append(animals, randString())
		languages = append(languages, randString())
		numbers = append(numbers, rand.Int())
	}

	return &TestItemComplexObject{
		ID:        id,
		Code:      rand.Int63() % 10000000,
		IsEnabled: (rand.Int() % 2) == 0,
		Desc:      randString(),
		Size:      rand.Int() % 200000,
		Employees: employees,
		Animals:   animals,
		Languages: languages,
		Numbers:   numbers,
		Objects:   nestedItemObject,
		MainObj: testItemObject{
			Name:   randString(),
			Age:    rand.Int() % 90,
			Year:   rand.Int() % 2019,
			Price:  rand.Int63() % 100000,
			Main:   testInnerObject{First: rand.Int() % 1000, Second: randString()},
			Nested: innerObjects,
		},
	}
}

func TestUpdate(b *testing.T) {
	FillTestItemsForInsertUpdate()
	CheckTestItemsInsertUpdate()
}

func TestUpdateFields(t *testing.T) {
	nsOpts := reindexer.DefaultNamespaceOptions()
	assertErrorMessage(t, DB.OpenNamespace(fieldsUpdateNs, nsOpts, TestItemComplexObject{}), nil)
	for i := 0; i < 1000; i++ {
		assertErrorMessage(t, DB.Upsert(fieldsUpdateNs, newTestItemComplexObject(i)), nil)
	}

	CheckIndexedFieldUpdate()
	CheckNonIndexedFieldUpdate()
	CheckNonIndexedArrayFieldUpdate()
	CheckIndexedArrayFieldUpdate()
	CheckNestedFieldUpdate()
	CheckNestedFieldUpdate2()
	CheckAddSimpleFields()
	CheckAddComplexField("nested2.nested3.nested4.val", []string{"nested2", "nested3", "nested4", "val"})
	CheckAddComplexField("main_obj.main.nested.val", []string{"main_obj", "main", "nested", "val"})
}

func UpdateField(fieldName string, values interface{}) (items []interface{}) {
	count, err := DB.Query(fieldsUpdateNs).Where("main_obj.age", reindexer.GE, 16).Set(fieldName, values).Update()
	if err != nil {
		panic(err)
	}
	if count == 0 {
		panic(fmt.Errorf("No items updated"))
	}

	results, err := DB.Query(fieldsUpdateNs).Where("main_obj.age", reindexer.GE, 16).Exec().FetchAll()
	if err != nil {
		panic(err)
	}

	if len(results) == 0 {
		panic(fmt.Errorf("No results found"))
	}

	return results
}

func CheckIndexedFieldUpdate() {
	results := UpdateField("main_obj.year", 2007)
	for i := 0; i < len(results); i++ {
		year := results[i].(*TestItemComplexObject).MainObj.Year
		if year != 2007 {
			panic(fmt.Errorf("Update of field 'main_obj.year' has shown wrong results %d", year))
		}
	}
}

func CheckNonIndexedFieldUpdate() {
	results := UpdateField("size", 45)
	for i := 0; i < len(results); i++ {
		size := results[i].(*TestItemComplexObject).Size
		if size != 45 {
			panic(fmt.Errorf("Update of field 'size' has shown wrong results %d", size))
		}
	}
}

func CheckNonIndexedArrayFieldUpdate() {
	newAnimals := make([]string, 0, 20)
	for i := 0; i < 20; i++ {
		newAnimals = append(newAnimals, randString())
	}
	results := UpdateField("animals", newAnimals)
	for i := 0; i < len(results); i++ {
		animals := results[i].(*TestItemComplexObject).Animals
		equal := (len(newAnimals) == len(animals))
		if equal {
			for i := 0; i < len(animals); i++ {
				if strings.Compare(newAnimals[i], animals[i]) != 0 {
					equal = false
					break
				}
			}
		}
		if !equal {
			panic(fmt.Errorf("Update of field 'animals' has shown wrong results"))
		}
	}
}

func CheckIndexedArrayFieldUpdate() {
	newEmployees := make([]string, 0, 20)
	for i := 0; i < 20; i++ {
		newEmployees = append(newEmployees, randString())
	}
	results := UpdateField("employees", newEmployees)
	for i := 0; i < len(results); i++ {
		employees := results[i].(*TestItemComplexObject).Employees
		equal := (len(newEmployees) == len(employees))
		if equal {
			for i := 0; i < len(employees); i++ {
				if strings.Compare(newEmployees[i], employees[i]) != 0 {
					equal = false
					break
				}
			}
		}
		if !equal {
			panic(fmt.Errorf("Update of field 'employees' has shown wrong results"))
		}
	}
}

func CheckNestedFieldUpdate() {
	results := UpdateField("main_obj.main.first", 777)
	for i := 0; i < len(results); i++ {
		first := results[i].(*TestItemComplexObject).MainObj.Main.First
		if first != 777 {
			panic(fmt.Errorf("Update of field 'nested_obj.main.first' has shown wrong results %d", first))
		}
	}
}

func CheckNestedFieldUpdate2() {
	results := UpdateField("main_obj.main.second", "bingo!")
	for i := 0; i < len(results); i++ {
		second := results[i].(*TestItemComplexObject).MainObj.Main.Second
		if second != "bingo!" {
			panic(fmt.Errorf("Update of field 'nested_obj.main.second' has shown wrong results %s", second))
		}
	}
}

func CheckAddSimpleFields() {
	results := UpdateField("optional", "new field")
	for i := 0; i < len(results); i++ {
		optional := results[i].(*TestItemComplexObject).Optional
		if optional != "new field" {
			panic(fmt.Errorf("Adding of field 'nested_obj.main.first' went wrong: %s", optional))
		}
	}

	results2 := UpdateField("main_obj.bonus", 777)
	for i := 0; i < len(results2); i++ {
		bonus := results2[i].(*TestItemComplexObject).MainObj.Bonus
		if bonus != 777 {
			panic(fmt.Errorf("Adding of field 'nested_obj.main.first' went wrong: %d", bonus))
		}
	}

	results3 := UpdateField("main_obj.main.extra", "new nested field")
	for i := 0; i < len(results3); i++ {
		extra := results3[i].(*TestItemComplexObject).MainObj.Main.Extra
		if extra != "new nested field" {
			panic(fmt.Errorf("Adding of field 'nested_obj.main.first' went wrong: %s", extra))
		}
	}
}

func hasJSONPath(path []string, data map[string]interface{}) bool {
	if len(path) > 0 {
		if child, ok := data[path[0]]; ok {
			if len(path) == 1 {
				return true
			} else {
				if childMap, ok := child.(map[string]interface{}); ok {
					return hasJSONPath(path[1:], childMap)
				}
			}
		}
	}
	return false
}

func CheckAddComplexField(path string, subfields []string) {
	UpdateField(path, "extra value")
	jsonIter := DB.Query(fieldsUpdateNs).Where("main_obj.age", reindexer.GE, 16).ExecToJson()
	for jsonIter.Next() {
		jsonB := jsonIter.JSON()
		var data map[string]interface{}
		if err := json.Unmarshal(jsonB, &data); err != nil {
			panic(err)
		}
		if hasJSONPath(subfields, data) == false {
			fmt.Println(string(jsonB[:]))
			fmt.Printf("Adding of field '%s' went wrong\n", path)
		}
	}
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
