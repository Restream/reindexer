package reindexer

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/restream/reindexer"
	"github.com/stretchr/testify/require"
)

const (
	fieldsUpdateNs = "test_items_fields_update"
	truncateNs     = "test_truncate"
	removeItemsNs  = "test_remove_items"
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

type testInnerObject struct {
	First  int    `reindex:"first" json:"first"`
	Second string `reindex:"second" json:"second"`
	Extra  string `json:"extra,omitempty"`
}

type testDummyObject struct {
	ID   int    `reindex:"id,,pk" json:"id"`
	Name string `reindex:"name" json:"name"`
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
	Optional  interface{}      `json:"optional,omitempty"`
}

func randInnerObject() []testInnerObject {
	innerObjectsCnt := rand.Int()%20 + 1
	innerObjects := make([]testInnerObject, 0, innerObjectsCnt)
	for i := 0; i < innerObjectsCnt; i++ {
		innerObjects = append(innerObjects, testInnerObject{
			First:  rand.Int() % 1000,
			Second: randString(),
		})
	}
	return innerObjects
}

func randTestItemObject() testItemObject {
	return testItemObject{
		Name:   randString(),
		Age:    rand.Int()%60 + 10,
		Year:   rand.Int() % 2019,
		Price:  rand.Int63() % 100000,
		Main:   testInnerObject{First: rand.Int() % 1000, Second: randString()},
		Nested: randInnerObject(),
	}
}

func newTestItemComplexObject(id int) *TestItemComplexObject {
	nestedItemObjectCnt := rand.Int()%10 + 1
	nestedItemObject := make([]testItemObject, 0, nestedItemObjectCnt)
	for i := 0; i < nestedItemObjectCnt; i++ {
		nestedItemObject = append(nestedItemObject, randTestItemObject())
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
		Size:      rand.Int()%200000 + 100,
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
			Nested: randInnerObject(),
		},
	}
}

func TestUpdate(t *testing.T) {
	FillTestItemsForInsertUpdate(t)
	CheckTestItemsInsertUpdate(t)
}

func TestUpdateFields(t *testing.T) {
	nsOpts := reindexer.DefaultNamespaceOptions()
	require.NoError(t, DB.OpenNamespace(fieldsUpdateNs, nsOpts, TestItemComplexObject{}))
	for i := 0; i < 1000; i++ {
		require.NoError(t, DB.Upsert(fieldsUpdateNs, newTestItemComplexObject(i)))
	}

	RemoveDummyItems(t)

	CheckFieldsDrop(t)
	CheckIndexedFieldUpdate(t)
	CheckNonIndexedFieldUpdate(t)
	CheckNonIndexedArrayFieldUpdate(t)
	CheckIndexedArrayFieldUpdate(t)
	CheckUpdateObject(t)
	CheckUpdateObject2(t)
	CheckAddObject(t)
	CheckAddObject2(t)
	CheckUpdateArrayOfObjects(t, 10)
	CheckUpdateArrayOfObjects(t, 1)
	CheckUpdateArrayOfObjects(t, 0)
	CheckNestedFieldUpdate(t)
	CheckNestedFieldUpdate2(t)
	CheckAddSimpleFields(t)
	CheckAddComplexField(t, "nested2.nested3.nested4.val", []string{"nested2", "nested3", "nested4", "val"})
	CheckAddComplexField(t, "main_obj.main.nested.val", []string{"main_obj", "main", "nested", "val"})
	CheckUpdateWithExpressions1(t)
	CheckUpdateWithExpressions2(t)
}

func RemoveDummyItems(t *testing.T) {
	nsOpts := reindexer.DefaultNamespaceOptions()
	require.NoError(t, DB.OpenNamespace(removeItemsNs, nsOpts, testDummyObject{}))
	for i := 0; i < 10; i++ {
		require.NoError(t, DB.Upsert(removeItemsNs, testDummyObject{
			ID:   i,
			Name: randString(),
		}))
	}

	count, err := DB.Query(removeItemsNs).WhereInt("id", reindexer.LT, 3).Delete()
	require.NoError(t, err)
	require.Equal(t, count, 3, "Remove failed")

}

func DropField(t *testing.T, fieldName string) (items []interface{}) {
	res1, err := DB.Query(fieldsUpdateNs).Where("is_enabled", reindexer.EQ, true).Drop(fieldName).Update().FetchAll()
	require.NoError(t, err)
	require.NotEqual(t, len(res1), 0, "No items updated")

	results, err := DB.Query(fieldsUpdateNs).Where("is_enabled", reindexer.EQ, true).Exec().AllowUnsafe(true).FetchAll()
	require.NoError(t, err)
	require.Equal(t, len(results), len(res1), "Different count of items")

	return results
}

func UpdateItemField(t *testing.T, fieldName string, values interface{}, objectField bool) (items []interface{}) {
	var q *queryTest
	if objectField {
		q = DB.Query(fieldsUpdateNs).Where("is_enabled", reindexer.EQ, true).SetObject(fieldName, values)
	} else {
		q = DB.Query(fieldsUpdateNs).Where("is_enabled", reindexer.EQ, true).Set(fieldName, values)
	}

	res1, err := q.Update().AllowUnsafe(true).FetchAll()
	require.NoError(t, err)
	require.NotEqual(t, len(res1), 0, "No items updated")

	results, err := DB.Query(fieldsUpdateNs).Where("is_enabled", reindexer.EQ, true).Exec().AllowUnsafe(true).FetchAll()
	require.NoError(t, err)
	require.Equal(t, len(results), len(res1), "Different count of items")

	return results
}

func UpdateField(t *testing.T, fieldName string, values interface{}) (items []interface{}) {
	return UpdateItemField(t, fieldName, values, false)
}

func UpdateObject(t *testing.T, fieldName string, values interface{}) (items []interface{}) {
	return UpdateItemField(t, fieldName, values, true)
}

func CheckUpdateWithExpressions1(t *testing.T) {
	res1, err := DB.Query(fieldsUpdateNs).SetExpression("size", "((7+8)*(10-5)*2)/25").Update().AllowUnsafe(true).FetchAll()
	require.NoError(t, err)
	require.NotEqual(t, len(res1), 0, "No items updated")

	results, err := DB.Query(fieldsUpdateNs).Exec().AllowUnsafe(true).FetchAll()
	require.NoError(t, err)
	require.NotEqual(t, len(results), 0, "No results found")

	for i := 0; i < len(results); i++ {
		size := results[i].(*TestItemComplexObject).Size
		require.Equal(t, size, 6, "Update of field 'Size' has shown wrong results %d", size)
	}
}

func CheckUpdateWithExpressions2(t *testing.T) {
	res1, err := DB.Query(fieldsUpdateNs).SetExpression("size", "((SERIAL() + 1)*4)/4").Update().AllowUnsafe(true).FetchAll()
	require.NoError(t, err)
	require.NotEqual(t, len(res1), 0, "No items updated")

	results, err := DB.Query(fieldsUpdateNs).Exec().AllowUnsafe(true).FetchAll()
	require.NoError(t, err)
	require.NotEqual(t, len(results), 0, "No results found")

	for i := 0; i < len(results); i++ {
		size := results[i].(*TestItemComplexObject).Size
		require.Equal(t, size, (((i + 2) * 4) / 4), "Update of field 'Size' has shown wrong results %d", size)
	}
}

func checkExtraFieldForEquality(t *testing.T, items []interface{}, val string) {
	for i := 0; i < len(items); i++ {
		obj := items[i].(*TestItemComplexObject).MainObj.Main
		require.Equal(t, obj.Extra, val, "Field 'extra' has a wrong value = %s", obj.Extra, val)
	}
}

func CheckFieldsDrop(t *testing.T) {
	const errorMessage = "Field '%s' was not removed from item"

	results := DropField(t, "numbers")
	require.False(t, CheckIfFieldInJSON(t, DB.Query(fieldsUpdateNs).Where("is_enabled", reindexer.EQ, true), "numbers"), errorMessage, "Numbers")

	for i := 0; i < len(results); i++ {
		obj := results[i].(*TestItemComplexObject)
		require.Nil(t, obj.Numbers, "Field 'Numbers' {%#v, %d} was not removed from item", obj.Numbers, len(obj.Numbers))
	}

	results2 := UpdateField(t, "main_obj.main.extra", "best value")
	checkExtraFieldForEquality(t, results2, "best value")

	results3 := DropField(t, "main_obj.main.extra")
	require.False(t, CheckIfFieldInJSON(t, DB.Query(fieldsUpdateNs).Where("is_enabled", reindexer.EQ, true), "main_obj.main.extra"), errorMessage, "main_obj.main.extra")

	checkExtraFieldForEquality(t, results3, "")
}

func CheckUpdateObject(t *testing.T) {
	for i := 0; i < 5; i++ {
		itemObj := randTestItemObject()
		results := UpdateField(t, "main_obj", itemObj)
		for i := 0; i < len(results); i++ {
			changedField := results[i].(*TestItemComplexObject).MainObj
			retObjJSON, err := json.Marshal(changedField)
			require.NoError(t, err)

			obj := testItemObject{}
			json.Unmarshal(retObjJSON, &obj)
			require.Equal(t, obj, itemObj)
		}
	}
}

type NestedObject struct {
	NestedID    int
	Description string
	IsFree      bool
}
type SmallObject struct {
	ID     int
	Name   string
	Price  float64
	Nested NestedObject
	Bonus  int
}

func CheckUpdateObject2(t *testing.T) {
	obj := randTestItemObject()
	objJson, err := json.Marshal(obj)
	require.NoError(t, err)
	UpdateObject(t, "main_obj", objJson)
	require.True(t, CheckIfFieldInJSON(t, DB.Query(fieldsUpdateNs).Where("is_enabled", reindexer.EQ, true), string(objJson)))
}

func CheckUpdateArrayOfObjects(t *testing.T, length int) {
	objects := make([]testItemObject, length)
	for i := 0; i < len(objects); i++ {
		objects[i] = randTestItemObject()
	}
	results := UpdateObject(t, "objects", objects)
	for i := 0; i < len(results); i++ {
		newObjects := results[i].(*TestItemComplexObject).Objects
		require.NotNil(t, newObjects)

		retObjJSON, err := json.Marshal(newObjects)
		require.NoError(t, err)

		var objects1 []testItemObject
		json.Unmarshal(retObjJSON, &objects1)
		require.Equal(t, objects, objects1)
	}
}

func CheckAddObject(t *testing.T) {
	obj := SmallObject{0, "new", 1000, NestedObject{0, "great", false}, 77}
	results := UpdateField(t, "optional", obj)
	for i := 0; i < len(results); i++ {
		newField := results[i].(*TestItemComplexObject).Optional
		require.NotNil(t, newField)
		retObjJSON, err := json.Marshal(newField)
		require.NoError(t, err)

		obj1 := SmallObject{}
		json.Unmarshal(retObjJSON, &obj1)
		require.Equal(t, obj1, obj)
	}
}

func CheckAddObject2(t *testing.T) {
	newClient := make(map[string]interface{})
	newClient["id"] = 1
	newClient["name"] = "Donald Trump"
	newClient["address"] = "Washington DC"
	nested := make(map[string]interface{})
	nested["nested_id"] = 100
	nested["description"] = "weird"
	nested["price"] = 699
	newClient["nested"] = nested
	UpdateObject(t, "optional", newClient)
	objJson, err := json.Marshal(newClient)
	require.NoError(t, err)
	require.True(t, CheckIfFieldInJSON(t, DB.Query(fieldsUpdateNs).Where("is_enabled", reindexer.EQ, true), string(objJson)))
}

func CheckIndexedFieldUpdate(t *testing.T) {
	results := UpdateField(t, "main_obj.year", 2007)
	for i := 0; i < len(results); i++ {
		year := results[i].(*TestItemComplexObject).MainObj.Year
		require.Equal(t, year, 2007, "Update of field 'main_obj.year' has shown wrong results %d", year)
	}
}

func CheckNonIndexedFieldUpdate(t *testing.T) {
	results := UpdateField(t, "size", 45)
	for i := 0; i < len(results); i++ {
		size := results[i].(*TestItemComplexObject).Size
		require.Equal(t, size, 45, "Update of field 'size' has shown wrong results %d", size)
	}
}

func CheckNonIndexedArrayFieldUpdate(t *testing.T) {
	newAnimals := make([]string, 0, 20)
	for i := 0; i < 20; i++ {
		newAnimals = append(newAnimals, randString())
	}
	results := UpdateField(t, "animals", newAnimals)
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
		require.True(t, equal, "Update of field 'animals' has shown wrong results")
	}
}

func CheckIndexedArrayFieldUpdate(t *testing.T) {
	newEmployees := make([]string, 0, 20)
	for i := 0; i < 20; i++ {
		newEmployees = append(newEmployees, randString())
	}
	results := UpdateField(t, "employees", newEmployees)
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
		require.True(t, equal, "Update of field 'employees' has shown wrong results")
	}
}

func CheckNestedFieldUpdate(t *testing.T) {
	results := UpdateField(t, "main_obj.main.first", 777)
	for i := 0; i < len(results); i++ {
		first := results[i].(*TestItemComplexObject).MainObj.Main.First
		require.Equal(t, first, 777, "Update of field 'nested_obj.main.first' has shown wrong results %d", first)
	}
}

func CheckNestedFieldUpdate2(t *testing.T) {
	results := UpdateField(t, "main_obj.main.second", "bingo!")
	for i := 0; i < len(results); i++ {
		second := results[i].(*TestItemComplexObject).MainObj.Main.Second
		require.Equal(t, second, "bingo!", "Update of field 'nested_obj.main.second' has shown wrong results %s", second)
	}
}

func CheckAddSimpleFields(t *testing.T) {
	results := UpdateField(t, "optional", "new field")
	for i := 0; i < len(results); i++ {
		optional := results[i].(*TestItemComplexObject).Optional
		require.Equal(t, optional, "new field", "Adding of field 'nested_obj.main.first' went wrong: %s", optional)
	}

	results2 := UpdateField(t, "main_obj.bonus", 777)
	for i := 0; i < len(results2); i++ {
		bonus := results2[i].(*TestItemComplexObject).MainObj.Bonus
		require.Equal(t, bonus, 777, "Adding of field 'nested_obj.main.first' went wrong: %d", bonus)
	}

	results3 := UpdateField(t, "main_obj.main.extra", "new nested field")
	for i := 0; i < len(results3); i++ {
		extra := results3[i].(*TestItemComplexObject).MainObj.Main.Extra
		require.Equal(t, extra, "new nested field", "Adding of field 'nested_obj.main.first' went wrong: %s", extra)
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

func CheckIfFieldInJSON(t *testing.T, q *queryTest, field string) bool {
	jsonIter := q.ExecToJson()
	for jsonIter.Next() {
		jsonB := jsonIter.JSON()
		var data map[string]interface{}
		require.NoError(t, json.Unmarshal(jsonB, &data))
		json := string(jsonB[:])
		if strings.Contains(json, field) {
			return true
		}
	}
	return false
}

func CheckAddComplexField(t *testing.T, path string, subfields []string) {
	UpdateField(t, path, "extra value")
	jsonIter := DB.Query(fieldsUpdateNs).Where("is_enabled", reindexer.EQ, true).ExecToJson()
	for jsonIter.Next() {
		jsonB := jsonIter.JSON()
		var data map[string]interface{}
		require.NoError(t, json.Unmarshal(jsonB, &data))
		if hasJSONPath(subfields, data) == false {
			fmt.Println(string(jsonB[:]))
			fmt.Printf("Adding of field '%s' went wrong\n", path)
		}
	}
}

func FillTestItemsForInsertUpdate(t *testing.T) {
	tx := newTestTx(DB, "test_items_insert_update")

	for _, item := range checkInsertUpdateExistsData {
		require.NoError(t, tx.Insert(item))
	}
	require.Equal(t, tx.MustCommit(), len(checkInsertUpdateExistsData), "Could not commit testSortModeDataCustomSource")
}

func CheckTestItemsInsertUpdate(t *testing.T) {
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

	preceptsMap := map[string][]string{
		"WITH PRECEPTS":    {"year=now(sec)"},
		"WITHOUT PRECEPTS": {},
	}

	for actionName, doAction := range actionMap {
		for preceptsText, precepts := range preceptsMap {
			for exists, dataset := range existsMap {
				t.Run(fmt.Sprintf("%s %s ITEMS %s", actionName, exists, preceptsText), func(t *testing.T) {
					for _, item := range dataset {
						var originalYear int = item.Year
						cnt, err := doAction("test_items_insert_update", item, precepts...)
						require.NoError(t, err)

						act := actionName + " " + exists

						switch act {
						case "INSERT EXISTING":
							require.Equal(t, cnt, 0, "Expected affected items count = 0, but got %d\n Item: %+v", cnt, item)
						case "INSERT NON EXISTING":
							require.Equal(t, cnt, 1, "Expected affected items count = 1, but got %d\n Item: %+v", cnt, item)
							require.False(t, preceptsText == "WITH PRECEPTS" && item.Year == originalYear,
								"Item has not been updated by Insert with precepts. Item: %+v", item)
							// need to update data before 'UPDATE NON EXISTING'
							updateNonExistsData(existsMap["NON EXISTING"])

						case "UPDATE EXISTING":
							require.Equal(t, cnt, 1, "Expected affected items count = 1, but got %d\n Item: %+v", cnt, item)
							require.False(t, preceptsText == "WITH PRECEPTS" && item.Year == originalYear,
								"Item has not been updated by Insert with precepts. Item: %+v", item)
							// need to update data before 'UPDATE NON EXISTING'
							updateNonExistsData(existsMap["NON EXISTING"])

						case "UPDATE NON EXISTING":
							require.Equal(t, cnt, 0, "Expected affected items count = 0, but got %d\n Item: %+v", cnt, item)

						}
					}
				})
			}
		}
	}
}

func checkItemsCount(t *testing.T, nsName string, expectedCount int) {
	results, err := DB.Query(nsName).Exec().AllowUnsafe(true).FetchAll()
	require.NoError(t, err)
	require.Equal(t, len(results), expectedCount, "Expected %d items, but got %d", expectedCount, len(results))
}

func TestTruncateNamespace(t *testing.T) {
	const itemsCount = 1000

	nsOpts := reindexer.DefaultNamespaceOptions()
	require.NoError(t, DB.OpenNamespace(truncateNs, nsOpts, TestItemComplexObject{}))

	for i := 0; i < itemsCount; i++ {
		_, err := DB.Insert(truncateNs, newTestItemComplexObject(i))
		require.NoError(t, err)
	}
	checkItemsCount(t, truncateNs, itemsCount)

	require.NoError(t, DB.TruncateNamespace(truncateNs))
	checkItemsCount(t, truncateNs, 0)

	require.NoError(t, DB.CloseNamespace(truncateNs))
	require.NoError(t, DB.OpenNamespace(truncateNs, nsOpts, TestItemComplexObject{}))
	checkItemsCount(t, truncateNs, 0)

	for i := 0; i < itemsCount; i++ {
		_, err := DB.Insert(truncateNs, newTestItemComplexObject(i))
		require.NoError(t, err)
	}
	checkItemsCount(t, truncateNs, itemsCount)

	require.NoError(t, DB.CloseNamespace(truncateNs))
	require.NoError(t, DB.OpenNamespace(truncateNs, nsOpts, TestItemComplexObject{}))
	checkItemsCount(t, truncateNs, itemsCount)

	require.NoError(t, DB.TruncateNamespace(truncateNs))
	checkItemsCount(t, truncateNs, 0)

	require.NoError(t, DB.CloseNamespace(truncateNs))
	require.NoError(t, DB.OpenNamespace(truncateNs, nsOpts, TestItemComplexObject{}))
	checkItemsCount(t, truncateNs, 0)
}
