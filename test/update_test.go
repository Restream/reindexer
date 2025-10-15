package reindexer

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"

	"github.com/restream/reindexer/v5"
	"github.com/stretchr/testify/require"
)

type ItemWithSparseArray struct {
	ID    int64   `json:"id" reindex:"id,hash,pk"`
	Array []int64 `json:"array_idx" reindex:"array_idx,hash,sparse"`
}

type ItemWithHeteroArrays struct {
	ID       int64         `json:"id" reindex:"id,hash,pk"`
	ArrayIdx []int64       `json:"array_idx" reindex:"array_idx,hash,sparse"`
	ArrayNon []interface{} `json:"array_hetero"`
}

type Nested struct {
	Field int `json:"field" reindex:"array_idx,-"`
}

type ItemWithHeteroArraysObj struct {
	ID       int64         `json:"id" reindex:"id,hash,pk"`
	Nested   []Nested      `json:"nested"`
	ArrayNon []interface{} `json:"array_nonidx"`
}

type testInnerObject struct {
	First  int      `reindex:"first" json:"first"`
	Second string   `reindex:"second" json:"second"`
	Third  []int    `reindex:"third" json:"third"`
	Fourth []string `json:"fourth"`
	Extra  string   `json:"extra,omitempty"`
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
	Vec       []float32        `reindex:"vec,hnsw,m=16,ef_construction=200,metric=inner_product,dimension=16"`
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

const (
	testFieldsUpdateNs          = "test_items_fields_update"
	testTruncateNs              = "test_truncate"
	testRemoveItemsNs           = "test_remove_items"
	testItemsInsertUpdate       = "test_items_insert_update"
	testSparseArrItemNs         = "test_sparse_array_update"
	testUpdateWithExpressionsNs = "test_expressions_updates"
	testUpdateHeteroArraysNs    = "test_heterogeneous_array_updates"
	testUpdateHeteroArraysObjNs = "test_heterogeneous_objects_array_updates"
)

func init() {
	tnamespaces[testFieldsUpdateNs] = TestItemComplexObject{}
	tnamespaces[testTruncateNs] = TestItemComplexObject{}
	tnamespaces[testRemoveItemsNs] = testDummyObject{}
	tnamespaces[testItemsInsertUpdate] = TestItemSimple{}
	tnamespaces[testSparseArrItemNs] = ItemWithSparseArray{}
	tnamespaces[testUpdateWithExpressionsNs] = ItemWithSparseArray{}
	tnamespaces[testUpdateHeteroArraysNs] = ItemWithHeteroArrays{}
	tnamespaces[testUpdateHeteroArraysObjNs] = ItemWithHeteroArraysObj{}
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

func randInnerObject() []testInnerObject {
	arraySize := 10
	third := make([]int, 0, arraySize)
	fourth := make([]string, 0, arraySize)
	for i := 0; i < arraySize; i++ {
		third = append(third, i)
		fourth = append(fourth, "ALMOST EMPTY "+strconv.Itoa(i))
	}
	innerObjectsCnt := rand.Int()%20 + 1
	innerObjects := make([]testInnerObject, 0, innerObjectsCnt)
	for i := 0; i < innerObjectsCnt; i++ {
		innerObjects = append(innerObjects, testInnerObject{
			First:  rand.Int() % 1000,
			Second: randString(),
			Third:  third,
			Fourth: fourth,
		})
	}
	return innerObjects
}

func randTestItemObject() testItemObject {
	arraySize := 10
	main := testInnerObject{First: rand.Int() % 1000, Second: randString()}
	for i := 0; i < arraySize; i++ {
		main.Third = append(main.Third, i)
		main.Fourth = append(main.Fourth, "ALMOST EMPTY "+strconv.Itoa(i))
	}
	return testItemObject{
		Name:   randString(),
		Age:    rand.Int()%60 + 10,
		Year:   rand.Int() % 2019,
		Price:  rand.Int63() % 100000,
		Main:   main,
		Nested: randInnerObject(),
	}
}

func newTestItemComplexObject(id int) *TestItemComplexObject {
	arraySize := 10
	main := testInnerObject{First: rand.Int() % 1000, Second: randString()}
	for i := 0; i < arraySize; i++ {
		main.Third = append(main.Third, i)
		main.Fourth = append(main.Fourth, "ALMOST EMPTY "+strconv.Itoa(i))
	}
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
		Vec:       randVect(16),
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
			Main:   main,
			Nested: randInnerObject(),
		},
	}
}

func fillTestItemsForInsertUpdate(t *testing.T, ns string) {
	tx := newTestTx(DB, ns)

	for _, item := range checkInsertUpdateExistsData {
		require.NoError(t, tx.Insert(item))
	}
	require.Equal(t, tx.MustCommit(), len(checkInsertUpdateExistsData), "Could not commit testSortModeDataCustomSource")
}

func selectAndFetchAll(t *testing.T, ns string) {
	_, err := DBD.Query(ns).Exec().FetchAll()
	require.NoError(t, err)
}

func FillAndDeleteDummyItems(t *testing.T) {
	for i := 0; i < 10; i++ {
		require.NoError(t, DB.Upsert(testRemoveItemsNs, testDummyObject{
			ID:   i,
			Name: randString(),
		}))
	}

	count, err := DB.Query(testRemoveItemsNs).WhereInt("id", reindexer.LT, 3).Delete()
	require.NoError(t, err)
	require.Equal(t, count, 3, "Delete failed")
}

func DropField(t *testing.T, fieldName string) (items []interface{}) {
	res1, err := DB.Query(testFieldsUpdateNs).Where("is_enabled", reindexer.EQ, true).Drop(fieldName).Update().FetchAll()
	require.NoError(t, err)
	require.NotEqual(t, len(res1), 0, "No items updated")

	results, err := DB.Query(testFieldsUpdateNs).Where("is_enabled", reindexer.EQ, true).Exec(t).AllowUnsafe(true).FetchAll()
	require.NoError(t, err)
	require.Equal(t, len(results), len(res1), "Different count of items")

	return results
}

func UpdateItemField(t *testing.T, fieldName string, values interface{}, jsonObject bool) (items []interface{}) {
	var q *queryTest
	if jsonObject {
		_, ok := values.([]byte)
		require.True(t, ok, "'%v' is not JSON", values)
		q = DB.Query(testFieldsUpdateNs).Where("is_enabled", reindexer.EQ, true).SetObject(fieldName, values)
	} else {
		q = DB.Query(testFieldsUpdateNs).Where("is_enabled", reindexer.EQ, true).Set(fieldName, values)
	}

	res1, err := q.Update().AllowUnsafe(true).FetchAll()
	require.NoError(t, err)
	require.NotEqual(t, len(res1), 0, "No items updated")

	results, err := DB.Query(testFieldsUpdateNs).Where("is_enabled", reindexer.EQ, true).Exec(t).AllowUnsafe(true).FetchAll()
	require.NoError(t, err)
	require.Equal(t, len(results), len(res1), "Different count of items")

	return results
}

func UpdateField(t *testing.T, fieldName string, values interface{}) (items []interface{}) {
	return UpdateItemField(t, fieldName, values, false)
}

func UpdateObjectJSON(t *testing.T, fieldName string, json []uint8) (items []interface{}) {
	return UpdateItemField(t, fieldName, json, true)
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

func CheckUpdateWithExpressions1(t *testing.T) {
	res1, err := DB.Query(testFieldsUpdateNs).SetExpression("size", "((7+8)*(10-5)*2)/25").Update().AllowUnsafe(true).FetchAll()
	require.NoError(t, err)
	require.NotEqual(t, len(res1), 0, "No items updated")

	results, err := DB.Query(testFieldsUpdateNs).Exec(t).AllowUnsafe(true).FetchAll()
	require.NoError(t, err)
	require.NotEqual(t, len(results), 0, "No results found")

	for i := 0; i < len(results); i++ {
		size := results[i].(*TestItemComplexObject).Size
		require.Equal(t, size, 6, "Update of field 'Size' has shown wrong results %d", size)
	}
}

func CheckUpdateWithExpressions2(t *testing.T) {
	res1, err := DB.Query(testFieldsUpdateNs).SetExpression("size", "((SERIAL() + 1)*4)/4").Update().AllowUnsafe(true).FetchAll()
	require.NoError(t, err)
	require.NotEqual(t, len(res1), 0, "No items updated")

	results, err := DB.Query(testFieldsUpdateNs).Exec(t).AllowUnsafe(true).FetchAll()
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
	require.False(t, CheckIfFieldInJSON(t, DB.Query(testFieldsUpdateNs).Where("is_enabled", reindexer.EQ, true), "numbers"), errorMessage, "Numbers")

	for i := 0; i < len(results); i++ {
		obj := results[i].(*TestItemComplexObject)
		require.Nil(t, obj.Numbers, "Field 'Numbers' {%#v, %d} was not removed from item", obj.Numbers, len(obj.Numbers))
	}

	results2 := UpdateField(t, "main_obj.main.extra", "best value")
	checkExtraFieldForEquality(t, results2, "best value")

	results3 := DropField(t, "main_obj.main.extra")
	require.False(t, CheckIfFieldInJSON(t, DB.Query(testFieldsUpdateNs).Where("is_enabled", reindexer.EQ, true), "main_obj.main.extra"), errorMessage, "main_obj.main.extra")

	checkExtraFieldForEquality(t, results3, "")

	/*results4 := DropField(t, "main_obj.main.third[0]") // TODO #1218
	for i := 0; i < len(results4); i++ {
		require.Equal(t, len(results4[i].(*TestItemComplexObject).MainObj.Main.Third), 9)
	}

	results5 := DropField(t, "main_obj.main.third[*]")
	for i := 0; i < len(results5); i++ {
		require.Equal(t, len(results4[i].(*TestItemComplexObject).MainObj.Main.Third), 0)
	}*/

	results6 := DropField(t, "main_obj.main.fourth[0]")
	for i := 0; i < len(results6); i++ {
		require.Equal(t, len(results6[i].(*TestItemComplexObject).MainObj.Main.Fourth), 9, errorMessage, "main_obj.main.fourth[0]")
	}

	results7 := DropField(t, "main_obj.main.fourth[*]")
	for i := 0; i < len(results7); i++ {
		require.Equal(t, len(results7[i].(*TestItemComplexObject).MainObj.Main.Fourth), 0, errorMessage, "main_obj.main.fourth[*]")
	}

	results8 := DropField(t, "objects[0].nested[0].fourth[0]")
	for i := 0; i < len(results8); i++ {
		require.Equal(t, len(results8[i].(*TestItemComplexObject).Objects[0].Nested[0].Fourth), 9, errorMessage, "objects[0].nested[0].fourth[0]")
	}

	results9 := DropField(t, "objects[0].nested[0].fourth[*]")
	for i := 0; i < len(results9); i++ {
		require.Equal(t, len(results9[i].(*TestItemComplexObject).Objects[0].Nested[0].Fourth), 0, errorMessage, "objects[0].nested[0].fourth[*]")
	}
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

func CheckUpdateObject2(t *testing.T) {
	obj := randTestItemObject()
	objJson, err := json.Marshal(obj)
	require.NoError(t, err)
	UpdateObjectJSON(t, "main_obj", objJson)
	require.True(t, CheckIfFieldInJSON(t, DB.Query(testFieldsUpdateNs).Where("is_enabled", reindexer.EQ, true), string(objJson)))
}

// Update 1 element of array objects.nested and make
// sure it stores correct values after update
func CheckUpdateArrayObject(t *testing.T) {
	// Generate new instance of testInnerObject
	arraySize := 10
	third := make([]int, 0, arraySize)
	fourth := make([]string, 0, arraySize)
	for i := 0; i < arraySize; i++ {
		third = append(third, i)
		fourth = append(fourth, "not empty")
	}
	obj := testInnerObject{
		First:  7777,
		Second: "updated",
		Third:  third,
		Fourth: fourth,
	}

	// Update objects[0].nested[0] with new value (set as JSON)
	objJson, err := json.Marshal(obj)
	require.NoError(t, err)
	results := UpdateObjectJSON(t, "objects[0].nested[0]", objJson)

	for i := 0; i < len(results); i++ {
		objects := results[i].(*TestItemComplexObject).Objects
		for j := 0; j < len(objects); j++ {
			for k := 0; k < len(objects[j].Nested); k++ {
				// Make sure first values of objects.nested are updated,
				// whereas the rest remains the same
				if k == 0 && j == 0 {
					require.Equal(t, objects[j].Nested[k].First, 7777)
					require.Equal(t, objects[j].Nested[k].Second, "updated")
				} else {
					require.Equal(t, objects[j].Nested[k].First != 7777, true)
					require.Equal(t, objects[j].Nested[k].Second != "updated", true)
				}
			}
		}
	}
	require.True(t, CheckIfFieldInJSON(t, DB.Query(testFieldsUpdateNs).Where("is_enabled", reindexer.EQ, true), string(objJson)))
}

// Check of simultaneous update of 2 fields: object field + indexed field
func CheckSimultaneousUpdateOfFields(t *testing.T) {
	// Generate new value for the object field
	obj := randTestItemObject()
	objJson, err := json.Marshal(obj)
	require.NoError(t, err)

	// Set this object value + new value of object field
	q := DB.Query(testFieldsUpdateNs).Where("is_enabled", reindexer.EQ, true).Set("code", 999).SetObject("main_obj", objJson)

	// Make sure Update query went well
	res, err := q.Update().AllowUnsafe(true).FetchAll()
	require.NoError(t, err)
	require.NotEqual(t, len(res), 0, "No items updated")

	// Fetch data for the same query
	results, err := DB.Query(testFieldsUpdateNs).Where("is_enabled", reindexer.EQ, true).Exec(t).AllowUnsafe(true).FetchAll()
	require.NoError(t, err)
	require.Equal(t, len(results), len(res), "Different count of items")

	// Make sure object field 'mainObj' and indexed field 'Code'
	// both have correct values
	for i := 0; i < len(results); i++ {
		mainObj := results[i].(*TestItemComplexObject).MainObj

		retObjJSON, err := json.Marshal(mainObj)
		require.NoError(t, err)

		currObj := testItemObject{}
		json.Unmarshal(retObjJSON, &currObj)
		require.Equal(t, currObj, obj)

		code := results[i].(*TestItemComplexObject).Code
		require.Equal(t, code, 999)
	}
}

func CheckUpdateArrayOfObjects(t *testing.T, length int) {
	objects := make([]testItemObject, length)
	for i := 0; i < len(objects); i++ {
		objects[i] = randTestItemObject()
	}
	results := UpdateField(t, "objects", objects)
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
	UpdateField(t, "optional", newClient)
	objJson, err := json.Marshal(newClient)
	require.NoError(t, err)
	require.True(t, CheckIfFieldInJSON(t, DB.Query(testFieldsUpdateNs).Where("is_enabled", reindexer.EQ, true), string(objJson)))
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

func CheckNonIndexedEmptyArrayFieldUpdate(t *testing.T) {
	newAnimals := make([]string, 0)
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

func CheckNonIndexedArrayWithSingleElementFieldUpdate(t *testing.T) {
	newAnimals := make([]string, 0, 1)
	for i := 0; i < 1; i++ {
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

// Update all items of array objects.nested.third
// and make sure it has correct value
func CheckIndexedArrayItemUpdate1(t *testing.T) {
	// Update array and set all items to 8888
	results := UpdateField(t, "objects[*].nested[*].third[*]", 8888)
	for i := 0; i < len(results); i++ {
		array := results[i].(*TestItemComplexObject).Objects
		for j := 0; j < len(array); j++ {
			for k := 0; k < len(array[j].Nested); k++ {
				// check if array size remains the same
				equal := (len(array[j].Nested[k].Third) == 10)
				if equal {
					for l := 0; l < len(array[j].Nested[k].Third); l++ {
						// make sure each element is equal to 8888
						equal = (array[j].Nested[k].Third[l] == 8888)
						if !equal {
							fmt.Printf("%+v\n", array[j].Nested[k].Third)
							break
						}
					}
				}
				require.True(t, equal, "Update of field 'objects[*].nested[*].third[*]' has shown wrong results")
			}
		}
	}
}

// Update one item of array objects.nested.third
// and make sure it has correct value
func CheckIndexedArrayItemUpdate2(t *testing.T) {
	// Set objects[0].nested[0].third[1] to 1111
	results := UpdateField(t, "objects[0].nested[0].third[1]", 1111)
	for i := 0; i < len(results); i++ {
		array := results[i].(*TestItemComplexObject).Objects
		// Make sure array has correct size
		equal := (len(array[0].Nested[0].Third) == 10)
		if equal {
			for j := 0; j < len(array[0].Nested[0].Third); j++ {
				value := array[0].Nested[0].Third[j]
				// thrid[1] should be equal to 1111, other items
				// should remain the same value
				if j == 1 {
					equal = (value == 1111)
				} else {
					equal = (value == 8888)
				}
				if !equal {
					fmt.Printf("%+v; i = %d\n", value, j)
					break
				}
			}
			require.True(t, equal, "Update of field 'objects[0].nested[0].third[1]' has shown wrong results")
		}
	}
}

// Update one item of string array objects.nested.fourth
// and make sure it has correct value
func CheckNonIndexedArrayItemUpdate1(t *testing.T) {
	// Set objects[*].nested[*].fourth[1] to a new value
	results := UpdateField(t, "objects[*].nested[*].fourth[1]", "best item of array")
	for i := 0; i < len(results); i++ {
		array := results[i].(*TestItemComplexObject).Objects
		for j := 0; j < len(array); j++ {
			for k := 0; k < len(array[j].Nested); k++ {
				equal := (len(array[j].Nested[k].Fourth) == 10)
				if equal {
					// fourth[i] should be set to a new value, all other
					// elements should remain the same value
					for l := 0; l < len(array[j].Nested[k].Fourth); l++ {
						value := array[j].Nested[k].Fourth[l]
						if l == 1 {
							equal = (value == "best item of array")
						} else {
							equal = (value == "ALMOST EMPTY "+strconv.Itoa(l))
						}
						if !equal {
							fmt.Printf("%+v; i = %d\n", value, l)
							break
						}
					}
				}
				require.True(t, equal, "Update of field 'objects[*].nested[*].fourth[1]' has shown wrong results")
			}
		}
	}
}

// Update all items of string array objects.nested.fourth
// and make sure it has correct value
func CheckNonIndexedArrayItemUpdate2(t *testing.T) {
	// Set all items objects.nested.fourth to a new value
	results := UpdateField(t, "objects[*].nested[*].fourth[*]", "we are equal")
	for i := 0; i < len(results); i++ {
		array := results[i].(*TestItemComplexObject).Objects
		for j := 0; j < len(array); j++ {
			for k := 0; k < len(array[j].Nested); k++ {
				// Make sure it's size is correct
				equal := (len(array[j].Nested[k].Fourth) == 10)
				if equal {
					for l := 0; l < len(array[j].Nested[k].Fourth); l++ {
						// Make sure each item is equal to "we are equal"
						equal = (array[j].Nested[k].Fourth[l] == "we are equal")
						if !equal {
							break
						}
					}
				}
				require.True(t, equal, "Update of field 'objects[*].nested[*].fourth[*]' has shown wrong results")
			}
		}
	}
}

// Update one item of string array objects.nested.fourth
// and make sure it has correct value
func CheckNonIndexedArrayItemUpdate3(t *testing.T) {
	// Set "objects[0].nested[0].fourth[0]" to a new value "FIRST ELEMENT"
	results := UpdateField(t, "objects[0].nested[0].fourth[0]", "FIRST ELEMENT")
	for i := 0; i < len(results); i++ {
		array := results[i].(*TestItemComplexObject).Objects
		equal := (len(array[0].Nested[0].Fourth) == 10)
		if equal {
			for j := 0; j < len(array[0].Nested[0].Fourth); j++ {
				// First element of array 'fourth' should be equal to "FIRST ELEMENT",
				// the rest should remain old values
				value := array[0].Nested[0].Fourth[j]
				if j == 0 {
					equal = (value == "FIRST ELEMENT")
				} else {
					equal = (value == "we are equal")
				}
				if !equal {
					fmt.Printf("%+v; i = %d\n", value, j)
					break
				}
			}
		}
		require.True(t, equal, "Update of field 'objects[0].nested[0].fourth[0]' has shown wrong results")
	}
}

// Extend array, add new items to the end
// and make sure after update it stores correct values
func CheckNonIndexedArrayAppend1(t *testing.T) {
	// Add 3 new items to array 'numbers'
	res1, err := DB.Query(testFieldsUpdateNs).SetExpression("numbers", "numbers || [11,22,33]").Update().AllowUnsafe(true).FetchAll()
	require.NoError(t, err)
	require.NotEqual(t, len(res1), 0, "No items updated")

	// Make sure results container is not empty
	results, err := DB.Query(testFieldsUpdateNs).Exec(t).AllowUnsafe(true).FetchAll()
	require.NoError(t, err)
	require.NotEqual(t, len(results), 0, "No results found")

	for i := 0; i < len(results); i++ {
		ok := true
		numbers := results[i].(*TestItemComplexObject).Numbers
		newSize := len(numbers)
		first := newSize - 3
		item := 1
		// Make sure last 3 values of array are equal to [11,22,33]
		for j := first; j < newSize; j++ {
			ok = (numbers[j] == 11*item)
			if !ok {
				fmt.Printf("%+v, %d\n", numbers, j)
				break
			}
			item++
		}
		require.True(t, ok, "Extending of array field 'numbers' has shown wrong results")

	}
}

// Extend array by adding 3 new values to the top
// and make sure it stores correct values after update
func CheckNonIndexedArrayAppend2(t *testing.T) {
	// Add 3 items to the top of 'numbers' array
	res1, err := DB.Query(testFieldsUpdateNs).SetExpression("numbers", "[111,222,333] || numbers").Update().AllowUnsafe(true).FetchAll()
	require.NoError(t, err)
	require.NotEqual(t, len(res1), 0, "No items updated")

	// Make sure results container is not empty
	results, err := DB.Query(testFieldsUpdateNs).Exec(t).AllowUnsafe(true).FetchAll()
	require.NoError(t, err)
	require.NotEqual(t, len(results), 0, "No results found")

	for i := 0; i < len(results); i++ {
		ok := true
		item := 1
		numbers := results[i].(*TestItemComplexObject).Numbers
		// Make sure first 3 items of array are [111,222,333]
		for j := 0; j < 3; j++ {
			ok = (numbers[j] == 111*item)
			if !ok {
				fmt.Printf("%+v, %d\n", numbers, j)
				break
			}
			item++
		}
		require.True(t, ok, "Extending of array field 'numbers' has shown wrong results")

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
	jsonIter := DB.Query(testFieldsUpdateNs).Where("is_enabled", reindexer.EQ, true).ExecToJson()
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

func checkTestItemsInsertUpdate(t *testing.T, ns string) {
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
						cnt, err := doAction(ns, item, precepts...)
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
	results, err := DB.Query(nsName).Exec(t).AllowUnsafe(true).FetchAll()
	require.NoError(t, err)
	require.Equal(t, len(results), expectedCount, "Expected %d items, but got %d", expectedCount, len(results))
}

func TestUpdate(t *testing.T) {
	fillTestItemsForInsertUpdate(t, testItemsInsertUpdate)
	checkTestItemsInsertUpdate(t, testItemsInsertUpdate)
	selectAndFetchAll(t, testItemsInsertUpdate)
}

func TestUpdateFields(t *testing.T) {
	const ns = testFieldsUpdateNs

	for i := 0; i < 1000; i++ {
		require.NoError(t, DB.Upsert(ns, newTestItemComplexObject(i)))
	}

	FillAndDeleteDummyItems(t)

	selectAndFetchAll(t, ns)
	CheckIndexedArrayItemUpdate1(t)
	selectAndFetchAll(t, ns)
	CheckIndexedArrayItemUpdate2(t)
	selectAndFetchAll(t, ns)
	CheckNonIndexedArrayItemUpdate1(t)
	selectAndFetchAll(t, ns)
	CheckNonIndexedArrayItemUpdate2(t)
	selectAndFetchAll(t, ns)
	CheckNonIndexedArrayItemUpdate3(t)
	selectAndFetchAll(t, ns)
	CheckNonIndexedArrayAppend1(t)
	selectAndFetchAll(t, ns)
	CheckNonIndexedArrayAppend2(t)
	selectAndFetchAll(t, ns)
	CheckUpdateArrayObject(t)
	selectAndFetchAll(t, ns)
	CheckFieldsDrop(t)
	selectAndFetchAll(t, ns)
	CheckIndexedFieldUpdate(t)
	selectAndFetchAll(t, ns)
	CheckNonIndexedFieldUpdate(t)
	selectAndFetchAll(t, ns)
	CheckNonIndexedEmptyArrayFieldUpdate(t)
	selectAndFetchAll(t, ns)
	CheckNonIndexedArrayWithSingleElementFieldUpdate(t)
	selectAndFetchAll(t, ns)
	CheckNonIndexedArrayFieldUpdate(t)
	selectAndFetchAll(t, ns)
	CheckIndexedArrayFieldUpdate(t)
	selectAndFetchAll(t, ns)
	CheckUpdateObject(t)
	selectAndFetchAll(t, ns)
	CheckUpdateObject2(t)
	selectAndFetchAll(t, ns)
	CheckAddObject(t)
	selectAndFetchAll(t, ns)
	CheckAddObject2(t)
	selectAndFetchAll(t, ns)
	CheckUpdateArrayOfObjects(t, 10)
	selectAndFetchAll(t, ns)
	CheckUpdateArrayOfObjects(t, 1)
	selectAndFetchAll(t, ns)
	CheckUpdateArrayOfObjects(t, 0)
	selectAndFetchAll(t, ns)
	CheckNestedFieldUpdate(t)
	selectAndFetchAll(t, ns)
	CheckNestedFieldUpdate2(t)
	selectAndFetchAll(t, ns)
	CheckAddSimpleFields(t)
	selectAndFetchAll(t, ns)
	CheckAddComplexField(t, "nested2.nested3.nested4.val", []string{"nested2", "nested3", "nested4", "val"})
	selectAndFetchAll(t, ns)
	CheckAddComplexField(t, "main_obj.main.nested.val", []string{"main_obj", "main", "nested", "val"})
	selectAndFetchAll(t, ns)
	CheckUpdateWithExpressions1(t)
	selectAndFetchAll(t, ns)
	CheckUpdateWithExpressions2(t)
	selectAndFetchAll(t, ns)
}

func TestTruncateNamespace(t *testing.T) {
	const ns = testTruncateNs

	const itemsCount = 1000

	nsOpts := reindexer.DefaultNamespaceOptions()

	for i := 0; i < itemsCount; i++ {
		_, err := DB.Insert(ns, newTestItemComplexObject(i))
		require.NoError(t, err)
	}
	checkItemsCount(t, ns, itemsCount)

	require.NoError(t, DB.TruncateNamespace(ns))
	checkItemsCount(t, ns, 0)

	require.NoError(t, DB.CloseNamespace(ns))
	require.NoError(t, DB.OpenNamespace(ns, nsOpts, TestItemComplexObject{}))
	checkItemsCount(t, ns, 0)

	for i := 0; i < itemsCount; i++ {
		_, err := DB.Insert(ns, newTestItemComplexObject(i))
		require.NoError(t, err)
	}
	checkItemsCount(t, ns, itemsCount)

	require.NoError(t, DB.CloseNamespace(ns))
	require.NoError(t, DB.OpenNamespace(ns, nsOpts, TestItemComplexObject{}))
	checkItemsCount(t, ns, itemsCount)

	require.NoError(t, DB.TruncateNamespace(ns))
	checkItemsCount(t, ns, 0)

	require.NoError(t, DB.CloseNamespace(ns))
	require.NoError(t, DB.OpenNamespace(ns, nsOpts, TestItemComplexObject{}))
	checkItemsCount(t, ns, 0)
}

func TestUpdateSparseArrayIndex(t *testing.T) {
	emptyItem := &ItemWithSparseArray{
		ID: int64(2),
	}

	item := &ItemWithSparseArray{
		ID:    int64(2),
		Array: []int64{1, 2, 3},
	}

	require.NoError(t, DB.Upsert(testSparseArrItemNs, emptyItem))

	require.NoError(t, DB.Upsert(testSparseArrItemNs, emptyItem))
	results := DB.ExecSQL("SELECT * FROM " + testSparseArrItemNs + " WHERE id = 2")
	checkResultItem(t, results, emptyItem)
	selectAndFetchAll(t, testSparseArrItemNs)

	require.NoError(t, DB.Upsert(testSparseArrItemNs, item))
	results = DB.ExecSQL("SELECT * FROM " + testSparseArrItemNs + " WHERE id = 2")
	checkResultItem(t, results, item)
	selectAndFetchAll(t, testSparseArrItemNs)

	require.NoError(t, DB.Upsert(testSparseArrItemNs, emptyItem))
	results = DB.ExecSQL("SELECT * FROM " + testSparseArrItemNs + " WHERE id = 2")
	checkResultItem(t, results, emptyItem)
	selectAndFetchAll(t, testSparseArrItemNs)
}

func TestUpdateExpressionWithArrayRemove(t *testing.T) {
	t.Parallel()

	const ns = testUpdateWithExpressionsNs

	item := &ItemWithSparseArray{ID: 1, Array: []int64{1, 2, 3, 4, 5, 1, 2, 3, 4, 5}}
	require.NoError(t, DB.Upsert(ns, item))

	t.Run("update with array_remove delete elements from array", func(t *testing.T) {
		res_slice, err := DB.Query(ns).SetExpression("array_idx", "array_remove(array_idx, [2, 3]) || [50]").
			Update().FetchAll()
		require.NoError(t, err)
		require.Len(t, res_slice, 1)
		res := res_slice[0].(*ItemWithSparseArray)
		expected := []int64{1, 4, 5, 1, 4, 5, 50}
		require.EqualValues(t, expected, res.Array)
		selectAndFetchAll(t, ns)
	})

	t.Run("update with array_remove_once delete elements from array", func(t *testing.T) {
		res_slice, err := DB.Query(ns).SetExpression("array_idx", "array_remove_once(array_idx, [1, 1, 5])").
			Update().FetchAll()
		require.NoError(t, err)
		require.Len(t, res_slice, 1)
		res := res_slice[0].(*ItemWithSparseArray)
		expected := []int64{4, 4, 5, 50}
		require.EqualValues(t, expected, res.Array)
		selectAndFetchAll(t, ns)
	})

	t.Run("update with array_remove delete by single value (scalar) from array", func(t *testing.T) {
		res_slice, err := DB.Query(ns).SetExpression("array_idx", "array_remove(array_idx, 4)").
			Update().FetchAll()
		require.NoError(t, err)
		require.Len(t, res_slice, 1)
		res := res_slice[0].(*ItemWithSparseArray)
		expected := []int64{5, 50}
		require.EqualValues(t, expected, res.Array)
		selectAndFetchAll(t, ns)
	})

}

func TestUpdateSetHeterogeneousArray(t *testing.T) {
	t.Parallel()

	t.Run("update with heterogeneous array", func(t *testing.T) {
		const ns = testUpdateHeteroArraysNs
		item := &ItemWithHeteroArrays{ID: 1, ArrayIdx: []int64{1, 2, 3}, ArrayNon: []interface{}{3.14, "hi", "bro", 111}}
		require.NoError(t, DB.Upsert(ns, item))

		updateArr := []interface{}{"777", 333, "555"}
		q := DB.Query(ns).Where("id", reindexer.EQ, 1).Set("array_idx", updateArr)
		_, err := q.Update().FetchAll()
		require.NoError(t, err)

		updateArrNon := []interface{}{"whatsup", 111, "bro"}
		q = DB.Query(ns).Where("id", reindexer.EQ, 1).Set("array_hetero", updateArrNon)
		_, err = q.Update().FetchAll()
		require.NoError(t, err)

		selectText := "SELECT * FROM " + ns + " WHERE id = 1"
		arrayIdxExpected := []int64{777, 333, 555}
		expected := &ItemWithHeteroArrays{ID: 1, ArrayIdx: arrayIdxExpected, ArrayNon: updateArrNon}
		checkResultItem(t, DB.ExecSQL(selectText), expected)

		selectAndFetchAll(t, ns)
	})

	t.Run("update with heterogeneous objects array", func(t *testing.T) {
		const ns = testUpdateHeteroArraysObjNs
		item := &ItemWithHeteroArraysObj{
			ID:       1,
			Nested:   []Nested{{Field: 1}, {Field: 2}},
			ArrayNon: []interface{}{"a", map[string]int{"field": 1}, 3},
		}
		require.NoError(t, DB.Upsert(ns, item))

		// indexed arr
		updateIdxArr := []interface{}{map[string]int{"field": 10}, map[string]string{"field": "20"}}
		q := DB.Query(ns).Where("id", reindexer.EQ, 1).SetObject("nested", updateIdxArr)
		_, err := q.Update().FetchAll()
		require.NoError(t, err)

		// nonidx arr
		updateNonidxArr := []interface{}{map[string]int{"field": 111}, map[string]string{"field": "abc"}}
		q = DB.Query(ns).Where("id", reindexer.EQ, 1).SetObject("array_nonidx", updateNonidxArr)
		_, err = q.Update().FetchAll()
		require.NoError(t, err)

		selectText := "SELECT * FROM " + ns + " WHERE id = 1"
		expected := &ItemWithHeteroArraysObj{
			ID:       1,
			Nested:   []Nested{{Field: 10}, {Field: 20}},
			ArrayNon: []interface{}{map[string]interface{}{"field": 111}, map[string]interface{}{"field": "abc"}},
		}
		checkResultItem(t, DB.ExecSQL(selectText), expected)

		selectAndFetchAll(t, ns)
	})
}
