package reindexer

import (
	"fmt"
	"math/rand"
	"reflect"
	"strconv"
	"testing"

	"github.com/restream/reindexer/v5"
	"github.com/restream/reindexer/v5/cjson"
	"github.com/stretchr/testify/require"
)

type TestItemHuge struct {
	ID   int `reindex:"id,,pk"`
	Data []int
}

const (
	testItemsHugeNs = "test_items_huge"
	// shouldn't be in init()
	testItemsWideNs = "test_items_wide"
)

func init() {
	tnamespaces[testItemsHugeNs] = TestItemHuge{}
}

func FillTestItemHuge(start int, count int) {
	tx := newTestTx(DB, testItemsHugeNs)

	for i := 0; i < count; i++ {
		dataCount := i * 1024 * 16
		item := &TestItemHuge{
			ID:   mkID(start + i),
			Data: randIntArr(dataCount, 10000, 50),
		}

		if err := tx.Upsert(item); err != nil {
			panic(err)
		}
	}
	tx.MustCommit()
}

func FillTestItemWide(ns string, typ reflect.Type, start int, count int) {
	tx := newTestTx(DB, ns)
	for i := 0; i < count; i++ {
		item := MakeTestItemWide(typ, i+start)
		if err := tx.Upsert(item); err != nil {
			panic(err)
		}
	}
	tx.MustCommit()
}

func MakeTestItemWide(typ reflect.Type, ID int) interface{} {
	item := reflect.New(typ).Elem()
	item.Field(0).SetInt(int64(ID))
	for j := 1; j < typ.NumField(); j++ {
		item.Field(j).SetString(typ.Field(j).Name + "_value")
	}
	return item.Interface()
}

func buildWideItemsExpectedExplain(initialIndexes int) []expectedExplain {
	resExplain := []expectedExplain{
		{
			Field:     "id",
			FieldType: "indexed",
			Method:    "index",
			Keys:      1,
			Matched:   1,
		},
	}
	// Part of the expected explain set manually and may be changed. Result depends on the internal core's substitutions ordering
	resExplain = append(resExplain, expectedExplain{
		Field:       "field_6",
		FieldType:   "indexed",
		Method:      "scan",
		Keys:        0,
		Matched:     1,
		Comparators: 1,
	})
	resExplain = append(resExplain, expectedExplain{
		Field:     "field_0+field_1+field_2+field_3",
		FieldType: "indexed",
		Method:    "index",
		Keys:      1,
		Matched:   1,
	})
	resExplain = append(resExplain, expectedExplain{
		Field:     "field_4",
		FieldType: "indexed",
		Method:    "index",
		Keys:      1,
		Matched:   1,
	})
	resExplain = append(resExplain, expectedExplain{
		Field:     "field_5",
		FieldType: "indexed",
		Method:    "index",
		Keys:      1,
		Matched:   1,
	})
	resExplain = append(resExplain, expectedExplain{
		Field:     "field_7",
		FieldType: "indexed",
		Method:    "index",
		Keys:      1,
		Matched:   1,
	})
	resExplain = append(resExplain, expectedExplain{
		Field:     "field_8",
		FieldType: "indexed",
		Method:    "index",
		Keys:      1,
		Matched:   1,
	})
	for i := 9; i < initialIndexes-1; {
		var explainField expectedExplain
		if i%3 == 0 {
			explainField = expectedExplain{
				Field:     fmt.Sprintf("field_%d+field_%d+field_%d+field_%d", i, i+1, i+2, i+3),
				FieldType: "indexed",
				Method:    "index",
				Keys:      1,
				Matched:   1,
			}
			i += 4
		} else {
			explainField = expectedExplain{
				Field:     fmt.Sprintf("field_%d", i),
				FieldType: "indexed",
				Method:    "index",
				Keys:      1,
				Matched:   1,
			}
			i += 1
		}
		resExplain = append(resExplain, explainField)
	}
	return resExplain
}

func TestItemsHuge(t *testing.T) {
	t.Parallel()

	// Fill items by cjson encoder
	FillTestItemHuge(0, 50)
	// get and decode all items by cjson decoder
	newTestQuery(DB, testItemsHugeNs).ExecAndVerify(t)
}

func TestItemWide(t *testing.T) {
	// Check basic select with a lot of indexes in item and in query
	const ns = testItemsWideNs
	const maxIndexes = cjson.MaxIndexes - 1

	fields := make([]reflect.StructField, 0, maxIndexes)
	fields = append(fields, reflect.StructField{
		Name: "ID",
		Type: reflect.TypeOf(int(0)),
		Tag:  `reindex:"id,,pk"`,
	})
	for i := 0; i < maxIndexes-1; i++ {
		fieldName := "Field" + strconv.Itoa(i)
		useStoreIndex := i%3 == 0
		var tag string
		if useStoreIndex {
			tag = fmt.Sprintf(`reindex:"field_%d,-", json:"field_%d"`, i, i)
		} else {
			tag = fmt.Sprintf(`reindex:"field_%d", json:"field_%d"`, i, i)
		}
		fields = append(fields, reflect.StructField{
			Name: fieldName,
			Type: reflect.TypeOf(string("")),
			Tag:  reflect.StructTag(tag),
		})
	}
	typ := reflect.StructOf(fields)

	DB.OpenNamespace(ns, reindexer.DefaultNamespaceOptions(), reflect.New(typ).Elem().Interface())
	FillTestItemWide(ns, typ, 0, 100)

	t.Run("simple select with 'maxIndexes' condition for each index (maxIndexes conditions in total)", func(t *testing.T) {
		const targetItemID = 58
		targetItem := MakeTestItemWide(typ, targetItemID)
		q := DB.Query(ns).Where("id", reindexer.EQ, targetItemID)
		for i := 1; i < maxIndexes; i++ {
			q.Where("field_"+strconv.Itoa(i-1), reindexer.EQ, reflect.ValueOf(targetItem).Field(i).String())
		}
		items, err := q.MustExec(t).FetchAll()
		require.NoError(t, err)
		require.Equal(t, len(items), 1)
		reflect.DeepEqual(items[0], targetItem)
	})

	t.Run("simple select with 'maxIndexes' condition for each index and composite indexes substituition", func(t *testing.T) {
		const targetItemID = 93
		// Create composite indexes: 4 fields in each, 1 field from N'th index overlaps 1 field from N+1-th index
		for i := 0; i < maxIndexes-4; i += 3 {
			indexType := "tree"
			if rand.Uint32()%4 == 0 {
				indexType = "hash"
			}
			indexDef := reindexer.IndexDef{
				Name:      fmt.Sprintf("field_%d+field_%d+field_%d+field_%d", i, i+1, i+2, i+3),
				JSONPaths: []string{"field_" + strconv.Itoa(i), "field_" + strconv.Itoa(i+1), "field_" + strconv.Itoa(i+2), "field_" + strconv.Itoa(i+3)},
				IndexType: indexType,
				FieldType: "composite",
			}
			err := DB.AddIndex(ns, indexDef)
			require.NoError(t, err)
		}
		FillTestItemWide(ns, typ, 100, 100)

		targetItem := MakeTestItemWide(typ, targetItemID)
		q := DB.Query(ns).Where("id", reindexer.EQ, targetItemID).Debug(5).Explain()
		for i := 1; i < maxIndexes; i++ {
			q.Where("field_"+strconv.Itoa(i-1), reindexer.EQ, reflect.ValueOf(targetItem).Field(i).String())
		}
		it := q.MustExec(t)
		explainRes, err := it.GetExplainResults()
		require.NoError(t, err)

		items, err := it.FetchAll()
		require.NoError(t, err)
		require.Equal(t, len(items), 1)
		reflect.DeepEqual(items[0], targetItem)

		resExplain := buildWideItemsExpectedExplain(maxIndexes)
		checkExplain(t, explainRes.Selectors, resExplain, "")
	})
}
