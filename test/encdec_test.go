package reindexer

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/restream/reindexer/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type Actor struct {
	Name string `reindex:"name"`
}

type TestEmbedItem struct {
	Genre int64 `reindex:"genre,tree"`
	Year  int   `reindex:"year,tree"`
}

type TestNest struct {
	Name string
	Age  int `reindex:"age,hash"`
}

type (
	TestCustomBytes []byte

	TestCustomString      string
	TestCustomStrings     []TestCustomString
	TestCustomStringsPtrs []*TestCustomString

	TestCustomInt64  int64
	TestCustomInts64 []TestCustomInt64

	TestCustomInt16  int16
	TestCustomInts16 []TestCustomInt16

	TestCustomFloat  float64
	TestCustomFloats []TestCustomFloat
)

type TestItemEncDec struct {
	ID int `reindex:"id,-"`
	*TestEmbedItem
	Prices              []*TestJoinItem       `reindex:"prices,,joined"`
	Pricesx             []*TestJoinItem       `reindex:"pricesx,,joined"`
	Packages            []int                 `reindex:"packages,hash"`
	UPackages           []uint                `reindex:"upackages,hash"`
	UPackages64         []uint64              `reindex:"upackages64,hash"`
	FPackages           []float32             `reindex:"fpackages,tree"`
	FPackages64         []float64             `reindex:"fpackages64,tree"`
	Bool                bool                  `reindex:"bool"`
	Bools               []bool                `reindex:"bools"`
	Name                string                `reindex:"name,tree"`
	Countries           []string              `reindex:"countries,tree"`
	Description         string                `reindex:"description,fuzzytext"`
	Rate                float64               `reindex:"rate,tree"`
	CustomStringsPtr    TestCustomStringsPtrs `reindex:"custom_strings_ptrs"`
	CustomStrings       TestCustomStrings     `reindex:"custom_strings"`
	CustomInts64        TestCustomInts64      `reindex:"custom_ints64"`
	CustomInts16        TestCustomInts16      `reindex:"custom_ints16"`
	CustomFloats        TestCustomFloats      `reindex:"custom_floats"`
	IsDeleted           bool                  `reindex:"isdeleted,-"`
	EmptyReindexTagStr1 string                `reindex:",-"`
	EmptyReindexTagStr2 string                `reindex:""`
	EmptyJsonTagStr     string                `json:""`
	TextLabel1          string                `reindex:"TextLabel,text" json:",omitempty"`
	TextLabel2          string                `reindex:",text" json:",omitempty"`
	PNested             *TestNest             `reindex:"-"`
	Nested              TestNest
	NestedA             [1]TestNest `reindex:"-"`
	NonIndexArr         []int
	NonIndexA           [20]float64
	NonIndexPA          []*int
	Actors              []Actor `reindex:"-"`
	PricesIDs           []int   `reindex:"price_id"`
	LocationID          string  `reindex:"location"`
	EndTime             uint32  `reindex:"end_time,-"`
	StartTime           uint64  `reindex:"start_time,tree"`
	PStrNull            *string
	PStr                *string
	Tmp                 string `reindex:"tmp,-"`
	Map1                map[string]int
	Map2                map[int64]Actor
	Map3                map[int]*Actor
	Map4                map[int]*int
	Map5                map[int][]int
	Map6                map[uint][]uint
	Interface           interface{}
	Interface2          interface{}
	InterfaceNull       interface{}
	MapNull             map[string]int
	SliceStrNull        []string
	SliceNull           []int
	SliceStr            []string
	SliceUInt           []uint
	SliceUInt64         []uint64
	NegativeSliceInt64  []int64
	SliceF64            []float64
	SliceF32            []float32
	SliceBool           []bool
	SliceIface          []interface{}
	SliceIface1         []interface{}
	NestedArrayFld1     []interface{}
	NestedArrayFld2     [][]int
	NestedArrayIdx1     [][]float64 `reindex:"nested_array,tree"`
	NestedArrayIdx2     [][]string  `reindex:"nested_array_sparse,hash,sparse"`
	UInt64              uint64
	UInt32              uint32
	UInt                uint

	Custom TestCustomBytes
	Time   time.Time
	PTime  *time.Time

	_ struct{} `reindex:"id+tmp,,composite,pk"`
	_ struct{} `reindex:"age+genre,,composite"`
}

type HeterogeneousArrayItem struct {
	ID        int `reindex:"id,,pk"`
	Interface interface{}
}

type SingleElemSliceItem struct {
	ID               int       `json:"id" reindex:"id,,pk"`
	IdxStrSlice      []string  `json:"idx_str_slice" reindex:"idx_str_slice"`
	IdxIntSlice      []int64   `json:"idx_int_slice" reindex:"idx_int_slice"`
	IdxFloatSlice    []float64 `json:"idx_float_slice" reindex:"idx_float_slice"`
	IdxBoolSlice     []bool    `json:"idx_bool_slice" reindex:"idx_bool_slice"`
	NonIdxStrSlice   []string  `json:"non_idx_str_slice"`
	NonIdxIntSlice   []int64   `json:"non_idx_int_slice"`
	NonIdxFloatSlice []float64 `json:"non_idx_float_slice"`
	NonIdxBoolSlice  []bool    `json:"non_idx_bool_slice"`
}

type SlicesConcatenationItem struct {
	ID                 int           `json:"id" reindex:"id,,pk"`
	IdxStrSlice        []string      `json:"idx_str_slice" reindex:"idx_str_slice"`
	IdxInt64Slice      []int64       `json:"idx_int64_slice" reindex:"idx_int64_slice"`
	IdxInt32Slice      []int32       `json:"idx_int32_slice" reindex:"idx_int32_slice"`
	IdxInt16Slice      []int16       `json:"idx_int16_slice" reindex:"idx_int16_slice"`
	IdxInt8Slice       []int8        `json:"idx_int8_slice" reindex:"idx_int8_slice"`
	IdxFloat64Slice    []float64     `json:"idx_float64_slice" reindex:"idx_float64_slice"`
	IdxBoolSlice       []bool        `json:"idx_bool_slice" reindex:"idx_bool_slice,-"`
	NonIdxStrSlice     []string      `json:"non_idx_str_slice"`
	NonIdxIntSlice     []int         `json:"non_idx_int_slice"`
	NonIdxFloat32Slice []float32     `json:"non_idx_float32_slice"`
	NonIdxBoolSlice    []bool        `json:"non_idx_bool_slice"`
	NonIdxIfaceSlice   []interface{} `json:"non_idx_iface_slice"`
}

type DBItemNew struct {
	UpdatedAt *time.Time `json:"updated_at" db:"updated_at" reindex:"updated_at"`
	DeletedAt *time.Time `json:"-" db:"deleted_at"`
	CreatedAt *time.Time `json:"-" db:"created_at"`
}

type testItemForCJson struct {
	ID int64 `json:"id" reindex:"id,,pk"`
	DBItemNew
}

type testItemCJson struct {
	ID   int    `json:"id" reindex:"id,,pk"`
	Name string `json:"name" reindex:"name"`
}

const (
	testItemsEncdecNs                = "test_items_encdec"
	testArrayEncdecNs                = "test_array_encdec"
	testSingleElemSliceNs            = "test_single_elem_slice"
	testSlicesConcatenationNs        = "test_slices_concatenation"
	testUnsupportedConversionCJsonNs = "test_unsupported_conversion_cjson"
	testCJsonEmojiNs                 = "test_cjson_emoji"
)

func init() {
	tnamespaces[testItemsEncdecNs] = TestItemEncDec{}
	tnamespaces[testArrayEncdecNs] = HeterogeneousArrayItem{}
	tnamespaces[testSingleElemSliceNs] = SingleElemSliceItem{}
	tnamespaces[testSlicesConcatenationNs] = SlicesConcatenationItem{}
	tnamespaces[testUnsupportedConversionCJsonNs] = testItemForCJson{}
	tnamespaces[testCJsonEmojiNs] = testItemCJson{}
}

func FillHeteregeneousArrayItem() {
	item := &HeterogeneousArrayItem{
		ID:        1,
		Interface: map[string]interface{}{"HeterogeneousArray": []interface{}{"John Doe", 32, 9.1, true, "Jesus Christ", 33, false}},
	}
	tx := newTestTx(DB, testArrayEncdecNs)
	if err := tx.UpsertJSON(item); err != nil {
		panic(err)
	}
	tx.MustCommit()
}

func FilltestItemsEncdecNs(start int, count int, pkgsCount int, asJson bool) {
	tx := newTestTx(DB, testItemsEncdecNs)

	for i := 0; i < count; i++ {
		startTime := uint64(rand.Int() % 50000)
		pstr := randString()
		vint1, vint2 := new(int), new(int)
		*vint1, *vint2 = int(rand.Int31()), int(rand.Int31())
		tm := time.Unix(1234567890, 1234567890)
		cs := TestCustomString(randString())
		cs2 := TestCustomString(randString())
		item := &TestItemEncDec{
			ID: mkID(start + i),
			TestEmbedItem: &TestEmbedItem{
				Genre: int64(rand.Int() % 50),
				Year:  rand.Int()%50 + 2000,
			},
			Name: randString(),
			Nested: TestNest{
				Age:  rand.Int() % 5,
				Name: randString(),
			},
			EmptyReindexTagStr1: randString(),
			EmptyReindexTagStr2: randString(),
			EmptyJsonTagStr:     randString(),
			TextLabel1:          randString(),
			TextLabel2:          randString(),
			PNested: &TestNest{
				Age:  rand.Int() % 5,
				Name: randString(),
			},
			NestedA: [1]TestNest{
				{
					Age:  rand.Int() % 5,
					Name: randString(),
				},
			},
			Map1: map[string]int{
				`dddd`: int(rand.Int31()),
				`xxxx`: int(rand.Int31()),
			},
			Map2: map[int64]Actor{
				1:   {randString()},
				100: {randString()},
			},
			Map3: map[int]*Actor{
				4: {randString()},
				2: {randString()},
			},
			Map4: map[int]*int{
				5:   vint1,
				120: vint2,
			},
			Map5: map[int][]int{
				0:  {1, 2, 3},
				-1: {9, 8, 7},
			},
			Map6: map[uint][]uint{
				0: {1, 2, 3},
				4: {9, 8, 7},
			},
			NonIndexPA: []*int{
				vint1,
				vint2,
			},
			Interface: map[string]interface{}{
				"strfield": randString(),
				"objectf": map[string]interface{}{
					"strfield2":   "xxx",
					"intfield":    4,
					"intarrfield": []int{1, 2, 3},
					"intfarr":     []interface{}{"xxx", 2, 1.2, false, true, "John Doe"},
					"time":        time.Unix(1234567890, 987654321),
				},
				"": "Empty field string value",
			},
			Interface2:         time.Unix(3736372363, 987654321),
			Custom:             []byte(randString()),
			Countries:          []string{randString(), randString()},
			SliceStr:           []string{randString(), randString()},
			Description:        randString(),
			Packages:           randIntArr(pkgsCount, 10000, 50),
			UPackages:          []uint{uint(rand.Uint32() >> 1), uint(rand.Uint32() >> 1)},
			UPackages64:        []uint64{uint64(rand.Int63()) >> 1, uint64(rand.Int63()) >> 1 /*, math.MaxUint64*/},
			Bools:              []bool{true, false},
			Bool:               (rand.Int() % 2) != 0,
			SliceUInt:          []uint{uint(rand.Uint32() >> 1), uint(rand.Uint32() >> 1)},
			SliceUInt64:        []uint64{uint64(rand.Int63()) >> 1, uint64(rand.Int63()) >> 1},
			NegativeSliceInt64: []int64{0 - rand.Int63(), 0 - rand.Int63()},
			FPackages:          []float32{rand.Float32(), rand.Float32()},
			FPackages64:        []float64{rand.Float64(), rand.Float64()},
			SliceF32:           []float32{rand.Float32(), rand.Float32()},
			SliceF64:           []float64{rand.Float64(), rand.Float64()},
			SliceBool:          []bool{false, true},
			SliceIface:         []interface{}{"aa", "bb"},
			SliceIface1:        []interface{}{"aa", "bb", 3},
			Rate:               float64(rand.Int()%100) / 10.0,
			IsDeleted:          rand.Int()%2 != 0,
			PricesIDs:          randIntArr(10, 7000, 50),
			LocationID:         randLocation(),
			StartTime:          startTime,
			EndTime:            uint32(startTime + uint64((rand.Int()%5)*1000)),
			PStr:               &pstr,
			NonIndexArr:        randIntArr(10, 100, 10),
			Time:               tm,
			PTime:              &tm,
			CustomStringsPtr:   TestCustomStringsPtrs{&cs, &cs2},
			CustomStrings:      TestCustomStrings{TestCustomString(randString()), TestCustomString(randString())},
			CustomInts64:       TestCustomInts64{TestCustomInt64(rand.Int63())},
			CustomInts16:       TestCustomInts16{TestCustomInt16(rand.Intn(128))},
			CustomFloats:       TestCustomFloats{TestCustomFloat(rand.Float64())},
			NestedArrayFld1:    []interface{}{"aa", []interface{}{"bb", []string{"cc", "dd"}}},
			NestedArrayFld2:    [][]int{{1, 2}, {3, 4}, {5, 6}},
			NestedArrayIdx1:    [][]float64{{1, 2}, {3, 4}, {5, 6}},
			NestedArrayIdx2:    [][]string{{"aa", "bb"}, {"cc", "dd"}, {"ee", "ff"}},
		}
		if asJson {
			if err := tx.UpsertJSON(item); err != nil {
				panic(err)
			}
		} else {
			if err := tx.Upsert(item); err != nil {
				panic(err)
			}
		}
	}
	tx.MustCommit()
}

func checkIndexesWithEmptyTags(t *testing.T) {
	expectedIndexes := map[string]string{
		"EmptyReindexTagStr1": "-",
		"EmptyReindexTagStr2": "hash",
		"TextLabel":           "text",
		"TextLabel2":          "text",
	}

	desc, err := DB.DescribeNamespace(testItemsEncdecNs)
	require.NoError(t, err)
	for _, index := range desc.Indexes {
		if typ, ok := expectedIndexes[index.Name]; ok {
			assert.Equal(t, typ, index.IndexType)
			delete(expectedIndexes, index.Name)
		}
	}
	assert.Empty(t, expectedIndexes, "Some of the indexes are missing")
}

func TestHeterogeneusArrayEncDec(t *testing.T) {
	FillHeteregeneousArrayItem()

	q := newTestQuery(DB, testArrayEncdecNs)
	it := q.ExecToJson()
	defer it.Close()
	require.NoError(t, it.Error())

	for it.Next() {
		item := &TestItemEncDec{}
		err := json.Unmarshal(it.JSON(), &item)
		require.NoError(t, err, "error json was: %s\n", it.JSON())
	}
}

func TestEncDec(t *testing.T) {
	t.Parallel()
	// Fill items by cjson encoder
	FilltestItemsEncdecNs(0, 5000, 20, false)

	// fill items in json format
	FilltestItemsEncdecNs(5000, 10000, 20, true)

	checkIndexesWithEmptyTags(t)

	// get and decode all items by cjson decoder
	newTestQuery(DB, testItemsEncdecNs).ExecAndVerify(t)

	// get and decode all items in json format
	q := newTestQuery(DB, testItemsEncdecNs)
	it := q.ExecToJson()
	defer it.Close()
	require.NoError(t, it.Error())

	iitems := make([]interface{}, 0, 5000)
	for it.Next() {
		item := &TestItemEncDec{}
		err := json.Unmarshal(it.JSON(), &item)
		require.NoError(t, err, "error json was: %s\n", it.JSON())
		iitems = append(iitems, item)
	}
	q.Verify(t, iitems, []reindexer.AggregationResult{}, true)
}

func TestSingleElemToSlice(t *testing.T) {
	t.Parallel()

	const ns = testSingleElemSliceNs
	item := SingleElemSliceItem{
		ID:               1,
		IdxStrSlice:      []string{"str1"},
		IdxIntSlice:      []int64{999},
		IdxFloatSlice:    []float64{10.0},
		IdxBoolSlice:     []bool{true},
		NonIdxStrSlice:   []string{"str2"},
		NonIdxIntSlice:   []int64{888},
		NonIdxFloatSlice: []float64{20.0},
		NonIdxBoolSlice:  []bool{false},
	}
	err := DB.Upsert(ns,
		[]byte(fmt.Sprintf(`{"id":%v, "idx_str_slice":"%v", "idx_int_slice":%v,"idx_float_slice":%v,"idx_bool_slice":%v,
			"non_idx_str_slice":"%v","non_idx_int_slice":%v,"non_idx_float_slice":%v,"non_idx_bool_slice":%v}`,
			item.ID, item.IdxStrSlice[0], item.IdxIntSlice[0], item.IdxFloatSlice[0], item.IdxBoolSlice[0],
			item.NonIdxStrSlice[0], item.NonIdxIntSlice[0], item.NonIdxFloatSlice[0], item.NonIdxBoolSlice[0])),
	)
	require.NoError(t, err)

	resItems, err := newTestQuery(DB, ns).MustExec(t).FetchAll()
	require.NoError(t, err)
	require.Equal(t, len(resItems), 1)
	resItem, ok := resItems[0].(*SingleElemSliceItem)
	require.True(t, ok)
	require.Equal(t, *resItem, item)
}

func TestSlicesConcatenation(t *testing.T) {
	t.Parallel()

	const ns = testSlicesConcatenationNs
	item := SlicesConcatenationItem{
		ID:                 1,
		IdxStrSlice:        []string{"str10", "str11", "str12", "str13"},
		IdxInt64Slice:      []int64{110, 120, 130, 140, 150, 160},
		IdxInt32Slice:      []int32{310, 320, 330, 340, 350, 360},
		IdxInt16Slice:      []int16{210, 420, 430, 440, 450, 460},
		IdxInt8Slice:       []int8{10, 20, 30, 40, 50, 60},
		IdxFloat64Slice:    []float64{120.0, 140.5, 160.0, 170.5, 180.92},
		IdxBoolSlice:       []bool{true, true, false, true},
		NonIdxStrSlice:     []string{"str20", "str21", "str22", "str23"},
		NonIdxIntSlice:     []int{10, 20, 30, 40, 50, 60},
		NonIdxFloat32Slice: []float32{20.0, 40.5, 60.0, 70.5, 80.92},
		NonIdxBoolSlice:    []bool{false, false, true, false},
		NonIdxIfaceSlice:   []interface{}{"istr1", "istr2", 11, 22.4, true, false},
	}
	appendInterface := func(slice []interface{}, a interface{}) []interface{} {
		ra := reflect.ValueOf(a)
		if ra.Kind() == reflect.Slice {
			for i := 0; i < ra.Len(); i++ {
				slice = append(slice, ra.Index(i).Interface())
			}
			return slice
		}
		return append(slice, a)
	}
	sl := make([]interface{}, 0)
	rv := reflect.ValueOf(item)
	for i := 0; i < rv.NumField(); i++ {
		field := rv.Field(i).Interface()
		sl = appendInterface(sl, field)
	}

	t.Run("slices concatenation variant 1", func(t *testing.T) {
		err := DB.Upsert(ns,
			[]byte(fmt.Sprintf(`{"id":%v,
			"idx_str_slice":"%v", "idx_str_slice":["%v","%v"], "idx_str_slice":"%v",
			"idx_int64_slice":%v, "idx_int64_slice":[%v,%v,%v], "idx_int64_slice":[%v], "idx_int64_slice":%v,
			"idx_int32_slice":%v, "idx_int32_slice":[%v,%v,%v], "idx_int32_slice":[%v], "idx_int32_slice":%v,
			"idx_int16_slice":%v, "idx_int16_slice":[%v,%v,%v], "idx_int16_slice":[%v], "idx_int16_slice":%v,
			"idx_int8_slice":%v, "idx_int8_slice":[%v,%v,%v], "idx_int8_slice":[%v], "idx_int8_slice":%v,
			"idx_float64_slice":%v, "idx_float64_slice":[%v,%v,%v], "idx_float64_slice":%v,
			"idx_bool_slice":%v, "idx_bool_slice":[%v,%v], "idx_bool_slice":%v,
			"non_idx_str_slice":"%v", "non_idx_str_slice":["%v","%v"], "non_idx_str_slice":"%v",
			"non_idx_int_slice":%v, "non_idx_int_slice":[%v,%v,%v], "non_idx_int_slice":[%v], "non_idx_int_slice":%v,
			"non_idx_float32_slice":%v, "non_idx_float32_slice":[%v,%v,%v], "non_idx_float32_slice":%v,
			"non_idx_bool_slice":%v, "non_idx_bool_slice":[%v,%v], "non_idx_bool_slice":%v,
			"non_idx_iface_slice":"%v", "non_idx_iface_slice":"%v", "non_idx_iface_slice":%v, "non_idx_iface_slice":%v, "non_idx_iface_slice":%v, "non_idx_iface_slice":%v}`,
				sl...)),
		)
		require.NoError(t, err)

		resItems, err := newTestQuery(DB, ns).MustExec(t).FetchAll()
		require.NoError(t, err)
		require.Equal(t, len(resItems), 1)
		resItem, ok := resItems[0].(*SlicesConcatenationItem)
		require.True(t, ok)
		require.Equal(t, *resItem, item)
	})

	t.Run("slices concatenation variant 2", func(t *testing.T) {
		err := DB.Upsert(ns,
			[]byte(fmt.Sprintf(`{"id":%v,
			"idx_str_slice":["%v","%v"], "idx_str_slice":"%v", "idx_str_slice":"%v",
			"idx_int64_slice":[%v,%v,%v], "idx_int64_slice":%v, "idx_int64_slice":[%v], "idx_int64_slice":%v,
			"idx_int32_slice":[%v,%v,%v], "idx_int32_slice":%v, "idx_int32_slice":[%v], "idx_int32_slice":%v,
			"idx_int16_slice":[%v,%v,%v], "idx_int16_slice":%v, "idx_int16_slice":[%v], "idx_int16_slice":%v,
			"idx_int8_slice":[%v,%v,%v], "idx_int8_slice":%v, "idx_int8_slice":[%v], "idx_int8_slice":%v,
			"idx_float64_slice":[%v,%v,%v], "idx_float64_slice":%v, "idx_float64_slice":%v,
			"idx_bool_slice":[%v,%v], "idx_bool_slice":%v, "idx_bool_slice":%v,
			"non_idx_str_slice":["%v","%v"], "non_idx_str_slice":"%v", "non_idx_str_slice":"%v",
			"non_idx_int_slice":[%v,%v,%v], "non_idx_int_slice":%v, "non_idx_int_slice":[%v], "non_idx_int_slice":%v,
			"non_idx_float32_slice":[%v,%v,%v], "non_idx_float32_slice":%v, "non_idx_float32_slice":%v,
			"non_idx_bool_slice":[%v,%v], "non_idx_bool_slice":%v, "non_idx_bool_slice":%v,
			"non_idx_iface_slice":["%v","%v"], "non_idx_iface_slice":%v, "non_idx_iface_slice":%v, "non_idx_iface_slice":[%v,%v]}`,
				sl...)),
		)
		require.NoError(t, err)

		resItems, err := newTestQuery(DB, ns).MustExec(t).FetchAll()
		require.NoError(t, err)
		require.Equal(t, len(resItems), 1)
		resItem, ok := resItems[0].(*SlicesConcatenationItem)
		require.True(t, ok)
		require.Equal(t, *resItem, item)
	})

	t.Run("slices concatenation variant 3", func(t *testing.T) {
		err := DB.Upsert(ns,
			[]byte(fmt.Sprintf(`{"id":%v,
			"idx_str_slice":["%v","%v"], "idx_str_slice":["%v","%v"],
			"idx_int64_slice":[%v,%v,%v], "idx_int64_slice":[%v,%v,%v],
			"idx_int32_slice":[%v,%v,%v], "idx_int32_slice":[%v,%v,%v],
			"idx_int16_slice":[%v,%v,%v], "idx_int16_slice":[%v,%v,%v],
			"idx_int8_slice":[%v,%v,%v], "idx_int8_slice":[%v,%v,%v],
			"idx_float64_slice":[%v,%v,%v], "idx_float64_slice":[%v,%v],
			"idx_bool_slice":[%v,%v], "idx_bool_slice":[%v,%v],
			"non_idx_str_slice":["%v","%v"], "non_idx_str_slice":["%v","%v"],
			"non_idx_int_slice":[%v,%v,%v], "non_idx_int_slice":[%v,%v,%v],
			"non_idx_float32_slice":[%v,%v,%v], "non_idx_float32_slice":[%v,%v],
			"non_idx_bool_slice":[%v,%v], "non_idx_bool_slice":[%v,%v],
			"non_idx_iface_slice":["%v","%v"], "non_idx_iface_slice":[%v,%v], "non_idx_iface_slice":[%v,%v]}`,
				sl...)),
		)
		require.NoError(t, err)

		resItems, err := newTestQuery(DB, ns).MustExec(t).FetchAll()
		require.NoError(t, err)
		require.Equal(t, len(resItems), 1)
		resItem, ok := resItems[0].(*SlicesConcatenationItem)
		require.True(t, ok)
		require.Equal(t, *resItem, item)
	})
}

func TestUnsupportedConversionCJson(t *testing.T) {
	t.Parallel()

	t.Run("check unsupported conversion cjson error", func(t *testing.T) {
		const ns = testUnsupportedConversionCJsonNs

		err := DBD.Upsert(ns, []byte(`{"id":1, "updated_at":12.3}`))
		require.NoError(t, err)
		_, err = DBD.Query(ns).
			Where("id", reindexer.SET, []int64{1}).
			Exec().FetchAll()
		require.ErrorContains(t, err, "can not convert 'double' to 'struct'")
	})
}

func TestEmojis(t *testing.T) {
	t.Parallel()

	t.Run("create and read items with emojis", func(t *testing.T) {
		const ns = testCJsonEmojiNs
		emojis := []string{
			"👀", "😀", "👌🤌👋", "👁️‍🗨️", "🧑‍🧑‍🧒‍🧒👁️‍🗨️", "\U0001F63E\U0001F63A",
			"\"🚅\" 🚝\"", "\\\"(ﾉ◕ヮ◕)ﾉ*:･ﾟ✧", "\t🏁💬\"🧑‍🧑‍🧒‍🧒\"👁️‍🗨️\\\"\U0001F64E😀",
		}

		items := make([]testItemCJson, 0, len(emojis))
		for i, emoji := range emojis {
			item := testItemCJson{ID: i, Name: emoji}
			err := DBD.Upsert(ns, item)
			require.NoError(t, err)
			items = append(items, item)
		}

		resItems, err := DBD.Query(ns).Exec().FetchAll()
		require.NoError(t, err)
		require.Len(t, resItems, len(emojis))

		resItemsVal := make([]testItemCJson, len(resItems))
		for i, resItem := range resItems {
			resItemsVal[i] = *resItem.(*testItemCJson)
		}
		require.Equal(t, items, resItemsVal)
	})
}
