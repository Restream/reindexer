package reindexer

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"
	"time"
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

type TestCustom []byte

type TestItemEncDec struct {
	ID int `reindex:"id,-"`
	*TestEmbedItem
	Prices        []*TestJoinItem `reindex:"prices,,joined"`
	Pricesx       []*TestJoinItem `reindex:"pricesx,,joined"`
	Packages      []int           `reindex:"packages,hash"`
	UPackages     []uint          `reindex:"upackages,hash"`
	UPackages64   []uint64        `reindex:"upackages64,hash"`
	FPackages     []float32       `reindex:"fpackages,tree"`
	FPackages64   []float64       `reindex:"fpackages64,tree"`
	Name          string          `reindex:"name,tree"`
	Countries     []string        `reindex:"countries,tree"`
	Description   string          `reindex:"description,fuzzytext"`
	Rate          float64         `reindex:"rate,tree"`
	IsDeleted     bool            `reindex:"isdeleted,-"`
	PNested       *TestNest       `reindex:"-"`
	Nested        TestNest
	NestedA       [1]TestNest `reindex:"-"`
	NonIndexArr   []int
	NonIndexA     [20]float64
	NonIndexPA    []*int
	Actors        []Actor `reindex:"-"`
	PricesIDs     []int   `reindex:"price_id"`
	LocationID    string  `reindex:"location"`
	EndTime       uint32  `reindex:"end_time,-"`
	StartTime     uint64  `reindex:"start_time,tree"`
	PStrNull      *string
	PStr          *string
	Tmp           string `reindex:"tmp,-"`
	Map1          map[string]int
	Map2          map[int64]Actor
	Map3          map[int]*Actor
	Map4          map[int]*int
	Map5          map[int][]int
	Map6          map[uint][]uint
	Interface     interface{}
	Interface2    interface{}
	InterfaceNull interface{}
	MapNull       map[string]int
	SliceStrNull  []string
	SliceNull     []int
	SliceStr      []string
	SliceUInt     []uint
	SliceUInt64   []uint64
	SliceF64      []float64
	SliceF32      []float32
	UInt64        uint64
	UInt32        uint32
	UInt          uint

	Custom TestCustom
	Time   time.Time
	PTime  *time.Time

	_ struct{} `reindex:"id+tmp,,composite,pk"`
	_ struct{} `reindex:"age+genre,,composite"`
}

func init() {
	tnamespaces["test_items_encdec"] = TestItemEncDec{}
}

func FillTestItemsEncDec(start int, count int, pkgsCount int, asJson bool) {
	tx := newTestTx(DB, "test_items_encdec")

	for i := 0; i < count; i++ {
		startTime := uint64(rand.Int() % 50000)
		pstr := randString()
		vint1, vint2 := new(int), new(int)
		*vint1, *vint2 = int(rand.Int31()), int(rand.Int31())
		tm := time.Unix(1234567890, 1234567890)
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
				1:   Actor{randString()},
				100: Actor{randString()},
			},
			Map3: map[int]*Actor{
				4: &Actor{randString()},
				2: &Actor{randString()},
			},
			Map4: map[int]*int{
				5:   vint1,
				120: vint2,
			},
			Map5: map[int][]int{
				0:  []int{1, 2, 3},
				-1: []int{9, 8, 7},
			},
			Map6: map[uint][]uint{
				0: []uint{1, 2, 3},
				4: []uint{9, 8, 7},
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
					"intfarr":     []interface{}{"xxx", 2, 1.2},
					"time":        time.Unix(1234567890, 987654321),
				},
			},
			Interface2:  time.Unix(3736372363, 987654321),
			Custom:      []byte(randString()),
			Countries:   []string{randString(), randString()},
			SliceStr:    []string{randString(), randString()},
			Description: randString(),
			Packages:    randIntArr(pkgsCount, 10000, 50),
			// TODO: fix uint overflow
			UPackages:   []uint{uint(rand.Uint32() >> 1), uint(rand.Uint32() >> 1)},
			UPackages64: []uint64{uint64(rand.Int63()) >> 1, uint64(rand.Int63()) >> 1},
			SliceUInt:   []uint{uint(rand.Uint32() >> 1), uint(rand.Uint32() >> 1)},
			SliceUInt64: []uint64{uint64(rand.Int63()) >> 1, uint64(rand.Int63()) >> 1},
			FPackages:   []float32{rand.Float32(), rand.Float32()},
			FPackages64: []float64{rand.Float64(), rand.Float64()},
			SliceF32:    []float32{rand.Float32(), rand.Float32()},
			SliceF64:    []float64{rand.Float64(), rand.Float64()},
			Rate:        float64(rand.Int()%100) / 10.0,
			IsDeleted:   rand.Int()%2 != 0,
			PricesIDs:   randIntArr(10, 7000, 50),
			LocationID:  randLocation(),
			StartTime:   startTime,
			EndTime:     uint32(startTime + uint64((rand.Int()%5)*1000)),
			PStr:        &pstr,
			NonIndexArr: randIntArr(10, 100, 10),
			Time:        tm,
			PTime:       &tm,
		}

		if err := tx.Upsert(item); err != nil {
			panic(err)
		}
	}
	tx.MustCommit(nil)
}

func TestEncDec(t *testing.T) {

	// Fill items by cjson encoder
	FillTestItemsEncDec(0, 5000, 20, false)

	// fill items in json format
	FillTestItemsEncDec(5000, 5000, 20, true)

	// get and decode all items by cjson decoder
	newTestQuery(DB, "test_items_encdec").ExecAndVerify()

	// get and decode all items in json format
	q := newTestQuery(DB, "test_items_encdec")
	it := q.ExecToJson()
	defer it.Close()
	if it.Error() != nil {
		panic(it.Error())
	}

	iitems := make([]interface{}, 0, 5000)
	for it.Next() {
		item := &TestItemEncDec{}
		err := json.Unmarshal(it.JSON(), &item)
		if err != nil {
			fmt.Printf("error json was: %s\n", it.JSON())
			panic(err)
		}
		iitems = append(iitems, item)
	}
	q.Verify(iitems, true)

}
