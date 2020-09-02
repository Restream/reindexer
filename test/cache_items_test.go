package reindexer

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/restream/reindexer"
)

func init() {
	tnamespaces["test_namespace_cache_items"] = NamespaceCacheItem{}
}

type NamespaceCacheItem struct {
	IntCjon         int               `json:"int_cjson" reindex:"int_cjson,,pk"`
	Int             int               `json:"int"`
	Int8Cjon        int8              `json:"int8_cjson" reindex:"int8_cjson"`
	Int8            int8              `json:"int8"`
	Int16Cjon       int16             `json:"int16_cjson" reindex:"int16_cjson"`
	Int16           int16             `json:"int16"`
	Int32Cjon       int32             `json:"int32_cjson" reindex:"int32_cjson"`
	Int32           int32             `json:"int32"`
	Int64Cjon       int64             `json:"int64_cjson" reindex:"int64_cjson"`
	Int64           int64             `json:"int64"`
	Float32Cjon     float32           `json:"float32_cjson" reindex:"float32_cjson"`
	Float32         float32           `json:"float32"`
	Float64Cjon     float64           `json:"float64_cjson" reindex:"float64_cjson"`
	Float64         float64           `json:"float64"`
	UIntCjon        uint              `json:"uint_cjson" reindex:"uint_cjson"`
	UInt            uint              `json:"uint"`
	UInt8Cjon       uint8             `json:"uint8_cjson" reindex:"uint8_cjson"`
	UInt8           uint8             `json:"uint8"`
	UInt16Cjon      uint16            `json:"uint16_cjson" reindex:"uint16_cjson"`
	UInt16          uint16            `json:"uint16"`
	UInt32Cjon      uint32            `json:"uint32_cjson" reindex:"uint32_cjson"`
	UInt32          uint32            `json:"uint32"`
	UInt64Cjon      uint64            `json:"uint64_cjson" reindex:"uint64_cjson"`
	UInt64          uint64            `json:"uint64"`
	BoolCjon        bool              `json:"bool_cjson" reindex:"bool_cjson"`
	Bool            bool              `json:"bool64"`
	SliceStrCjon    []string          `json:"slice_str_cjson" reindex:"slice_str_cjson"`
	SliceStr        []string          `json:"slice_str"`
	SliceIntCjon    []int             `json:"slice_int_cjson" reindex:"slice_int_cjson"`
	SliceInt        []int             `json:"slice_int"`
	SliceIntPtrCjon *[]int            `json:"slice_int_ptr_cjson" reindex:"slice_int_ptr_cjson"`
	SliceIntPtr     *[]int            `json:"slice_int_ptr"`
	MapStr          map[string]string `json:"map_str"`
	MapInt          map[string]int    `json:"map_int"`
}

func (item *NamespaceCacheItem) DeepCopy() interface{} {
	return &NamespaceCacheItem{}
}

func TestNamespaceCacheItemsWithObjCacheSize(t *testing.T) {
	ns := "test_namespace_cache_items"
	slice1 := []int{1, 2}
	slice2 := []int{1}
	item := NamespaceCacheItem{
		IntCjon:         1,
		Int:             2,
		SliceStrCjon:    []string{"a"},
		SliceStr:        []string{"a"},
		SliceIntCjon:    []int{1, 2},
		SliceInt:        []int{2},
		SliceIntPtrCjon: &slice1,
		SliceIntPtr:     &slice2,
		MapStr:          map[string]string{"a": "qwe1", "b": "1"},
		MapInt:          map[string]int{"c": 1},
	}
	err := DBD.OpenNamespace(ns, reindexer.DefaultNamespaceOptions().ObjCacheSize(1), NamespaceCacheItem{})
	require.NoError(t, err)

	// fill obj cache > 1MB
	for i := 0; i < 2900; i++ {
		itemIns := item
		itemIns.IntCjon = i + 2
		n, err := DBD.Insert(ns, itemIns)
		require.NoError(t, err)
		require.Equal(t, 1, n)
	}

	items, err := DBD.Query(ns).Exec().FetchAll()
	require.NoError(t, err)
	require.Equal(t, 2900, len(items))
}
