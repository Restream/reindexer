package reindexer

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/restream/reindexer/v5"
)

type emptyObjectData map[string]any

type emptyObjectItem struct {
	ID             int64                  `json:"id" reindex:"id,,pk"`
	Map            map[string]any         `json:"map"`
	NamedMap       emptyObjectData        `json:"named_map"`
	TypedMap       map[int]string         `json:"typed_map"`
	Data           emptyObjectData        `json:"data,omitempty"`
	Fields         emptyObjectFields      `json:"fields"`
	NestedMap      map[int]map[string]int `json:"nested_map"`
	Objects        []any                  `json:"objects"`
	TypedObjects   []map[string]int       `json:"typed_objects"`
	ObjectPointers []*map[string]int      `json:"object_pointers"`
}

type emptyObjectFields struct {
	Map       map[string]int  `json:"map"`
	Interface any             `json:"interface"`
	Pointer   *map[string]int `json:"pointer"`
	Null      map[string]int  `json:"null"`
	NullIface any             `json:"null_interface"`
	Absent    map[string]int  `json:"absent,omitempty"`
}

const testEmptyObjectNs = "test_empty_object"

func init() {
	tnamespaces[testEmptyObjectNs] = emptyObjectItem{}
}

func TestPreserveEmptyNestedObjects(t *testing.T) {
	emptyMap := map[string]int{}

	for i, tt := range []struct {
		name string
		item emptyObjectItem
	}{
		{"empty map", emptyObjectItem{Map: map[string]any{}}},
		{"named map", emptyObjectItem{NamedMap: emptyObjectData{}}},
		{"typed map", emptyObjectItem{TypedMap: map[int]string{}}},
		{"nested object", emptyObjectItem{Data: emptyObjectData{
			"empty_object": map[string]any{},
			"empty_array":  []any{},
		}}},
		{"object fields", emptyObjectItem{Fields: emptyObjectFields{
			Map:       emptyMap,
			Interface: map[string]any{},
			Pointer:   &emptyMap,
		}}},
		{"typed nested map", emptyObjectItem{NestedMap: map[int]map[string]int{1: {}}}},
		{"objects in array", emptyObjectItem{Objects: []any{map[string]any{}, []any{map[string]any{}}}}},
		{"typed objects in array", emptyObjectItem{TypedObjects: []map[string]int{{}, {"value": 1}}}},
		{"object pointers in array", emptyObjectItem{ObjectPointers: []*map[string]int{&emptyMap}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for j, format := range []string{"cjson", "json"} {
				t.Run(format, func(t *testing.T) {
					item := tt.item
					item.ID = int64(2*i + j + 1)
					raw, err := json.Marshal(item)
					require.NoError(t, err)
					if format == "json" {
						err = DB.Upsert(testEmptyObjectNs, raw)
					} else {
						err = DB.Upsert(testEmptyObjectNs, item)
					}
					require.NoError(t, err)

					jsonIt := DB.Query(testEmptyObjectNs).Where("id", reindexer.EQ, item.ID).ExecToJson()
					defer jsonIt.Close()
					require.True(t, jsonIt.Next())
					require.NoError(t, jsonIt.Error())
					require.JSONEq(t, string(raw), string(jsonIt.JSON()))

					result, err := DB.Query(testEmptyObjectNs).Where("id", reindexer.EQ, item.ID).MustExec(t).FetchOne()
					require.NoError(t, err)
					require.Equal(t, &item, result)
					encoded, err := json.Marshal(result)
					require.NoError(t, err)
					require.JSONEq(t, string(raw), string(encoded))
				})
			}
		})
	}
}
