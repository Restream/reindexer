package reindexer

import (
	"context"
	"math/rand"
	"testing"

	"github.com/restream/reindexer/v5"
	"github.com/restream/reindexer/v5/test/custom_struct_another"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testItemsIterNs        = "test_items_iter"
	testItemsIterNextObjNs = "test_items_iter_next_obj"
)

func init() {
	tnamespaces[testItemsIterNs] = TestItem{}
	tnamespaces[testItemsIterNextObjNs] = TestItem{}
}

func TestQueryIter(t *testing.T) {
	const ns = testItemsIterNs

	total := 10
	for i := 0; i < total; i++ {
		assert.NoError(t, DB.Upsert(ns, newTestItem(1000+i, 5)))
	}

	// check next
	limit := 5
	iter := DB.Query(ns).ReqTotal().Limit(limit).Exec(t)
	defer iter.Close()
	i := 0
	for iter.Next() {
		_ = iter.Object()
		i++
	}
	require.NoError(t, iter.Error())
	assert.Equal(t, i, limit, "Unexpected result count: %d (want %d)", i, limit)

	// check all
	items, err := DB.Query(ns).Exec(t).FetchAll()
	require.NoError(t, err)
	assert.Equal(t, len(items), total, "Unexpected result count: %d (want %d)", len(items), total)

	// check one
	item, err := DB.Query(ns).Exec(t).FetchOne()
	require.NoError(t, err)
	assert.NotNil(t, item, "Iterator.FetchOne: item is nil")
}

func TestNextObj(t *testing.T) {
	const ns = testItemsIterNextObjNs

	ctx := context.Background()

	itemsExp := []*TestItem{
		newTestItem(20000, 5).(*TestItem),
		newTestItem(20001, 5).(*TestItem),
		newTestItem(20002, 5).(*TestItem),
	}
	itemCustomExp := &TestItemCustom{
		ID:                itemsExp[1].ID,
		Name:              itemsExp[1].Name,
		Genre:             itemsExp[1].Genre,
		Year:              itemsExp[1].Year,
		CustomUniqueField: rand.Int(),
		Actor:             itemsExp[1].Actor,
	}
	for _, v := range itemsExp {
		assert.NoError(t, DB.Upsert(ns, v))
	}

	// should use two structs in one test, to use cTagsCache
	t.Run("parse to custom and original structs", func(t *testing.T) {
		it := DB.Query(ns).
			WhereInt("id", reindexer.SET, itemsExp[0].ID, itemsExp[1].ID, itemsExp[2].ID).
			ExecCtx(t, ctx)
		defer it.Close()

		// use one original struct
		item := TestItem{}
		if it.NextObj(&item) {
			assert.Equal(t, itemsExp[0].ID, item.ID)
		}

		// use custom struct
		itemCustom := TestItemCustom{}
		if it.NextObj(&itemCustom) {
			assert.False(t,
				itemCustomExp.ID != itemCustom.ID || itemCustomExp.Name != itemCustom.Name || itemCustomExp.Genre != itemCustom.Genre ||
					itemCustomExp.Year != itemCustom.Year || itemCustomExp.Actor != itemCustom.Actor || 0 != itemCustom.CustomUniqueField,
				"unexpected item, exp=%#v, current=%#v", itemCustomExp, itemCustom)

		}

		// and second original struct
		item2 := TestItem{}
		if it.NextObj(&item2) {
			assert.Equal(t, itemsExp[2].ID, item2.ID)
		}
		assert.NoError(t, it.Error())

	})

	t.Run("parse to the same struct from separate packages", func(t *testing.T) {
		it := DB.Query(ns).
			WhereInt("id", reindexer.SET, itemsExp[0].ID, itemsExp[1].ID).
			ExecCtx(t, ctx)
		defer it.Close()

		// use another custom struct
		itemCustomAnother := custom_struct_another.TestItemCustom{}
		if it.NextObj(&itemCustomAnother) {
			assert.Equal(t, itemsExp[0].ID, itemCustomAnother.ID)
		}

		// use custom struct
		itemCustom := TestItemCustom{}
		if it.NextObj(&itemCustom) {
			assert.False(t, itemCustomExp.ID != itemCustom.ID || itemCustomExp.Name != itemCustom.Name || itemCustomExp.Genre != itemCustom.Genre ||
				itemCustomExp.Year != itemCustom.Year || itemCustomExp.Actor != itemCustom.Actor || 0 != itemCustom.CustomUniqueField,
				"unexpected item, exp=%#v, current=%#v", itemCustomExp, itemCustom)
		}
		assert.NoError(t, it.Error())
	})
}
