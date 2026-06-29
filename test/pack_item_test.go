package reindexer

import (
	"testing"

	"github.com/stretchr/testify/require"
)

const testPackItemTypeCheckNs = "test_pack_item_type_check"

type packItemTypeCheckItem struct {
	ID int `json:"id" reindex:"id,,pk"`
}

type packItemWrongNamedItem struct {
	ID int `json:"id" reindex:"id,,pk"`
}

func init() {
	tnamespaces[testPackItemTypeCheckNs] = packItemTypeCheckItem{}
}

func TestPackItemTypeCheck(t *testing.T) {
	const ns = testPackItemTypeCheckNs

	require.NoError(t, DB.Upsert(ns, packItemTypeCheckItem{ID: 1}))
	require.NoError(t, DB.Upsert(ns, &packItemTypeCheckItem{ID: 2}))
	require.NoError(t, DB.Upsert(ns, []byte(`{"id":3}`)))
	require.Panics(t, func() {
		_ = DB.Upsert(ns, packItemWrongNamedItem{ID: 4})
	})
}
