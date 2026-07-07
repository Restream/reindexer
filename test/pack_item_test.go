package reindexer

import (
	"testing"
	"time"

	rx "github.com/restream/reindexer/v5"
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
	require.Equal(t, rx.ErrWrongType, DB.Upsert(ns, packItemWrongNamedItem{ID: 4}))
	require.NotPanics(t, func() {
		require.Equal(t, rx.ErrWrongType, DB.Upsert(ns, packItemWrongNamedItem{ID: 5}))
	})
}

func TestPackItemTypeCheckTx(t *testing.T) {
	const ns = testPackItemTypeCheckNs

	tx := DB.Reindexer.MustBeginTx(ns)
	defer tx.Rollback()
	require.Equal(t, rx.ErrWrongType, tx.Upsert(packItemWrongNamedItem{ID: 1}))
}

func TestPackItemTypeCheckTxAsync(t *testing.T) {
	const ns = testPackItemTypeCheckNs

	tx := DB.Reindexer.MustBeginTx(ns)
	defer tx.Rollback()
	errCh := make(chan error, 1)
	require.Equal(t, rx.ErrWrongType, tx.UpsertAsync(packItemWrongNamedItem{ID: 1}, func(err error) {
		errCh <- err
	}))
	tx.AwaitResults()
	select {
	case err := <-errCh:
		require.Equal(t, rx.ErrWrongType, err)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for async upsert completion")
	}
}
