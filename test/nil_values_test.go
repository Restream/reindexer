package reindexer

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const nilValuesTestNamespace = "test_nil_values_namespace"

func init() {
	tnamespaces[nilValuesTestNamespace] = TestItemSimple{}
}

func TestNilValuesErrors(t *testing.T) {
	t.Run("Single items", func(t *testing.T) {
		err := DBD.Upsert(nilValuesTestNamespace, nil)
		require.Error(t, err)

		count, err := DBD.Insert(nilValuesTestNamespace, nil)
		require.Error(t, err)
		require.Equal(t, count, 0)

		count, err = DBD.Update(nilValuesTestNamespace, nil)
		require.Error(t, err)
		require.Equal(t, count, 0)

		err = DBD.Delete(nilValuesTestNamespace, nil)
		require.Error(t, err)
	})

	t.Run("Transaction items", func(t *testing.T) {
		tx := DBD.MustBeginTx(nilValuesTestNamespace)
		defer tx.Rollback()

		err := tx.Upsert(nil)
		require.Error(t, err)

		err = tx.UpsertJSON(nil)
		require.Error(t, err)

		err = tx.Insert(nil)
		require.Error(t, err)

		err = tx.Update(nil)
		require.Error(t, err)

		err = tx.Delete(nil)
		require.Error(t, err)

		err = tx.UpsertAsync(nil, func(e error) {
			assert.Error(t, e)
		})
		require.Error(t, err)

		err = tx.UpsertJSONAsync(nil, func(e error) {
			assert.Error(t, e)
		})
		require.Error(t, err)

		err = tx.InsertAsync(nil, func(e error) {
			assert.Error(t, e)
		})
		require.Error(t, err)

		err = tx.UpdateAsync(nil, func(e error) {
			assert.Error(t, e)
		})
		require.Error(t, err)

		err = tx.DeleteAsync(nil, func(e error) {
			assert.Error(t, e)
		})
		require.Error(t, err)

		err = tx.DeleteJSONAsync(nil, func(e error) {
			assert.Error(t, e)
		})
		require.Error(t, err)
	})
}
