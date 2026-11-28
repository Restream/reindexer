package reindexer

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuilderErrorsReleaseQuery(t *testing.T) {
	t.Run("exec", func(t *testing.T) {
		q := newQuery(nil, "test", nil).CloseBracket()
		it := q.Exec()
		require.Error(t, it.Error())
		require.True(t, q.closed)
		it.Close()
	})

	t.Run("update", func(t *testing.T) {
		q := newQuery(nil, "test", nil).CloseBracket()
		it := q.Update()
		require.Error(t, it.Error())
		require.True(t, q.closed)
		it.Close()
	})

	t.Run("delete", func(t *testing.T) {
		q := newQuery(nil, "test", nil).CloseBracket()
		_, err := q.Delete()
		require.Error(t, err)
		require.True(t, q.closed)
	})
}

func TestQueryCloseStillDetectsDoubleClose(t *testing.T) {
	q := newQuery(nil, "test", nil)
	q.close()
	require.Panics(t, q.close)
}
