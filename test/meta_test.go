package reindexer

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

const testMetaAPINs = "test_meta"

func init() {
	tnamespaces[testMetaAPINs] = TestItemSimple{}
}

func TestMetaAPI(t *testing.T) {
	const ns = testMetaAPINs

	t.Run("processing meta when storage is empty", func(t *testing.T) {
		const emptyKey = ""
		inputValue := []byte("testVal")
		err := DB.PutMeta(ns, emptyKey, inputValue)
		require.Error(t, err, "Empty key is not supported")

		readMetaData, err := DB.GetMeta(t, ns, emptyKey)
		require.Error(t, err, "Empty key is not supported")
		require.Equal(t, readMetaData, []byte(nil))

		readMetaData, err = DB.GetMeta(t, ns, "Blah-blah")
		require.NoError(t, err)
		require.Equal(t, readMetaData, []byte{})

		keys, err := DB.EnumMeta(t, ns)
		require.NoError(t, err)
		require.Len(t, keys, 0)

		err = DB.DeleteMeta(ns, emptyKey)
		require.Error(t, err, "Empty key is not supported")
	})

	t.Run("put to nil and overwrite key meta value", func(t *testing.T) {
		const key = "testKey"
		inputValueEmpty := []byte("")
		inputValue := []byte("testVal")

		err := DB.PutMeta(ns, key, nil)
		require.NoError(t, err)

		readMetaData, err := DB.GetMeta(t, ns, key)
		require.NoError(t, err)
		require.Equal(t, readMetaData, []byte{})

		err = DB.PutMeta(ns, key, inputValueEmpty)
		require.NoError(t, err)

		readMetaData, err = DB.GetMeta(t, ns, key)
		require.NoError(t, err)
		require.Equal(t, readMetaData, inputValueEmpty)

		err = DB.PutMeta(ns, key, inputValue)
		require.NoError(t, err)

		readMetaData, err = DB.GetMeta(t, ns, key)
		require.NoError(t, err)
		require.Equal(t, readMetaData, inputValue)

		keys, err := DB.EnumMeta(t, ns)
		require.NoError(t, err)
		require.Len(t, keys, 1)

		err = DB.DeleteMeta(ns, key)
		require.NoError(t, err)

		keys, err = DB.EnumMeta(t, ns)
		require.NoError(t, err)
		require.Len(t, keys, 0)

		err = DB.DeleteMeta(ns, string(inputValue))
		require.NoError(t, err)
	})

	t.Run("iterate meta", func(t *testing.T) {
		const k = 3
		const v = "overwrite"

		keys := make([]string, k)
		for i := 0; i < k; i++ {
			s := strconv.Itoa(i)
			keys[i] = s

			err := DB.PutMeta(ns, s, []byte(s))
			require.NoError(t, err)
		}

		readKeys, err := DB.EnumMeta(t, ns)
		require.NoError(t, err)
		require.Len(t, readKeys, k)

		for _, key := range readKeys {
			readMetaData, err := DB.GetMeta(t, ns, key)
			require.NoError(t, err)
			require.Equal(t, string(readMetaData), key)

			err = DB.PutMeta(ns, key, []byte(v))
			require.NoError(t, err)
		}

		err = DB.DeleteMeta(ns, v)
		require.NoError(t, err)

		readKeys, err = DB.EnumMeta(t, ns)
		require.NoError(t, err)
		require.Len(t, readKeys, k)

		for _, key := range keys {
			readMetaData, err := DB.GetMeta(t, ns, key)
			require.NoError(t, err)
			require.Equal(t, string(readMetaData), v)

			err = DB.DeleteMeta(ns, key)
			require.NoError(t, err)
		}

		readKeys, err = DB.EnumMeta(t, ns)
		require.NoError(t, err)
		require.Len(t, readKeys, 0)
	})
}
