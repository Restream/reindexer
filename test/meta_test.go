package reindexer

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

const testMetaAPINs = "test_meta"
const testMetaEmojiNs = "test_meta_emoji"

func init() {
	tnamespaces[testMetaAPINs] = TestItemSimple{}
	tnamespaces[testMetaEmojiNs] = TestItemSimple{}
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

	t.Run("add and get meta with escaping", func(t *testing.T) {
		metadata := map[string]string{
			"key\"":     "value\"",
			"key\\\"":   "value\\\"",
			"key\\\\\"": "value\\\\\"",
		}
		for k, v := range metadata {
			err := DB.PutMeta(ns, k, []byte(v))
			require.NoError(t, err)
		}

		keys, err := DB.EnumMeta(t, ns)
		require.NoError(t, err)
		expectedKeys := make([]string, 0, len(metadata))
		for k := range metadata {
			expectedKeys = append(expectedKeys, k)
		}
		require.ElementsMatch(t, expectedKeys, keys)

		for k, v := range metadata {
			value, err := DB.GetMeta(t, ns, k)
			require.NoError(t, err)
			require.Equal(t, []byte(v), value)
		}
	})

	t.Run("add and get meta with emoji", func(t *testing.T) {
		const ns2 = testMetaEmojiNs
		metadata := map[string]string{
			"👀":     "😀",
			"👌🤌👋":   "👌🤌👋",
			"👁️‍🗨️": "👁️‍🗨️",
			"🧑‍🧑‍🧒‍🧒👁️‍🗨️":         "🧑‍🧑‍🧒‍🧒👁️‍🗨️",
			"\U0001F63E\U0001F64D": "\U0001F64D\U0001F64E",
			"\"🚅\"🚝\"":             "\"🚅\"🚝\"",
			"\\\"ヽ(>∀<☆)ノ":         "\\\"(ﾉ◕ヮ◕)ﾉ*:･ﾟ✧",
			"\t🏁💬\" 🧑‍🧑‍🧒‍🧒\"👁️‍🗨️\\\"\U0001F64E😀": "🧑‍🧑‍🧒‍🧒\\\"👁️‍🗨️\"\t\U0001F64E😀 🏁\"\\\"💬",
		}
		for k, v := range metadata {
			err := DB.PutMeta(ns2, k, []byte(v))
			require.NoError(t, err)
		}

		keys, err := DB.EnumMeta(t, ns2)
		require.NoError(t, err)
		expectedKeys := make([]string, 0, len(metadata))
		for k := range metadata {
			expectedKeys = append(expectedKeys, k)
		}
		require.ElementsMatch(t, expectedKeys, keys)

		for k, v := range metadata {
			value, err := DB.GetMeta(t, ns2, k)
			require.NoError(t, err)
			require.Equal(t, []byte(v), value)
		}
	})
}
