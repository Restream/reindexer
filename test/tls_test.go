//go:build tls_test

package reindexer

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/restream/reindexer/v5"
	_ "github.com/restream/reindexer/v5/bindings/cproto"
)

const testTlsNs = "test_tls_namespace"

func init() {
	tnamespaces[testTlsNs] = testDummyObject{}
}

func TestBaseTLS(t *testing.T) {
	db, err := reindexer.NewReindex("cprotos://127.0.0.1:6535/test" /*reindexer.WithTLSConfig(&tls.Config{})*/)
	require.NoError(t, err)
	defer db.Close()

	checkData := "Some TLS Data"
	err = db.RegisterNamespace(testTlsNs, reindexer.DefaultNamespaceOptions(), testDummyObject{})
	require.Nil(t, err)

	tx, err := db.BeginTx(testTlsNs)
	require.NoError(t, err)

	count := 1000

	for i := 0; i < count; i++ {
		err = tx.UpsertAsync(&testDummyObject{
			ID:   int(i),
			Name: checkData + strconv.Itoa(i),
		}, func(err error) {
			require.NoError(t, err)
		})
		require.NoError(t, err)
	}

	cnt, err := tx.CommitWithCount()
	require.Equal(t, cnt, count)
	require.NoError(t, err)

	items, err := db.Query(testTlsNs).Sort("id", false).Exec().FetchAll()
	require.NoError(t, err)
	require.Equal(t, len(items), count)

	for i, item := range items {
		require.Equal(t, i, item.(*testDummyObject).ID)
		require.Equal(t, checkData+strconv.Itoa(i), item.(*testDummyObject).Name)
	}
}
