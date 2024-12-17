//go:build tls_test

package reindexer

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/restream/reindexer/v4"
	_ "github.com/restream/reindexer/v4/bindings/cproto"
)

const (
	testNsTls = "test_tls_namespace"
)

func init() {
	tnamespaces[testNsTls] = TlsTestItem{}
}

type TlsTestItem struct {
	ID   int    `reindex:"id,,pk" json:"id"`
	Data string `reindex:"data" json:"data"`
}

func TestBaseTLS(t *testing.T) {
	db := reindexer.NewReindex("cprotos://127.0.0.1:6535/test" /*reindexer.WithTLSConfig(&tls.Config{})*/)
	require.NotNil(t, db)
	require.Nil(t, db.Status().Err)

	defer db.Close()

	checkData := "Some TLS Data"
	err := db.RegisterNamespace(testNsTls, reindexer.DefaultNamespaceOptions(), TlsTestItem{})
	require.Nil(t, err)

	tx, err := db.BeginTx(testNsTls)
	require.NoError(t, err)

	count := 1000

	for i := 0; i < count; i++ {
		err = tx.UpsertAsync(&TlsTestItem{
			ID:   int(i),
			Data: checkData + strconv.Itoa(i),
		}, func(err error) {
			require.NoError(t, err)
		})
		require.NoError(t, err)
	}

	cnt, err := tx.CommitWithCount()
	require.Equal(t, cnt, count)
	require.NoError(t, err)

	items, err := db.Query(testNsTls).Sort("id", false).Exec().FetchAll()
	require.NoError(t, err)
	require.Equal(t, len(items), count)

	for i, item := range items {
		require.Equal(t, i, item.(*TlsTestItem).ID)
		require.Equal(t, checkData+strconv.Itoa(i), item.(*TlsTestItem).Data)
	}
}
