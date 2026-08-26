package ft

import (
	"strconv"
	"testing"

	"github.com/restream/reindexer/v5"
	_ "github.com/restream/reindexer/v5/bindings/builtin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testFtIndexCopyNs       = "ft_index_copy"
	testFtSynonymsFfterTxNs = "ft_synonyms_after_tx"
)

func fillTestItemsTx(namespace string, from int, count int, baseData string, rx *reindexer.Reindexer) {
	tx := rx.MustBeginTx(namespace)
	for i := from; i < from+count; i++ {
		if err := tx.Upsert(&TextItem{
			ID:        i,
			TextField: baseData + "_" + strconv.Itoa(i),
		}); err != nil {
			panic(err)
		}
	}
	tx.MustCommit()
}

func setNsCopyConfigs(namespace string, rx *reindexer.Reindexer) error {
	nsConfig := make([]reindexer.DBNamespacesConfig, 1)
	nsConfig[0].StartCopyPolicyTxSize = 10000
	nsConfig[0].StartCopyPolicyTxSize = 10
	nsConfig[0].StartCopyPolicyTxSize = 100000
	nsConfig[0].Namespace = namespace
	item := reindexer.DBConfigItem{
		Type:       "namespaces",
		Namespaces: &nsConfig,
	}
	return rx.Upsert(reindexer.ConfigNamespaceName, item)
}

func TestFTIndexCopy(t *testing.T) {
	rx, err := reindexer.NewReindex(*dsn)
	require.NoError(t, err)
	defer rx.Close()

	const ns = testFtIndexCopyNs

	const (
		dataCount   = 5000
		thrashCount = 30000
	)

	createReindexDbInstance(rx, ns, thrashCount+dataCount)
	err = setNsCopyConfigs(ns, rx)
	assert.NoError(t, err)

	fillTestItemsTx(ns, 0, dataCount, "data", rx)
	fillTestItemsTx(ns, dataCount, thrashCount, "trash", rx)

	dbItems := rx.Query(ns).
		WhereString("text_field", reindexer.EQ, "data_*", "").
		MustExec()
	assert.Equal(t, dataCount, dbItems.Count())
	assert.NoError(t, dbItems.Error())
	dbItems.Close()

	dbItems = rx.Query(ns).
		WhereString("text_field", reindexer.EQ, "trash_*", "").
		Exec()
	assert.Equal(t, thrashCount, dbItems.Count())
	assert.NoError(t, dbItems.Error())
	dbItems.Close()
}

func TestFTSynonymsAfterTx(t *testing.T) {
	rx, err := reindexer.NewReindex(*dsn)
	require.NoError(t, err)
	defer rx.Close()

	const ns = testFtSynonymsFfterTxNs

	const dataCount = 50000

	err = rx.OpenNamespace(ns, reindexer.DefaultNamespaceOptions(), TextItem{})
	assert.NoError(t, err)

	config := reindexer.DefaultFtFastConfig()
	config.Synonyms = []struct {
		Tokens       []string `json:"tokens"`
		Alternatives []string `json:"alternatives"`
	}{
		{
			[]string{"word"},
			[]string{"слово"},
		},
	}

	rx.DropIndex(ns, "text_field")
	err = rx.AddIndex(ns, reindexer.IndexDef{
		Name:      "text_field",
		JSONPaths: []string{"TextField"},
		Config:    config,
		IndexType: "text",
		FieldType: "string",
	})
	assert.NoError(t, err)

	fillReindexWithData(rx, ns, []string{"word", "слово"})

	dbItems, err := rx.Query(ns).
		WhereString("text_field", reindexer.EQ, "word", "").
		Exec().
		FetchAll()
	assert.NoError(t, err)
	assert.Equal(t, 2, len(dbItems))

	fillTestItemsTx(ns, 10, dataCount, "data", rx)

	dbItems, err = rx.Query(ns).
		WhereString("text_field", reindexer.EQ, "word", "").
		Exec().
		FetchAll()
	assert.NoError(t, err)
	assert.Equal(t, 2, len(dbItems))
}
