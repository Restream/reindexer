package ft

import (
	"strconv"
	"testing"

	"github.com/restream/reindexer"
	_ "github.com/restream/reindexer/bindings/builtin"
	"github.com/stretchr/testify/assert"
)

func doSearchTest(t *testing.T, indexType string) {

	rx := reindexer.NewReindex(*dsn)
	defer rx.Close()

	for i, testC := range ParseBasicTestCases() {
		// Need to make a local copy to use it in the closure below
		testCase := testC

		namespace := "ft_" + strconv.Itoa(i)
		createReindexDbInstance(rx, namespace, indexType, 0) //
		fillReindexWithData(rx, namespace, testCase.AllDocuments)
		t.Run("Running test case: "+testCase.Description, func(t *testing.T) {
			for _, validQ := range testCase.ValidQueries {
				// Need to make a local copy to use it in the closure below
				validQuery := validQ
				t.Run("should match: "+validQuery, func(t *testing.T) {
					dbItems, err := rx.Query(namespace).
						WhereString("text_field", reindexer.EQ, validQuery, "").
						Exec().
						FetchAll()

					assert.NoError(t, err)
					expected := testCase.ExpectedDocuments
					actual := dbItemsToSliceOfDocuments(dbItems)
					for _, s := range expected {
						assert.Contains(t, actual, s)
					}

				})
			}
			for _, invalidQ := range testCase.InvalidQueries {
				// Need to make a local copy to use it in the closure below
				invalidQuery := invalidQ
				t.Run("shouldn't match: "+invalidQuery, func(t *testing.T) {
					dbItems, err := rx.Query(namespace).
						WhereString("text_field", reindexer.EQ, invalidQuery).
						Exec().
						FetchAll()

					assert.NoError(t, err)
					for _, document := range dbItemsToSliceOfDocuments(dbItems) {
						assert.NotContains(t, testCase.ExpectedDocuments, document)
					}
				})
			}
		})
	}
}

func fillTestItemsTx(namespace string, from int, count int, baseData string, rx *reindexer.Reindexer) {
	tx := rx.MustBeginTx(namespace)
	for i := from; i < from+count; i++ {
		if err := tx.Upsert(&TextItem{
			ID:        0 + i,
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

func doFTIndexCopy(t *testing.T, indexType string) {
	rx := reindexer.NewReindex(*dsn)
	defer rx.Close()

	dataCount := 5000
	thrashCount := 30000
	namespace := "ft_index_copy"
	createReindexDbInstance(rx, namespace, indexType, thrashCount+dataCount)
	err := setNsCopyConfigs(namespace, rx)
	assert.NoError(t, err)

	fillTestItemsTx(namespace, 0, dataCount, "data", rx)
	fillTestItemsTx(namespace, dataCount, thrashCount, "trash", rx)

	dbItems := rx.Query(namespace).
		WhereString("text_field", reindexer.EQ, "data_*", "").
		MustExec()
	assert.Equal(t, dataCount, dbItems.Count())
	assert.NoError(t, dbItems.Error())

	dbItems = rx.Query(namespace).
		WhereString("text_field", reindexer.EQ, "trash_*", "").
		Exec()
	assert.Equal(t, thrashCount, dbItems.Count())
	assert.NoError(t, dbItems.Error())
}

func TestFTFastSearch(t *testing.T) {
	doSearchTest(t, "text")
}
func TestFTFuzzySearch(t *testing.T) {
	doSearchTest(t, "fuzzytext")
}
func TestFTIndexCopy(t *testing.T) {
	doFTIndexCopy(t, "text")
}
