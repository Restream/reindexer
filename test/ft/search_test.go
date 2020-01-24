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
		createReindexDbInstance(rx, namespace, indexType) //
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

func TestFTFastSearch(t *testing.T) {
	doSearchTest(t, "text")
}
func TestFTFuzzySearch(t *testing.T) {
	doSearchTest(t, "fuzzytext")
}
