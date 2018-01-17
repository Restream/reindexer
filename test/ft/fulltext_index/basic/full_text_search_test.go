package basic

import (
	"fmt"
	"sort"

	. "github.com/restream/reindexer/test/ft/specs"
	"github.com/restream/reindexer"
	_ "github.com/restream/reindexer/bindings/builtin"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("", func() {
	for _, testC := range ParseBasicTestCases() {
		// Need to make a local copy to use it in the closure below
		testCase := testC

		namespace := testCase.Description
		reindexDB := createReindexDbInstance(namespace)
		fillReindexWithData(reindexDB, namespace, testCase.AllDocuments)

		Context("Running test case: "+testCase.Description, func() {
			for _, validQ := range testCase.ValidQueries {
				// Need to make a local copy to use it in the closure below
				validQuery := validQ
				It("should match: "+validQuery, func() {
					dbItems, err := reindexDB.Query(namespace).
						WhereString("text_field", reindexer.EQ, validQuery).
						Exec().
						FetchAll()

					Expect(err).To(BeNil())
					expected := testCase.ExpectedDocuments
					actual := dbItemsToSliceOfDocuments(dbItems)
					sort.Strings(expected)
					sort.Strings(actual)
					Expect(actual).To(ConsistOf(expected))
				})
			}
			for _, invalidQ := range testCase.InvalidQueries {
				// Need to make a local copy to use it in the closure below
				invalidQuery := invalidQ
				It("shouldn't match: "+invalidQuery, func() {
					dbItems, err := reindexDB.Query(namespace).
						WhereString("text_field", reindexer.EQ, invalidQuery).
						Exec().
						FetchAll()

					Expect(err).To(BeNil())
					for _, document := range dbItemsToSliceOfDocuments(dbItems) {
						Expect(testCase.ExpectedDocuments).NotTo(ContainElement(document))
					}
				})
			}
		})
	}
})

type TextItem struct {
	ID        int    `reindex:"id,,pk"`
	TextField string `reindex:"text_field,fulltext"`
}

func createReindexDbInstance(namespace string) *reindexer.Reindexer {
	reindexDB := reindexer.NewReindex("builtin")
	err := reindexDB.NewNamespace(namespace, reindexer.DefaultNamespaceOptions(), TextItem{})
	if err != nil {
		panic(fmt.Errorf("Couldn't create namespace: "+namespace, err))
	}
	return reindexDB
}

func fillReindexWithData(reindexDB *reindexer.Reindexer, namespace string, documents []string) {
	nextId := 1
	for _, document := range documents {
		item := TextItem{
			ID:        nextId,
			TextField: document,
		}
		if _, err := reindexDB.Insert(namespace, &item); err != nil {
			panic(err)
		}
		nextId++
	}
}

func dbItemsToSliceOfDocuments(dbItems []interface{}) []string {
	result := []string{}

	for _, dbItem := range dbItems {
		item := dbItem.(*TextItem)
		result = append(result, item.TextField)
	}

	return result
}
