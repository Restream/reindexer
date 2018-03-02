package ranking

import (
	"fmt"

	"sort"

	"github.com/restream/reindexer"
	_ "github.com/restream/reindexer/bindings/builtin"
	. "github.com/restream/reindexer/test/ft/specs"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("", func() {
	totalSearchQuality := 0.0

	testCases := ParseRankingTestCases()
	for _, testC := range testCases {
		// Need to make a local copy to use it in the closure below
		testCase := testC

		namespace := testCase.Description
		reindexDB := createReindexDbInstance(namespace)
		fillReindexWithData(reindexDB, namespace, testCase.AllDocuments)

		Context("Running test case: "+testCase.Description, func() {
			for _, q := range testCase.Queries {
				// Need to make a local copy to use it in the closure below
				query := q
				It("should match: "+query, func() {
					dbItems, err := reindexDB.Query(namespace).
						WhereString("text_field", reindexer.EQ, query, "").
						Exec().
						FetchAll()

					Expect(err).To(BeNil())
					expected := testCase.ExpectedDocuments
					actual := dbItemsToSliceOfDocuments(dbItems)
					sort.Strings(expected)
					sort.Strings(actual)
					Expect(actual).To(ConsistOf(expected))

					quality := calculateQuality(testCase.ExpectedDocuments, dbItemsToSliceOfDocuments(dbItems), testCase.AnyOrderClasses)
					quality = quality * 100
					totalSearchQuality += quality
					fmt.Printf("\n%s\nQuality - %.2f %%\n", testCase.Description, quality)
				})
			}
		})
	}

	It("Total full text search Quality", func() {
		totalSearchQuality = totalSearchQuality / float64(len(testCases))
		fmt.Printf("\nTotal Quality - %.2f %%\n", totalSearchQuality)
	})
})

type TextItem struct {
	ID        int    `reindex:"id,,pk"`
	TextField string `reindex:"text_field,fulltext"`
}

func createReindexDbInstance(namespace string) *reindexer.Reindexer {
	reindexDB := reindexer.NewReindex("builtin")
	err := reindexDB.OpenNamespace(namespace, reindexer.DefaultNamespaceOptions(), TextItem{})
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

// This function calculates the quality of full text search based on
// Levenstein distance between itemsInExpectedOrder and itemsInActualOrder
// It also accepts a slice of equivalentClassesOfItems which represents classes of items that are equivalent to each other with respect to order
func calculateQuality(itemsInExpectedOrder []string, itemsInActualOrder []string, equivalentClassesOfItems [][]string) float64 {
	result := 0.0

	equivalentItemsMap := getEquivalentItemsMap(equivalentClassesOfItems)

	for i, expectedItem := range itemsInExpectedOrder {
		if expectedItem == itemsInActualOrder[i] {
			result += 1
		} else if elemIsInSlice(expectedItem, equivalentItemsMap[expectedItem]) {
			result += 1
		}
	}

	return result / float64(len(itemsInExpectedOrder))
}

func getEquivalentItemsMap(equivalentClassesOfItems [][]string) map[string][]string {
	result := map[string][]string{}

	for _, items := range equivalentClassesOfItems {
		for _, item := range items {
			result[item] = items
		}
	}

	return result
}

func elemIsInSlice(elem string, slice []string) bool {
	for _, sliceElem := range slice {
		if sliceElem == elem {
			return true
		}
	}
	return false
}
