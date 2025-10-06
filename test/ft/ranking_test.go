package ft

import (
	"fmt"
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/restream/reindexer/v5"
	_ "github.com/restream/reindexer/v5/bindings/builtin"
)

func doRankingTest(t *testing.T, indexType string) {

	totalSearchQuality := 0.0

	rx, err := reindexer.NewReindex(*dsn)
	require.NoError(t, err)
	defer rx.Close()

	testCases := ParseRankingTestCases()
	expectedQualities := ParseRankingQuality()
	newQualities := []RankingQuality{}
	for i := 0; i < len(testCases); i++ {
		newQualities = append(newQualities, RankingQuality{expectedQualities[i].Description, expectedQualities[i].FastRankingQuality, expectedQualities[i].FuzzRankingQuality})
	}

	for i, testC := range testCases {
		// Need to make a local copy to use it in the closure below
		testCase := testC

		namespace := "ft_rank_" + strconv.Itoa(i)
		createReindexDbInstance(rx, namespace, indexType, 0)
		fillReindexWithData(rx, namespace, testCase.AllDocuments)

		t.Run("Running test case: "+testCase.Description, func(t *testing.T) {
			for _, q := range testCase.Queries {
				// Need to make a local copy to use it in the closure below
				query := q
				t.Run("should match: "+query, func(t *testing.T) {
					dbItems, err := rx.Query(namespace).
						WhereString("text_field", reindexer.EQ, query, "").
						Exec().
						FetchAll()

					assert.NoError(t, err)
					expected := testCase.ExpectedDocuments
					actual := dbItemsToSliceOfDocuments(dbItems)
					assert.ElementsMatch(t, expected, actual)

					quality := calculateQuality(testCase.ExpectedDocuments, dbItemsToSliceOfDocuments(dbItems), testCase.AnyOrderClasses)
					quality = quality * 100
					totalSearchQuality += quality
					fmt.Printf("\n%s\nQuality - %.2f %%\n", testCase.Description, quality)

					if *qualityCheck {
						qualityRound2f := math.Round(quality*100) / 100

						k, is_found := getCasePositionInReference(testCase.Description, expectedQualities)
						if is_found {
							if indexType == "fuzzytext" {
								assert.GreaterOrEqualf(t, qualityRound2f, expectedQualities[k].FuzzRankingQuality, "Fuzz ranking quality has dropped")
								if qualityRound2f > expectedQualities[k].FuzzRankingQuality {
									newQualities[k].FuzzRankingQuality = qualityRound2f
								}
							} else {
								assert.GreaterOrEqualf(t, qualityRound2f, expectedQualities[k].FastRankingQuality, "Fast ranking quality has dropped")
								if qualityRound2f > expectedQualities[k].FastRankingQuality {
									newQualities[k].FastRankingQuality = qualityRound2f
								}
							}
						} else {
							t.Fail()
							t.Logf("Testcase <%s> not found in reference file", testCase.Description)
						}
						assert.Equal(t, len(testCases), len(expectedQualities), "Test cases number do not match reference file")
					}
				})
			}
		})
	}

	totalSearchQuality = totalSearchQuality / float64(len(testCases))
	fmt.Printf("\nTotal Quality - %.2f %%\n", totalSearchQuality)
	if *qualityCheck && *saveTestArtifacts {
		SaveRankingQuality(newQualities)
	}
}

func getCasePositionInReference(description string, slice []RankingQuality) (int, bool) {
	for k, value := range slice {
		if value.Description == description {
			return k, true
		}
	}
	return 0, false
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

func TestFTFastRankingTest(t *testing.T) {
	doRankingTest(t, "text")
}

func TestFTFuzzyRankingTest(t *testing.T) {
	doRankingTest(t, "fuzzytext")
}
