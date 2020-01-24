package ft

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
)

type BasicTestCase struct {
	Description       string   `json:"test_case_description"`
	AllDocuments      []string `json:"all_documents"`
	ExpectedDocuments []string `json:"documents_expected_to_match"`
	ValidQueries      []string `json:"valid_queries"`
	InvalidQueries    []string `json:"invalid_queries"`
}

type RankingTestCase struct {
	Description       string     `json:"test_case_description"`
	AllDocuments      []string   `json:"all_documents"`
	ExpectedDocuments []string   `json:"expected_documents_order"`
	AnyOrderClasses   [][]string `json:"any_order_classes"`
	Queries           []string   `json:"queries"`
}

func ParseBasicTestCases() []BasicTestCase {
	bytes, err := ioutil.ReadFile("specs/full_text_search_basic_test_data.json")
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	var result []BasicTestCase
	json.Unmarshal(bytes, &result)
	return result
}

func ParseRankingTestCases() []RankingTestCase {
	bytes, err := ioutil.ReadFile("specs/full_text_search_ranking_test_data.json")
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	var result []RankingTestCase
	json.Unmarshal(bytes, &result)
	return result
}
