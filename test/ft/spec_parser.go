package ft

import (
	"embed"
	"encoding/json"
	"fmt"
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

type RankingQuality struct {
	Description        string  `json:"test_case_description"`
	FastRankingQuality float64 `json:"fast_ranking_quality"`
	FuzzRankingQuality float64 `json:"fuzz_ranking_quality"`
}

//go:embed specs/*
var ftSpecs embed.FS

func ParseBasicTestCases() []BasicTestCase {
	bytes, err := ftSpecs.ReadFile("specs/full_text_search_basic_test_data.json")
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	var result []BasicTestCase
	json.Unmarshal(bytes, &result)
	return result
}

func ParseRankingTestCases() []RankingTestCase {
	bytes, err := ftSpecs.ReadFile("specs/full_text_search_ranking_test_data.json")
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	var result []RankingTestCase
	json.Unmarshal(bytes, &result)
	return result
}

func ParseRankingQuality() []RankingQuality {
	bytes, err := ftSpecs.ReadFile("specs/full_text_search_ranking_quality.json")
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	var result []RankingQuality
	json.Unmarshal(bytes, &result)
	return result
}

func SaveRankingQuality(newQualities []RankingQuality) {
	file, _ := json.MarshalIndent(newQualities, "", " ")
	_ = os.WriteFile("new_ranking_quality.json", file, 0644)
}
