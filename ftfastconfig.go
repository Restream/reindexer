package reindexer

type FtFastFieldConfig struct {
	FieldName string `json:"field_name"`
	// boost of bm25 ranking. default value 1.
	Bm25Boost float64 `json:"bm25_boost"`
	// weight of bm25 rank in final rank.
	// 0: bm25 will not change final rank.
	// 1: bm25 will affect to final rank in 0 - 100% range
	Bm25Weight float64 `json:"bm25_weight"`
	// boost of search query term length. default value 1
	TermLenBoost float64 `json:"term_len_boost"`
	// weight of search query term length in final rank.
	// 0: term length will not change final rank.
	// 1: term length will affect to final rank in 0 - 100% range
	TermLenWeight float64 `json:"term_len_weight"`
	// boost of search query term position. default value 1
	PositionBoost float64 `json:"position_boost"`
	// weight of search query term position in final rank.
	// 0: term position will not change final rank.
	// 1: term position will affect to final rank in 0 - 100% range
	PositionWeight float64 `json:"position_weight"`
}

// FtFastConfig configurarion of FullText search index
type FtFastConfig struct {
	// boost of bm25 ranking. default value 1.
	Bm25Boost float64 `json:"bm25_boost"`
	// weight of bm25 rank in final rank.
	// 0: bm25 will not change final rank.
	// 1: bm25 will affect to final rank in 0 - 100% range
	Bm25Weight float64 `json:"bm25_weight"`
	// boost of search query term distance in found document. default vaule 1
	DistanceBoost float64 `json:"distance_boost"`
	// weight of search query terms distance in found document in final rank.
	// 0: distance will not change final rank.
	// 1: distance will affect to final rank in 0 - 100% range
	DistanceWeight float64 `json:"distance_weight"`
	// boost of search query term length. default value 1
	TermLenBoost float64 `json:"term_len_boost"`
	// weight of search query term length in final rank.
	// 0: term length will not change final rank.
	// 1: term length will affect to final rank in 0 - 100% range
	TermLenWeight float64 `json:"term_len_weight"`
	// boost of search query term position. default value 1
	PositionBoost float64 `json:"position_boost"`
	// weight of search query term position in final rank.
	// 0: term position will not change final rank.
	// 1: term position will affect to final rank in 0 - 100% range
	PositionWeight float64 `json:"position_weight"`
	// Boost of full match of search phrase with doc
	FullMatchBoost float64 `json:"full_match_boost"`
	// Relevancy step of partial match: relevancy = kFullMatchProc - partialMatchDecrease * (non matched symbols) / (matched symbols)
	// For example: partialMatchDecrease: 15, word in index 'terminator', pattern 'termin'. matched: 6 symbols, unmatched: 4. relevancy = 100 - (15*4)/6 = 80
	PartialMatchDecrease int `json:"partial_match_decrease"`
	// Minimum rank of found documents
	MinRelevancy float64 `json:"min_relevancy"`
	// Maximum possible typos in word.
	// 0: typos is disabled, words with typos will not match
	// N: words with N possible typos will match
	// It is not recommended to set more than 2 possible typo: It will serously increase RAM usage, and decrease search speed
	MaxTypos int `json:"max_typos"`
	// Maximum word length for building and matching variants with typos. Default value is 15
	MaxTypoLen int `json:"max_typo_len"`
	// Maximum commit steps - set it 1 for always full rebuild - it can be from 1 to 500
	MaxRebuildSteps int `json:"max_rebuild_steps"`
	// Maximum words in one commit - it can be from 5 to DOUBLE_MAX
	MaxStepSize int `json:"max_step_size"`
	// Maximum documents which will be processed in merge query results
	// Default value is 20000. Increasing this value may refine ranking
	// of queries with high frequency words
	MergeLimit int `json:"merge_limit"`
	// List of used stemmers
	Stemmers []string `json:"stemmers"`
	// Enable translit variants processing
	EnableTranslit bool `json:"enable_translit"`
	// Enable wrong keyboard layout variants processing
	EnableKbLayout bool `json:"enable_kb_layout"`
	// List of stop words. Words from this list will be ignored in documents and queries
	StopWords []string `json:"stop_words"`
	// List of synonyms for replacement
	Synonyms []struct {
		// List source tokens in query, which will be replaced with alternatives
		Tokens []string `json:"tokens"`
		// List of alternatives, which will be used for search documents
		Alternatives []string `json:"alternatives"`
	} `json:"synonyms"`
	// Log level of full text search engine
	LogLevel int `json:"log_level"`
	// Enable search by numbers as words and backwards
	EnableNumbersSearch bool `json:"enable_numbers_search"`
	// Enable auto index warmup after atomic namespace copy on transaction
	EnableWarmupOnNsCopy bool `json:"enable_warmup_on_ns_copy"`
	// Extra symbols, which will be threated as parts of word to addition to letters and digits
	ExtraWordSymbols string `json:"extra_word_symbols"`
	// Ratio of summation of ranks of match one term in several fields
	SumRanksByFieldsRatio float64 `json:"sum_ranks_by_fields_ratio"`
	// Max number of highlighted areas for each field in each document (for snippet() and highlight()). '-1' means unlimited
	MaxAreasInDoc int `json:"max_areas_in_doc"`
	// Max total number of highlighted areas in ft result, when result still remains cacheable. '-1' means unlimited
	MaxTotalAreasToCache int `json:"max_total_areas_to_cache"`
	// Configuration for certain field
	FieldsCfg []FtFastFieldConfig `json:"fields,omitempty"`
	// Optimize the index by memory or by cpu
	Optimization string `json:"optimization,omitempty"`
}

func DefaultFtFastConfig() FtFastConfig {
	return FtFastConfig{
		Bm25Boost:             1.0,
		Bm25Weight:            0.1,
		DistanceBoost:         1.0,
		DistanceWeight:        0.5,
		TermLenBoost:          1.0,
		TermLenWeight:         0.3,
		PositionBoost:         1.0,
		PositionWeight:        0.1,
		FullMatchBoost:        1.1,
		PartialMatchDecrease:  15,
		MinRelevancy:          0.05,
		MaxTypos:              2,
		MaxTypoLen:            15,
		MaxRebuildSteps:       50,
		MaxStepSize:           4000,
		MergeLimit:            20000,
		Stemmers:              []string{"en", "ru"},
		EnableTranslit:        true,
		EnableKbLayout:        true,
		LogLevel:              0,
		ExtraWordSymbols:      "/-+",
		SumRanksByFieldsRatio: 0.0,
		MaxAreasInDoc:         5,
		MaxTotalAreasToCache:  -1,
		Optimization:          "Memory",
	}
}

func DefaultFtFastFieldConfig(fieldName string) FtFastFieldConfig {
	return FtFastFieldConfig{
		FieldName:      fieldName,
		Bm25Boost:      1.0,
		Bm25Weight:     0.1,
		TermLenBoost:   1.0,
		TermLenWeight:  0.3,
		PositionBoost:  1.0,
		PositionWeight: 0.1,
	}
}
