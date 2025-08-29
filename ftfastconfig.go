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

type FtTyposDetailedConfig struct {
	// Maximum distance between symbols in initial and target words to perform substitution
	// Values range: [-1,100]
	// Default: 0
	MaxTypoDistance int `json:"max_typo_distance"`
	// Maximum distance between same symbols in initial and target words to perform substitution (to handle cases, when two symbols were switched with each other)
	// Values range: [-1,100]
	// Default: 1
	MaxSymbolPermutationDistance int `json:"max_symbol_permutation_distance"`
	// Maximum number of symbols, which may be removed from the initial term to transform it into the result word
	// Values range: [-1,2]
	// Default: 2
	MaxMissingLetters int `json:"max_missing_letters"`
	// Maximum number of symbols, which may be added to the initial term to transform it into the result word
	// Values range: [-1,2]
	// Default: 2
	MaxExtraLetters int `json:"max_extra_letters"`
}

type FtBaseRanking struct {
	// Relevancy of full word match
	// Values range: [0,500]
	// Default: 100
	FullMatch int `json:"full_match_proc"`
	// Minimum relevancy of prefix word match.
	// Values range: [0,500]
	// Default: 50
	PrefixMin int `json:"prefix_min_proc"`
	// Minimum relevancy of suffix word match.
	// Values range: [0,500]
	// Default: 10
	SuffixMin int `json:"suffix_min_proc"`
	// Base relevancy of typo match
	// Values range: [0,500]
	// Default: 85
	Typo int `json:"base_typo_proc"`
	// Extra penalty for each word's permutation (addition/deletion of the symbol) in typo algorithm
	// Values range: [0,500]
	// Default: 15
	TypoPenalty int `json:"typo_proc_penalty"`
	// Penalty for the variants, created by stemming
	// Values range: [0,500]
	// Default: 15
	StemmerPenalty int `json:"stemmer_proc_penalty"`
	// Relevancy of the match in incorrect kblayout
	// Values range: [0,500]
	// Default: 90
	Kblayout int `json:"kblayout_proc"`
	// Relevancy of the match in translit
	// Values range: [0,500]
	// Default: 90
	Translit int `json:"translit_proc"`
	// Relevancy of the synonym match
	// Values range: [0,500]
	// Default: 95
	Synonyms int `json:"synonyms_proc"`
	// Relevancy of the delimited word part match
	// Values range: [0,500]
	// Default: 80
	Delimited int `json:"delimited_proc"`
}

type StopWord struct {
	Word       string `json:"word"`
	IsMorpheme bool   `json:"is_morpheme"`
}

type Bm25ConfigType struct {
	// Coefficient k1 in the formula for calculating bm25
	Bm25k1 float64 `json:"bm25_k1"`
	// Coefficient b in the formula for calculating bm25
	Bm25b float64 `json:"bm25_b"`
	// Formula for calculating document relevance (rx, classic, word_count)
	Bm25Type string `json:"bm25_type"`
}

// FtFastConfig configuration of FullText search index
type FtFastConfig struct {
	// boost of bm25 ranking. default value 1.
	Bm25Boost float64 `json:"bm25_boost"`
	// weight of bm25 rank in final rank.
	// 0: bm25 will not change final rank.
	// 1: bm25 will affect to final rank in 0 - 100% range
	Bm25Weight float64 `json:"bm25_weight"`
	// boost of search query term distance in found document. default value 1
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
	// Values range: [0,4]
	// Default: 2
	// It is not recommended to set more than 2 possible typo: It will seriously increase RAM usage, and decrease search speed
	MaxTypos int `json:"max_typos"`
	// Maximum word length for building and matching variants with typos. Default value is 15
	MaxTypoLen int `json:"max_typo_len"`
	// Config for more precise typos algorithm tuning
	TyposDetailedConfig *FtTyposDetailedConfig `json:"typos_detailed_config,omitempty"`
	// Maximum commit steps - set it 1 for always full rebuild - it can be from 1 to 500
	MaxRebuildSteps int `json:"max_rebuild_steps"`
	// Maximum words in one commit - it can be from 5 to DOUBLE_MAX
	MaxStepSize int `json:"max_step_size"`
	// Maximum documents which will be processed in merge query results
	// Default value is 20000. Increasing this value may refine ranking
	// of queries with high frequency words
	MergeLimit int `json:"merge_limit"`
	// List of symbol types for keeping diacritic symbols (acc/accent, ara/arabic, heb/hebrew or cyr/cyrillic)
	KeepDiacritics []string `json:"keep_diacritics"`
	// List of used stemmers
	Stemmers []string `json:"stemmers"`
	// Enable translit variants processing
	EnableTranslit bool `json:"enable_translit"`
	// Enable wrong keyboard layout variants processing
	EnableKbLayout bool `json:"enable_kb_layout"`
	// List of objects of stop words. Words from this list will be ignored when building indexes
	// but can be included in search results in queries such as 'word*', 'word~' etc. if for the stop-word attribute is_morpheme is true.
	// The list item can be either a reindexer.StopWord, or string
	StopWords []interface{} `json:"stop_words"`
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
	// *DEPRECATED* - all of the fulltex indexes will perform commit/warmup after copying transaction
	// Enable auto index warmup after atomic namespace copy on transaction
	EnableWarmupOnNsCopy bool `json:"enable_warmup_on_ns_copy"`
	// Extra symbols, which will be treated as parts of word to addition to letters and digits
	ExtraWordSymbols string `json:"extra_word_symbols"`
	// Symbols, which will be treated as word part delimiters
	WordPartDelimiters string `json:"word_part_delimiters"`
	// Min word part size for indexing and searching
	MinWordPartSize int `json:"min_word_part_size"`
	// Ratio of summation of ranks of match one term in several fields
	SumRanksByFieldsRatio float64 `json:"sum_ranks_by_fields_ratio"`
	// Max number of highlighted areas for each field in each document (for snippet() and highlight()). '-1' means unlimited
	MaxAreasInDoc int `json:"max_areas_in_doc"`
	// Max total number of highlighted areas in ft result, when result still remains cacheable. '-1' means unlimited
	MaxTotalAreasToCache int `json:"max_total_areas_to_cache"`
	// Configuration for certain field
	FieldsCfg []FtFastFieldConfig `json:"fields,omitempty"`
	// Optimize the index by memory or by cpu. Default 'memory'.
	// 'memory': compressed vector of document identifiers
	// 'cpu':  uncompressed vector of document identifiers
	Optimization string `json:"optimization,omitempty"`
	// Enable to execute others queries before the ft query
	EnablePreselectBeforeFt bool `json:"enable_preselect_before_ft"`
	// Config for subterm rank multiplier
	FtBaseRankingConfig *FtBaseRanking `json:"base_ranking,omitempty"`
	// Config for document ranking
	Bm25Config *Bm25ConfigType `json:"bm25_config,omitempty"`
	// Text tokenization algorithm. Default 'fast'.
	// 'fast' :    splits text by spaces, special characters and unsupported UTF-8 symbols.
	//             Each token is a combination of letters from supported UTF-8 subset, numbers and extra word symbols.
	// 'mmseg_cn': algorithm based on friso mmseg for Chinese and English
	SplitterType string `json:"splitter,omitempty"`
}

func DefaultFtFastConfig() FtFastConfig {
	return FtFastConfig{
		Bm25Boost:               1.0,
		Bm25Weight:              0.1,
		DistanceBoost:           1.0,
		DistanceWeight:          0.5,
		TermLenBoost:            1.0,
		TermLenWeight:           0.3,
		PositionBoost:           1.0,
		PositionWeight:          0.1,
		FullMatchBoost:          1.1,
		PartialMatchDecrease:    15,
		MinRelevancy:            0.05,
		MaxTypos:                2,
		MaxTypoLen:              15,
		TyposDetailedConfig:     &FtTyposDetailedConfig{MaxTypoDistance: 0, MaxSymbolPermutationDistance: 1, MaxExtraLetters: 2, MaxMissingLetters: 2},
		MaxRebuildSteps:         50,
		MaxStepSize:             4000,
		MergeLimit:              20000,
		KeepDiacritics:          []string{},
		Stemmers:                []string{"en", "ru"},
		EnableTranslit:          true,
		EnableKbLayout:          true,
		LogLevel:                0,
		ExtraWordSymbols:        "-/+_`'",
		WordPartDelimiters:      "-/+_`'",
		MinWordPartSize:         3,
		SumRanksByFieldsRatio:   0.0,
		MaxAreasInDoc:           5,
		MaxTotalAreasToCache:    -1,
		Optimization:            "Memory",
		EnablePreselectBeforeFt: false,
		FtBaseRankingConfig:     &FtBaseRanking{FullMatch: 100, PrefixMin: 50, SuffixMin: 10, Typo: 85, TypoPenalty: 15, StemmerPenalty: 15, Kblayout: 90, Translit: 90, Synonyms: 95, Delimited: 80},
		Bm25Config:              &Bm25ConfigType{Bm25k1: 2.0, Bm25b: 0.75, Bm25Type: "rx_bm25"},
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
