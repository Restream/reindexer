package reindexer

// FtFuzzyConfig configurarion of FuzzyFullText search index
type FtFuzzyConfig struct {
	// max proc geting from src reqest
	MaxSrcProc float64 `json:"max_src_proc"`
	// max proc geting from dst reqest
	//usualy maxDstProc = 100 -MaxSrcProc but it's not nessary
	MaxDstProc float64 `json:"max_dst_proc"`
	// increse proc when found pos that are near between  source and dst string (0.0001-2)
	PosSourceBoost float64 `json:"pos_source_boost"`
	// Minim coof for pos that are neaer in src and dst (0.0001-2)
	PosSourceDistMin float64 `json:"pos_source_dist_min"`
	// increse proc when found pos that are near in source string (0.0001-2)
	PosSourceDistBoost float64 `json:"pos_source_dist_boost"`
	// increse proc when found pos that are near in dst string (0.0001-2)
	PosDstBoost float64 `json:"pos_dst_boost"`
	// decrese proc when found  not full thregramm - only start and end (0.0001-2)
	StartDecreeseBoost float64 `json:"start_decreese_boost"`
	// base decrese proc when found  not full thregramm - only start and end (0.0001-2)
	StartDefaultDecreese float64 `json:"start_default_decreese"`
	// Min relevance to show reqest
	MinOkProc float64 `json:"min_ok_proc"`
	// size of gramm (1-10)- for example
	//terminator BufferSize=3 __t _te ter erm rmi ...
	//terminator BufferSize=4 __te _ter term ermi rmin
	BufferSize int `json:"buffer_size"`
	// size of space in start and end of gramm (0-9) - for example
	//terminator SpaceSize=2 __t _te ter   ... tor or_ r__
	//terminator SpaceSize=1 _te  ter  ... tor or_
	SpaceSize int `json:"space_size"`
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
	// List of objects of stop words. Words from this list will be ignored when building indexes
	// but can be included in search results in queries such as 'word*', 'word~' etc. if for the stop-word attribute is_morpheme is true
	StopWords []interface{} `json:"stop_words"`
	// Log level of full text search engine
	LogLevel int `json:"log_level"`
	// Extra symbols, which will be threated as parts of word to addition to letters and digits
	ExtraWordSymbols string `json:"extra_word_symbols"`
	// Config for subterm rank multiplier
	FtBaseRankingConfig *FtBaseRanking `json:"base_ranking,omitempty"`
}

func DefaultFtFuzzyConfig() FtFuzzyConfig {
	return FtFuzzyConfig{
		MaxSrcProc:           78,
		MaxDstProc:           22,
		PosSourceBoost:       1.5,
		PosSourceDistMin:     0.3,
		PosSourceDistBoost:   1.2,
		PosDstBoost:          1,
		StartDecreeseBoost:   1.2,
		StartDefaultDecreese: 0.7,
		MinOkProc:            10,
		BufferSize:           4,
		SpaceSize:            1,
		MergeLimit:           20000,
		Stemmers:             []string{"en", "ru"},
		EnableTranslit:       true,
		EnableKbLayout:       true,
		LogLevel:             0,
		ExtraWordSymbols:     "/-+",
		FtBaseRankingConfig:  &FtBaseRanking{FullMatch: 100, PrefixMin: 50, SuffixMin: 10, Typo: 85, TypoPenalty: 15, StemmerPenalty: 15, Kblayout: 90, Translit: 90, Synonyms: 95, Delimited: 80},
	}
}
