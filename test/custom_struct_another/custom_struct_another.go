package custom_struct_another

// TestItemCustom struct with same name but another package
type TestItemCustom struct {
	CustomUniqueField int
	ID                int `reindex:"id,,pk"`
	Year              int `reindex:"year,tree"`
	Genre             int64
	Name              string `reindex:"name"`
}

type TestItemCustom2 struct {
	CustomUniqueField int
	ID                int    `reindex:"id,,pk"`
	Name              string `reindex:"name"`
	EmptyInt          int    `json:"empty_int,omitempty"`
	Year              int    `reindex:"year,tree"`
	Genre             int64
}

type TestItemCustom3 struct {
	CustomUniqueField int
	EmptyInt          int    `json:"empty_int,omitempty"`
	Name              string `reindex:"name"`
	ID                int    `reindex:"id,,pk"`
	Year              int    `reindex:"year,tree"`
	Genre             int64
	Description       string `reindex:"description,fuzzytext"`
}

type TestItemCustom4 struct {
	CustomUniqueField int
	ID                int     `reindex:"id,,pk"`
	Rate              float64 `reindex:"rate,tree"`
	Year              int     `reindex:"year,tree"`
	Genre             int64
	Name              string `reindex:"name"`
	EmptyInt          int    `json:"empty_int,omitempty"`
	Description       string `reindex:"description,fuzzytext"`
}

type TestItemCustom5 struct {
	CustomUniqueField int
	ID                int     `reindex:"id,,pk"`
	Description       string  `reindex:"description,fuzzytext"`
	Rate              float64 `reindex:"rate,tree"`
	Year              int     `reindex:"year,tree"`
	Genre             int64
	Name              string  `reindex:"name"`
	EmptyInt          int     `json:"empty_int,omitempty"`
	ExchangeRate      float64 `json:"exchange_rate"`
}

type TestItemCustom6 struct {
	ExchangeRate      float64 `json:"exchange_rate"`
	PollutionRate     float32 `json:"pollution_rate"`
	CustomUniqueField int
	ID                int `reindex:"id,,pk"`
	Year              int `reindex:"year,tree"`
	Genre             int64
	Name              string  `reindex:"name"`
	EmptyInt          int     `json:"empty_int,omitempty"`
	Description       string  `reindex:"description,fuzzytext"`
	Rate              float64 `reindex:"rate,tree"`
}

type TestItemCustom7 struct {
	CustomUniqueField int
	ID                int `reindex:"id,,pk"`
	Year              int `reindex:"year,tree"`
	Genre             int64
	Name              string  `reindex:"name"`
	ExchangeRate      float64 `json:"exchange_rate"`
	PollutionRate     float32 `json:"pollution_rate"`
	IsDeleted         bool    `reindex:"isdeleted,-"`
	EmptyInt          int     `json:"empty_int,omitempty"`
	Description       string  `reindex:"description,fuzzytext"`
	Rate              float64 `reindex:"rate,tree"`
}
