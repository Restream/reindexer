package custom_struct_another

// TestItemCustom struct with same name but another package
type TestItemCustom struct {
	CustomUniqueField int
	ID                int `reindex:"id,,pk"`
	Year              int `reindex:"year,tree"`
	Genre             int64
	Name              string `reindex:"name"`
}
