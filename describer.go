package reindexer

import (
	"strings"
)

// Map from cond name to index type
var queryTypes = map[string]int{
	"EQ":     EQ,
	"GT":     GT,
	"LT":     LT,
	"GE":     GE,
	"LE":     LE,
	"SET":    SET,
	"RANGE":  RANGE,
	"ANY":    ANY,
	"EMPTY":  EMPTY,
	"ALLSET": ALLSET,
}

func GetCondType(name string) (int, error) {
	cond, ok := queryTypes[strings.ToUpper(name)]
	if ok {
		return cond, nil
	} else {
		return 0, ErrCondType
	}
}

// Map from index type to cond name
var queryNames = map[int]string{
	EQ:    "EQ",
	GT:    "GT",
	LT:    "LT",
	GE:    "GE",
	LE:    "LE",
	SET:   "SET",
	RANGE: "RANGE",
	ANY:   "ANY",
	EMPTY: "EMPTY",
}

type NamespaceDescription struct {
	Name            string             `json:"name"`
	UpdatedUnixNano int64              `json:"updated_unix_nano"`
	Indices         []IndexDescription `json:"indexes"`
	StorageEnabled  bool               `json:"storage_enabled"`
	StorageOK       bool               `json:"storage_ok"`
	StoragePath     string             `json:"storage_path,omitempty"`
	StorageError    string             `json:"storage_error,omitempty"`
	ItemsCount      int                `json:"items_count,omitempty"`
}

type IndexDescription struct {
	Name       string   `json:"name"`
	FieldType  string   `json:"field_type"`
	IsArray    bool     `json:"is_array"`
	Sortable   bool     `json:"sortable"`
	PK         bool     `json:"pk"`
	Fulltext   bool     `json:"fulltext"`
	Conditions []string `json:"conditions"`
}
