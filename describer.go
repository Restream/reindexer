package reindexer

import (
	"fmt"
	"reflect"
	"time"
)

type NamespaceDescription struct {
	Name           string             `json:"name"`
	UpdateAt       time.Time          `json:"update_at"`
	Indices        []IndexDescription `json:"indexes"`
	StorageEnabled bool               `json:"storage_enabled"`
	StorageOK      bool               `json:"storage_ok"`
	StoragePath    string             `json:"storage_path,omitempty"`
	StorageError   string             `json:"storage_error,omitempty"`
	ItemsCount     int                `json:"items_count,omitempty"`
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

func condsByType(indexType int) []string {

	switch indexType {
	case IndexTree, IndexInt, IndexDouble, IndexInt64, IndexHash, IndexIntHash, IndexInt64Hash,
		IndexIntStore, IndexStrStore, IndexInt64Store, IndexDoubleStore:
		return []string{"SET", "EQ", "ANY", "EMPTY", "LT", "LE", "GT", "GE", "RANGE"}
	case IndexBool:
		return []string{"SET", "EQ", "ANY", "EMPTY"}
	case IndexFullText, IndexNewFullText:
		return []string{"EQ"}
	}

	return nil
}

func describeIndices(target *[]IndexDescription, fields []reindexerField) {
	for _, field := range fields {
		if len(field.subField) > 0 {
			describeIndices(target, field.subField)
			continue
		}

		if field.idxName == "" || field.isJoined {
			continue
		}

		index := IndexDescription{
			Name:       field.idxName,
			FieldType:  field.fieldType.String(),
			PK:         field.isPK,
			Sortable:   field.idxType == IndexTree || field.idxType == IndexInt || field.idxType == IndexDouble || field.idxType == IndexInt64,
			Fulltext:   field.idxType == IndexFullText || field.idxType == IndexNewFullText,
			Conditions: condsByType(field.idxType),
		}

		if field.fieldType == reflect.Array || field.fieldType == reflect.Slice {
			index.FieldType = field.elemType.String()
			index.IsArray = true
		}
		found := false
		for i := 0; i < len(*target); i++ {
			if (*target)[i].Name == field.idxName {
				(*target)[i].IsArray = true
				found = true
			}
		}
		if !found {
			*target = append(*target, index)
		}
	}
}

func (db *Reindexer) DescribeNamespace(namespace string) (*NamespaceDescription, error) {
	nsDesc, ok := db.ns[namespace]
	if !ok {
		return nil, fmt.Errorf("rq: '%s' namespace not found", namespace)
	}

	q := db.Query(namespace).Limit(1).ReqTotal()
	q.Exec()

	ns := &NamespaceDescription{
		Name:           nsDesc.name,
		UpdateAt:       nsDesc.UpdatedAt,
		StorageEnabled: len(db.storagePath) != 0,
		StorageOK:      nsDesc.storageErr == nil,
		StoragePath:    db.storagePath,
		ItemsCount:     q.GetTotal(),
	}
	if nsDesc.storageErr != nil {
		ns.StorageError = nsDesc.storageErr.Error()
	}

	describeIndices(&ns.Indices, nsDesc.fields)

	return ns, nil
}

func (db *Reindexer) DescribeNamespaces() ([]*NamespaceDescription, error) {
	result := []*NamespaceDescription{}

	for namespace := range db.ns {
		ns, err := db.DescribeNamespace(namespace)
		if err != nil {
			return nil, err
		}

		result = append(result, ns)
	}

	return result, nil
}
