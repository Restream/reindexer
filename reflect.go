package reindexer

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/restream/reindexer/bindings"
)

const (
	CollateNone    = bindings.CollateNone
	CollateASCII   = bindings.CollateASCII
	CollateUTF8    = bindings.CollateUTF8
	CollateNumeric = bindings.CollateNumeric

	IndexOptPK         = bindings.IndexOptPK
	IndexOptArray      = bindings.IndexOptArray
	IndexOptDense      = bindings.IndexOptDense
	IndexOptAppendable = bindings.IndexOptAppendable
)

func (db *Reindexer) createIndex(namespace string, st reflect.Type, subArray bool, reindexBasePath, jsonBasePath string, joined *map[string][]int) (err error) {

	if len(jsonBasePath) != 0 {
		jsonBasePath = jsonBasePath + "."
	}

	if len(reindexBasePath) != 0 {
		reindexBasePath = reindexBasePath + "."
	}

	if st.Kind() == reflect.Ptr {
		st = st.Elem()
	}

	for i := 0; i < st.NumField(); i++ {

		t := st.Field(i).Type
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
		// Get and parse tags
		tagsSlice := strings.Split(st.Field(i).Tag.Get("reindex"), ",")
		jsonPath := strings.Split(st.Field(i).Tag.Get("json"), ",")[0]

		if len(jsonPath) == 0 && !st.Field(i).Anonymous {
			jsonPath = st.Field(i).Name
		}
		jsonPath = jsonBasePath + jsonPath

		idxName, idxType, idxOpts := tagsSlice[0], "", ""
		if idxName == "-" {
			continue
		}

		if len(tagsSlice) > 1 {
			idxType = tagsSlice[1]
		}
		if len(tagsSlice) > 2 {
			idxOpts = tagsSlice[2]
		}

		if idxOpts == "pk" && strings.TrimSpace(idxName) == "" {
			return fmt.Errorf("No index name is specified for primary key in field %s", st.Field(i).Name)
		}

		reindexPath := reindexBasePath + idxName

		if idxOpts == "composite" {
			if t.Kind() != reflect.Struct || t.NumField() != 0 {
				return fmt.Errorf("'composite' tag allowed only on empty on structs: Invalid tags %v on field %s", tagsSlice, st.Field(i).Name)
			} else if err := db.binding.AddIndex(namespace, reindexPath, "", idxType, "composite", 0, CollateNumeric); err != nil {
				return err
			}
		} else if t.Kind() == reflect.Struct {
			if err := db.createIndex(namespace, t, subArray, reindexPath, jsonPath, joined); err != nil {
				return err
			}
		} else if (t.Kind() == reflect.Slice || t.Kind() == reflect.Array) &&
			(t.Elem().Kind() == reflect.Struct || (t.Elem().Kind() == reflect.Ptr && t.Elem().Elem().Kind() == reflect.Struct)) {
			// Check if field nested slice of struct
			if (idxOpts == "joined") && len(idxName) > 0 {
				(*joined)[tagsSlice[0]] = st.Field(i).Index
			} else if err := db.createIndex(namespace, t.Elem(), true, reindexPath, jsonPath, joined); err != nil {
				return err
			}
		} else if len(idxName) > 0 {
			collateMode := CollateNone
			switch idxOpts {
			case "collate_ascii":
				collateMode = CollateASCII
			case "collate_utf8":
				collateMode = CollateUTF8
			case "collate_numeric":
				collateMode = CollateNumeric
			}

			var opts bindings.IndexOptions
			opts.Array(t.Kind() == reflect.Slice || t.Kind() == reflect.Array || subArray)
			opts.PK(idxOpts == "pk")
			opts.Dense(idxOpts == "dense")
			opts.Appendable(idxOpts == "appendable")

			if fieldType, err := getFieldType(t); err != nil {
				return err
			} else if err = db.binding.AddIndex(
				namespace,
				reindexPath,
				jsonPath,
				idxType,
				fieldType,
				opts,
				collateMode,
			); err != nil {
				return err
			}
		}
	}
	return nil
}

func getFieldType(t reflect.Type) (string, error) {

	switch t.Kind() {
	case reflect.Bool:
		return "bool", nil
	case reflect.Int, reflect.Int8, reflect.Int32, reflect.Int16,
		reflect.Uint, reflect.Uint8, reflect.Uint32, reflect.Uint16:
		return "int", nil
	case reflect.Int64, reflect.Uint64:
		return "int64", nil
	case reflect.String:
		return "string", nil
	case reflect.Float32, reflect.Float64:
		return "double", nil
	case reflect.Struct:
		return "composite", nil
	case reflect.Array, reflect.Slice, reflect.Ptr:
		return getFieldType(t.Elem())
	}
	return "", errInvalidReflection
}

func getJoinedField(val reflect.Value, joined map[string][]int, name string) (ret reflect.Value) {

	if idx, ok := joined[name]; ok {
		ret = reflect.Indirect(reflect.Indirect(val).FieldByIndex(idx))
	}
	return ret
}
