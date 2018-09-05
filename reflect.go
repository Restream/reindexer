package reindexer

import (
	"bytes"
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
	CollateCustom  = bindings.CollateCustom

	IndexOptPK         = bindings.IndexOptPK
	IndexOptArray      = bindings.IndexOptArray
	IndexOptDense      = bindings.IndexOptDense
	IndexOptSparse     = bindings.IndexOptSparse
	IndexOptAppendable = bindings.IndexOptAppendable
)

var collateModes = map[string]int{
	"collate_ascii":   CollateASCII,
	"collate_utf8":    CollateUTF8,
	"collate_numeric": CollateNumeric,
	"collate_custom":  CollateCustom,
}

func (db *Reindexer) parseIndex(namespace string, st reflect.Type, subArray bool, reindexBasePath, jsonBasePath string, joined *map[string][]int) (err error) {

	if len(jsonBasePath) != 0 && !strings.HasSuffix(jsonBasePath, ".") {
		jsonBasePath = jsonBasePath + "."
	}

	if len(reindexBasePath) != 0 && !strings.HasSuffix(reindexBasePath, ".") {
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
		tagsSlice := strings.SplitN(st.Field(i).Tag.Get("reindex"), ",", 3)
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

		reindexPath := reindexBasePath + idxName

		idxSettings := splitOptions(idxOpts)

		opts := parseOpts(&idxSettings)
		if t.Kind() == reflect.Slice || t.Kind() == reflect.Array || subArray {
			opts.Array(true)
		}

		if opts.IsPK() && strings.TrimSpace(idxName) == "" {
			return fmt.Errorf("No index name is specified for primary key in field %s", st.Field(i).Name)
		}

		if parseByKeyWord(&idxSettings, "composite") {
			if t.Kind() != reflect.Struct || t.NumField() != 0 {
				return fmt.Errorf("'composite' tag allowed only on empty on structs: Invalid tags %v on field %s", tagsSlice, st.Field(i).Name)
			} else if err := db.binding.AddIndex(namespace, reindexPath, "", idxType, "composite", opts, CollateNone, ""); err != nil {
				return err
			}
		} else if t.Kind() == reflect.Struct {
			if err := db.parseIndex(namespace, t, subArray, reindexPath, jsonPath, joined); err != nil {
				return err
			}
		} else if (t.Kind() == reflect.Slice || t.Kind() == reflect.Array) &&
			(t.Elem().Kind() == reflect.Struct || (t.Elem().Kind() == reflect.Ptr && t.Elem().Elem().Kind() == reflect.Struct)) {
			// Check if field nested slice of struct
			if parseByKeyWord(&idxSettings, "joined") && len(idxName) > 0 {
				(*joined)[tagsSlice[0]] = st.Field(i).Index
			} else if err := db.parseIndex(namespace, t.Elem(), true, reindexPath, jsonPath, joined); err != nil {
				return err
			}
		} else if len(idxName) > 0 {
			collateMode, sortOrderLetters := parseCollate(&idxSettings)

			if len(idxSettings) > 0 {
				return fmt.Errorf("Unknown index settings are found: %v", idxSettings)
			}

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
				sortOrderLetters,
			); err != nil {
				return err
			}
		}
	}
	return nil
}

func splitOptions(str string) []string {
	words := make([]string, 0)

	var word bytes.Buffer

	strLen := len(str)

	for i := 0; i < strLen; i++ {
		if str[i] == '\\' && i < strLen-1 && str[i+1] == ',' {
			word.WriteByte(str[i+1])
			i++
			continue
		}

		if str[i] == ',' {
			words = append(words, word.String())
			word.Reset()
			continue
		}

		word.WriteByte(str[i])

		if i == strLen-1 {
			words = append(words, word.String())
			word.Reset()
			continue
		}
	}

	return words
}

func parseOpts(idxSettingsBuf *[]string) bindings.IndexOptions {
	newIdxSettingsBuf := make([]string, 0)

	var indexOptions bindings.IndexOptions

	for _, idxSetting := range *idxSettingsBuf {
		switch idxSetting {
		case "pk":
			indexOptions.PK(true)
		case "dense":
			indexOptions.Dense(true)
		case "sparse":
			indexOptions.Sparse(true)
		case "appendable":
			indexOptions.Appendable(true)
		default:
			newIdxSettingsBuf = append(newIdxSettingsBuf, idxSetting)
		}
	}

	*idxSettingsBuf = newIdxSettingsBuf

	return indexOptions
}

func parseCollate(idxSettingsBuf *[]string) (int, string) {
	newIdxSettingsBuf := make([]string, 0)

	collateMode := CollateNone
	var sortOrderLetters string

	for _, idxSetting := range *idxSettingsBuf {
		// split by "=" for k-v collate setting
		kvIdxSettings := strings.SplitN(idxSetting, "=", 2)

		if newCollateMode, ok := collateModes[kvIdxSettings[0]]; ok {
			if collateMode != CollateNone {
				panic(fmt.Errorf("Collate mode is already set to %d. Misunderstanding %s", collateMode, idxSetting))
			}

			collateMode = newCollateMode

			if len(kvIdxSettings) == 2 {
				sortOrderLetters = kvIdxSettings[1]
			}

			continue
		}

		newIdxSettingsBuf = append(newIdxSettingsBuf, idxSetting)
	}

	*idxSettingsBuf = newIdxSettingsBuf

	return collateMode, sortOrderLetters
}

func parseByKeyWord(idxSettingsBuf *[]string, keyWord string) bool {
	newIdxSettingsBuf := make([]string, 0)

	isPresented := false
	for _, idxSetting := range *idxSettingsBuf {
		if strings.Compare(idxSetting, keyWord) == 0 {
			isPresented = true
			continue
		}

		newIdxSettingsBuf = append(newIdxSettingsBuf, idxSetting)
	}

	*idxSettingsBuf = newIdxSettingsBuf

	return isPresented
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
