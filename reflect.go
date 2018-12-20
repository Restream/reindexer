package reindexer

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"
	"unsafe"

	"github.com/restream/reindexer/bindings"
)

const (
	CollateNone    = bindings.CollateNone
	CollateASCII   = bindings.CollateASCII
	CollateUTF8    = bindings.CollateUTF8
	CollateNumeric = bindings.CollateNumeric
	CollateCustom  = bindings.CollateCustom
)

var collateModes = map[string]int{
	"collate_ascii":   CollateASCII,
	"collate_utf8":    CollateUTF8,
	"collate_numeric": CollateNumeric,
	"collate_custom":  CollateCustom,
}

type indexOptions struct {
	isArray     bool
	isAppenable bool
	isDense     bool
	isPk        bool
	isSparse    bool
}

func (db *Reindexer) parseIndex(namespace string, st reflect.Type, joined *map[string][]int) (indexDefs []bindings.IndexDef, err error) {

	if err = parse(&indexDefs, st, false, "", "", joined); err != nil {
		return nil, err
	}

	return indexDefs, nil
}

func parse(indexDefs *[]bindings.IndexDef, st reflect.Type, subArray bool, reindexBasePath, jsonBasePath string, joined *map[string][]int) (err error) {
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
			opts.isArray = true
		}

		if opts.isPk && strings.TrimSpace(idxName) == "" {
			return fmt.Errorf("No index name is specified for primary key in field %s", st.Field(i).Name)
		}

		if parseByKeyWord(&idxSettings, "composite") {
			if t.Kind() != reflect.Struct || t.NumField() != 0 {
				return fmt.Errorf("'composite' tag allowed only on empty on structs: Invalid tags %v on field %s", tagsSlice, st.Field(i).Name)
			}

			indexDef := makeIndexDef(parseCompositeName(reindexPath), parseCompositeJsonPaths(reindexPath), idxType, "composite", opts, CollateNone, "")
			if err := indexDefAppend(indexDefs, indexDef, opts.isAppenable); err != nil {
				return err
			}
		} else if t.Kind() == reflect.Struct {
			if err := parse(indexDefs, t, subArray, reindexPath, jsonPath, joined); err != nil {
				return err
			}
		} else if (t.Kind() == reflect.Slice || t.Kind() == reflect.Array) &&
			(t.Elem().Kind() == reflect.Struct || (t.Elem().Kind() == reflect.Ptr && t.Elem().Elem().Kind() == reflect.Struct)) {
			// Check if field nested slice of struct
			if parseByKeyWord(&idxSettings, "joined") && len(idxName) > 0 {
				(*joined)[tagsSlice[0]] = st.Field(i).Index
			} else if err := parse(indexDefs, t.Elem(), true, reindexPath, jsonPath, joined); err != nil {
				return err
			}
		} else if len(idxName) > 0 {
			collateMode, sortOrderLetters := parseCollate(&idxSettings)

			if fieldType, err := getFieldType(t); err != nil {
				return err
			} else {
				indexDef := makeIndexDef(reindexPath, []string{jsonPath}, idxType, fieldType, opts, collateMode, sortOrderLetters)
				if err := indexDefAppend(indexDefs, indexDef, opts.isAppenable); err != nil {
					return err
				}
			}
		}
		if len(idxSettings) > 0 {
			return fmt.Errorf("Unknown index settings are found: %v", idxSettings)
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

func parseOpts(idxSettingsBuf *[]string) indexOptions {
	newIdxSettingsBuf := make([]string, 0)

	var opts indexOptions

	for _, idxSetting := range *idxSettingsBuf {
		switch idxSetting {
		case "pk":
			opts.isPk = true
		case "dense":
			opts.isDense = true
		case "sparse":
			opts.isSparse = true
		case "appendable":
			opts.isAppenable = true
		default:
			newIdxSettingsBuf = append(newIdxSettingsBuf, idxSetting)
		}
	}

	*idxSettingsBuf = newIdxSettingsBuf

	return opts
}

func parseCompositeName(indexName string) string {
	indexConents := strings.Split(indexName, "=")
	if len(indexConents) > 1 {
		indexName = indexConents[1]
	}
	return indexName
}

func parseCompositeJsonPaths(indexName string) []string {

	indexConents := strings.Split(indexName, "=")
	return strings.Split(indexConents[0], "+")
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
	case reflect.Int8, reflect.Int32, reflect.Int16,
		reflect.Uint8, reflect.Uint32, reflect.Uint16:
		return "int", nil
	case reflect.Int, reflect.Uint:
		if unsafe.Sizeof(int(0)) == unsafe.Sizeof(int64(0)) {
			return "int64", nil
		} else {
			return "int", nil
		}
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

func makeIndexDef(index string, jsonPaths []string, indexType, fieldType string, opts indexOptions, collateMode int, sortOrder string) bindings.IndexDef {
	cm := ""
	switch collateMode {
	case bindings.CollateASCII:
		cm = "ascii"
	case bindings.CollateUTF8:
		cm = "utf8"
	case bindings.CollateNumeric:
		cm = "numeric"
	case bindings.CollateCustom:
		cm = "custom"
	}

	return bindings.IndexDef{
		Name:        index,
		JSONPaths:   jsonPaths,
		IndexType:   indexType,
		FieldType:   fieldType,
		IsArray:     opts.isArray,
		IsPK:        opts.isPk,
		IsDense:     opts.isDense,
		IsSparse:    opts.isSparse,
		CollateMode: cm,
		SortOrder:   sortOrder,
	}
}

func indexDefAppend(indexDefs *[]bindings.IndexDef, indexDef bindings.IndexDef, isAppendable bool) error {
	name := indexDef.Name

	var foundIndexPos int
	var foundIndexDef bindings.IndexDef

	indexDefExists := false
	for pos, indexDef := range *indexDefs {
		if (*indexDefs)[pos].Name == name {
			indexDefExists = true
			foundIndexDef = indexDef
			foundIndexPos = pos

			break
		}
	}

	if !indexDefExists {
		*indexDefs = append(*indexDefs, indexDef)

		return nil
	}

	if indexDef.IndexType != foundIndexDef.IndexType {
		return fmt.Errorf("Index %s has another type: %+v", name, indexDef)
	}

	if len(indexDef.JSONPaths) > 0 && indexDef.IndexType != "composite" {
		jsonPaths := foundIndexDef.JSONPaths
		isPresented := false
		for _, jsonPath := range jsonPaths {
			if jsonPath == indexDef.JSONPaths[0] {
				isPresented = true
				break
			}
		}

		if !isPresented {
			if !isAppendable {
				return fmt.Errorf("Index %s is not appendable", name)
			}

			foundIndexDef.JSONPaths = append(foundIndexDef.JSONPaths, indexDef.JSONPaths[0])
		}

		foundIndexDef.IsArray = true
		(*indexDefs)[foundIndexPos] = foundIndexDef
	}
	return nil
}
