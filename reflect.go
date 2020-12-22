package reindexer

import (
	"bytes"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"unsafe"

	"github.com/restream/reindexer/bindings"
	"github.com/restream/reindexer/cjson"
	"github.com/restream/reindexer/jsonschema"
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
	rtreeType   string
}

func parseRxTags(field reflect.StructField) (idxName string, idxType string, expireAfter string, idxSettings []string) {
	tagsSlice := strings.SplitN(field.Tag.Get("reindex"), ",", 3)
	var idxOpts string
	idxName, idxType, expireAfter, idxOpts = tagsSlice[0], "", "", ""

	if len(tagsSlice) > 1 {
		idxType = tagsSlice[1]
	}
	if len(tagsSlice) > 2 {
		if idxType == "ttl" {
			expireAfter = strings.SplitN(tagsSlice[2], "=", 2)[1]
		} else {
			idxOpts = tagsSlice[2]
		}
	}
	idxSettings = splitOptions(idxOpts)
	return
}

func parseIndexes(namespace string, st reflect.Type, joined *map[string][]int) (indexDefs []bindings.IndexDef, err error) {
	if err = parseIndexesImpl(&indexDefs, st, false, "", "", joined, nil); err != nil {
		return nil, err
	}

	return indexDefs, nil
}

func parseSchema(namespace string, st reflect.Type) *bindings.SchemaDef {
	reflector := &jsonschema.Reflector{}
	reflector.FieldIsInScheme = func(f reflect.StructField) bool {
		_, _, _, idxSettings := parseRxTags(f)
		if parseByKeyWord(&idxSettings, "joined") || parseByKeyWord(&idxSettings, "composite") {
			return false
		}
		return true
	}
	reflector.DoNotReference = true
	reflector.FullyQualifyTypeNames = true
	if schema := reflector.ReflectFromType(st); schema != nil {
		schemaDef := bindings.SchemaDef(*schema)
		return &schemaDef
	}
	return nil

}

func parseIndexesImpl(indexDefs *[]bindings.IndexDef, st reflect.Type, subArray bool, reindexBasePath, jsonBasePath string, joined *map[string][]int, parsed *map[string]bool) (err error) {
	if len(jsonBasePath) != 0 && !strings.HasSuffix(jsonBasePath, ".") {
		jsonBasePath = jsonBasePath + "."
	}

	if len(reindexBasePath) != 0 && !strings.HasSuffix(reindexBasePath, ".") {
		reindexBasePath = reindexBasePath + "."
	}

	if st.Kind() == reflect.Ptr {
		st = st.Elem()
	}

	isParsed, parsed := cjson.IsStructParsed(st, parsed)
	if isParsed {
		return nil
	}

	for i := 0; i < st.NumField(); i++ {
		t := st.Field(i).Type
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
		// Get and parse tags
		jsonPath := strings.Split(st.Field(i).Tag.Get("json"), ",")[0]

		if len(jsonPath) == 0 && !st.Field(i).Anonymous {
			jsonPath = st.Field(i).Name
		}
		jsonPath = jsonBasePath + jsonPath

		idxName, idxType, expireAfter, idxSettings := parseRxTags(st.Field(i))
		if idxName == "-" {
			continue
		}
		reindexPath := reindexBasePath + idxName

		opts := parseOpts(&idxSettings)
		if t.Kind() == reflect.Slice || t.Kind() == reflect.Array || subArray {
			opts.isArray = true
		}

		if opts.isPk && strings.TrimSpace(idxName) == "" {
			return fmt.Errorf("No index name is specified for primary key in field %s", st.Field(i).Name)
		}

		if idxType == "rtree" {
			if t.Kind() != reflect.Array || t.Len() != 2 || t.Elem().Kind() != reflect.Float64 {
				return fmt.Errorf("'rtree' index allowed only for [2]float64 field type")
			}
		}
		if parseByKeyWord(&idxSettings, "composite") {
			if t.Kind() != reflect.Struct || t.NumField() != 0 {
				return fmt.Errorf("'composite' tag allowed only on empty on structs: Invalid tags %v on field %s", strings.SplitN(st.Field(i).Tag.Get("reindex"), ",", 3), st.Field(i).Name)
			}

			indexDef := makeIndexDef(parseCompositeName(reindexPath), parseCompositeJsonPaths(reindexPath), idxType, "composite", opts, CollateNone, "", parseExpireAfter(expireAfter))
			if err := indexDefAppend(indexDefs, indexDef, opts.isAppenable); err != nil {
				return err
			}
		} else if t.Kind() == reflect.Struct {
			if err := parseIndexesImpl(indexDefs, t, subArray, reindexPath, jsonPath, joined, parsed); err != nil {
				return err
			}
		} else if (t.Kind() == reflect.Slice || t.Kind() == reflect.Array) &&
			(t.Elem().Kind() == reflect.Struct || (t.Elem().Kind() == reflect.Ptr && t.Elem().Elem().Kind() == reflect.Struct)) {
			// Check if field nested slice of struct
			if parseByKeyWord(&idxSettings, "joined") && len(idxName) > 0 {
				(*joined)[idxName] = st.Field(i).Index
			} else if err := parseIndexesImpl(indexDefs, t.Elem(), true, reindexPath, jsonPath, joined, parsed); err != nil {
				return err
			}
		} else if len(idxName) > 0 {
			collateMode, sortOrderLetters := parseCollate(&idxSettings)
			var fieldType string
			if idxType == "rtree" {
				fieldType = "point"
			} else if fieldType, err = getFieldType(t); err != nil {
				return err
			}
			indexDef := makeIndexDef(reindexPath, []string{jsonPath}, idxType, fieldType, opts, collateMode, sortOrderLetters, parseExpireAfter(expireAfter))
			if err := indexDefAppend(indexDefs, indexDef, opts.isAppenable); err != nil {
				return err
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
	opts.rtreeType = "rstar"

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
		case "linear", "quadratic", "greene", "rstar":
			opts.rtreeType = idxSetting
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

func parseExpireAfter(str string) int {
	expireAfter := 0
	if len(str) > 0 {
		var err error
		expireAfter, err = strconv.Atoi(str)
		if err != nil {
			panic(fmt.Errorf("'ExpireAfter' should be an integer value"))
		}
	}
	return expireAfter
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

func makeFieldDef(JSONPath string, fieldType string, isArray bool) bindings.FieldDef {
	return bindings.FieldDef{
		JSONPath: JSONPath,
		Type:     fieldType,
		IsArray:  isArray,
	}
}

func makeIndexDef(index string, jsonPaths []string, indexType, fieldType string, opts indexOptions, collateMode int, sortOrder string, expireAfter int) bindings.IndexDef {
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
		ExpireAfter: expireAfter,
		RTreeType:   opts.rtreeType,
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
