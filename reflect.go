package reindexer

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"unsafe"

	"github.com/restream/reindexer/v5/bindings"
	"github.com/restream/reindexer/v5/cjson"
	"github.com/restream/reindexer/v5/jsonschema"
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
	isNoColumn  bool
	isPk        bool
	isSparse    bool
	rtreeType   string
	isUuid      bool
	isJoined    bool
	isComposite bool
}

func parseRxTags(field reflect.StructField) (idxName string, idxType string, idxSettings []string) {
	tag, isSet := field.Tag.Lookup("reindex")
	tagsSlice := strings.SplitN(tag, ",", 3)
	var idxOpts string
	idxName, idxType, idxOpts = tagsSlice[0], "", ""
	if isSet && len(idxName) == 0 && !field.Anonymous && field.Name != "_" {
		idxName = field.Name
	}

	if len(tagsSlice) > 1 {
		idxType = tagsSlice[1]
	}
	if len(tagsSlice) > 2 {
		idxOpts = tagsSlice[2]
	}
	idxSettings = cjson.SplitFieldOptions(idxOpts)
	return
}

func parseIndexes(st reflect.Type, joined *map[string][]int) (indexDefs []bindings.IndexDef, err error) {
	if err = parseIndexesImpl(&indexDefs, st, false, "", "", joined, nil); err != nil {
		return nil, err
	}

	return indexDefs, nil
}

func parseSchema(st reflect.Type) *bindings.SchemaDef {
	reflector := &jsonschema.Reflector{}
	reflector.FieldIsInScheme = func(f reflect.StructField) bool {
		_, _, idxSettings := parseRxTags(f)
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

func peekNamedOption(name string, options *[]string) string {
	for i := 0; i < len(*options); {
		values := strings.Split((*options)[i], "=")
		if len(values) == 2 && values[0] == name {
			*options = append((*options)[:i], (*options)[i+1:]...)
			return values[1]
		} else {
			i++
		}
	}
	return ""
}

func parseNamedOptions(options *[]string) map[string]string {
	result := make(map[string]string)
	for i := 0; i < len(*options); {
		values := strings.Split((*options)[i], "=")
		if len(values) == 2 {
			result[values[0]] = values[1]
			*options = append((*options)[:i], (*options)[i+1:]...)
		} else {
			i++
		}
	}
	return result
}

func getOptionalIntValue(name string, values map[string]string) (int, error) {
	if v, ok := values[name]; ok {
		intValue, err := strconv.Atoi(v)
		if err != nil {
			return 0, err
		}
		return intValue, nil
	} else {
		return 0, nil
	}
}

func getOptionalFloat32Value(name string, values map[string]string) (float32, error) {
	if v, ok := values[name]; ok {
		intValue, err := strconv.ParseFloat(v, 32)
		if err != nil {
			return 0, err
		}
		return float32(intValue), nil
	} else {
		return 0, nil
	}
}

func isIndexFloatVector(idxType string) bool {
	return idxType == "hnsw" || idxType == "vec_bf" || idxType == "ivf"
}

func copyParsedPointer(parsed *map[string]bool) *map[string]bool {
	var parsedCopy *map[string]bool
	if parsed != nil {
		m := make(map[string]bool, len(*parsed))
		for key, value := range *parsed {
			m[key] = value
		}
		parsedCopy = &m
	}
	return parsedCopy
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
		field := st.Field(i)
		t := field.Type
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
		// Get and parse tags
		jsonTag := strings.Split(field.Tag.Get("json"), ",")[0]

		if len(jsonTag) == 0 && !field.Anonymous {
			jsonTag = field.Name
		}
		jsonPath := jsonBasePath + jsonTag

		idxName, idxType, idxSettings := parseRxTags(field)

		if idxName == "-" {
			continue
		}
		reindexPath := reindexBasePath + idxName

		opts := parseOpts(&idxSettings)

		expireAfter := ""
		if idxType == "ttl" {
			expireAfter = peekNamedOption("expire_after", &idxSettings)
		}

		if t.Kind() == reflect.Slice || t.Kind() == reflect.Array || subArray {
			opts.isArray = true
		}

		if opts.isPk && strings.TrimSpace(idxName) == "" {
			return fmt.Errorf("no index name is specified for primary key in field '%s'; jsonpath: '%s'", field.Name, jsonPath)
		}

		if idxType == "rtree" {
			if t.Kind() != reflect.Array || t.Len() != 2 || t.Elem().Kind() != reflect.Float64 {
				return fmt.Errorf("'rtree' index allowed only for [2]float64 or reindexer.Point field type (index name: '%s', field name: '%s', jsonpath: '%s')",
					reindexPath, field.Name, jsonPath)
			}
		}

		if jsonTag == "-" && !opts.isComposite && !opts.isJoined {
			if reindexTag, isSet := field.Tag.Lookup("reindex"); isSet {
				return fmt.Errorf("non-composite/non-joined field ('%s'), marked with `json:-` can not have explicit reindex tags, but it does (reindex:\"%s\")", field.Name, reindexTag)
			}
			continue
		}
		if !opts.isComposite && !field.IsExported() {
			if reindexTag, isSet := field.Tag.Lookup("reindex"); isSet {
				return fmt.Errorf("unexported non-composite field ('%s') can not have reindex tags, but it does (reindex:\"%s\")", field.Name, reindexTag)
			}
			continue
		}

		if isIndexFloatVector(idxType) {
			if subArray {
				return fmt.Errorf("array float_vector index is not supported")
			}
			if (t.Kind() != reflect.Array && t.Kind() != reflect.Slice) || t.Elem().Kind() != reflect.Float32 {
				return fmt.Errorf("float_vector index allowed only for float32 array/slice field type")
			}
			opts.isArray = false
			namedOpts := parseNamedOptions(&idxSettings)

			var fvOpts bindings.FloatVectorIndexOpts
			fvOpts.Metric = namedOpts["metric"]
			if t.Kind() == reflect.Array {
				fvOpts.Dimension = t.Len()
			} else {
				fvOpts.Dimension, err = strconv.Atoi(namedOpts["dimension"])
				if err != nil {
					return err
				}
			}
			fvOpts.Radius, err = getOptionalFloat32Value("radius", namedOpts)
			if err != nil {
				return err
			}
			fvOpts.StartSize, err = getOptionalIntValue("start_size", namedOpts)
			if err != nil {
				return err
			}
			fvOpts.M, err = getOptionalIntValue("m", namedOpts)
			if err != nil {
				return err
			}
			fvOpts.EfConstruction, err = getOptionalIntValue("ef_construction", namedOpts)
			if err != nil {
				return err
			}
			fvOpts.CentroidsCount, err = getOptionalIntValue("centroids_count", namedOpts)
			if err != nil {
				return err
			}
			fvOpts.MultithreadingMode, err = getOptionalIntValue("multithreading", namedOpts)
			if err != nil {
				return err
			}
			indexDef := makeIndexDef(reindexPath, []string{jsonPath}, idxType, "float_vector", opts, CollateNone, "", 0, &fvOpts)
			if err := indexDefAppend(indexDefs, indexDef, opts.isAppenable); err != nil {
				return err
			}
		} else if opts.isComposite {
			if t.Kind() != reflect.Struct || t.NumField() != 0 {
				return fmt.Errorf("'composite' tag allowed only on empty on structs: Invalid tags '%v' on field '%s'",
					strings.SplitN(field.Tag.Get("reindex"), ",", 3), field.Name)
			}

			indexDef := makeIndexDef(parseCompositeName(reindexPath), parseCompositeJsonPaths(reindexPath), idxType, "composite", opts, CollateNone, "", parseExpireAfter(expireAfter), nil)
			if err := indexDefAppend(indexDefs, indexDef, opts.isAppenable); err != nil {
				return err
			}
		} else if t.Kind() == reflect.Struct {
			if opts.isJoined {
				return fmt.Errorf("joined index must be a slice of structs/pointers, but it is a single struct (index name: '%s', field name: '%s', jsonpath: '%s')",
					reindexPath, field.Name, jsonPath)
			}
			if err := parseIndexesImpl(indexDefs, t, subArray, reindexPath, jsonPath, joined, copyParsedPointer(parsed)); err != nil {
				return err
			}
		} else if (t.Kind() == reflect.Slice || t.Kind() == reflect.Array) &&
			(t.Elem().Kind() == reflect.Struct || (t.Elem().Kind() == reflect.Ptr && t.Elem().Elem().Kind() == reflect.Struct)) {
			// Check if field nested slice of struct
			if opts.isJoined && len(idxName) > 0 {
				(*joined)[idxName] = st.Field(i).Index
			} else if err := parseIndexesImpl(indexDefs, t.Elem(), true, reindexPath, jsonPath, joined, copyParsedPointer(parsed)); err != nil {
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
			if opts.isUuid {
				if fieldType != "string" {
					return fmt.Errorf("UUID index is not applicable with '%v' field, only with 'string' (index name: '%s', field name: '%s', jsonpath: '%s')",
						fieldType, reindexPath, field.Name, jsonPath)
				}
				fieldType = "uuid"
			}
			if opts.isJoined {
				return fmt.Errorf("joined index must be a slice of objects/pointers, but it is a scalar value (index name: '%s', field name: '%s', jsonpath: '%s')",
					reindexPath, field.Name, jsonPath)
			}

			indexDef := makeIndexDef(reindexPath, []string{jsonPath}, idxType, fieldType, opts, collateMode, sortOrderLetters, parseExpireAfter(expireAfter), nil)
			if err := indexDefAppend(indexDefs, indexDef, opts.isAppenable); err != nil {
				return err
			}
		}
		if len(idxSettings) > 0 {
			return fmt.Errorf("unknown index settings are found: '%v' (len = %d)", idxSettings, len(idxSettings))
		}
	}

	return nil
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
		case "is_no_column":
			opts.isNoColumn = true
		case "sparse":
			opts.isSparse = true
		case "appendable":
			opts.isAppenable = true
		case "linear", "quadratic", "greene", "rstar":
			opts.rtreeType = idxSetting
		case "uuid":
			opts.isUuid = true
		case "joined":
			opts.isJoined = true
		case "composite":
			opts.isComposite = true
		case "":
			// Skip empty
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
				panic(fmt.Errorf("collate mode is already set to '%d'. Misunderstanding '%s'", collateMode, idxSetting))
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

func makeIndexDef(index string, jsonPaths []string, indexType, fieldType string, opts indexOptions, collateMode int, sortOrder string, expireAfter int, fv *bindings.FloatVectorIndexOpts) bindings.IndexDef {
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
		IsNoColumn:  opts.isNoColumn,
		IsSparse:    opts.isSparse,
		CollateMode: cm,
		SortOrder:   sortOrder,
		ExpireAfter: expireAfter,
		Config:      fv,
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
		return fmt.Errorf("index '%s' has another type: found index def is '%+v' and new index def is '%+v'", name, foundIndexDef, indexDef)
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
				return fmt.Errorf("index '%s' is not appendable. Attempt to create array index with multiple JSON-paths: %v", name, indexDef.JSONPaths)
			}
			foundIndexDef.JSONPaths = append(foundIndexDef.JSONPaths, indexDef.JSONPaths[0])
		}

		foundIndexDef.IsArray = true
		(*indexDefs)[foundIndexPos] = foundIndexDef
	}
	return nil
}
