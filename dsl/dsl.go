package dsl

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

/*

Go interface for JSON DSL queries.
JSON format mostly corresponds to the 'Query' definition from https://github.com/restream/reindexer/-/blob/develop/cpp_src/server/contrib/server.yml,
however some of the fields are not implemented or may have a little bit different behaviour due to compatibility reasons.

- Only select queries are supported
- 'merge_queries', 'select_functions', 'select_filter' (explicit fields list to output), 'strict_mode' are not supported and will not be unmarshaled
- 'value' may be json array, scalar or null (in the original DSL it is always must be array)
- if 'value' is 'null' and 'cond' is not 'any'/'empty', then this filter will be skipped

Usage:
rxDB := reindexer.NewReindex(...)
jsonDSL := "{...}" // Contains JSON string, corresponding the DSL's format
var dslQ dsl.DSL
err := json.Unmarshal([]byte(jsonDSL), &dslQ)
q, err := rxDB.QueryFrom(dslQ)

*/

type DSL struct {
	Namespace    string        `json:"namespace"`
	Offset       int           `json:"offset"`
	Limit        int           `json:"limit"`
	Distinct     string        `json:"distinct"` // deprecated, use aggregation with type AggDistinct instead
	Sort         Sort          `json:"sort"`
	Filters      []Filter      `json:"filters"`
	Explain      bool          `json:"explain,omitempty"`
	ReqTotal     bool          `json:"req_total,omitempty"`
	WithRank     bool          `json:"select_with_rank,omitempty"`
	Aggregations []Aggregation `json:"aggregations"`
}

type Aggregation struct {
	AggType int      `json:"type"`
	Sort    []Sort   `json:"sort"`
	Limit   int      `json:"limit"`
	Offset  int      `json:"offset"`
	Fields  []string `json:"fields"`
}

type sort Sort

type Sort struct {
	Field  string        `json:"field"`
	Desc   bool          `json:"desc"`
	Values []interface{} `json:"values,omitempty"`
}

type Filter struct {
	Op      string
	Field   string
	Joined  *JoinQuery `json:"Joined,omitempty"`
	Cond    string
	Value   interface{}
	Filters []Filter `json:"Filters,omitempty"`
}

type JoinOnCondition struct {
	LeftField  string `json:"left_field"`
	RightField string `json:"right_field"`
	Op         string `json:"op"`
	Cond       string `json:"cond"`
}

type JoinQuery struct {
	Namespace string
	Type      string
	Sort      Sort
	Filters   []Filter
	Offset    int
	Limit     int
	On        []JoinOnCondition
}

type joinQuery struct {
	Namespace string            `json:"namespace"`
	Type      string            `json:"type,omitempty"`
	Sort      Sort              `json:"sort,omitempty"`
	Filters   []filter          `json:"filters,omitempty"`
	Offset    int               `json:"offset,omitempty"`
	Limit     int               `json:"limit,omitempty"`
	On        []JoinOnCondition `json:"on"`
}

type filter struct {
	Op      string     `json:"op"`
	Field   string     `json:"field,omitempty"`
	Joined  *joinQuery `json:"join_query,omitempty"`
	Cond    string     `json:"cond,omitempty"`
	Value   value      `json:"value,omitempty"`
	Filters []filter   `json:"filters,omitempty"`
}

type value struct {
	data string
}

func (s *Sort) UnmarshalJSON(data []byte) error {
	sort := sort{}
	err := json.Unmarshal(data, &sort)
	if err != nil {
		return err
	}

	s.Field = sort.Field
	s.Desc = sort.Desc
	s.Values = sort.Values
	return s.CheckValuesType()
}

func (s *Sort) CheckValuesType() error {
	if len(s.Values) == 0 {
		return nil
	}

	t := reflect.TypeOf(s.Values[0])

	for _, value := range s.Values {
		if t != reflect.TypeOf(value) {
			return errors.New("array must be homogeneous")
		}
	}

	return nil
}

func (v *value) UnmarshalJSON(data []byte) error {
	v.data = string(data)
	return nil
}

func (f *Filter) fillFilter(flt *filter) error {
	f.Op = flt.Op
	f.Cond = flt.Cond
	f.Field = flt.Field
	if flt.Joined != nil {
		f.Joined = &JoinQuery{Namespace: flt.Joined.Namespace,
			Type: flt.Joined.Type, Sort: flt.Joined.Sort,
			Limit: flt.Joined.Limit, Offset: flt.Joined.Offset,
			On: flt.Joined.On}
		f.Joined.Filters = make([]Filter, len(flt.Joined.Filters))
		for i := range f.Joined.Filters {
			err := f.Joined.Filters[i].fillFilter(&flt.Joined.Filters[i])
			if err != nil {
				return err
			}
		}
	}
	if len(flt.Filters) != 0 {
		f.Filters = make([]Filter, len(flt.Filters))
		for i := range f.Filters {
			f.Filters[i].fillFilter(&flt.Filters[i])
		}
	}
	if f.Field != "" {
		err := f.parseValue(flt.Value.data)
		if err != nil {
			return err
		}
	}
	return nil
}

func (f *Filter) UnmarshalJSON(data []byte) error {
	flt := filter{}
	err := json.Unmarshal(data, &flt)
	if err != nil {
		return err
	}
	return f.fillFilter(&flt)
}

func (f *Filter) parseValuesArray(rawValues []interface{}) (interface{}, error) {
	switch rawValues[0].(type) {
	case bool:
		values := make([]bool, len(rawValues))
		for i, v := range rawValues {
			if value, ok := v.(bool); !ok {
				return nil, errors.New("array must be homogeneous (bool)")
			} else {
				values[i] = value
			}
		}
		return values, nil
	case float64:
		values := make([]int, len(rawValues))
		for i, v := range rawValues {
			if value, ok := v.(float64); !ok {
				return nil, errors.New("array must be homogeneous (int/float)")
			} else {
				values[i] = int(value)
			}
		}
		return values, nil
	case string:
		values := make([]string, len(rawValues))
		for i, v := range rawValues {
			if value, ok := v.(string); !ok {
				return nil, errors.New("array must be homogeneous (string)")
			} else {
				values[i] = value
			}
		}
		return values, nil
	default:
		return nil, fmt.Errorf("unexpected array type: %s", reflect.TypeOf(rawValues[0]).Name())
	}
}

func (f *Filter) parseValue(data string) error {
	if strings.HasPrefix(data, `"$`) {
		f.Value = strings.Trim(data, `"`)
		return nil
	}

	lcond := strings.ToLower(f.Cond)

	switch lcond {
	case "gt", "lt", "ge", "le", "eq":
		if len(data) == 0 || data == `""` || data == "null" {
			f.Value = nil
			break
		}
		if data == "true" {
			f.Value = true
			break
		}
		if data == "false" {
			f.Value = false
			break
		}
		if strings.HasPrefix(data, `[`) && strings.HasSuffix(data, `]`) {
			var rawValues []interface{}
			err := json.Unmarshal([]byte(data), &rawValues)
			if err != nil {
				return err
			}
			if len(rawValues) != 1 && lcond != "eq" {
				return fmt.Errorf("filter value can not be array with 0 or multiple values for '%s' condition", f.Cond)
			}
			if lcond != "eq" {
				if len(rawValues) != 1 {
					return fmt.Errorf("filter value can not be array with 0 or multiple values for '%s' condition", f.Cond)
				}
			} else if len(rawValues) == 0 {
				f.Value = nil
				return nil
			}
			f.Value, err = f.parseValuesArray(rawValues)
			return err
		}
		if strings.HasPrefix(data, `{`) && strings.HasSuffix(data, `}`) {
			return errors.New("filter value can not be object")
		}
		if strings.HasPrefix(data, `"`) && strings.HasSuffix(data, `"`) {
			f.Value = strings.Trim(data, `"`)
			break
		}
		if strings.Contains(data, ".") {
			if v, err := strconv.ParseFloat(data, 64); err != nil {
				return err
			} else {
				f.Value = v
			}
			break
		}
		if v, err := strconv.ParseInt(data, 10, 64); err != nil {
			return err
		} else {
			f.Value = v
		}
	case "set", "range", "allset":
		if len(data) == 0 || data == `[]` || data == "null" {
			f.Value = nil
			break
		}
		if !strings.HasPrefix(data, `[`) || !strings.HasSuffix(data, `]`) {
			return fmt.Errorf("filter expects array or null for '%s' condition", f.Cond)
		}

		var rawValues []interface{}
		err := json.Unmarshal([]byte(data), &rawValues)
		if err != nil {
			return err
		}
		if lcond == "range" && len(rawValues) != 2 {
			return errors.New("range argument array must has 2 elements")
		}
		if len(rawValues) == 0 {
			f.Value = nil
			break
		}
		f.Value, err = f.parseValuesArray(rawValues)
		return err
	case "any", "empty":
		f.Value = 0
	default:
		return fmt.Errorf("cond type '%s' not found", f.Cond)
	}

	return nil
}
