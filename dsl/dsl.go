package dsl

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

type DSL struct {
	Namespace    string        `json:"namespace"`
	Offset       int           `json:"offset"`
	Limit        int           `json:"limit"`
	Distinct     string        `json:"distinct"`  // deprecated, use aggregation with type AggDistinct instead
	Sort         Sort          `json:"sort"`
	Filters      []Filter      `json:"filters"`
	Explain      bool          `json:"explain,omitempty"`
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
	Op    string
	Field string
	Cond  string
	Value interface{}
}

type filter struct {
	Op    string `json:"op"`
	Field string `json:"field"`
	Cond  string `json:"cond"`
	Value value  `json:"value"`
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

func (f *Filter) UnmarshalJSON(data []byte) error {
	flt := filter{}
	err := json.Unmarshal(data, &flt)
	if err != nil {
		return err
	}

	f.Op = flt.Op
	f.Cond = flt.Cond
	f.Field = flt.Field

	return f.ParseValue(flt.Value.data)
}

func (f *Filter) ParseValue(data string) error {
	if strings.HasPrefix(data, `"$`) {
		f.Value = strings.Trim(data, `"`)
		return nil
	}

	switch f.Cond {
	case "EQ", "GT", "LT", "GE", "LE":
		if len(data) == 0 || data == `""` || data == "null" {
			f.Value = nil
			break
		}
		if data == "true" {
			f.Value = true
		}
		if data == "false" {
			f.Value = false
		}
		if strings.HasPrefix(data, `[`) && strings.HasSuffix(data, `]`) {
			return errors.New("filter value must not be array")
		}
		if strings.HasPrefix(data, `{`) && strings.HasSuffix(data, `}`) {
			return errors.New("filter value must not be object")
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
	case "SET", "RANGE", "ALLSET":
		if len(data) == 0 || data == `[]` || data == "null" {
			f.Value = nil
			break
		}

		var rawValues []interface{}
		err := json.Unmarshal([]byte(data), &rawValues)
		if err != nil {
			return err
		}
		if f.Cond == "RANGE" && len(rawValues) != 2 {
			return errors.New("range argument array must has 2 elements")
		}
		if len(rawValues) == 0 {
			f.Value = nil
			break
		}

		switch rawValues[0].(type) {
		case int:
			values := make([]int, len(rawValues), len(rawValues))
			for i, v := range rawValues {
				if value, ok := v.(int); !ok {
					return errors.New("array must be homogeneous")
				} else {
					values[i] = value
				}
			}
			f.Value = values
		case float64:
			values := make([]int, len(rawValues), len(rawValues))
			for i, v := range rawValues {
				if value, ok := v.(float64); !ok {
					return errors.New("array must be homogeneous")
				} else {
					values[i] = int(value)
				}
			}
			f.Value = values
		case string:
			values := make([]string, len(rawValues), len(rawValues))
			for i, v := range rawValues {
				if value, ok := v.(string); !ok {
					return errors.New("array must be homogeneous")
				} else {
					values[i] = value
				}
			}
			f.Value = values
		}
	case "ANY", "EMPTY":
		f.Value = 0
	default:
		return fmt.Errorf("cond type %s not found", f.Cond)
	}

	return nil
}
