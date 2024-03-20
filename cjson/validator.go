package cjson

import (
	"fmt"
	"reflect"
)

type Validator struct {
}

func IsStructParsed(src reflect.Type, parsed *map[string]bool) (bool, *map[string]bool) {
	if src.Kind() == reflect.Struct {
		fullPath := src.PkgPath() + src.Name()
		if parsed == nil {
			m := make(map[string]bool)
			parsed = &m
		}
		if _, ok := (*parsed)[fullPath]; ok {
			return true, parsed
		}

		(*parsed)[fullPath] = true
	}

	return false, parsed
}

func (enc *Validator) validateLevel(src reflect.Type, fieldName string, parsed *map[string]bool) error {
	tags := map[string]struct{}{}

	isParsed, parsed := IsStructParsed(src, parsed)
	if isParsed {
		return nil
	}

	for i := 0; i < src.NumField(); i++ {
		field := src.Field(i)
		tag, _ := splitStr(field.Tag.Get("json"), ',')

		if len(tag) == 0 && field.Name != "_" {
			tag = field.Name
		}

		fname := fieldName
		if len(fieldName) > 0 {
			fname += "."
		}
		fname += field.Name

		if _, found := tags[tag]; !found {
			if len(tag) > 0 && tag != "-" {
				tags[tag] = struct{}{}
			}
		} else {
			return fmt.Errorf("Struct is invalid. JSON tag '%s' duplicate at field '%s' (type: %s)", tag, fname, field.Type.String())
		}

		t := field.Type
		if (t.Kind() == reflect.Slice || t.Kind() == reflect.Array) &&
			(t.Elem().Kind() == reflect.Struct || (t.Elem().Kind() == reflect.Ptr && t.Elem().Elem().Kind() == reflect.Struct)) {
			t = t.Elem()
		} else if t.Kind() == reflect.Ptr {
			t = t.Elem()
		}

		if t.Kind() == reflect.Struct {
			if err := enc.validateLevel(t, fname, parsed); err != nil {
				return err
			}
		}
	}

	return nil
}

func (enc *Validator) Validate(src interface{}) error {
	t := reflect.TypeOf(src)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	return enc.validateLevel(t, "", nil)
}
