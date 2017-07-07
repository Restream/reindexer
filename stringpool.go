package reindexer

import (
	"reflect"
	"sync"
)

type stringEntry struct {
	s      string
	refCnt int
}

type stringPool struct {
	strings map[string]stringEntry
	lock    sync.Mutex
}

func (pool *stringPool) put(s string) string {
	pool.lock.Lock()
	defer pool.lock.Unlock()
	e, ok := pool.strings[s]
	if !ok {
		e = stringEntry{s: s}
	}
	e.refCnt++
	pool.strings[s] = e
	return e.s
}

func (pool *stringPool) delete(s string) {
	pool.lock.Lock()
	defer pool.lock.Unlock()
	e, ok := pool.strings[s]
	if !ok {
		return
	}
	e.refCnt--
	if e.refCnt != 0 {
		pool.strings[s] = e
	} else {
		delete(pool.strings, s)
	}
}

func (pool *stringPool) mergeStrings(val reflect.Value, fields []reindexerField) error {

	for _, f := range fields {
		v := reflect.Indirect(val.Field(f.fieldIdx))
		if !v.IsValid() {
			continue
		}
		tk := f.fieldType
		if (tk == reflect.Slice || tk == reflect.Array) && f.subField != nil {
			// Check if field nested slice of struct
			for i := 0; i < v.Len(); i++ {
				if err := pool.mergeStrings(v.Index(i), f.subField); err != nil {
					return err
				}
			}
			continue
		}
		if tk == reflect.Struct {
			if err := pool.mergeStrings(v, f.subField); err != nil {
				return err
			}
			continue
		}
		if tk == reflect.String {
			// string dict
			v.SetString(pool.put(v.String()))
		}
	}
	return nil
}

func (pool *stringPool) deleteStrings(val reflect.Value, fields []reindexerField) error {

	for _, f := range fields {
		v := reflect.Indirect(val.Field(f.fieldIdx))
		if !v.IsValid() {
			continue
		}
		tk := f.fieldType
		if (tk == reflect.Slice || tk == reflect.Array) && f.subField != nil {
			// Check if field nested slice of struct
			for i := 0; i < v.Len(); i++ {
				if err := pool.deleteStrings(v.Index(i), f.subField); err != nil {
					return err
				}
			}
			continue
		}
		if tk == reflect.Struct {
			if err := pool.deleteStrings(v, f.subField); err != nil {
				return err
			}
			continue
		}

		if tk == reflect.String {
			pool.delete(v.String())
		}
	}
	return nil
}
