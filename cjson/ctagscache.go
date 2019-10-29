package cjson

import (
	"reflect"
)

type ctagsCacheEntry struct {
	structIdx []int
	subCache  ctagsCache
}

type ctagsCache []ctagsCacheEntry

type structCache map[reflect.Type]*ctagsCache

func (tc *ctagsCache) Reset() {
	*tc = (*tc)[:0]
}

func (tc *ctagsCache) Lockup(cachePath []int, canAdd bool) *[]int {
	ctag := cachePath[0]
	if len(*tc) <= ctag {
		if !canAdd {
			return nil
		}
		if cap(*tc) <= ctag {
			nc := make([]ctagsCacheEntry, len(*tc), ctag+1)
			copy(nc, *tc)
			*tc = nc
		}
		for n := len(*tc); n < ctag+1; n++ {
			*tc = append(*tc, ctagsCacheEntry{})
		}
	}
	if len(cachePath) == 1 {
		return &(*tc)[ctag].structIdx
	}

	return (*tc)[ctag].subCache.Lockup(cachePath[1:], canAdd)
}

type ctagsWCacheEntry struct {
	fieldInfo
	subCache ctagsWCache
}

type ctagsWCache []ctagsWCacheEntry

func (tc *ctagsWCache) Reset() {
	(*tc) = (*tc)[:0]
}

func (tc *ctagsWCache) Lookup(idx []int, canAdd bool) *ctagsWCacheEntry {
	if len(idx) == 0 {
		return &ctagsWCacheEntry{}
	}

	field := idx[0]

	if len(*tc) <= field {
		if !canAdd {
			return nil
		}
		if cap(*tc) <= field {
			nc := make([]ctagsWCacheEntry, len(*tc), field+1)
			copy(nc, *tc)
			*tc = nc
		}
		for n := len(*tc); n < field+1; n++ {
			(*tc) = append((*tc), ctagsWCacheEntry{})
		}
	}
	if len(idx) == 1 {
		return &(*tc)[field]
	}

	return (*tc)[field].subCache.Lookup(idx[1:], canAdd)
}
