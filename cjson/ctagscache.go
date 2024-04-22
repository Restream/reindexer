package cjson

import (
	"reflect"
)

type ctagsCacheEntry struct {
	structIdx []int
	subCache  ctagsCache
}

type missingTagsCache map[string]struct{}

type ctagsCache struct {
	entries []ctagsCacheEntry
	missing missingTagsCache
}

type structCache map[reflect.Type]*ctagsCache

func (tc *ctagsCache) Reset() {
	tc.entries = tc.entries[:0]
	tc.missing = make(missingTagsCache)
}

func (tc *ctagsCache) Find(cachePath []int16) *[]int {
	ctag := cachePath[0]
	if len(tc.entries) <= int(ctag) {
		return nil
	}
	if len(cachePath) == 1 {
		return &tc.entries[ctag].structIdx
	}
	return tc.entries[ctag].subCache.Find(cachePath[1:])
}

func (tc *ctagsCache) FindOrAdd(cachePath []int16) *[]int {
	ctag := int(cachePath[0])
	if len(tc.entries) <= ctag {
		if cap(tc.entries) <= ctag {
			nc := make([]ctagsCacheEntry, len(tc.entries), ctag+1)
			copy(nc, tc.entries)
			tc.entries = nc
		}
		for n := len(tc.entries); n < ctag+1; n++ {
			tc.entries = append(tc.entries, ctagsCacheEntry{})
		}
	}
	if len(cachePath) == 1 {
		return &tc.entries[ctag].structIdx
	}

	return tc.entries[ctag].subCache.FindOrAdd(cachePath[1:])
}

type ctagsWCacheEntry struct {
	fieldInfo
	subCache ctagsWCache
}

type ctagsWCache []ctagsWCacheEntry

func (tc *ctagsWCache) Reset() {
	(*tc) = (*tc)[:0]
}

func (tc *ctagsWCache) Lookup(idx []int) *ctagsWCacheEntry {
	if len(idx) == 0 {
		return &ctagsWCacheEntry{}
	}

	field := idx[0]

	if len(*tc) <= field {
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

	return (*tc)[field].subCache.Lookup(idx[1:])
}
