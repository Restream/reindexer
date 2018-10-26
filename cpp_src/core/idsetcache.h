#pragma once

#include "core/idset.h"
#include "core/keyvalue/variant.h"
#include "core/lrucache.h"

namespace reindexer {

struct IdSetCacheKey {
	IdSetCacheKey(const VariantArray &keys, CondType cond, SortType sort) : keys(&keys), cond(cond), sort(sort) {}
	IdSetCacheKey(const IdSetCacheKey &other) : keys(&hkeys), cond(other.cond), sort(other.sort), hkeys(*other.keys) {}
	IdSetCacheKey &operator=(const IdSetCacheKey &other) {
		hkeys = *other.keys;
		keys = &hkeys;
		cond = other.cond;
		sort = other.sort;
		return *this;
	}
	size_t Size() const { return sizeof(IdSetCacheKey) + hkeys.size() * sizeof(VariantArray::value_type); }

	const VariantArray *keys;
	CondType cond;
	SortType sort;
	VariantArray hkeys;
};

struct IdSetCacheVal {
	IdSetCacheVal() : ids(nullptr) {}
	IdSetCacheVal(const IdSet::Ptr &i) : ids(i) {}
	size_t Size() const { return ids ? sizeof(*ids.get()) + ids->heap_size() : 0; }

	IdSet::Ptr ids;
};

struct equal_idset_cache_key {
	bool operator()(const IdSetCacheKey &lhs, const IdSetCacheKey &rhs) const {
		return lhs.cond == rhs.cond && lhs.sort == rhs.sort && *lhs.keys == *rhs.keys;
	}
};
struct hash_idset_cache_key {
	size_t operator()(const IdSetCacheKey &s) const { return (s.cond << 8) ^ (s.sort << 16) ^ s.keys->Hash(); }
};

class IdSetCache : public LRUCache<IdSetCacheKey, IdSetCacheVal, hash_idset_cache_key, equal_idset_cache_key> {};

}  // namespace reindexer
