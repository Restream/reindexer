#pragma once

#include "core/idset.h"
#include "core/keyvalue.h"
#include "core/lrucache.h"

namespace reindexer {

struct IdSetCacheKey {
	KeyValues keys;
	CondType cond;
	SortType sort;
};

struct IdSetCacheVal {
	IdSetCacheVal() : ids(make_shared<IdSet>()) {}
	IdSetCacheVal(const IdSet::Ptr &i) : ids(i) {}
	size_t Size() const { return ids->size() * sizeof(IdSet::value_type); }

	IdSet::Ptr ids;
};

struct equal_idset_cache_key {
	bool operator()(const IdSetCacheKey &lhs, const IdSetCacheKey &rhs) const {
		return lhs.cond == rhs.cond && lhs.sort == rhs.sort && lhs.keys.EQ(rhs.keys);
	}
};
struct hash_idset_cache_key {
	size_t operator()(const IdSetCacheKey &s) const { return (s.cond << 8) ^ (s.sort << 16) ^ s.keys.Hash(); }
};

class IdSetCache : public LRUCache<IdSetCacheKey, IdSetCacheVal, hash_idset_cache_key, equal_idset_cache_key> {};

}  // namespace reindexer
