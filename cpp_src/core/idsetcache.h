#pragma once

#include <bitset>
#include "core/idset.h"
#include "core/keyvalue/variant.h"
#include "core/lrucache.h"
#include "core/payload/fieldsset.h"
#include "core/type_consts_helpers.h"

namespace reindexer {

struct IdSetCacheKey {
	IdSetCacheKey(const VariantArray &keys, CondType cond, SortType sort) noexcept : keys(&keys), cond(cond), sort(sort) {}
	IdSetCacheKey(const IdSetCacheKey &other) : keys(&hkeys), cond(other.cond), sort(other.sort), hkeys(*other.keys) {}
	IdSetCacheKey(IdSetCacheKey &&other) noexcept : keys(&hkeys), cond(other.cond), sort(other.sort) {
		if (&other.hkeys == other.keys) {
			hkeys = std::move(other.hkeys);
		} else {
			hkeys = *other.keys;
		}
	}
	IdSetCacheKey &operator=(const IdSetCacheKey &other) {
		if (&other != this) {
			hkeys = *other.keys;
			keys = &hkeys;
			cond = other.cond;
			sort = other.sort;
		}
		return *this;
	}
	IdSetCacheKey &operator=(IdSetCacheKey &&other) noexcept {
		if (&other != this) {
			if (&other.hkeys == other.keys) {
				hkeys = std::move(other.hkeys);
			} else {
				hkeys = *other.keys;
			}
			keys = &hkeys;
			cond = other.cond;
			sort = other.sort;
		}
		return *this;
	}

	size_t Size() const noexcept { return sizeof(IdSetCacheKey) + keys->size() * sizeof(VariantArray::value_type); }

	const VariantArray *keys;
	CondType cond;
	SortType sort;
	VariantArray hkeys;
};

template <typename T>
T &operator<<(T &os, const IdSetCacheKey &k) {
	os << "{cond: " << CondTypeToStr(k.cond) << ", sort: " << k.sort << ", keys: ";
	k.hkeys.Dump(os);
	return os << '}';
}

struct IdSetCacheVal {
	IdSetCacheVal() = default;
	IdSetCacheVal(IdSet::Ptr &&i) noexcept : ids(std::move(i)) {}
	size_t Size() const noexcept { return ids ? (sizeof(*ids.get()) + ids->heap_size()) : 0; }

	IdSet::Ptr ids;
};

template <typename T>
T &operator<<(T &os, const IdSetCacheVal &v) {
	if (v.ids) {
		return os << *v.ids;
	} else {
		return os << "[]";
	}
}

struct equal_idset_cache_key {
	bool operator()(const IdSetCacheKey &lhs, const IdSetCacheKey &rhs) const noexcept {
		return lhs.cond == rhs.cond && lhs.sort == rhs.sort && *lhs.keys == *rhs.keys;
	}
};
struct hash_idset_cache_key {
	size_t operator()(const IdSetCacheKey &s) const noexcept { return (size_t(s.cond) << 8) ^ (size_t(s.sort) << 16) ^ s.keys->Hash(); }
};

using IdSetCacheBase = LRUCache<IdSetCacheKey, IdSetCacheVal, hash_idset_cache_key, equal_idset_cache_key>;

class IdSetCache : public IdSetCacheBase {
public:
	IdSetCache(size_t sizeLimit, uint32_t hitCount) : IdSetCacheBase(sizeLimit, hitCount) {}
	void ClearSorted(const std::bitset<kMaxIndexes> &s) {
		if (s.any()) {
			Clear([&s](const IdSetCacheKey &k) { return s.test(k.sort); });
		}
	}
};

}  // namespace reindexer
