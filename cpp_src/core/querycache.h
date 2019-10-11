#pragma once

#include "core/lrucache.h"
#include "core/query/query.h"
#include "estl/h_vector.h"
#include "tools/serializer.h"
#include "vendor/murmurhash/MurmurHash3.h"

using std::vector;
using std::string;

namespace reindexer {

struct QueryCacheVal {
	QueryCacheVal() = default;
	QueryCacheVal(const size_t& total) : total_count(total) {}

	size_t Size() const { return 0; }

	int total_count = -1;
};

struct QueryCacheKey {
	QueryCacheKey() {}
	QueryCacheKey(const QueryCacheKey& other) : buf(other.buf) {}
	QueryCacheKey& operator=(const QueryCacheKey& other) {
		if (this != &other) {
			buf = other.buf;
		}
		return *this;
	}
	QueryCacheKey(const Query& q) {
		WrSerializer ser;
		q.Serialize(ser, (SkipJoinQueries | SkipMergeQueries | SkipLimitOffset));
		buf.reserve(ser.Len());
		buf.assign(ser.Buf(), ser.Buf() + ser.Len());
	}
	size_t Size() const { return sizeof(QueryCacheKey) + buf.size(); }

	QueryCacheKey(WrSerializer& ser) : buf(ser.Buf(), ser.Buf() + ser.Len()) {}
	h_vector<uint8_t, 256> buf;
};

struct EqQueryCacheKey {
	bool operator()(const QueryCacheKey& lhs, const QueryCacheKey& rhs) const {
		return (lhs.buf.size() == rhs.buf.size()) && (memcmp(lhs.buf.data(), rhs.buf.data(), lhs.buf.size()) == 0);
	}
};

struct HashQueryCacheKey {
	size_t operator()(const QueryCacheKey& q) const {
		uint64_t hash[2];
		MurmurHash3_x64_128(q.buf.data(), q.buf.size(), 0, &hash);
		return hash[0];
	}
};

struct QueryCache : LRUCache<QueryCacheKey, QueryCacheVal, HashQueryCacheKey, EqQueryCacheKey> {
	QueryCache(size_t sizeLimit = kDefaultCacheSizeLimit, int hitCount = kDefaultHitCountToCache) : LRUCache(sizeLimit, hitCount) {}
};

}  // namespace reindexer
