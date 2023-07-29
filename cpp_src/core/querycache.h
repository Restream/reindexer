#pragma once

#include "core/lrucache.h"
#include "core/query/query.h"
#include "estl/h_vector.h"
#include "tools/serializer.h"
#include "vendor/murmurhash/MurmurHash3.h"

namespace reindexer {

struct QueryTotalCountCacheVal {
	QueryTotalCountCacheVal() = default;
	QueryTotalCountCacheVal(size_t total) noexcept : total_count(total) {}

	size_t Size() const noexcept { return 0; }

	int total_count = -1;
};

struct QueryCacheKey {
	QueryCacheKey() = default;
	QueryCacheKey(QueryCacheKey&& other) = default;
	QueryCacheKey(const QueryCacheKey& other) = default;
	QueryCacheKey& operator=(QueryCacheKey&& other) = default;
	QueryCacheKey& operator=(const QueryCacheKey& other) = delete;
	QueryCacheKey(const Query& q) {
		WrSerializer ser;
		q.Serialize(ser, (SkipJoinQueries | SkipMergeQueries | SkipLimitOffset));
		buf.reserve(ser.Len());
		buf.assign(ser.Buf(), ser.Buf() + ser.Len());
	}
	size_t Size() const noexcept { return sizeof(QueryCacheKey) + (buf.is_hdata() ? 0 : buf.size()); }

	QueryCacheKey(WrSerializer& ser) : buf(ser.Buf(), ser.Buf() + ser.Len()) {}
	h_vector<uint8_t, 256> buf;
};

struct EqQueryCacheKey {
	bool operator()(const QueryCacheKey& lhs, const QueryCacheKey& rhs) const noexcept {
		return (lhs.buf.size() == rhs.buf.size()) && (memcmp(lhs.buf.data(), rhs.buf.data(), lhs.buf.size()) == 0);
	}
};

struct HashQueryCacheKey {
	size_t operator()(const QueryCacheKey& q) const noexcept {
		uint64_t hash[2];
		MurmurHash3_x64_128(q.buf.data(), q.buf.size(), 0, &hash);
		return hash[0];
	}
};

struct QueryTotalCountCache : LRUCache<QueryCacheKey, QueryTotalCountCacheVal, HashQueryCacheKey, EqQueryCacheKey> {
	QueryTotalCountCache(size_t sizeLimit = kDefaultCacheSizeLimit, int hitCount = kDefaultHitCountToCache)
		: LRUCache(sizeLimit, hitCount) {}
};

}  // namespace reindexer
