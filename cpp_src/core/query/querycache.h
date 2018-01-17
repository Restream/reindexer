#pragma once

#include <vector>

#include "core/lrucache.h"
#include "query.h"
#include "tools/serializer.h"
#include "vendor/murmurhash/MurmurHash3.h"

using std::vector;
using std::string;

namespace reindexer {

struct QueryCacheVal {
	QueryCacheVal() = default;
	QueryCacheVal(const size_t& total) : total_count(total) {}

	size_t Size() const { return sizeof total_count; }

	size_t total_count = 0;
};

struct QueryCacheKey {
	QueryCacheKey() {}
	QueryCacheKey(const Query& q) {
		WrSerializer ser;
		q.Serialize(ser, (SkipJoinQueries | SkipMergeQueries | SkipLimitOffset));
		buf.reserve(ser.Len());
		buf.assign(ser.Buf(), ser.Buf() + ser.Len());
	}

	QueryCacheKey(WrSerializer& ser) : buf(ser.Buf(), ser.Buf() + ser.Len()) {}
	vector<uint8_t> buf;
};

struct EqQueryCacheKey {
	bool operator()(const QueryCacheKey& lhs, const QueryCacheKey& rhs) const {
		return (lhs.buf.size() == rhs.buf.size()) &&
			   (memcmp(lhs.buf.data(), rhs.buf.data(), std::max(lhs.buf.size(), lhs.buf.size())) == 0);
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
	bool Empty() const { return items_.empty(); }
};

}  // namespace reindexer
