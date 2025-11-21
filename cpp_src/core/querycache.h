#pragma once

#include "core/lrucache.h"
#include "core/query/query.h"
#include "estl/h_vector.h"
#include "tools/serializer.h"
#include "vendor/murmurhash/MurmurHash3.h"

namespace reindexer {

struct [[nodiscard]] QueryCountCacheVal {
	QueryCountCacheVal() = default;
	QueryCountCacheVal(size_t total) noexcept : totalCount(total) {}

	size_t Size() const noexcept { return 0; }
	bool IsInitialized() const noexcept { return totalCount >= 0; }

	int totalCount = -1;
};

constexpr uint8_t kCountCachedKeyMode =
	SkipMergeQueries | SkipLimitOffset | SkipAggregations | SkipSortEntries | SkipExtraParams | SkipLeftJoinQueries;

class [[nodiscard]] QueryCacheKey {
public:
	using BufT = h_vector<uint8_t, 256>;

	QueryCacheKey() = default;
	QueryCacheKey(QueryCacheKey&& other) = default;
	QueryCacheKey(const QueryCacheKey& other) = default;
	QueryCacheKey& operator=(QueryCacheKey&& other) = default;
	QueryCacheKey& operator=(const QueryCacheKey& other) = delete;
	template <typename JoinedSelectorsT>
	QueryCacheKey(const Query& q, uint8_t mode, const JoinedSelectorsT* jnss) {
		WrSerializer ser;
		q.Serialize(ser, mode);
		if (jnss) {
			for (auto& jns : *jnss) {
				ser.PutVString(jns.RightNsName());
				ser.PutUInt64(jns.LastUpdateTime());
			}
		}
		if (ser.Len() > BufT::max_size()) [[unlikely]] {
			throw Error(errLogic, "QueryCacheKey: buffer overflow");
		}
		buf_.assign(ser.Buf(), ser.Buf() + ser.Len());
	}
	size_t Size() const noexcept { return sizeof(QueryCacheKey) + (buf_.is_hdata() ? 0 : buf_.size()); }

	QueryCacheKey(WrSerializer& ser) : buf_(ser.Buf(), ser.Buf() + ser.Len()) {}
	const BufT& buf() const noexcept { return buf_; }

private:
	BufT buf_;
};

struct [[nodiscard]] EqQueryCacheKey {
	bool operator()(const QueryCacheKey& lhs, const QueryCacheKey& rhs) const noexcept {
		return (lhs.buf().size() == rhs.buf().size()) && (memcmp(lhs.buf().data(), rhs.buf().data(), lhs.buf().size()) == 0);
	}
};

struct [[nodiscard]] HashQueryCacheKey {
	size_t operator()(const QueryCacheKey& q) const noexcept {
		uint64_t hash[2];
		MurmurHash3_x64_128(q.buf().data(), q.buf().size(), 0, &hash);
		return hash[0];
	}
};

using QueryCountCache = LRUCache<LRUCacheImpl<QueryCacheKey, QueryCountCacheVal, HashQueryCacheKey, EqQueryCacheKey>, LRUWithAtomicPtr::No>;

}  // namespace reindexer
