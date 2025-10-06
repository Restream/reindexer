#pragma once

#include "core/idset.h"
#include "core/keyvalue/variant.h"
#include "core/lrucache.h"
#include "core/query/query.h"
#include "tools/serializer.h"
#include "vendor/murmurhash/MurmurHash3.h"
namespace reindexer {

struct [[nodiscard]] JoinCacheKey {
	JoinCacheKey() = default;
	JoinCacheKey(JoinCacheKey&& other) = default;
	JoinCacheKey(const JoinCacheKey& other) = default;
	JoinCacheKey& operator=(JoinCacheKey&& other) = default;
	JoinCacheKey& operator=(const JoinCacheKey& other) = delete;
	void SetData(const Query& q) {
		WrSerializer ser;
		q.Serialize(ser, (SkipJoinQueries | SkipMergeQueries));
		buf_.reserve(buf_.size() + ser.Len());
		buf_.insert(buf_.end(), ser.Buf(), ser.Buf() + ser.Len());
	}
	void SetData(const Query& q1, const Query& q2) {
		WrSerializer ser;
		q1.Serialize(ser, (SkipJoinQueries | SkipMergeQueries));
		q2.Serialize(ser, (SkipJoinQueries | SkipMergeQueries));
		buf_.reserve(buf_.size() + ser.Len());
		buf_.insert(buf_.end(), ser.Buf(), ser.Buf() + ser.Len());
	}
	size_t Size() const noexcept { return sizeof(JoinCacheKey) + (buf_.is_hdata() ? 0 : buf_.size()); }

	h_vector<uint8_t, 256> buf_;
};
struct [[nodiscard]] equal_join_cache_key {
	bool operator()(const JoinCacheKey& lhs, const JoinCacheKey& rhs) const {
		return (lhs.buf_.size() == rhs.buf_.size() && memcmp(lhs.buf_.data(), rhs.buf_.data(), lhs.buf_.size()) == 0);
	}
};
struct [[nodiscard]] hash_join_cache_key {
	size_t operator()(const JoinCacheKey& cache) const {
		uint64_t hash[2];
		MurmurHash3_x64_128(cache.buf_.data(), cache.buf_.size(), 0, &hash);
		return hash[0];
	}
};

struct JoinPreResult;

struct [[nodiscard]] JoinCacheVal {
	JoinCacheVal() = default;
	size_t Size() const noexcept { return ids ? (sizeof(*ids.get()) + ids->heap_size()) : 0; }
	bool IsInitialized() const noexcept { return inited; }

	IdSet::Ptr ids;
	bool matchedAtLeastOnce = false;
	bool inited = false;
	std::shared_ptr<const JoinPreResult> preResult;
};

using JoinCache = LRUCache<LRUCacheImpl<JoinCacheKey, JoinCacheVal, hash_join_cache_key, equal_join_cache_key>, LRUWithAtomicPtr::No>;

struct [[nodiscard]] JoinCacheRes {
	bool haveData = false;
	bool needPut = false;

	JoinCacheKey key;
	JoinCache::Iterator it;
};

}  // namespace reindexer
