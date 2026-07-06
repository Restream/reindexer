#pragma once

#include "core/idset/idset.h"
#include "core/lrucache.h"
#include "core/query/query.h"
#include "tools/serilize/wrserializer.h"
#include "vendor/murmurhash/MurmurHash3.h"

namespace reindexer::joins {

struct [[nodiscard]] CacheKey {
	CacheKey() = default;
	CacheKey(CacheKey&& other) = default;
	CacheKey(const CacheKey& other) = default;
	CacheKey& operator=(CacheKey&& other) = default;
	CacheKey& operator=(const CacheKey& other) = delete;
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
	size_t Size() const noexcept { return sizeof(CacheKey) + (buf_.is_hdata() ? 0 : buf_.size()); }

	h_vector<uint8_t, 256> buf_;
};
struct [[nodiscard]] equal_cache_key {
	bool operator()(const CacheKey& lhs, const CacheKey& rhs) const {
		return (lhs.buf_.size() == rhs.buf_.size() && memcmp(lhs.buf_.data(), rhs.buf_.data(), lhs.buf_.size()) == 0);
	}
};
struct [[nodiscard]] hash_cache_key {
	size_t operator()(const CacheKey& cache) const {
		uint64_t hash[2];
		MurmurHash3_x64_128(cache.buf_.data(), cache.buf_.size(), 0, &hash);
		return hash[0];
	}
};

struct PreSelect;

struct [[nodiscard]] CacheVal {
	CacheVal() = default;
	size_t Size() const noexcept { return ids ? (sizeof(*ids.get()) + ids->HeapSize()) : 0; }
	bool IsInitialized() const noexcept { return inited; }

	IdSetPlain::Ptr ids;
	bool matchedAtLeastOnce = false;
	bool inited = false;
	std::shared_ptr<const joins::PreSelect> preSelect;
};

using Cache = LRUCache<LRUCacheImpl<CacheKey, CacheVal, hash_cache_key, equal_cache_key>, LRUWithAtomicPtr::No>;

struct [[nodiscard]] CacheRes {
	bool haveData = false;
	bool needPut = false;

	CacheKey key;
	Cache::Iterator it;
};

}  // namespace reindexer::joins
