#pragma once

#include "core/idset.h"
#include "core/keyvalue/variant.h"
#include "core/lrucache.h"
#include "core/nsselecter/nsselecter.h"
#include "core/query/query.h"
#include "tools/serializer.h"
#include "vendor/murmurhash/MurmurHash3.h"
namespace reindexer {

struct JoinCacheKey {
	JoinCacheKey() {}
	void SetData(const Query &q) {
		WrSerializer ser;
		q.Serialize(ser, (SkipJoinQueries | SkipMergeQueries));
		buf_.reserve(buf_.size() + ser.Len());
		buf_.insert(buf_.end(), ser.Buf(), ser.Buf() + ser.Len());
	}
	void SetData(const Query &q1, const Query &q2) {
		WrSerializer ser;
		q1.Serialize(ser, (SkipJoinQueries | SkipMergeQueries));
		q2.Serialize(ser, (SkipJoinQueries | SkipMergeQueries));
		buf_.reserve(buf_.size() + ser.Len());
		buf_.insert(buf_.end(), ser.Buf(), ser.Buf() + ser.Len());
	}
	JoinCacheKey(const JoinCacheKey &other) { buf_ = other.buf_; }
	size_t Size() const { return sizeof(JoinCacheKey) + buf_.size(); }

	h_vector<uint8_t, 256> buf_;
};
struct equal_join_cache_key {
	bool operator()(const JoinCacheKey &lhs, const JoinCacheKey &rhs) const {
		return (lhs.buf_.size() == rhs.buf_.size() && memcmp(lhs.buf_.data(), rhs.buf_.data(), lhs.buf_.size()) == 0);
	}
};
struct hash_join_cache_key {
	size_t operator()(const JoinCacheKey &cache) const {
		uint64_t hash[2];
		MurmurHash3_x64_128(cache.buf_.data(), cache.buf_.size(), 0, &hash);
		return hash[0];
	}
};

struct JoinCacheVal {
	JoinCacheVal() {}
	size_t Size() const { return ids_ ? sizeof(*ids_.get()) + ids_->heap_size() : 0; }
	IdSet::Ptr ids_;
	bool matchedAtLeastOnce = false;
	bool inited = false;
	SelectCtx::PreResult::Ptr preResult;
};
typedef LRUCache<JoinCacheKey, JoinCacheVal, hash_join_cache_key, equal_join_cache_key> MainLruCache;

class JoinCache : public MainLruCache {
public:
	JoinCache() : MainLruCache(kDefaultCacheSizeLimit * 2, 2) {}

	typedef shared_ptr<JoinCache> Ptr;
};

struct JoinCacheRes {
	bool haveData = false;
	bool needPut = false;

	JoinCacheKey key;
	JoinCache::Iterator it;
};

}  // namespace reindexer
