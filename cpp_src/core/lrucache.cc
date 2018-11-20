
#include "core/ft/ftsetcashe.h"
#include "core/idset.h"
#include "core/idsetcache.h"
#include "core/keyvalue/variant.h"
#include "core/query/querycache.h"
#include "joincache.h"
#include "tools/logger.h"

namespace reindexer {

const size_t kElemSizeOverhead = 256;

template <typename K, typename V, typename hash, typename equal>
typename LRUCache<K, V, hash, equal>::Iterator LRUCache<K, V, hash, equal>::Get(const K &key) {
	if (cacheSizeLimit_ == 0) return Iterator();

	std::lock_guard<mutex> lk(lock_);

	auto it = items_.find(key);

	if (it == items_.end()) {
		it = items_.emplace(key, Entry{}).first;
		totalCacheSize_ += kElemSizeOverhead + sizeof(Entry) + key.Size();
		it->second.lruPos = lru_.insert(lru_.end(), &it->first);
		eraseLRU();
	} else if (std::next(it->second.lruPos) != lru_.end()) {
		lru_.splice(lru_.end(), lru_, it->second.lruPos, std::next(it->second.lruPos));
		it->second.lruPos = std::prev(lru_.end());
	}
	if (++it->second.hitCount < hitCountToCache_) {
		return Iterator();
	}
	++getCount_;

	// logPrintf(LogInfo, "Cache::Get (cond=%d,sortId=%d,keys=%d), total in cache items=%d,size=%d", key.cond, key.sort,
	// 		  (int)key.keys.size(), items_.size(), totalCacheSize_);
	return Iterator(&it->first, it->second.val);
}

template <typename K, typename V, typename hash, typename equal>
void LRUCache<K, V, hash, equal>::Put(const K &key, const V &v) {
	if (cacheSizeLimit_ == 0) return;

	std::lock_guard<mutex> lk(lock_);
	auto it = items_.find(key);
	if (it == items_.end()) return;

	totalCacheSize_ += v.Size() - it->second.val.Size();
	it->second.val = v;

	// logPrintf(LogInfo, "IdSetCache::Put () add %d,left %d,fwdCnt=%d,sz=%d", endIt - begIt, left, it->second.fwdCount,
	// 		  it->second.ids->size());
	++putCount_;

	eraseLRU();
}

template <typename K, typename V, typename hash, typename equal>
void LRUCache<K, V, hash, equal>::eraseLRU() {
	typename LRUList::iterator it = lru_.begin();

	while (totalCacheSize_ > cacheSizeLimit_) {
		assert(it != lru_.end());
		auto mIt = items_.find(**it);
		assert(mIt != items_.end());
		totalCacheSize_ = totalCacheSize_ - (sizeof(Entry) + kElemSizeOverhead + mIt->first.Size() + mIt->second.val.Size());
		items_.erase(mIt);
		it = lru_.erase(it);
		++eraseCount_;
	}

	if (eraseCount_ && putCount_ * 16 > getCount_) {
		logPrintf(LogWarning, "IdSetCache::eraseLRU () cache invalidates too fast eraseCount=%d,putCount=%d,getCount=%d", eraseCount_,
				  putCount_, eraseCount_);
		eraseCount_ = 0;
		hitCountToCache_ *= 2;
		putCount_ = 0;
		getCount_ = 0;
	}
}
template <typename K, typename V, typename hash, typename equal>
bool LRUCache<K, V, hash, equal>::Clear() {
	std::lock_guard<mutex> lk(lock_);
	bool res = !items_.empty();
	totalCacheSize_ = 0;
	items_.clear();
	lru_.clear();
	getCount_ = 0;
	putCount_ = 0;
	eraseCount_ = 0;
	return res;
}

template <typename K, typename V, typename hash, typename equal>
LRUCacheMemStat LRUCache<K, V, hash, equal>::GetMemStat() {
	std::lock_guard<mutex> lk(lock_);
	LRUCacheMemStat ret;
	ret.totalSize = totalCacheSize_;
	ret.itemsCount = items_.size();
	ret.emptyCount = 0;
	// for (auto &item : items_) {
	// 	if (item.second.val.Empty()) ret.emptyCount++;
	// }

	ret.hitCountLimit = hitCountToCache_;

	return ret;
};
template class LRUCache<IdSetCacheKey, IdSetCacheVal, hash_idset_cache_key, equal_idset_cache_key>;
template class LRUCache<IdSetCacheKey, FtIdSetCacheVal, hash_idset_cache_key, equal_idset_cache_key>;
template class LRUCache<QueryCacheKey, QueryCacheVal, HashQueryCacheKey, EqQueryCacheKey>;
template class LRUCache<JoinCacheKey, JoinCacheVal, hash_join_cache_key, equal_join_cache_key>;

}  // namespace reindexer
