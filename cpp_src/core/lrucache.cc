
#include "core/ft/ftsetcashe.h"
#include "core/idsetcache.h"
#include "core/querycache.h"
#include "joincache.h"
#include "tools/logger.h"

namespace reindexer {

constexpr uint32_t kMaxHitCountToCache = 1024;

template <typename K, typename V, typename HashT, typename EqualT>
LRUCacheImpl<K, V, HashT, EqualT>::LRUCacheImpl(size_t sizeLimit, uint32_t hitCount) noexcept
	: totalCacheSize_(0), cacheSizeLimit_(sizeLimit), hitCountToCache_(hitCount) {}

template <typename K, typename V, typename HashT, typename EqualT>
typename LRUCacheImpl<K, V, HashT, EqualT>::Iterator LRUCacheImpl<K, V, HashT, EqualT>::Get(const K& key) {
	if rx_unlikely (cacheSizeLimit_ == 0) {
		return Iterator();
	}

	std::lock_guard lk(lock_);

	auto [it, emplaced] = items_.try_emplace(key);
	if (emplaced) {
		totalCacheSize_ += kElemSizeOverhead + sizeof(Entry) + key.Size();
		it->second.lruPos = lru_.insert(lru_.end(), &it->first);
		if rx_unlikely (!eraseLRU()) {
			return Iterator();
		}
	} else if (std::next(it->second.lruPos) != lru_.end()) {
		lru_.splice(lru_.end(), lru_, it->second.lruPos, std::next(it->second.lruPos));
		it->second.lruPos = std::prev(lru_.end());
	}

	if (++it->second.hitCount < int(hitCountToCache_)) {
		return Iterator();
	}
	++getCount_;
	return Iterator(true, it->second.val);
}

template <typename K, typename V, typename HashT, typename EqualT>
void LRUCacheImpl<K, V, HashT, EqualT>::Put(const K& key, V&& v) {
	if rx_unlikely (cacheSizeLimit_ == 0) {
		return;
	}

	std::lock_guard lk(lock_);
	auto it = items_.find(key);
	if (it == items_.end()) {
		return;
	}

	totalCacheSize_ += v.Size() - it->second.val.Size();
	it->second.val = std::move(v);

	++putCount_;

	eraseLRU();

	if rx_unlikely (putCount_ * 16 > getCount_ && eraseCount_) {
		logFmt(LogWarning, "IdSetCache::eraseLRU () cache invalidates too fast eraseCount={},putCount={},getCount={},hitCountToCache={}",
				  eraseCount_, putCount_, eraseCount_, hitCountToCache_);
		eraseCount_ = 0;
		hitCountToCache_ = hitCountToCache_ ? std::min(hitCountToCache_ * 2, kMaxHitCountToCache) : 2;
		putCount_ = 0;
		getCount_ = 0;
	}
}

template <typename K, typename V, typename HashT, typename EqualT>
RX_ALWAYS_INLINE bool LRUCacheImpl<K, V, HashT, EqualT>::eraseLRU() {
	typename LRUList::iterator it = lru_.begin();

	while (totalCacheSize_ > cacheSizeLimit_) {
		// just to save us if totalCacheSize_ >0 and lru is empty
		// someone can make bad key or val with wrong size
		// TODO: Probably we should remove this logic, since there is no access to sizes outside of the lrucache
		if rx_unlikely (lru_.empty()) {
			clearAll();
			logFmt(LogError, "IdSetCache::eraseLRU () Cache restarted because wrong cache size totalCacheSize_={}", totalCacheSize_);
			return false;
		}
		auto mIt = items_.find(**it);
		assertrx_throw(mIt != items_.end());

		const size_t oldSize = sizeof(Entry) + kElemSizeOverhead + mIt->first.Size() + mIt->second.val.Size();

		if rx_unlikely (oldSize > totalCacheSize_) {
			clearAll();
			logFmt(LogError, "IdSetCache::eraseLRU () Cache restarted because wrong cache size totalCacheSize_={},oldSize={}",
					  totalCacheSize_, oldSize);
			return false;
		}
		totalCacheSize_ = totalCacheSize_ - oldSize;
		items_.erase(mIt);
		it = lru_.erase(it);
		++eraseCount_;
	}

	return !lru_.empty();
}

template <typename K, typename V, typename HashT, typename EqualT>
bool LRUCacheImpl<K, V, HashT, EqualT>::clearAll() {
	const bool res = !items_.empty();
	totalCacheSize_ = 0;
	std::unordered_map<K, Entry, HashT, EqualT>().swap(items_);
	LRUList().swap(lru_);
	getCount_ = 0;
	putCount_ = 0;
	eraseCount_ = 0;
	return res;
}

template <typename K, typename V, typename HashT, typename EqualT>
LRUCacheMemStat LRUCacheImpl<K, V, HashT, EqualT>::GetMemStat() const {
	LRUCacheMemStat ret;

	std::lock_guard lk(lock_);
	ret.totalSize = totalCacheSize_;
	ret.itemsCount = items_.size();
	// for (auto &item : items_) {
	// 	if (item.second.val.Empty()) ret.emptyCount++;
	// }

	ret.hitCountLimit = hitCountToCache_;

	return ret;
}

template <typename K, typename V, typename HashT, typename EqualT>
void LRUCacheImpl<K, V, HashT, EqualT>::Clear() {
	std::lock_guard lk(lock_);
	clearAll();
}

template <typename K, typename V, typename HashT, typename EqualT>
void LRUCacheImpl<K, V, HashT, EqualT>::Clear(std::function<bool(const Key&)> cond) {
	std::lock_guard lock(lock_);
	for (auto it = lru_.begin(); it != lru_.end();) {
		if (!cond(**it)) {
			++it;
			continue;
		}
		auto mIt = items_.find(**it);
		assertrx(mIt != items_.end());
		const size_t oldSize = sizeof(Entry) + kElemSizeOverhead + mIt->first.Size() + mIt->second.val.Size();
		if rx_unlikely (oldSize > totalCacheSize_) {
			clearAll();
			return;
		}
		totalCacheSize_ -= oldSize;
		items_.erase(mIt);
		it = lru_.erase(it);
		++eraseCount_;
	}
}

template class LRUCacheImpl<IdSetCacheKey, IdSetCacheVal, hash_idset_cache_key, equal_idset_cache_key>;
template class LRUCacheImpl<IdSetCacheKey, FtIdSetCacheVal, hash_idset_cache_key, equal_idset_cache_key>;
template class LRUCacheImpl<QueryCacheKey, QueryCountCacheVal, HashQueryCacheKey, EqQueryCacheKey>;
template class LRUCacheImpl<JoinCacheKey, JoinCacheVal, hash_join_cache_key, equal_join_cache_key>;

}  // namespace reindexer
