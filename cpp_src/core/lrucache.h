#pragma once

#include <estl/fast_hash_set.h>
#include <list>
#include <mutex>
#include <unordered_map>
#include "namespacestat.h"

namespace reindexer {
using std::list;
using std::mutex;
using std::unordered_map;

const size_t kDefaultCacheSizeLimit = 1024 * 1024 * 128;
const int kDefaultHitCountToCache = 2;

template <typename K, typename V, typename hash, typename equal>
class LRUCache {
public:
	LRUCache(size_t sizeLimit = kDefaultCacheSizeLimit, int hitCount = kDefaultHitCountToCache)
		: totalCacheSize_(0), cacheSizeLimit_(sizeLimit), hitCountToCache_(hitCount) {}
	struct Iterator {
		Iterator(const K *k = nullptr, const V &v = V()) : key(k), val(v) {}
		const K *key;
		V val;
	};
	// Get cached val. Create new entry in cache if unexists
	Iterator Get(const K &k);
	// Put cached val
	void Put(const K &k, const V &v);

	LRUCacheMemStat GetMemStat();

	bool Clear();

protected:
	void eraseLRU();

	typedef list<const K *> LRUList;

	struct Entry {
		V val;
		typename LRUList::iterator lruPos;
		int hitCount = 0;
	};

	unordered_map<K, Entry, hash, equal> items_;
	LRUList lru_;
	mutex lock_;
	size_t totalCacheSize_;
	size_t cacheSizeLimit_;
	int hitCountToCache_;

	int getCount_ = 0, putCount_ = 0, eraseCount_ = 0;
};

}  // namespace reindexer
