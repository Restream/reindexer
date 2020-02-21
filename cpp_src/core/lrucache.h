#pragma once

#include <estl/fast_hash_set.h>
#include <atomic>
#include <list>
#include <mutex>
#include <unordered_map>
#include "namespace/namespacestat.h"

namespace reindexer {

const size_t kDefaultCacheSizeLimit = 1024 * 1024 * 128;
const int kDefaultHitCountToCache = 2;

template <typename K, typename V, typename hash, typename equal>
class LRUCache {
public:
	LRUCache(size_t sizeLimit = kDefaultCacheSizeLimit, int hitCount = kDefaultHitCountToCache)
		: totalCacheSize_(0), cacheSizeLimit_(sizeLimit), hitCountToCache_(hitCount) {}
	struct Iterator {
		Iterator(bool k = false, const V &v = V()) : valid(k), val(v) {}
		Iterator(const Iterator &other) = delete;
		Iterator &operator=(const Iterator &other) = delete;
		Iterator(Iterator &&other) : valid(other.valid), val(std::move(other.val)) { other.valid = false; }
		Iterator &operator=(Iterator &&other) {
			if (this != &other) {
				valid = other.valid;
				val = std::move(other.val);
				other.valid = false;
			}
			return *this;
		}
		bool valid;
		V val;
	};
	// Get cached val. Create new entry in cache if unexists
	Iterator Get(const K &k);
	// Put cached val
	void Put(const K &k, const V &v);

	LRUCacheMemStat GetMemStat();

	bool Clear();

protected:
	bool eraseLRU();

	bool clearAll();

	typedef std::list<const K *> LRUList;

	struct Entry {
		V val;
		typename LRUList::iterator lruPos;
		int hitCount = 0;
	};

	std::unordered_map<K, Entry, hash, equal> items_;
	LRUList lru_;
	std::mutex lock_;
	size_t totalCacheSize_;
	size_t cacheSizeLimit_;
	int hitCountToCache_;

	int getCount_ = 0, putCount_ = 0, eraseCount_ = 0;
};

}  // namespace reindexer
