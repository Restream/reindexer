#pragma once

#include <atomic>
#include <list>
#include <mutex>
#include <unordered_map>
#include "namespace/namespacestat.h"

namespace reindexer {

constexpr size_t kDefaultCacheSizeLimit = 1024 * 1024 * 128;
constexpr int kDefaultHitCountToCache = 2;
constexpr size_t kElemSizeOverhead = 256;

template <typename K, typename V, typename hash, typename equal>
class LRUCache {
public:
	using Key = K;
	LRUCache(size_t sizeLimit = kDefaultCacheSizeLimit, int hitCount = kDefaultHitCountToCache) noexcept
		: totalCacheSize_(0), cacheSizeLimit_(sizeLimit), hitCountToCache_(hitCount) {}
	struct Iterator {
		Iterator(bool k = false, const V &v = V()) : valid(k), val(v) {}
		Iterator(const Iterator &other) = delete;
		Iterator &operator=(const Iterator &other) = delete;
		Iterator(Iterator &&other) noexcept : valid(other.valid), val(std::move(other.val)) { other.valid = false; }
		Iterator &operator=(Iterator &&other) noexcept {
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
	void Put(const K &k, V &&v);

	LRUCacheMemStat GetMemStat();

	bool Clear() {
		std::lock_guard lk(lock_);
		return clearAll();
	}

	template <typename T>
	void Dump(T &os, std::string_view step, std::string_view offset) const {
		std::string newOffset{offset};
		newOffset += step;
		os << "{\n" << newOffset << "totalCacheSize: ";
		std::lock_guard lock{lock_};
		os << totalCacheSize_ << ",\n"
		   << newOffset << "cacheSizeLimit: " << cacheSizeLimit_ << ",\n"
		   << newOffset << "hitCountToCache: " << hitCountToCache_ << ",\n"
		   << newOffset << "getCount: " << getCount_ << ",\n"
		   << newOffset << "putCount: " << putCount_ << ",\n"
		   << newOffset << "eraseCount: " << eraseCount_ << ",\n"
		   << newOffset << "items: [";
		if (!items_.empty()) {
			for (auto b = items_.begin(), it = b, e = items_.end(); it != e; ++it) {
				if (it != b) os << ',';
				os << '\n' << newOffset << '{' << it->first << ": ";
				it->second.Dump(os);
				os << '}';
			}
			os << '\n' << newOffset;
		}
		os << "],\n" << newOffset << "lruList: [";
		for (auto b = lru_.begin(), it = b, e = lru_.end(); it != e; ++it) {
			if (it != b) os << ", ";
			os << **it;
		}
		os << "]\n" << offset << '}';
	}

	template <typename F>
	void Clear(const F &cond) {
		std::lock_guard lock(lock_);
		for (auto it = lru_.begin(); it != lru_.end();) {
			if (!cond(**it)) {
				++it;
				continue;
			}
			auto mIt = items_.find(**it);
			assertrx(mIt != items_.end());
			const size_t oldSize = sizeof(Entry) + kElemSizeOverhead + mIt->first.Size() + mIt->second.val.Size();
			if (rx_unlikely(oldSize > totalCacheSize_)) {
				clearAll();
				return;
			}
			totalCacheSize_ -= oldSize;
			items_.erase(mIt);
			it = lru_.erase(it);
			++eraseCount_;
		}
	}

protected:
	typedef std::list<const K *> LRUList;
	struct Entry {
		V val;
		typename LRUList::iterator lruPos;
		int hitCount = 0;
		template <typename T>
		void Dump(T &os) const {
			os << "{val: " << val << ", hitCount: " << hitCount << '}';
		}
	};

	bool eraseLRU();
	bool clearAll();

	std::unordered_map<K, Entry, hash, equal> items_;
	LRUList lru_;
	mutable std::mutex lock_;
	size_t totalCacheSize_;
	const size_t cacheSizeLimit_;
	int hitCountToCache_;

	uint64_t getCount_ = 0, putCount_ = 0, eraseCount_ = 0;
};

}  // namespace reindexer
