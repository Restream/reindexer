#pragma once

#include <unordered_map>
#include "estl/atomic_unique_ptr.h"
#include "estl/elist.h"
#include "estl/lock.h"
#include "estl/mutex.h"
#include "namespace/namespacestat.h"

namespace reindexer {

constexpr size_t kElemSizeOverhead = 256;

template <typename K, typename V, typename HashT, typename EqualT>
class [[nodiscard]] LRUCacheImpl {
public:
	using Key = K;
	using Value = V;
	LRUCacheImpl(size_t sizeLimit, uint32_t hitCount) noexcept;
	struct [[nodiscard]] Iterator {
		Iterator(bool k = false, const V& v = V()) : valid(k), val(v) {}
		Iterator(const Iterator& other) = delete;
		Iterator& operator=(const Iterator& other) = delete;
		Iterator(Iterator&& other) noexcept : valid(other.valid), val(std::move(other.val)) { other.valid = false; }
		Iterator& operator=(Iterator&& other) noexcept {
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
	// Get cached val. Create new entry in cache if it does not exist
	Iterator Get(const K& k);
	// Put cached val
	void Put(const K& k, V&& v);
	LRUCacheMemStat GetMemStat() const;
	void Clear();

	template <typename T>
	void Dump(T& os, std::string_view step, std::string_view offset) const {
		std::string newOffset{offset};
		newOffset += step;
		os << "{\n" << newOffset << "totalCacheSize: ";
		lock_guard lock{lock_};
		os << totalCacheSize_ << ",\n"
		   << newOffset << "cacheSizeLimit: " << cacheSizeLimit_ << ",\n"
		   << newOffset << "hitCountToCache: " << hitCountToCache_ << ",\n"
		   << newOffset << "getCount: " << getCount_ << ",\n"
		   << newOffset << "putCount: " << putCount_ << ",\n"
		   << newOffset << "eraseCount: " << eraseCount_ << ",\n"
		   << newOffset << "items: [";
		if (!items_.empty()) {
			for (auto b = items_.begin(), it = b, e = items_.end(); it != e; ++it) {
				if (it != b) {
					os << ',';
				}
				os << '\n' << newOffset << '{' << it->first << ": ";
				it->second.Dump(os);
				os << '}';
			}
			os << '\n' << newOffset;
		}
		os << "],\n" << newOffset << "lruList: [";
		for (auto b = lru_.begin(), it = b, e = lru_.end(); it != e; ++it) {
			if (it != b) {
				os << ", ";
			}
			os << **it;
		}
		os << "]\n" << offset << '}';
	}

private:
	typedef elist<const K*> LRUList;
	struct [[nodiscard]] Entry {
		V val;
		typename LRUList::iterator lruPos;
		int hitCount = 0;
		template <typename T>
		void Dump(T& os) const {
			os << "{val: " << val << ", hitCount: " << hitCount << '}';
		}
	};

	bool eraseLRU();
	void clearAll();

	std::unordered_map<K, Entry, HashT, EqualT> items_;
	LRUList lru_;
	mutable mutex lock_;
	size_t totalCacheSize_;
	const size_t cacheSizeLimit_;
	uint32_t hitCountToCache_;

	uint64_t getCount_ = 0, putCount_ = 0, eraseCount_ = 0;
};

enum class [[nodiscard]] LRUWithAtomicPtr : bool { Yes, No };

template <typename CacheT, LRUWithAtomicPtr withAtomicPtr>
class [[nodiscard]] LRUCache {
	using CachePtrT = std::conditional_t<withAtomicPtr == LRUWithAtomicPtr::Yes, atomic_unique_ptr<CacheT>, std::unique_ptr<CacheT>>;

public:
	using Iterator = typename CacheT::Iterator;

	LRUCache() = default;
	template <typename... Args>
	LRUCache(Args&&... args) noexcept : ptr_(makePtr(std::forward<Args>(args)...)) {
		(void)alignment1_;
		(void)alignment2_;
#if defined(__x86_64__) || defined(_M_X64) || defined(_M_IX86)
		static_assert(sizeof(LRUCache) == 128, "Unexpected size. Check alignment");
#endif	// defined(__x86_64__) || defined(_M_X64) || defined(_M_IX86)
	}
	virtual ~LRUCache() = default;

	typename CacheT::Iterator Get(const typename CacheT::Key& k) const {
		typename CacheT::Iterator it;
		if (ptr_) {
			it = ptr_->Get(k);
			if (it.valid && it.val.IsInitialized()) {
				stats_.hits.fetch_add(1, std::memory_order_relaxed);
			} else {
				stats_.misses.fetch_add(1, std::memory_order_relaxed);
			}
		}
		return it;
	}
	void Put(const typename CacheT::Key& k, typename CacheT::Value&& v) const {
		if (ptr_) {
			ptr_->Put(k, std::move(v));
		}
	}
	LRUCacheMemStat GetMemStat() const { return ptr_ ? ptr_->GetMemStat() : LRUCacheMemStat(); }
	LRUCachePerfStat GetPerfStat() const noexcept {
		auto stats = stats_.GetPerfStat();
		stats.state = ptr_ ? LRUCachePerfStat::State::Active : LRUCachePerfStat::State::Inactive;
		return stats;
	}
	void ResetPerfStat() noexcept { stats_.Reset(); }
	void Clear() {
		if (ptr_) {
			ptr_->Clear();
		}
	}
	template <typename F>
	void Clear(const F& cond) {
		if (ptr_) {
			ptr_->Clear(cond);
		}
	}

	template <typename T>
	void Dump(T& os, std::string_view step, std::string_view offset) const {
		if (ptr_) {
			ptr_->Dump(os, step, offset);
		} else {
			os << "<empty>";
		}
	}
	void ResetImpl() noexcept { ptr_.reset(); }
	template <typename... Args>
	void Reinitialize(Args&&... args) {
		ptr_ = makePtr(std::forward<Args>(args)...);
	}
	bool IsActive() const noexcept { return ptr_.get(); }
	void CopyInternalPerfStatsFrom(const LRUCache& o) noexcept { stats_ = o.stats_; }

private:
	template <typename... Args>
	CachePtrT makePtr(Args&&... args) {
		return CachePtrT(new CacheT(std::forward<Args>(args)...));
	}

	class [[nodiscard]] Stats {
	public:
		Stats(uint64_t _hits = 0, uint64_t _misses = 0) noexcept : hits{_hits}, misses{_misses} {}
		Stats(const Stats& o) : hits(o.hits.load(std::memory_order_relaxed)), misses(o.misses.load(std::memory_order_relaxed)) {}
		LRUCachePerfStat GetPerfStat() const noexcept {
			return LRUCachePerfStat{.hits = hits.load(std::memory_order_relaxed), .misses = misses.load(std::memory_order_relaxed)};
		}
		void Reset() noexcept {
			hits.store(0, std::memory_order_relaxed);
			misses.store(0, std::memory_order_relaxed);
		}
		Stats& operator=(const Stats& o) {
			if (&o != this) {
				hits.store(o.hits.load());
				misses.store(o.misses.load());
			}
			return *this;
		}

		std::atomic_uint64_t hits;
		std::atomic_uint64_t misses;
	};

	// Cache line alignment to avoid contention between atomic cache ptr and cache stats (alignas would be better, but it does not work
	// properly with tcmalloc on CentOS7)
	uint8_t alignment1_[48];
	CachePtrT ptr_;
	uint8_t alignment2_[48];
	mutable Stats stats_;
};

}  // namespace reindexer
