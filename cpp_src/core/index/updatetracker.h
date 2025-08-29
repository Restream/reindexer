#pragma once

#include <functional>
#include <type_traits>
#include "core/index/payload_map.h"
#include "core/index/string_map.h"
#include "core/keyvalue/geometry.h"
#include "estl/fast_hash_set.h"

namespace reindexer {

template <typename T>
class [[nodiscard]] UpdateTracker {
public:
	// UpdateTracker is storing unique index key values, wich has been updated, and not commited
	// For non-trivial types like key_string or PayloadType - payload pointer comparator is used.

	template <typename T1>
	struct [[nodiscard]] hash_ptr {
		size_t operator()(const T1& obj) const noexcept { return std::hash<uintptr_t>()(reinterpret_cast<uintptr_t>(obj.get())); }
	};
	template <typename T1>
	struct [[nodiscard]] equal_ptr {
		bool operator()(const T1& lhs, const T1& rhs) const noexcept {
			return reinterpret_cast<uintptr_t>(lhs.get()) == reinterpret_cast<uintptr_t>(rhs.get());
		}
	};
	template <typename T1>
	struct [[nodiscard]] less_ptr {
		bool operator()(const T1& lhs, const T1& rhs) const noexcept {
			return reinterpret_cast<uintptr_t>(lhs.get()) < reinterpret_cast<uintptr_t>(rhs.get());
		}
	};

	using key_type = typename std::conditional<std::is_same_v<typename T::key_type, PayloadValueWithHash>, PayloadValue,
											   typename std::conditional<std::is_same_v<typename T::key_type, key_string_with_hash>,
																		 key_string, typename T::key_type>::type>::type;
	using pointers_set = fast_hash_set_s<key_type, hash_ptr<key_type>, equal_ptr<key_type>, less_ptr<key_type>>;
	using points_set = fast_hash_set_s<Point, std::hash<Point>, point_strict_equal, point_strict_less>;
	using generic_set = fast_hash_set_s<key_type, std::hash<key_type>, std::equal_to<key_type>, std::less<key_type>>;
	constexpr static bool kHoldsPointers = std::is_same_v<key_type, PayloadValue> || std::is_same_v<key_type, key_string>;
	constexpr static bool kHoldsPoints = std::is_same_v<key_type, Point>;

	using hash_map =
		typename std::conditional_t<kHoldsPointers, pointers_set, typename std::conditional_t<kHoldsPoints, points_set, generic_set>>;

	UpdateTracker() = default;
	UpdateTracker(const UpdateTracker<T>& other)
		: completeUpdate_(other.updated_.size() || other.completeUpdate_),
		  simpleCounting_(other.simpleCounting_),
		  updatesCounter_(other.updatesCounter_) {
		updatesBuckets_.store(updated_.bucket_count(), std::memory_order_relaxed);
	}
	UpdateTracker& operator=(const UpdateTracker<T>& other) = delete;

	void markUpdated(T& idx_map, typename T::iterator& k, bool skipCommitted = true) {
		if (skipCommitted && k->second.Unsorted().IsCommitted()) {
			return;
		}
		if (simpleCounting_) {
			++updatesCounter_;
			return;
		}
		if (completeUpdate_) {
			return;
		}
		if (updated_.size() > static_cast<size_t>(idx_map.size() / 8) || updated_.size() > 10000000) {
			completeUpdate_ = true;
			clearUpdates();
			return;
		}
		emplaceUpdate(k);
	}

	void commitUpdated(T& idx_map) {
		for (const auto& valIt : updated_) {
			auto keyIt = idx_map.find(valIt);
			assertrx(keyIt != idx_map.end());
			keyIt->second.Unsorted().Commit();
			assertrx(keyIt->second.Unsorted().size());
		}
	}

	void markDeleted(typename T::iterator& k) {
		if (simpleCounting_) {
			++updatesCounter_;
		} else {
			eraseUpdate(k);
		}
	}
	bool isUpdated() const noexcept { return !updated_.empty() || completeUpdate_ || (simpleCounting_ && updatesCounter_); }
	bool isCompleteUpdated() const noexcept { return completeUpdate_ || (simpleCounting_ && updatesCounter_); }
	void clear() {
		completeUpdate_ = false;
		updatesCounter_ = 0;
		clearUpdates();
	}
	hash_map& updated() { return updated_; }
	const hash_map& updated() const { return updated_; }
	uint32_t updatesSize() const noexcept { return updatesSize_.load(std::memory_order_relaxed); }
	uint32_t updatesBuckets() const noexcept { return updatesBuckets_.load(std::memory_order_relaxed); }
	uint32_t allocated() const noexcept { return allocatedMem_.load(std::memory_order_relaxed); }
	uint32_t overflow() const noexcept { return overflowSize_.load(std::memory_order_relaxed); }
	void enableCountingMode(bool val) noexcept {
		if (!simpleCounting_ && val) {
			hash_map m;
			std::swap(m, updated_);
			updatesSize_.store(0, std::memory_order_relaxed);
			updatesBuckets_.store(updated_.bucket_count(), std::memory_order_relaxed);
			allocatedMem_.store(getMapAllocatedMemSize(), std::memory_order_relaxed);
			overflowSize_.store(getMapOverflowSize(), std::memory_order_relaxed);
		} else if (simpleCounting_ && !val) {
			completeUpdate_ = true;
		}
		simpleCounting_ = val;
	}

protected:
	void eraseUpdate(typename T::iterator& k) {
		updated_.erase(k->first);
		updatesSize_.store(updated_.size(), std::memory_order_relaxed);
		updatesBuckets_.store(updated_.bucket_count(), std::memory_order_relaxed);
		allocatedMem_.store(getMapAllocatedMemSize(), std::memory_order_relaxed);
		overflowSize_.store(getMapOverflowSize(), std::memory_order_relaxed);
	}
	void emplaceUpdate(typename T::iterator& k) {
		updated_.emplace(k->first);
		updatesSize_.store(updated_.size(), std::memory_order_relaxed);
		updatesBuckets_.store(updated_.bucket_count(), std::memory_order_relaxed);
		allocatedMem_.store(getMapAllocatedMemSize(), std::memory_order_relaxed);
		overflowSize_.store(getMapOverflowSize(), std::memory_order_relaxed);
	}
	void clearUpdates() {
		updated_.clear();
		updatesSize_.store(0, std::memory_order_relaxed);
		updatesBuckets_.store(updated_.bucket_count(), std::memory_order_relaxed);
		allocatedMem_.store(getMapAllocatedMemSize(), std::memory_order_relaxed);
		overflowSize_.store(getMapOverflowSize(), std::memory_order_relaxed);
	}
	size_t getMapAllocatedMemSize() {
		if constexpr (kUsingSTDFastHashSet) {
			return 0;
		} else {
			return updated_.allocated_mem_size();
		}
	}
	size_t getMapOverflowSize() {
		if constexpr (kUsingSTDFastHashSet) {
			return 0;
		} else {
			return updated_.overflow_size();
		}
	}

	// Set of updated keys. Depends on safe/unsafe indexes' map iterator implementation.
	hash_map updated_;
	std::atomic<uint32_t> updatesSize_ = {0};
	std::atomic<uint32_t> updatesBuckets_ = {0};
	std::atomic<uint64_t> allocatedMem_ = {0};
	std::atomic<uint32_t> overflowSize_ = {0};

	// Becomes true, when updates set is to large. In this case indexes must be fully scanned to optimize
	bool completeUpdate_ = false;
	// Simple counting mode. If true - does not track actual updates, simply counts them
	bool simpleCounting_ = false;
	uint64_t updatesCounter_ = 0;
};

}  // namespace reindexer
