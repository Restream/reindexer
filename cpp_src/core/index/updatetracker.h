#pragma once

#include <type_traits>
#include "core/index/payload_map.h"
#include "core/index/string_map.h"
#include "estl/fast_hash_set.h"

namespace reindexer {

template <typename T>
struct is_safe_iterators_map : std::false_type {};
template <typename K, typename V>
struct is_safe_iterators_map<std::unordered_map<K, V>> : std::true_type {};
template <typename K, typename V, typename H>
struct is_safe_iterators_map<std::unordered_map<K, V, H>> : std::true_type {};
template <typename K, typename V, typename H, typename E>
struct is_safe_iterators_map<std::unordered_map<K, V, H, E>> : std::true_type {};
template <typename K, typename V, typename H, typename E, typename A>
struct is_safe_iterators_map<std::unordered_map<K, V, H, E, A>> : std::true_type {};
template <typename T1>
struct is_safe_iterators_map<unordered_str_map<T1>> : std::true_type {};

template <typename T>
class UpdateTracker {
public:
	UpdateTracker(const UpdateTracker<T> &other) : completeUpdated_(other.updated_.size() || other.completeUpdated_) {}
	UpdateTracker() {}
	UpdateTracker &operator=(const UpdateTracker<T> &other) = delete;

	// Partial update of index routines. Thera 2 implementations:
	// 1. For safe iterators maps (like std::map and std::unordered_map), which do not invalidate references on insert.
	// 2. For unsafe iterators maps (like btree_map), which invalidate references on insert

	// Safe iterators implementation:
	// Store pointers to keys, which already in the index map

	template <typename U = T, typename std::enable_if<is_safe_iterators_map<U>::value && !is_payload_map_key<T>::value>::type * = nullptr>
	void markUpdated(T &idx_map, typename T::value_type *k) {
		if (completeUpdated_) return;
		if (updated_.size() > idx_map.size() / 2) {
			completeUpdated_ = true;
			updated_.clear();
			return;
		}
		updated_.emplace(k);
	}

	template <typename U = T, typename std::enable_if<is_safe_iterators_map<U>::value && !is_payload_map_key<U>::value>::type * = nullptr>
	void commitUpdated(T &idx_map, const CommitContext &ctx) {
		for (auto keyIt : updated_) {
			keyIt->second.Unsorted().Commit(ctx);
			if (!keyIt->second.Unsorted().size()) idx_map.erase(keyIt->first);
		}
	}

	// Unsafe iterators implementation:
	// Store copy key values
	template <typename U = T, typename std::enable_if<!is_safe_iterators_map<U>::value && !is_payload_map_key<U>::value>::type * = nullptr>
	void markUpdated(T &idx_map, typename T::value_type *k) {
		if (completeUpdated_) return;
		if (updated_.size() > static_cast<size_t>(idx_map.size() / 8)) {
			completeUpdated_ = true;
			updated_.clear();
			return;
		}
		updated_.emplace(k->first);
	}

	template <typename U = T, typename std::enable_if<!is_safe_iterators_map<U>::value && !is_payload_map_key<U>::value>::type * = nullptr>
	void commitUpdated(T &idx_map, const CommitContext &ctx) {
		for (auto valIt : updated_) {
			auto keyIt = idx_map.find(valIt);
			assert(keyIt != idx_map.end());
			keyIt->second.Unsorted().Commit(ctx);
			if (!keyIt->second.Unsorted().size()) idx_map.erase(keyIt->first);
		}
	}
	template <typename U = T, typename std::enable_if<is_payload_map_key<U>::value>::type * = nullptr>
	void markUpdated(T &, typename T::value_type *) {
		completeUpdated_ = true;
	}
	template <typename U = T, typename std::enable_if<is_payload_map_key<U>::value>::type * = nullptr>
	void commitUpdated(T &, const CommitContext &) {}

	// map of updated keys. depends on safe/unsafe index map's iterator implemntation
	typename std::conditional<is_safe_iterators_map<T>::value || is_payload_map_key<T>::value, fast_hash_set<typename T::value_type *>,
							  fast_hash_set<typename T::key_type>>::type updated_;

	bool completeUpdated_;
};

}  // namespace reindexer
