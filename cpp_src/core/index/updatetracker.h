#pragma once

#include <functional>
#include <mutex>
#include <type_traits>
#include "core/index/payload_map.h"
#include "core/index/string_map.h"
#include "estl/fast_hash_set.h"

namespace reindexer {

template <typename T>
class UpdateTracker {
public:
	// UpdateTracker is storing unique index key values, wich has been updated, and not commited
	// For non-trivial types like key_string or PayloadType - payload pointer comparator is used.

	template <typename T1>
	struct hash_ptr {
		size_t operator()(const T1 &obj) const { return std::hash<uintptr_t>()(reinterpret_cast<uintptr_t>(obj.get())); }
	};

	template <typename T1>
	struct equal_ptr {
		bool operator()(const T1 &lhs, const T1 &rhs) const {
			return reinterpret_cast<uintptr_t>(lhs.get()) == reinterpret_cast<uintptr_t>(rhs.get());
		}
	};

	using key_type = typename T::key_type;
	using hash_map =
		typename std::conditional<std::is_same<key_type, PayloadValue>::value || std::is_same<key_type, key_string>::value,
								  fast_hash_set<key_type, hash_ptr<key_type>, equal_ptr<key_type>>, fast_hash_set<key_type>>::type;

	UpdateTracker() = default;
	UpdateTracker(const UpdateTracker<T> &other) : completeUpdate_(other.updated_.size() || other.completeUpdate_) {}
	UpdateTracker &operator=(const UpdateTracker<T> &other) = delete;

	void markUpdated(T &idx_map, typename T::iterator &k, bool skipCommited = true) {
		if (skipCommited && k->second.Unsorted().IsCommited()) return;
		if (completeUpdate_) return;
		if (updated_.size() > static_cast<size_t>(idx_map.size() / 8)) {
			completeUpdate_ = true;
			updated_.clear();
			return;
		}
		updated_.emplace(k->first);
	}

	void commitUpdated(T &idx_map) {
		for (auto valIt : updated_) {
			auto keyIt = idx_map.find(valIt);
			assert(keyIt != idx_map.end());
			keyIt->second.Unsorted().Commit();
			assert(keyIt->second.Unsorted().size());
		}
	}

	void markDeleted(typename T::iterator &k) { updated_.erase(k->first); }
	bool isUpdated() const { return !updated_.empty() || completeUpdate_; }
	bool isCompleteUpdated() const { return completeUpdate_; }
	void clear() {
		updated_.clear();
		completeUpdate_ = false;
	}
	hash_map &updated() { return updated_; }
	const hash_map &updated() const { return updated_; }

protected:
	// Set of updated keys. Depends on safe/unsafe indexes' map iterator implementation.
	hash_map updated_;

	bool completeUpdate_;
};

}  // namespace reindexer
