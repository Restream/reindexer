#pragma once

#include "core/idset.h"
#include "core/keyvalue/variant.h"
#include "core/lrucache.h"
#include "core/type_consts_helpers.h"

namespace reindexer {

class [[nodiscard]] IdSetCacheKey {
	using SortT = std::common_type_t<SortType, std::underlying_type_t<RankSortType>>;

public:
	struct [[nodiscard]] Equal {
		bool operator()(const IdSetCacheKey& lhs, const IdSetCacheKey& rhs) const noexcept {
			try {
				return lhs.cond_ == rhs.cond_ && lhs.sort_ == rhs.sort_ && *lhs.keys_ == *rhs.keys_;
			} catch (...) {
				return false;  // For non-comparable variant arrays (really rare case in this context)
			}
		}
	};
	struct [[nodiscard]] Hash {
		size_t operator()(const IdSetCacheKey& s) const noexcept {
			return (size_t(s.cond_) << 8) ^ (size_t(s.sort_) << 16) ^ s.keys_->Hash();
		}
	};

	IdSetCacheKey(const VariantArray& keys, CondType cond, SortType sort) noexcept : keys_(&keys), cond_(cond), sort_(sort) {}
	IdSetCacheKey(const VariantArray& keys, CondType cond, RankSortType sort) noexcept : keys_(&keys), cond_(cond), sort_(SortT(sort)) {}
	IdSetCacheKey(const IdSetCacheKey& other) : keys_(&hkeys_), cond_(other.cond_), sort_(other.sort_), hkeys_(*other.keys_) {}
	IdSetCacheKey(IdSetCacheKey&& other) noexcept : keys_(&hkeys_), cond_(other.cond_), sort_(other.sort_) {
		if (&other.hkeys_ == other.keys_) {
			hkeys_ = std::move(other.hkeys_);
		} else {
			hkeys_ = *other.keys_;
		}
	}
	IdSetCacheKey& operator=(const IdSetCacheKey& other) {
		if (&other != this) {
			hkeys_ = *other.keys_;
			keys_ = &hkeys_;
			cond_ = other.cond_;
			sort_ = other.sort_;
		}
		return *this;
	}
	IdSetCacheKey& operator=(IdSetCacheKey&& other) noexcept {
		if (&other != this) {
			if (&other.hkeys_ == other.keys_) {
				hkeys_ = std::move(other.hkeys_);
			} else {
				hkeys_ = *other.keys_;
			}
			keys_ = &hkeys_;
			cond_ = other.cond_;
			sort_ = other.sort_;
		}
		return *this;
	}

	size_t Size() const noexcept { return sizeof(IdSetCacheKey) + keys_->size() * sizeof(VariantArray::value_type); }
	SortType Sort() const noexcept { return sort_; }

	template <typename T>
	friend T& operator<<(T& os, const IdSetCacheKey& k) {
		os << "{cond: " << CondTypeToStr(k.cond_) << ", sort: " << k.sort_ << ", keys: ";
		k.hkeys_.Dump(os);
		return os << '}';
	}

private:
	const VariantArray* keys_;
	CondType cond_;
	SortT sort_;
	VariantArray hkeys_;
};

struct [[nodiscard]] IdSetCacheVal {
	IdSetCacheVal() = default;
	IdSetCacheVal(IdSet::Ptr&& i) noexcept : ids(std::move(i)) {}
	size_t Size() const noexcept { return ids ? (sizeof(*ids.get()) + ids->heap_size()) : 0; }
	bool IsInitialized() const noexcept { return bool(ids); }

	IdSet::Ptr ids;
};

template <typename T>
T& operator<<(T& os, const IdSetCacheVal& v) {
	if (v.ids) {
		return os << *v.ids;
	} else {
		return os << "[]";
	}
}

using IdSetCacheBase =
	LRUCache<LRUCacheImpl<IdSetCacheKey, IdSetCacheVal, IdSetCacheKey::Hash, IdSetCacheKey::Equal>, LRUWithAtomicPtr::Yes>;

class [[nodiscard]] IdSetCache : public IdSetCacheBase {
public:
	IdSetCache() = default;
	IdSetCache(size_t sizeLimit, uint32_t hitCount) : IdSetCacheBase(sizeLimit, hitCount) {}
};

}  // namespace reindexer
