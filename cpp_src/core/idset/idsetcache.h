#pragma once

#include <utility>
#include "core/idset/idset.h"
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
	IdSetCacheKey(const IdSetCacheKey& other) : keys_(&hkeys_), cond_(other.cond_), sort_(other.sort_), hkeys_(*other.keys_) {
		hkeys_.EnsureHold();
	}

	// NOLINTNEXTLINE (bugprone-exception-escape)
	IdSetCacheKey(IdSetCacheKey&& other) : keys_(&hkeys_), cond_(std::move(other.cond_)), sort_(std::move(other.sort_)) {
		if (&other.hkeys_ == other.keys_) {
			hkeys_ = std::move(other.hkeys_);
		} else {
			hkeys_ = *other.keys_;
			hkeys_.EnsureHold();
		}
	}
	IdSetCacheKey& operator=(const IdSetCacheKey& other) {
		IdSetCacheKey tmp(other);
		swap(tmp);
		return *this;
	}
	// NOLINTNEXTLINE (bugprone-exception-escape)
	IdSetCacheKey& operator=(IdSetCacheKey&& other) {
		if (this != &other) {
			IdSetCacheKey tmp(std::move(other));
			swap(tmp);
		}
		return *this;
	}

	size_t Size() const noexcept {
		size_t size = sizeof(IdSetCacheKey) + keys_->size() * sizeof(Variant);
		for (const Variant& key : *keys_) {
			size += key.HeldHeapSize();
		}
		return size;
	}
	SortType Sort() const noexcept { return sort_; }

	template <typename T>
	friend T& operator<<(T& os, const IdSetCacheKey& k) {
		os << "{cond: " << CondTypeToStr(k.cond_) << ", sort: " << k.sort_ << ", keys: ";
		k.keys_->Dump(os);
		return os << '}';
	}

private:
	void swap(IdSetCacheKey& other) noexcept {
		hkeys_.swap(other.hkeys_);
		std::swap(cond_, other.cond_);
		std::swap(sort_, other.sort_);
		keys_ = &hkeys_;
		other.keys_ = &other.hkeys_;
	}

	const VariantArray* keys_;
	CondType cond_;
	SortT sort_;
	VariantArray hkeys_;
};

struct [[nodiscard]] IdSetCacheVal {
	IdSetCacheVal() noexcept = default;
	IdSetCacheVal(IdSetPlain::Ptr&& i) noexcept : ids(std::move(i)) {}
	size_t Size() const noexcept { return ids ? (sizeof(*ids.get()) + ids->HeapSize()) : 0; }
	bool IsInitialized() const noexcept { return bool(ids); }
	void Dump(std::ostream& os) const {
		if (ids) {
			ids->Dump(os);
		} else {
			os << "[]";
		}
	}

	IdSetPlain::Ptr ids;
};

using IdSetCacheBase =
	LRUCache<LRUCacheImpl<IdSetCacheKey, IdSetCacheVal, IdSetCacheKey::Hash, IdSetCacheKey::Equal>, LRUWithAtomicPtr::Yes>;

class [[nodiscard]] IdSetCache : public IdSetCacheBase {
public:
	IdSetCache() = default;
	IdSetCache(size_t sizeLimit, uint32_t hitCount) : IdSetCacheBase(sizeLimit, hitCount) {}
};

}  // namespace reindexer
