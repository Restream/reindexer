#pragma once

#include <string>
#include <string_view>
#include <variant>

#include "estl/fast_hash_set.h"
#include "tools/stringstools.h"

namespace reindexer {

template <typename T, typename SetType = fast_hash_set<T>>
class ComparatorIndexedDistinct {
public:
	ComparatorIndexedDistinct() : values_{std::make_unique<SetType>()} {}
	ComparatorIndexedDistinct(ComparatorIndexedDistinct&&) = default;
	ComparatorIndexedDistinct& operator=(ComparatorIndexedDistinct&&) = default;
	ComparatorIndexedDistinct(const ComparatorIndexedDistinct& o)
		: values_{o.values_ ? std::make_unique<SetType>(*o.values_) : std::make_unique<SetType>()} {}

	[[nodiscard]] RX_ALWAYS_INLINE bool Compare(const T& v) const noexcept {
		assertrx_dbg(values_);
		return values_->find(v) == values_->cend();
	}
	void ClearValues() noexcept {
		assertrx_dbg(values_);
		values_->clear();
	}
	void ExcludeValues(const T& v) {
		assertrx_dbg(values_);
		values_->insert(v);
	}

private:
	using SetPtrType = std::unique_ptr<SetType>;

	SetPtrType values_;
};

class ComparatorIndexedDistinctString {
public:
	ComparatorIndexedDistinctString(const CollateOpts& collate) : values_{std::make_unique<SetType>(collate)}, collateOpts_{&collate} {}
	ComparatorIndexedDistinctString(ComparatorIndexedDistinctString&&) = default;
	ComparatorIndexedDistinctString& operator=(ComparatorIndexedDistinctString&&) = default;
	ComparatorIndexedDistinctString(const ComparatorIndexedDistinctString& o)
		: values_{o.values_ ? std::make_unique<SetType>(*o.values_) : std::make_unique<SetType>(*o.collateOpts_)} {}

	[[nodiscard]] RX_ALWAYS_INLINE bool Compare(std::string_view str) const noexcept {
		assertrx_dbg(values_);
		return values_->find(str) == values_->cend();
	}
	void ClearValues() noexcept {
		assertrx_dbg(values_);
		values_->clear();
	}
	void ExcludeValues(std::string_view str) {
		assertrx_dbg(values_);
		values_->insert(str);
	}

private:
	struct less_string_view {
		less_string_view(const CollateOpts& collateOpts) noexcept : collateOpts_(&collateOpts) {}
		bool operator()(std::string_view lhs, std::string_view rhs) const noexcept {
			return collateCompare(lhs, rhs, *collateOpts_) == ComparationResult::Lt;
		}

	private:
		const CollateOpts* collateOpts_;
	};

	struct equal_string_view {
		equal_string_view(const CollateOpts& collateOpts) noexcept : collateOpts_(&collateOpts) {}
		bool operator()(std::string_view lhs, std::string_view rhs) const noexcept {
			return collateCompare(lhs, rhs, *collateOpts_) == ComparationResult::Eq;
		}

	private:
		const CollateOpts* collateOpts_;
	};

	struct hash_string_view {
		hash_string_view(CollateMode collateMode = CollateNone) noexcept : collateMode_(collateMode) {}
		size_t operator()(std::string_view s) const noexcept { return collateHash(s, collateMode_); }

	private:
		CollateMode collateMode_;
	};

	class string_view_set : public tsl::hopscotch_sc_set<std::string_view, hash_string_view, equal_string_view, less_string_view> {
	public:
		string_view_set(const CollateOpts& opts)
			: tsl::hopscotch_sc_set<std::string_view, hash_string_view, equal_string_view, less_string_view>(
				  1000, hash_string_view(opts.mode), equal_string_view(opts), std::allocator<std::string_view>(), less_string_view(opts)) {}
	};

	using SetType = string_view_set;
	using SetPtrType = std::unique_ptr<SetType>;

	SetPtrType values_;
	const CollateOpts* collateOpts_;
};

}  // namespace reindexer
