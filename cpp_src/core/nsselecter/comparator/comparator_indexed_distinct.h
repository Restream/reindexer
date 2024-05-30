#pragma once

#include <string>
#include <string_view>
#include <unordered_set>
#include <variant>

#include "tools/stringstools.h"
#include "vendor/hopscotch/hopscotch_sc_set.h"

namespace reindexer {

template <typename T>
class ComparatorIndexedDistinct {
public:
	ComparatorIndexedDistinct() : values_{make_intrusive<SetWrpType>()} {}
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
	using SetType = std::unordered_set<T>;
	using SetWrpType = intrusive_rc_wrapper<SetType>;
	using SetPtrType = intrusive_ptr<SetWrpType>;

	SetPtrType values_;
};

class ComparatorIndexedDistinctString {
public:
	ComparatorIndexedDistinctString(const CollateOpts& collate) : values_{make_intrusive<SetWrpType>(collate)} {}
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
	using SetWrpType = intrusive_rc_wrapper<SetType>;
	using SetPtrType = intrusive_ptr<SetWrpType>;

	SetPtrType values_;
};

}  // namespace reindexer
