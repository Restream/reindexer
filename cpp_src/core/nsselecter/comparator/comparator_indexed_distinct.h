#pragma once

#include <string_view>

#include "core/index/payload_map.h"
#include "estl/fast_hash_set.h"
#include "tools/stringstools.h"

namespace reindexer {

template <typename T, typename SetType = fast_hash_set<T>>
class [[nodiscard]] ComparatorIndexedDistinct {
public:
	RX_ALWAYS_INLINE bool Compare(const T& v) const noexcept { return values_.find(v) == values_.cend(); }
	void ExcludeValues(const T& v) { values_.insert(v); }

private:
	SetType values_;
};

class [[nodiscard]] ComparatorIndexedDistinctPayload {
public:
	ComparatorIndexedDistinctPayload(const PayloadType& payloadType, const FieldsSet& fieldSet)
		: values_(PayloadType{payloadType}, FieldsSet{fieldSet}) {}

	RX_ALWAYS_INLINE bool Compare(const PayloadValue& pv) const { return values_.find(pv) == values_.cend(); }
	void ExcludeValues(const PayloadValue& pv) { values_.insert(pv); }

private:
	unordered_payload_set<false> values_;
};

class [[nodiscard]] ComparatorIndexedDistinctString {
public:
	ComparatorIndexedDistinctString(const CollateOpts& collate) : values_{SetType{collate}} {}

	RX_ALWAYS_INLINE bool Compare(std::string_view str) const noexcept { return values_.find(str) == values_.cend(); }
	void ExcludeValues(std::string_view str) { values_.insert(str); }

private:
	struct [[nodiscard]] less_string_view {
		less_string_view(const CollateOpts& collateOpts) noexcept : collateOpts_(&collateOpts) {}
		bool operator()(std::string_view lhs, std::string_view rhs) const noexcept {
			return collateCompare(lhs, rhs, *collateOpts_) == ComparationResult::Lt;
		}

	private:
		const CollateOpts* collateOpts_;
	};

	struct [[nodiscard]] equal_string_view {
		equal_string_view(const CollateOpts& collateOpts) noexcept : collateOpts_(&collateOpts) {}
		bool operator()(std::string_view lhs, std::string_view rhs) const noexcept {
			return collateCompare(lhs, rhs, *collateOpts_) == ComparationResult::Eq;
		}

	private:
		const CollateOpts* collateOpts_;
	};

	struct [[nodiscard]] hash_string_view {
		hash_string_view(CollateMode collateMode = CollateNone) noexcept : collateMode_(collateMode) {}
		size_t operator()(std::string_view s) const noexcept { return collateHash(s, collateMode_); }

	private:
		CollateMode collateMode_;
	};

	class [[nodiscard]] string_view_set
		: public tsl::hopscotch_sc_set<std::string_view, hash_string_view, equal_string_view, less_string_view> {
	public:
		string_view_set(const CollateOpts& opts)
			: tsl::hopscotch_sc_set<std::string_view, hash_string_view, equal_string_view, less_string_view>(
				  1000, hash_string_view(opts.mode), equal_string_view(opts), std::allocator<std::string_view>(), less_string_view(opts)) {}
	};

	using SetType = string_view_set;

	SetType values_;
};

}  // namespace reindexer
