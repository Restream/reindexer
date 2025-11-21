#pragma once

#include "core/keyvalue/relaxed_variant_hash.h"
#include "core/nsselecter/itemcomparator.h"
#include "core/queryresults/itemref.h"
#include "estl/multihash_map.h"
#include "tools/logger.h"

namespace reindexer::force_sort_helpers {

constexpr std::string_view kForcedSortArrayErrorMsg = "Forced sort can't be applied to a field of array type";

class [[nodiscard]] ForcedSortMap {
public:
	using mapped_type = size_t;

private:
	using MultiMap = MultiHashMap<const Variant, mapped_type, RelaxedHasher<NotComparable::Return>::indexesCount,
								  RelaxedHasher<NotComparable::Return>, RelaxedComparator<NotComparable::Return>>;

	using Iterator = MultiMap::Iterator;

public:
	ForcedSortMap(Variant k, mapped_type v, size_t size) : data_{size} {
		throwIfNotSupportedValueType(k);
		data_.emplace(std::move(k), v);
	}
	std::pair<Iterator, bool> emplace(Variant k, mapped_type v) & {
		throwIfNotSupportedValueType(k);
		const auto [iter, success] = data_.emplace(std::move(k), v);
		return std::make_pair(Iterator{iter}, success);
	}
	bool contain(const Variant& k) const {
		throwIfNotSupportedValueType(k);
		return data_.find(k) != data_.cend();
	}
	mapped_type get(const Variant& k) const {
		throwIfNotSupportedValueType(k);
		const auto it = data_.find(k);
		assertrx_throw(it != data_.cend());
		return it->second;
	}

private:
	static void throwIfNotSupportedValueType(const Variant& value) {
		value.Type().EvaluateOneOf(overloaded{
			[](concepts::OneOf<KeyValueType::Null, KeyValueType::Bool, KeyValueType::Int, KeyValueType::Int64, KeyValueType::Float,
							   KeyValueType::Double, KeyValueType::String, KeyValueType::Uuid> auto) noexcept {},
			[&value](
				concepts::OneOf<KeyValueType::Undefined, KeyValueType::Tuple, KeyValueType::Composite, KeyValueType::FloatVector> auto) {
				throw Error{errQueryExec, "Unsupported value type for forced sort by non indexed field: '{}'", value.Type().Name()};
			}});
	}

	MultiMap data_;
};

template <typename Map>
class [[nodiscard]] ForcedMapInserter {
public:
	explicit ForcedMapInserter(Map& m) noexcept : map_{m} {}
	template <typename V>
	void Insert(V&& value) {
		if (const auto [iter, success] = map_.emplace(std::forward<V>(value), cost_); success) {
			++cost_;
		} else if (iter->second != cost_ - 1) {
			static constexpr auto errMsg = "Forced sort value '{}' is duplicated. Deduplicated by the first occurrence.";
			if constexpr (std::is_same_v<V, Variant>) {
				logFmt(LogInfo, errMsg, iter->first.template As<std::string>());
			} else {
				logFmt(LogInfo, errMsg, Variant{iter->first}.template As<std::string>());
			}
		}
	}

private:
	Map& map_;
	typename Map::mapped_type cost_ = 1;
};

template <bool desc, typename ValueGetter>
class [[nodiscard]] ForcedPartitionerIndexed {
public:
	ForcedPartitionerIndexed(int idx, const ValueGetter& valueGetter, const fast_hash_map<Variant, std::ptrdiff_t>& sortMap) noexcept
		: valueGetter_{valueGetter}, sortMap_{sortMap}, idx_{idx} {}

	bool operator()(const ItemRef& itemRef) {
		valueGetter_.Payload(itemRef).Get(idx_, keyRefs_);
		if constexpr (desc) {
			return keyRefs_.empty() || (sortMap_.find(keyRefs_[0]) == sortMap_.end());
		} else {
			return !keyRefs_.empty() && (sortMap_.find(keyRefs_[0]) != sortMap_.end());
		}
	}

	bool operator()(const ItemRefRanked& itemRefRanked) { return operator()(itemRefRanked.NotRanked()); }

private:
	const ValueGetter& valueGetter_;
	VariantArray keyRefs_;
	const fast_hash_map<Variant, std::ptrdiff_t>& sortMap_;
	const int idx_;
};

template <bool desc, typename ValueGetter>
class [[nodiscard]] ForcedPartitionerIndexedJsonPath {
public:
	ForcedPartitionerIndexedJsonPath(const TagsPath& tagsPath, KeyValueType indexKVT, const std::string_view fieldName,
									 const ValueGetter& valueGetter, const fast_hash_map<Variant, std::ptrdiff_t>& sortMap) noexcept
		: valueGetter_{valueGetter}, sortMap_{sortMap}, tagsPath_{tagsPath}, fieldName_{fieldName}, indexKVT_{indexKVT} {}

	bool operator()(const ItemRef& itemRef) {
		try {
			valueGetter_.Payload(itemRef).GetByJsonPath(tagsPath_, keyRefs_, indexKVT_);
		} catch (const Error& e) {
			logFmt(LogInfo, "Unable to convert sparse value for forced sort (index name: '{}'): '{}'", fieldName_, e.what());
			keyRefs_.resize(0);
		}

		if constexpr (desc) {
			return keyRefs_.empty() || (sortMap_.find(keyRefs_[0]) == sortMap_.end());
		} else {
			return !keyRefs_.empty() && (sortMap_.find(keyRefs_[0]) != sortMap_.end());
		}
	}

	bool operator()(const ItemRefRanked& itemRefRanked) { return operator()(itemRefRanked.NotRanked()); }

private:
	const ValueGetter& valueGetter_;
	VariantArray keyRefs_;
	const fast_hash_map<Variant, std::ptrdiff_t>& sortMap_;
	const TagsPath& tagsPath_;
	const std::string_view fieldName_;
	const KeyValueType indexKVT_;
};

template <bool desc, typename ValueGetter>
class [[nodiscard]] ForcedPartitionerNotIndexed {
public:
	ForcedPartitionerNotIndexed(const TagsPath& tagsPath, const ValueGetter& valueGetter, const ForcedSortMap& sortMap) noexcept
		: valueGetter_{valueGetter}, sortMap_{sortMap}, tagsPath_{tagsPath} {}

	bool operator()(const ItemRef& itemRef) {
		valueGetter_.Payload(itemRef).GetByJsonPath(tagsPath_, keyRefs_, KeyValueType::Undefined{});
		if constexpr (desc) {
			return keyRefs_.empty() || !sortMap_.contain(keyRefs_[0]);
		} else {
			return !keyRefs_.empty() && sortMap_.contain(keyRefs_[0]);
		}
	}

	bool operator()(const ItemRefRanked& itemRefRanked) { return operator()(itemRefRanked.NotRanked()); }

private:
	const ValueGetter& valueGetter_;
	VariantArray keyRefs_;
	const ForcedSortMap& sortMap_;
	const TagsPath& tagsPath_;
};

template <bool desc, typename ValueGetter>
class [[nodiscard]] ForcedPartitionerComposite {
public:
	ForcedPartitionerComposite(const ValueGetter& valueGetter, unordered_payload_map<std::ptrdiff_t, false>& sortMap) noexcept
		: valueGetter_{valueGetter}, sortMap_{sortMap} {}

	bool operator()(const ItemRef& itemRef) {
		if constexpr (desc) {
			return (sortMap_.find(valueGetter_.Value(itemRef)) == sortMap_.end());
		} else {
			return (sortMap_.find(valueGetter_.Value(itemRef)) != sortMap_.end());
		}
	}

	bool operator()(const ItemRefRanked& itemRefRanked) { return operator()(itemRefRanked.NotRanked()); }

private:
	const ValueGetter& valueGetter_;
	const unordered_payload_map<std::ptrdiff_t, false>& sortMap_;
};

template <bool desc, bool multiColumnSort, typename CompareT>
RX_ALWAYS_INLINE bool forceCompareImpl(const ItemRef& lhs, size_t lhsPos, const ItemRef& rhs, size_t rhsPos, CompareT& compare) {
	if (lhsPos == rhsPos) {
		if constexpr (multiColumnSort) {
			return compare(lhs, rhs);
		} else {
			if constexpr (desc) {
				return lhs.Id() > rhs.Id();
			} else {
				return lhs.Id() < rhs.Id();
			}
		}
	} else {
		if constexpr (desc) {
			return lhsPos > rhsPos;
		} else {
			return lhsPos < rhsPos;
		}
	}
}

template <bool desc, bool multiColumnSort, typename ValueGetter>
class [[nodiscard]] ForcedComparatorIndexed {
public:
	ForcedComparatorIndexed(int idx, const ValueGetter& valueGetter, const ItemComparator& compare,
							const fast_hash_map<Variant, std::ptrdiff_t>& sortMap) noexcept
		: valueGetter_{valueGetter}, sortMap_{sortMap}, compare_{compare}, idx_{idx} {}

	bool operator()(const ItemRef& lhs, const ItemRef& rhs) {
		valueGetter_.Payload(lhs).Get(idx_, lhsItemValue_);
		assertrx_throw(!lhsItemValue_.empty());
		const auto lhsIt = sortMap_.find(lhsItemValue_[0]);
		assertrx_throw(lhsIt != sortMap_.end());

		valueGetter_.Payload(rhs).Get(idx_, rhsItemValue_);
		assertrx_throw(!rhsItemValue_.empty());
		const auto rhsIt = sortMap_.find(rhsItemValue_[0]);
		assertrx_throw(rhsIt != sortMap_.end());

		const auto lhsPos = lhsIt->second;
		const auto rhsPos = rhsIt->second;
		return forceCompareImpl<desc, multiColumnSort>(lhs, lhsPos, rhs, rhsPos, compare_);
	}
	bool operator()(const ItemRefRanked& lhs, const ItemRefRanked& rhs) { return operator()(lhs.NotRanked(), rhs.NotRanked()); }

private:
	const ValueGetter& valueGetter_;
	VariantArray lhsItemValue_;
	VariantArray rhsItemValue_;
	const fast_hash_map<Variant, std::ptrdiff_t>& sortMap_;
	const ItemComparator& compare_;
	const int idx_;
};

template <bool desc, bool multiColumnSort, typename ValueGetter>
class [[nodiscard]] ForcedComparatorIndexedJsonPath {
public:
	ForcedComparatorIndexedJsonPath(const TagsPath& tagsPath, KeyValueType indexKVT, const ValueGetter& valueGetter,
									const ItemComparator& compare, const fast_hash_map<Variant, std::ptrdiff_t>& sortMap) noexcept
		: valueGetter_{valueGetter}, sortMap_{sortMap}, compare_{compare}, tagsPath_{tagsPath}, indexKVT_{indexKVT} {}

	bool operator()(const ItemRef& lhs, const ItemRef& rhs) {
		try {
			valueGetter_.Payload(lhs).GetByJsonPath(tagsPath_, lhsItemValue_, indexKVT_);
			valueGetter_.Payload(rhs).GetByJsonPath(tagsPath_, rhsItemValue_, indexKVT_);
		} catch (const Error& e) {
			logFmt(LogInfo, "Unable to convert sparse value for forced sort (index name: '{}'): '{}'", fieldName_, e.what());
			return false;
		}

		assertrx_throw(!lhsItemValue_.empty());
		const auto lhsIt = sortMap_.find(lhsItemValue_[0]);
		assertrx_throw(lhsIt != sortMap_.end());

		assertrx_throw(!rhsItemValue_.empty());
		const auto rhsIt = sortMap_.find(rhsItemValue_[0]);
		assertrx_throw(rhsIt != sortMap_.end());

		const auto lhsPos = lhsIt->second;
		const auto rhsPos = rhsIt->second;
		return forceCompareImpl<desc, multiColumnSort>(lhs, lhsPos, rhs, rhsPos, compare_);
	}
	bool operator()(const ItemRefRanked& lhs, const ItemRefRanked& rhs) { return operator()(lhs.NotRanked(), rhs.NotRanked()); }

private:
	const ValueGetter& valueGetter_;
	VariantArray lhsItemValue_;
	VariantArray rhsItemValue_;
	const fast_hash_map<Variant, std::ptrdiff_t>& sortMap_;
	const ItemComparator& compare_;
	const std::string_view fieldName_;
	const TagsPath& tagsPath_;
	const KeyValueType indexKVT_;
};

template <bool desc, bool multiColumnSort, typename ValueGetter>
class [[nodiscard]] ForcedComparatorNotIndexed {
public:
	ForcedComparatorNotIndexed(const TagsPath& tagsPath, const ValueGetter& valueGetter, const ItemComparator& compare,
							   const ForcedSortMap& sortMap) noexcept
		: valueGetter_{valueGetter}, sortMap_{sortMap}, compare_{compare}, tagsPath_{tagsPath} {}

	bool operator()(const ItemRef& lhs, const ItemRef& rhs) {
		valueGetter_.Payload(lhs).GetByJsonPath(tagsPath_, lhsItemValue_, KeyValueType::Undefined{});
		valueGetter_.Payload(rhs).GetByJsonPath(tagsPath_, rhsItemValue_, KeyValueType::Undefined{});

		if (lhsItemValue_.IsArrayValue() || rhsItemValue_.IsArrayValue()) {
			throw Error(errQueryExec, kForcedSortArrayErrorMsg);
		}

		assertrx_throw(!lhsItemValue_.empty());
		const auto lhsPos = sortMap_.get(lhsItemValue_[0]);
		assertrx_throw(!rhsItemValue_.empty());
		const auto rhsPos = sortMap_.get(rhsItemValue_[0]);

		return forceCompareImpl<desc, multiColumnSort>(lhs, lhsPos, rhs, rhsPos, compare_);
	}
	bool operator()(const ItemRefRanked& lhs, const ItemRefRanked& rhs) { return operator()(lhs.NotRanked(), rhs.NotRanked()); }

private:
	const ValueGetter& valueGetter_;
	VariantArray lhsItemValue_;
	VariantArray rhsItemValue_;
	const ForcedSortMap& sortMap_;
	const ItemComparator& compare_;
	const TagsPath& tagsPath_;
};

template <bool desc, bool multiColumnSort, typename ValueGetter>
class [[nodiscard]] ForcedComparatorComposite {
public:
	ForcedComparatorComposite(const ValueGetter& valueGetter, const ItemComparator& compare,
							  const unordered_payload_map<std::ptrdiff_t, false>& sortMap) noexcept
		: valueGetter_{valueGetter}, sortMap_{sortMap}, compare_{compare} {}

	bool operator()(const ItemRef& lhs, const ItemRef& rhs) {
		const auto lhsPos = sortMap_.find(valueGetter_.Value(lhs))->second;
		const auto rhsPos = sortMap_.find(valueGetter_.Value(rhs))->second;
		return forceCompareImpl<desc, multiColumnSort>(lhs, lhsPos, rhs, rhsPos, compare_);
	}
	bool operator()(const ItemRefRanked& lhs, const ItemRefRanked& rhs) { return operator()(lhs.NotRanked(), rhs.NotRanked()); }

private:
	const ValueGetter& valueGetter_;
	const unordered_payload_map<std::ptrdiff_t, false>& sortMap_;
	const ItemComparator& compare_;
};

}  // namespace reindexer::force_sort_helpers
