#pragma once


#include "estl/multihash_map.h"
#include "core/keyvalue/relaxed_variant_hash.h"
#include "core/queryresults/itemref.h"
#include "tools/logger.h"
#include "core/nsselecter/itemcomparator.h"

namespace reindexer::force_sort_helpers {

class ForcedSortMap {
public:
	using mapped_type = size_t;

private:
	using MultiMap = MultiHashMap<const Variant, mapped_type, RelaxedHasher<NotComparable::Return>::indexesCount,
								  RelaxedHasher<NotComparable::Return>, RelaxedComparator<NotComparable::Return>>;
	struct SingleTypeMap : tsl::hopscotch_sc_map<Variant, mapped_type> {
		KeyValueType type_;
	};
	using DataType = std::variant<MultiMap, SingleTypeMap>;
	class Iterator : private std::variant<MultiMap::Iterator, SingleTypeMap::const_iterator> {
		using Base = std::variant<MultiMap::Iterator, SingleTypeMap::const_iterator>;

	public:
		using Base::Base;
		const auto* operator->() const {
			return std::visit(overloaded{[](MultiMap::Iterator it) { return it.operator->(); },
										 [](SingleTypeMap::const_iterator it) { return it.operator->(); }},
							  static_cast<const Base&>(*this));
		}
		const auto& operator*() const {
			return std::visit(overloaded{[](MultiMap::Iterator it) -> const auto& { return *it; },
										 [](SingleTypeMap::const_iterator it) -> const auto& { return *it; }},
							  static_cast<const Base&>(*this));
		}
	};

public:
	ForcedSortMap(Variant k, mapped_type v, size_t size)
		: data_{k.Type().Is<KeyValueType::String>() || k.Type().Is<KeyValueType::Uuid>() || k.Type().IsNumeric()
					? DataType{MultiMap{size}}
					: DataType{SingleTypeMap{{}, k.Type()}}} {
		std::visit(overloaded{[&](MultiMap& m) { m.emplace(std::move(k), v); }, [&](SingleTypeMap& m) { m.emplace(std::move(k), v); }},
				   data_);
	}
	std::pair<Iterator, bool> emplace(Variant k, mapped_type v) & {
		return std::visit(overloaded{[&](MultiMap& m) {
										 const auto [iter, success] = m.emplace(std::move(k), v);
										 return std::make_pair(Iterator{iter}, success);
									 },
									 [&](SingleTypeMap& m) {
										 if (!m.type_.IsSame(k.Type())) {
											 throw Error{errQueryExec, "Items of different types in forced sort list"};
										 }
										 const auto [iter, success] = m.emplace(std::move(k), v);
										 return std::make_pair(Iterator{iter}, success);
									 }},
						  data_);
	}
	bool contain(const Variant& k) const {
		return std::visit(overloaded{[&k](const MultiMap& m) { return m.find(k) != m.cend(); },
									 [&k](const SingleTypeMap& m) {
										 if (!m.type_.IsSame(k.Type())) {
											 throw Error{errQueryExec, "Items of different types in forced sort list"};
										 }
										 return m.find(k) != m.end();
									 }},
						  data_);
	}
	mapped_type get(const Variant& k) const {
		return std::visit(overloaded{[&k](const MultiMap& m) {
										 const auto it = m.find(k);
										 assertrx_throw(it != m.cend());
										 return it->second;
									 },
									 [&k](const SingleTypeMap& m) {
										 if (!m.type_.IsSame(k.Type())) {
											 throw Error{errQueryExec, "Items of different types in forced sort list"};
										 }
										 const auto it = m.find(k);
										 assertrx_throw(it != m.end());
										 return it->second;
									 }},
						  data_);
	}

private:
	DataType data_;
};

template <typename Map>
class ForcedMapInserter {
public:
	explicit ForcedMapInserter(Map& m) noexcept : map_{m} {}
	template <typename V>
	void Insert(V&& value) {
		if (const auto [iter, success] = map_.emplace(std::forward<V>(value), cost_); success) {
			++cost_;
		} else if (iter->second != cost_ - 1) {
			static constexpr auto errMsg = "Forced sort value '{}' is duplicated. Deduplicated by the first occurrence.";
			if constexpr (std::is_same_v<V, Variant>) {
				// NOLINTNEXTLINE (bugprone-use-after-move,-warnings-as-errors)
				logFmt(LogInfo, errMsg, value.template As<std::string>());
			} else {
				// NOLINTNEXTLINE (bugprone-use-after-move,-warnings-as-errors)
				logFmt(LogInfo, errMsg, Variant{std::forward<V>(value)}.template As<std::string>());
			}
		}
	}

private:
	Map& map_;
	typename Map::mapped_type cost_ = 1;
};

template <bool desc, typename ValueGetter>
class ForcedPartitionerIndexed {
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
class ForcedPartitionerIndexedJsonPath {
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
class ForcedPartitionerNotIndexed {
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
class ForcedPartitionerComposite {
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
class ForcedComparatorIndexed {
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
class ForcedComparatorIndexedJsonPath {
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
class ForcedComparatorNotIndexed {
public:
	ForcedComparatorNotIndexed(const TagsPath& tagsPath, const ValueGetter& valueGetter, const ItemComparator& compare,
							   const ForcedSortMap& sortMap) noexcept
		: valueGetter_{valueGetter}, sortMap_{sortMap}, compare_{compare}, tagsPath_{tagsPath} {}

	bool operator()(const ItemRef& lhs, const ItemRef& rhs) {
		valueGetter_.Payload(lhs).GetByJsonPath(tagsPath_, lhsItemValue_, KeyValueType::Undefined{});
		valueGetter_.Payload(rhs).GetByJsonPath(tagsPath_, rhsItemValue_, KeyValueType::Undefined{});

		const auto lhsPos = sortMap_.get(lhsItemValue_[0]);
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
class ForcedComparatorComposite {
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

}
