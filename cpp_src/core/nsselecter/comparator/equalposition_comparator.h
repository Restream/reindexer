#pragma once

#include <cstddef>
#include <deque>
#include "const.h"
#include "core/id_type.h"
#include "core/keyvalue/variant.h"
#include "core/payload/fieldsset.h"
#include "core/payload/payloadtype.h"
#include "core/queryresults/fields_filter.h"
#include "equalposition_comparator_impl.h"

namespace reindexer {

enum class [[nodiscard]] PathPartType { Name, AnyValue, ArrayTarget };
struct [[nodiscard]] FieldPathPart {
	PathPartType type;
	std::string_view name;
	TagName tag = TagName::Empty();
};

using FieldPath = h_vector<FieldPathPart, 4>;
namespace equal_position_helpers {
void ParseStrPath(std::string_view str, FieldPath& path);
}  // namespace equal_position_helpers

class [[nodiscard]] EqualPositionComparator {
public:
	EqualPositionComparator(const PayloadType& payloadType, [[maybe_unused]] const TagsMatcher*)
		: payloadType_{payloadType}, name_{"EqualPositions"} {}

	void BindField(const std::string& name, int field, const VariantArray&, CondType, const CollateOpts&);
	void BindField(const std::string& name, const FieldsPath&, const VariantArray&, CondType);
	bool Compare(const PayloadValue&, IdType);
	bool IsBinded() noexcept { return !ctx_.empty(); }
	int GetMatchedCount(bool invert) const noexcept {
		assertrx_dbg(totalCalls_ >= matchedCount_);
		return invert ? (totalCalls_ - matchedCount_) : matchedCount_;
	}
	int FieldsCount() const noexcept { return ctx_.size(); }
	const std::string& Name() const& noexcept { return name_; }
	const std::string& Dump() const& noexcept { return Name(); }
	double Cost(double expectedIterations) const noexcept {
		const auto nonIdxFields = fields_.getTagsPathsLength();
		const auto idxFields = fields_.size() - nonIdxFields;
		// Comparatos with non index fields must have much higher cost, than comparators with index fields
		return comparators::kNonIdxFieldComparatorCostMultiplier * double(expectedIterations) * double(nonIdxFields) +
			   comparators::kIdxOffsetComparatorCostMultiplier * double(expectedIterations) * double(idxFields) + 2.0;
	}
	void ExcludeDistinctValues(const PayloadValue& /*item*/, IdType /*rowId*/) const noexcept {}
	reindexer::IsDistinct IsDistinct() const noexcept { return IsDistinct_False; }

	auto Name() const&& = delete;
	auto Dump() const&& = delete;

private:
	bool compareField(size_t field, const Variant&);
	void bindField(const std::string& name, const VariantArray&, CondType, const CollateOpts&);

	struct [[nodiscard]] Context {
		Context(const CollateOpts& collate) : cmpString{collate} {}
		CondType cond;
		EqualPositionComparatorTypeImpl<bool> cmpBool;
		EqualPositionComparatorTypeImpl<int> cmpInt;
		EqualPositionComparatorTypeImpl<int64_t> cmpInt64;
		EqualPositionComparatorTypeImpl<float> cmpFloat;
		EqualPositionComparatorTypeImpl<double> cmpDouble;
		EqualPositionComparatorTypeImpl<key_string> cmpString;
		EqualPositionComparatorTypeImpl<Uuid> cmpUuid;
	};

	int totalCalls_{0};
	int matchedCount_{0};
	PayloadType payloadType_;
	FieldsSet fields_;
	std::vector<Context> ctx_;
	std::string name_;
};

class [[nodiscard]] FieldEqPosCacheImpl {
public:
	void Clear(size_t n) {
		lastCacheIndex_ = 0;
		vals_.resize(n);
		for (auto& v : vals_) {
			v.clear<false>();
		}
	}
	void AddValue(Variant&& v, size_t fieldIndex, size_t index);
	void Resize(size_t fieldIndex, size_t k) {
		while (vals_[fieldIndex].size() < k) {
			if (lastCacheIndex_ >= cache_.size()) {
				cache_.emplace_back(VariantArray{});
			}
			cache_[lastCacheIndex_].Clear();
			vals_[fieldIndex].emplace_back(lastCacheIndex_);
			lastCacheIndex_++;
		}
	}
	size_t LevelSize(size_t fieldIndex) const { return vals_[fieldIndex].size(); }
	size_t ValuesSize(size_t fieldIndex, size_t level) const { return cache_[vals_[fieldIndex][level]].size(); }
	const Variant& Value(size_t fieldIndex, size_t level, size_t index) const { return cache_[vals_[fieldIndex][level]][index]; }

private:
	h_vector<h_vector<size_t, 2>, 2> vals_;
	std::vector<VariantArray> cache_;
	size_t lastCacheIndex_ = 0;
};

class [[nodiscard]] FieldEqPosCache {
public:
	FieldEqPosCache(FieldEqPosCacheImpl& v, size_t fieldIndex) : fieldIndex_(fieldIndex), v_(v) {}
	RX_ALWAYS_INLINE void AddValue(Variant&& v, size_t index) { v_.AddValue(std::move(v), fieldIndex_, index); }
	void Resize(size_t k) { v_.Resize(fieldIndex_, k); }
	size_t Size() { return v_.LevelSize(fieldIndex_); }

private:
	size_t fieldIndex_;
	FieldEqPosCacheImpl& v_;
};

class [[nodiscard]] GroupingEqualPositionComparator {
public:
	GroupingEqualPositionComparator(const PayloadType& payloadType, const TagsMatcher* tm)
		: payloadType_{payloadType}, tm_(tm), name_{"GroupingEqualPosition"} {}

	void BindField(const std::string& name, const VariantArray&, CondType, const std::string& fieldStr);
	bool Compare(const PayloadValue&, IdType);
	bool IsBinded() noexcept { return !ctx_.empty(); }
	int GetMatchedCount(bool invert) const noexcept {
		assertrx_dbg(totalCalls_ >= matchedCount_);
		return invert ? (totalCalls_ - matchedCount_) : matchedCount_;
	}
	int FieldsCount() const noexcept { return ctx_.size(); }
	const std::string& Name() const& noexcept { return name_; }
	const std::string& Dump() const& noexcept { return Name(); }
	double Cost(double expectedIterations) const noexcept {
		// This comparator always requires CJSON parsing
		return comparators::kNonIdxFieldComparatorCostMultiplier * double(expectedIterations) * double(FieldsCount()) + 3.0;
	}
	void ExcludeDistinctValues(const PayloadValue& /*item*/, IdType /*rowId*/) const noexcept {}
	reindexer::IsDistinct IsDistinct() const noexcept { return IsDistinct_False; }

	auto Name() const&& = delete;
	auto Dump() const&& = delete;

private:
	bool compareField(size_t field, const Variant&);

	struct [[nodiscard]] Context {
		Context(const CollateOpts& collate) : cmpString{collate} {}
		CondType cond;
		EqualPositionComparatorTypeImpl<bool> cmpBool;
		EqualPositionComparatorTypeImpl<int> cmpInt;
		EqualPositionComparatorTypeImpl<int64_t> cmpInt64;
		EqualPositionComparatorTypeImpl<float> cmpFloat;
		EqualPositionComparatorTypeImpl<double> cmpDouble;
		EqualPositionComparatorTypeImpl<key_string> cmpString;
		EqualPositionComparatorTypeImpl<Uuid> cmpUuid;
	};

	int totalCalls_{0};
	int matchedCount_{0};
	PayloadType payloadType_;
	const TagsMatcher* tm_;
	std::deque<std::string> fieldStrs_; //fieldPath - string_view on this string, recreating is not allowed
	std::vector<FieldPath> fieldPaths_;
	std::vector<FieldsFilter> filters_;
	std::vector<Context> ctx_;
	std::string name_;
	FieldEqPosCacheImpl eqPosVals;
};

}  // namespace reindexer
