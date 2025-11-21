#pragma once

#include "const.h"
#include "core/payload/fieldsset.h"
#include "core/payload/payloadtype.h"
#include "equalposition_comparator_impl.h"

namespace reindexer {

enum class [[nodiscard]] FieldPathPartFlags { None = 0, Array };
struct [[nodiscard]] FieldPathPart {
	std::string_view name;
	FieldPathPartFlags flags = FieldPathPartFlags::None;
	TagName tag = TagName::Empty();
};

using FieldPath = h_vector<FieldPathPart, 4>;
namespace equal_position_helpers {
void ParseStrPath(std::string_view str, FieldPath& path);
}

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
		const auto jsonPathComparators = fields_.getTagsPathsLength();
		// Comparatos with non index fields must have much higher cost, than comparators with index fields
		return jsonPathComparators
				   ? (comparators::kNonIdxFieldComparatorCostMultiplier * double(expectedIterations) + jsonPathComparators + 1.0)
				   : (double(expectedIterations) + 1.0);
	}
	void ExcludeDistinctValues(const PayloadValue& /*item*/, IdType /*rowId*/) const noexcept {}
	reindexer::IsDistinct IsDistinct() const noexcept { return IsDistinct_False; }

	auto Name() const&& = delete;
	auto Dump() const&& = delete;

private:
	bool compareField(size_t field, const Variant&);
	template <typename F>
	void bindField(const std::string& name, F field, const VariantArray&, CondType, const CollateOpts&);

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
		return comparators::kNonIdxFieldComparatorCostMultiplier * double(expectedIterations) + fieldPathPart_.size() + 1.0;
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
	std::vector<FieldPath> fieldPathPart_;
	std::vector<Context> ctx_;
	std::string name_;
};

}  // namespace reindexer
