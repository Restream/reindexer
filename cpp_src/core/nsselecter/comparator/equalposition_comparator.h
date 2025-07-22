#pragma once

#include "const.h"
#include "core/payload/fieldsset.h"
#include "core/payload/payloadtype.h"
#include "equalposition_comparator_impl.h"

namespace reindexer {

class EqualPositionComparator {
public:
	EqualPositionComparator(const PayloadType& payloadType) : payloadType_{payloadType}, name_{"EqualPositions"} {}

	void BindField(const std::string& name, int field, const VariantArray&, CondType, const CollateOpts&);
	void BindField(const std::string& name, const FieldsPath&, const VariantArray&, CondType);
	bool Compare(const PayloadValue&, IdType);
	bool IsBinded() noexcept { return !ctx_.empty(); }
	[[nodiscard]] int GetMatchedCount() const noexcept { return matchedCount_; }
	[[nodiscard]] int FieldsCount() const noexcept { return ctx_.size(); }
	[[nodiscard]] const std::string& Name() const& noexcept { return name_; }
	[[nodiscard]] const std::string& Dump() const& noexcept { return Name(); }
	[[nodiscard]] double Cost(double expectedIterations) const noexcept {
		const auto jsonPathComparators = fields_.getTagsPathsLength();
		// Comparatos with non index fields must have much higher cost, than comparators with index fields
		return jsonPathComparators
				   ? (comparators::kNonIdxFieldComparatorCostMultiplier * double(expectedIterations) + jsonPathComparators + 1.0)
				   : (double(expectedIterations) + 1.0);
	}

	auto Name() const&& = delete;
	auto Dump() const&& = delete;

private:
	bool compareField(size_t field, const Variant&);
	template <typename F>
	void bindField(const std::string& name, F field, const VariantArray&, CondType, const CollateOpts&);

	struct Context {
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

	std::vector<Context> ctx_;
	FieldsSet fields_;
	PayloadType payloadType_;
	std::string name_;
	int matchedCount_{0};
};

}  // namespace reindexer
