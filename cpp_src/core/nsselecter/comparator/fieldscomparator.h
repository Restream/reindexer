#pragma once

#include "const.h"
#include "core/id_type.h"
#include "core/indexopts.h"
#include "core/payload/fieldsset.h"
#include "core/payload/payloadtype.h"
#include "core/payload/payloadtypeimpl.h"
#include "core/payload/payloadvalue.h"

namespace reindexer {

class [[nodiscard]] FieldsComparator {
public:
	FieldsComparator(std::string_view lField, CondType cond, std::string_view rField, PayloadType plType);

	bool Compare(const PayloadValue& item, IdType /*rowId*/) {
		++totalCalls_;
		if (ctx_.size() == 1) {
			const bool res = compare(item, ctx_[0]);
			matchedCount_ += int(res);
			return res;
		}
		for (const auto& c : ctx_) {
			if (!compare(item, c)) {
				return false;
			}
		}
		++matchedCount_;
		return true;
	}
	double Cost(int expectedIterations) const noexcept {
		double cost = 1.0;
		bool hasNonIdxFields = true;
		if (ctx_.size()) {
			if (ctx_[0].lCtx_.fields_.getTagsPathsLength() > 0) {
				cost += double(expectedIterations) * comparators::kNonIdxFieldComparatorCostMultiplier;
				hasNonIdxFields = false;
			}
			if (ctx_[0].rCtx_.fields_.getTagsPathsLength() > 0) {
				cost += double(expectedIterations) * comparators::kNonIdxFieldComparatorCostMultiplier;
				hasNonIdxFields = false;
			}
		}
		return hasNonIdxFields ? cost : (double(expectedIterations) + cost);
	}
	const std::string& Name() const& noexcept { return name_; }
	const std::string& Name() const&& = delete;
	const std::string& Dump() const& noexcept { return Name(); }
	const std::string& Dump() const&& = delete;
	void SetLeftField(const FieldsSet& fields);
	void SetRightField(const FieldsSet& fields);
	void SetLeftField(const FieldsSet& fset, KeyValueType type, IsArray isArray, const CollateOpts& cOpts);
	void SetRightField(const FieldsSet& fset, KeyValueType type, IsArray isArray);
	int GetMatchedCount(bool invert) const noexcept {
		assertrx_dbg(totalCalls_ >= matchedCount_);
		return invert ? (totalCalls_ - matchedCount_) : matchedCount_;
	}
	void ExcludeDistinctValues(const PayloadValue& /*item*/, IdType /*rowId*/) const noexcept {}
	reindexer::IsDistinct IsDistinct() const noexcept { return IsDistinct_False; }

private:
	struct [[nodiscard]] FieldContext {
		FieldsSet fields_;
		KeyValueType type_ = KeyValueType::Undefined{};
		IsArray isArray_ = IsArray_False;
		unsigned offset_ = 0;
		unsigned sizeof_ = 0;
	};
	struct [[nodiscard]] Context {
		FieldContext lCtx_;
		FieldContext rCtx_;
	};

	void validateFieldsSizes(const FieldsSet& newFields);
	void setField(const TagsPath& tpath, FieldContext& fctx) { fctx.fields_.push_back(tpath); }
	void setField(const FieldsSet& fields, FieldContext& fctx);
	void setField(FieldContext& fctx, FieldsSet fset, KeyValueType type, IsArray isArray);
	template <bool left>
	void setCompositeField(const FieldsSet& fields);
	template <typename LArr, typename RArr>
	bool compare(const LArr& lhs, const RArr& rhs) const;
	bool compare(const PayloadValue& item, const Context&) const;
	void validateTypes(KeyValueType lType, KeyValueType rType) const;
	inline static bool compareTypes(KeyValueType lType, KeyValueType rType) noexcept {
		if (lType.IsSame(rType)) {
			return true;
		}
		return lType.EvaluateOneOf(
			[&](concepts::OneOf<KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double, KeyValueType::Float> auto) noexcept {
				return rType.EvaluateOneOf(
					[](concepts::OneOf<KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double, KeyValueType::Float> auto) noexcept {
						return true;
					},
					[](concepts::OneOf<KeyValueType::Bool, KeyValueType::String, KeyValueType::Null, KeyValueType::Undefined,
									   KeyValueType::Composite, KeyValueType::Tuple, KeyValueType::Uuid,
									   KeyValueType::FloatVector> auto) noexcept { return false; });
			},
			[](concepts::OneOf<KeyValueType::Bool, KeyValueType::String, KeyValueType::Null, KeyValueType::Undefined,
							   KeyValueType::Composite, KeyValueType::Tuple, KeyValueType::Uuid, KeyValueType::FloatVector> auto) noexcept {
				return false;
			});
	}

	std::string name_;
	int totalCalls_{0};
	int matchedCount_{0};
	std::vector<Context> ctx_;
	CondType condition_;
	PayloadType payloadType_;
	const CollateOpts* collateOpts_{nullptr};
	bool leftFieldSet_{false};
};

}  // namespace reindexer
