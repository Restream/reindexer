#pragma once

#include "const.h"
#include "core/indexopts.h"
#include "core/payload/fieldsset.h"
#include "core/payload/payloadtype.h"
#include "core/payload/payloadtypeimpl.h"
#include "core/payload/payloadvalue.h"
#include "estl/one_of.h"

namespace reindexer {

class FieldsComparatorImpl : public intrusive_rc_base {
public:
	FieldsComparatorImpl(std::string_view lField, CondType cond, std::string_view rField, PayloadType plType);
	FieldsComparatorImpl(const FieldsComparatorImpl&) = delete;
	FieldsComparatorImpl(FieldsComparatorImpl&&) = delete;
	FieldsComparatorImpl& operator=(const FieldsComparatorImpl&) = delete;
	FieldsComparatorImpl& operator=(FieldsComparatorImpl&&) = delete;

	bool Compare(const PayloadValue& item, IdType /*rowId*/) {
		if (ctx_.size() > 1) {
			for (const auto& c : ctx_) {
				if (!compare(item, c)) {
					return false;
				}
			}
			return true;
		}
		return compare(item, ctx_[0]);
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
	int GetMatchedCount() const noexcept { return matchedCount_; }
	void SetLeftField(const FieldsSet& fields) {
		setField(fields, ctx_[0].lCtx_);
		leftFieldSet = true;
	}
	void SetRightField(const FieldsSet& fields) {
		assertrx_dbg(leftFieldSet);
		setField(fields, ctx_[0].rCtx_);
	}
	void SetLeftField(const FieldsSet& fset, KeyValueType type, bool isArray) {
		if (type.Is<KeyValueType::Composite>()) {
			ctx_.clear();
			ctx_.resize(fset.size());
			setCompositeField<true>(fset);
		} else {
			setField(ctx_[0].lCtx_, fset, type, isArray);
		}
		leftFieldSet = true;
	}
	void SetRightField(const FieldsSet& fset, KeyValueType type, bool isArray) {
		assertrx_dbg(leftFieldSet);
		if ((ctx_.size() > 1) != type.Is<KeyValueType::Composite>()) {
			throw Error{errQueryExec, "A composite index cannot be compared with a non-composite one: %s", name_};
		}
		if (type.Is<KeyValueType::Composite>()) {
			if (ctx_.size() != fset.size()) {
				throw Error{errQueryExec, "Comparing composite indexes should be the same size: %s", name_};
			}
			setCompositeField<false>(fset);
		} else {
			validateTypes(ctx_[0].lCtx_.type_, type);
			setField(ctx_[0].rCtx_, fset, type, isArray);
		}
	}
	void SetCollateOpts(const CollateOpts& cOpts) { collateOpts_ = cOpts; }

private:
	struct FieldContext {
		FieldsSet fields_;
		KeyValueType type_ = KeyValueType::Undefined{};
		bool isArray_ = false;
		unsigned offset_ = 0;
		unsigned sizeof_ = 0;
	};
	struct Context {
		FieldContext lCtx_;
		FieldContext rCtx_;
	};

	void setField(const TagsPath& tpath, FieldContext& fctx) { fctx.fields_.push_back(tpath); }
	void setField(const FieldsSet& fields, FieldContext& fctx) {
		assertrx_dbg(fields.size() == 1);
		assertrx_dbg(fields[0] == IndexValueType::SetByJsonPath);
		setField(fields.getTagsPath(0), fctx);
	}
	void setField(FieldContext& fctx, FieldsSet fset, KeyValueType type, bool isArray) {
		fctx.fields_ = std::move(fset);
		fctx.type_ = type;
		fctx.isArray_ = isArray;
		if (fctx.fields_.getTagsPathsLength() == 0) {
			const auto ft{payloadType_->Field(fctx.fields_[0])};
			fctx.offset_ = ft.Offset();
			fctx.sizeof_ = ft.ElemSizeof();
		}
	}
	template <bool left>
	void setCompositeField(const FieldsSet& fields) {
		size_t tagsPathIdx = 0;
		for (size_t i = 0; i < fields.size(); ++i) {
			const bool isRegularIndex = fields[i] != IndexValueType::SetByJsonPath && fields[i] < payloadType_.NumFields();
			if (isRegularIndex) {
				FieldsSet f;
				f.push_back(fields[i]);
				const auto ft{payloadType_.Field(fields[i])};
				setField(left ? ctx_[i].lCtx_ : ctx_[i].rCtx_, std::move(f), ft.Type(), ft.IsArray());
				if constexpr (!left) {
					validateTypes(ctx_[i].lCtx_.type_, ctx_[i].rCtx_.type_);
				}
			} else {
				assertrx_dbg(tagsPathIdx < fields.getTagsPathsLength());
				setField(fields.getTagsPath(tagsPathIdx++), left ? ctx_[i].lCtx_ : ctx_[i].rCtx_);
			}
		}
	}
	template <typename LArr, typename RArr>
	bool compare(const LArr& lhs, const RArr& rhs);
	bool compare(const PayloadValue& item, const Context&);
	void validateTypes(KeyValueType lType, KeyValueType rType) const;
	inline static bool compareTypes(KeyValueType lType, KeyValueType rType) noexcept {
		if (lType.IsSame(rType)) {
			return true;
		}
		return lType.EvaluateOneOf(
			[&](OneOf<KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double>) noexcept {
				return rType.EvaluateOneOf(
					[](OneOf<KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double>) noexcept { return true; },
					[](OneOf<KeyValueType::Bool, KeyValueType::String, KeyValueType::Null, KeyValueType::Undefined, KeyValueType::Composite,
							 KeyValueType::Tuple, KeyValueType::Uuid>) noexcept { return false; });
			},
			[](OneOf<KeyValueType::Bool, KeyValueType::String, KeyValueType::Null, KeyValueType::Undefined, KeyValueType::Composite,
					 KeyValueType::Tuple, KeyValueType::Uuid>) noexcept { return false; });
	}

	std::string name_;
	CondType condition_;
	PayloadType payloadType_;
	CollateOpts collateOpts_;
	h_vector<Context, 1> ctx_{Context{}};
	int matchedCount_ = 0;
	bool leftFieldSet = false;
};

class FieldsComparator {
public:
	FieldsComparator(std::string_view lField, CondType cond, std::string_view rField, PayloadType plType)
		: impl_{make_intrusive<FieldsComparatorImpl>(lField, cond, rField, plType)} {}
	bool Compare(const PayloadValue& item, IdType rowId) { return impl_->Compare(item, rowId); }
	double Cost(int expectedIterations) const noexcept { return impl_->Cost(expectedIterations); }
	const std::string& Name() const& noexcept { return impl_->Name(); }
	const std::string& Name() const&& = delete;
	const std::string& Dump() const& noexcept { return impl_->Dump(); }
	const std::string& Dump() const&& = delete;
	int GetMatchedCount() const noexcept { return impl_->GetMatchedCount(); }
	void SetLeftField(const FieldsSet& fields) { return impl_->SetLeftField(fields); }
	void SetRightField(const FieldsSet& fields) { return impl_->SetRightField(fields); }
	void SetLeftField(const FieldsSet& fset, KeyValueType type, bool isArray) { return impl_->SetLeftField(fset, type, isArray); }
	void SetRightField(const FieldsSet& fset, KeyValueType type, bool isArray) { return impl_->SetRightField(fset, type, isArray); }
	void SetCollateOpts(const CollateOpts& cOpts) { impl_->SetCollateOpts(cOpts); }

private:
	// Using pointer to reduce ExpressionTree Node size
	intrusive_ptr<FieldsComparatorImpl> impl_;
};

}  // namespace reindexer
