#pragma once

#include "core/indexopts.h"
#include "core/payload/fieldsset.h"
#include "core/payload/payloadtype.h"
#include "core/payload/payloadtypeimpl.h"
#include "core/payload/payloadvalue.h"

namespace reindexer {

class FieldsComparator {
public:
	FieldsComparator(std::string_view lField, CondType cond, std::string_view rField, PayloadType plType);
	bool Compare(const PayloadValue& item);
	double Cost(int expectedIterations) const noexcept { return expectedIterations + 1; }
	const std::string& Name() const noexcept { return name_; }
	std::string Dump() const { return Name(); }
	int GetMatchedCount() const noexcept { return matchedCount_; }
	void SetLeftField(const TagsPath& tpath) {
		setField(tpath, ctx_[0].lCtx_);
		leftFieldSet = true;
	}
	void SetRightField(const TagsPath& tpath) {
		assert(leftFieldSet);
		setField(tpath, ctx_[0].rCtx_);
	}
	void SetLeftField(FieldsSet fset, KeyValueType type, bool isArray) {
		if (type == KeyValueComposite) {
			ctx_.clear();
			ctx_.resize(fset.size());
			setCompositeField<true>(fset);
		} else {
			setField(ctx_[0].lCtx_, fset, type, isArray);
		}
		leftFieldSet = true;
	}
	void SetRightField(FieldsSet fset, KeyValueType type, bool isArray) {
		assert(leftFieldSet);
		if ((ctx_.size() > 1) != (type == KeyValueComposite)) {
			throw Error{errQueryExec, "A composite index cannot be compared with a non-composite one: %s", name_};
		}
		if (type == KeyValueComposite) {
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
		KeyValueType type_ = KeyValueUndefined;
		bool isArray_ = false;
		unsigned offset_ = 0;
		unsigned sizeof_ = 0;
	};
	struct Context {
		FieldContext lCtx_;
		FieldContext rCtx_;
	};

	void setField(const TagsPath& tpath, FieldContext& fctx) { fctx.fields_.push_back(tpath); }
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
	void setCompositeField(FieldsSet fields) {
		size_t tagsPathIdx = 0;
		for (size_t i = 0; i < fields.size(); ++i) {
			const bool isRegularIndex = fields[i] != IndexValueType::SetByJsonPath && fields[i] < payloadType_.NumFields();
			if (isRegularIndex) {
				FieldsSet f;
				f.push_back(fields[i]);
				const auto ft{payloadType_.Field(fields[i])};
				setField(left ? ctx_[i].lCtx_ : ctx_[i].rCtx_, f, ft.Type(), ft.IsArray());
				if constexpr (!left) {
					validateTypes(ctx_[i].lCtx_.type_, ctx_[i].rCtx_.type_);
				}
			} else {
				assert(tagsPathIdx < fields.getTagsPathsLength());
				setField(fields.getTagsPath(tagsPathIdx++), left ? ctx_[i].lCtx_ : ctx_[i].rCtx_);
			}
		}
	}
	template <typename LArr, typename RArr>
	bool compare(const LArr& lhs, const RArr& rhs);
	bool compare(const PayloadValue& item, const Context&);
	void validateTypes(KeyValueType lType, KeyValueType rType) const;
	inline static bool compareTypes(KeyValueType lType, KeyValueType rType) noexcept {
		if (lType == rType) return true;
		switch (lType) {
			case KeyValueInt:
			case KeyValueInt64:
			case KeyValueDouble:
				return rType == KeyValueInt || rType == KeyValueInt64 || rType == KeyValueDouble;
			default:
				return false;
		}
	}

	std::string name_;
	CondType condition_;
	PayloadType payloadType_;
	CollateOpts collateOpts_;
	h_vector<Context, 1> ctx_{Context{}};
	int matchedCount_ = 0;
	bool leftFieldSet = false;
};

}  // namespace reindexer
