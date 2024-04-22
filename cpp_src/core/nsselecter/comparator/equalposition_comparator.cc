#include "equalposition_comparator.h"

#include <limits.h>

#include "core/payload/payloadiface.h"

namespace reindexer {

void EqualPositionComparator::BindField(const std::string &name, int field, const VariantArray &values, CondType cond,
										const CollateOpts &collate) {
	bindField(name, field, values, cond, collate);
}

void EqualPositionComparator::BindField(const std::string &name, const FieldsPath &fieldPath, const VariantArray &values, CondType cond) {
	bindField(name, fieldPath, values, cond, CollateOpts{});
}

template <typename F>
void EqualPositionComparator::bindField(const std::string &name, F field, const VariantArray &values, CondType cond,
										const CollateOpts &collate) {
	fields_.push_back(field);
	Context &ctx = ctx_.emplace_back(collate);

	ctx.cond = cond;
	ctx.cmpBool.SetValues(cond, values);
	ctx.cmpInt.SetValues(cond, values);
	ctx.cmpInt64.SetValues(cond, values);
	ctx.cmpString.SetValues(cond, values);
	ctx.cmpDouble.SetValues(cond, values);
	ctx.cmpUuid.SetValues(cond, values);
	assertrx(ctx_.size() == fields_.size());

	name_ += ' ' + name;
}

bool EqualPositionComparator::Compare(const PayloadValue &pv, IdType /*rowId*/) {
	ConstPayload pl(payloadType_, pv);
	size_t len = INT_MAX;

	h_vector<VariantArray, 2> vals;
	size_t tagsPathIdx = 0;
	vals.reserve(fields_.size());
	for (size_t j = 0; j < fields_.size(); ++j) {
		auto &v = vals.emplace_back();
		bool isRegularIndex = fields_[j] != IndexValueType::SetByJsonPath && fields_[j] < payloadType_.NumFields();
		if (isRegularIndex) {
			pl.Get(fields_[j], v);
		} else {
			assertrx(tagsPathIdx < fields_.getTagsPathsLength());
			pl.GetByJsonPath(fields_.getTagsPath(tagsPathIdx++), v, KeyValueType::Undefined{});
		}
		if (v.size() < len) len = vals.back().size();
	}

	for (size_t i = 0; i < len; ++i) {
		bool cmpRes = true;
		for (size_t j = 0; j < fields_.size(); ++j) {
			assertrx(i < vals[j].size());
			cmpRes &= !vals[j][i].Type().Is<KeyValueType::Null>() && compareField(j, vals[j][i]);
			if (!cmpRes) break;
		}

		if (cmpRes) {
			++matchedCount_;
			return true;
		}
	}
	return false;
}

bool EqualPositionComparator::compareField(size_t field, const Variant &v) {
	return v.Type().EvaluateOneOf(
		[&](KeyValueType::Bool) { return ctx_[field].cmpBool.Compare(ctx_[field].cond, static_cast<bool>(v)); },
		[&](KeyValueType::Int) { return ctx_[field].cmpInt.Compare(ctx_[field].cond, static_cast<int>(v)); },
		[&](KeyValueType::Int64) { return ctx_[field].cmpInt64.Compare(ctx_[field].cond, static_cast<int64_t>(v)); },
		[&](KeyValueType::Double) { return ctx_[field].cmpDouble.Compare(ctx_[field].cond, static_cast<double>(v)); },
		[&](KeyValueType::String) { return ctx_[field].cmpString.Compare(ctx_[field].cond, static_cast<p_string>(v)); },
		[&](KeyValueType::Uuid) { return ctx_[field].cmpUuid.Compare(ctx_[field].cond, Uuid{v}); },
		[](OneOf<KeyValueType::Null, KeyValueType::Undefined, KeyValueType::Composite, KeyValueType::Tuple>) noexcept { return false; });
}

}  // namespace reindexer
