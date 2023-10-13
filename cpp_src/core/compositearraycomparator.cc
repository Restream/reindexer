#include "compositearraycomparator.h"
#include <limits.h>
namespace reindexer {

CompositeArrayComparator::CompositeArrayComparator() {}

void CompositeArrayComparator::BindField(int field, const VariantArray &values, CondType condType) {
	fields_.push_back(field);
	Context &ctx = ctx_.emplace_back();

	ctx.cond = condType;
	ctx.cmpBool.SetValues(condType, values);
	ctx.cmpInt.SetValues(condType, values);
	ctx.cmpInt64.SetValues(condType, values);
	ctx.cmpString.SetValues(condType, values, CollateOpts());
	ctx.cmpDouble.SetValues(condType, values);
	ctx.cmpUuid.SetValues(condType, values);

	assertrx(ctx_.size() == fields_.size());
}

void CompositeArrayComparator::BindField(const TagsPath &tagsPath, const VariantArray &values, CondType condType) {
	fields_.push_back(tagsPath);
	Context &ctx = ctx_.emplace_back();

	ctx.cond = condType;
	ctx.cmpBool.SetValues(condType, values);
	ctx.cmpInt.SetValues(condType, values);
	ctx.cmpInt64.SetValues(condType, values);
	ctx.cmpString.SetValues(condType, values, CollateOpts());
	ctx.cmpDouble.SetValues(condType, values);
	ctx.cmpUuid.SetValues(condType, values);
}

bool CompositeArrayComparator::Compare(const PayloadValue &pv, const ComparatorVars &vars) {
	ConstPayload pl(vars.payloadType_, pv);
	size_t len = INT_MAX;

	h_vector<VariantArray, 2> vals;
	size_t tagsPathIdx = 0;
	vals.reserve(fields_.size());
	for (size_t j = 0; j < fields_.size(); ++j) {
		auto &v = vals.emplace_back();
		bool isRegularIndex = fields_[j] != IndexValueType::SetByJsonPath && fields_[j] < vars.payloadType_.NumFields();
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
			cmpRes &= !vals[j][i].Type().Is<KeyValueType::Null>() && compareField(j, vals[j][i], vars);
			if (!cmpRes) break;
		}

		if (cmpRes) return true;
	}
	return false;
}

bool CompositeArrayComparator::compareField(size_t field, const Variant &v, const ComparatorVars &vars) {
	return v.Type().EvaluateOneOf(
		[&](KeyValueType::Bool) { return ctx_[field].cmpBool.Compare(ctx_[field].cond, static_cast<bool>(v)); },
		[&](KeyValueType::Int) { return ctx_[field].cmpInt.Compare(ctx_[field].cond, static_cast<int>(v)); },
		[&](KeyValueType::Int64) { return ctx_[field].cmpInt64.Compare(ctx_[field].cond, static_cast<int64_t>(v)); },
		[&](KeyValueType::Double) { return ctx_[field].cmpDouble.Compare(ctx_[field].cond, static_cast<double>(v)); },
		[&](KeyValueType::String) { return ctx_[field].cmpString.Compare(ctx_[field].cond, static_cast<p_string>(v), vars.collateOpts_); },
		[&](KeyValueType::Uuid) { return ctx_[field].cmpUuid.Compare(ctx_[field].cond, Uuid{v}); },
		[](OneOf<KeyValueType::Null, KeyValueType::Undefined, KeyValueType::Composite, KeyValueType::Tuple>) noexcept { return false; });
}

}  // namespace reindexer
