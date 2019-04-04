#include "compositearraycomparator.h"
#include <limits.h>
namespace reindexer {

CompositeArrayComparator::CompositeArrayComparator() {}

void CompositeArrayComparator::BindField(int field, const VariantArray &values, CondType condType) {
	fields_.push_back(field);
	ctx_.push_back(Context());
	Context &ctx = ctx_.back();

	ctx.cond = condType;
	ctx.cmpBool.SetValues(condType, values);
	ctx.cmpInt.SetValues(condType, values);
	ctx.cmpInt64.SetValues(condType, values);
	ctx.cmpString.SetValues(condType, values);
	ctx.cmpDouble.SetValues(condType, values);

	assert(ctx_.size() == fields_.size());
}

void CompositeArrayComparator::BindField(const TagsPath &tagsPath, const VariantArray &values, CondType condType) {
	fields_.push_back(tagsPath);
	ctx_.push_back(Context());
	Context &ctx = ctx_.back();

	ctx.cond = condType;
	ctx.cmpBool.SetValues(condType, values);
	ctx.cmpInt.SetValues(condType, values);
	ctx.cmpInt64.SetValues(condType, values);
	ctx.cmpString.SetValues(condType, values);
	ctx.cmpDouble.SetValues(condType, values);
}

bool CompositeArrayComparator::Compare(const PayloadValue &pv, const ComparatorVars &vars) {
	ConstPayload pl(vars.payloadType_, pv);
	size_t len = INT_MAX;

	h_vector<VariantArray, 2> vals;
	size_t tagsPathIdx = 0;
	for (size_t j = 0; j < fields_.size(); ++j) {
		vals.push_back({});
		bool isRegularIndex = fields_[j] != IndexValueType::SetByJsonPath && fields_[j] < vars.payloadType_.NumFields();
		if (isRegularIndex) {
			pl.Get(fields_[j], vals.back());
		} else {
			assert(tagsPathIdx < fields_.getTagsPathsLength());
			pl.GetByJsonPath(fields_.getTagsPath(tagsPathIdx++), vals.back(), KeyValueUndefined);
		}
		if (vals.back().size() < len) len = vals.back().size();
	}

	for (size_t i = 0; i < len; ++i) {
		bool cmpRes = true;
		for (size_t j = 0; j < fields_.size(); ++j) {
			assert(i < vals[j].size());
			cmpRes &= vals[j][i].Type() != KeyValueNull && compareField(j, vals[j][i], vars);
			if (!cmpRes) break;
		}

		if (cmpRes) return true;
	}
	return false;
}

bool CompositeArrayComparator::compareField(size_t field, const Variant &v, const ComparatorVars &vars) {
	bool result = true;
	switch (v.Type()) {
		case KeyValueBool:
			result = ctx_[field].cmpBool.Compare(ctx_[field].cond, static_cast<bool>(v));
			break;
		case KeyValueInt:
			result = ctx_[field].cmpInt.Compare(ctx_[field].cond, static_cast<int>(v));
			break;
		case KeyValueInt64:
			result = ctx_[field].cmpInt64.Compare(ctx_[field].cond, static_cast<int64_t>(v));
			break;
		case KeyValueDouble:
			result = ctx_[field].cmpDouble.Compare(ctx_[field].cond, static_cast<double>(v));
			break;
		case KeyValueString: {
			result = ctx_[field].cmpString.Compare(ctx_[field].cond, static_cast<p_string>(v), vars.collateOpts_);
			break;
		}
		case KeyValueNull:
		case KeyValueUndefined:
		case KeyValueComposite:
		case KeyValueTuple:
			result = false;
			break;
	}

	return result;
}

}  // namespace reindexer
