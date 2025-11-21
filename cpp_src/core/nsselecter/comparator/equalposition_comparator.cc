#include "equalposition_comparator.h"

#include <limits.h>

#include "core/cjson/baseencoder.h"
#include "core/cjson/field_extractor_grouping.h"
#include "core/payload/payloadiface.h"

namespace reindexer {

void EqualPositionComparator::BindField(const std::string& name, int field, const VariantArray& values, CondType cond,
										const CollateOpts& collate) {
	bindField(name, field, values, cond, collate);
}

void EqualPositionComparator::BindField(const std::string& name, const FieldsPath& fieldPath, const VariantArray& values, CondType cond) {
	bindField(name, fieldPath, values, cond, CollateOpts{});
}

template <typename F>
void EqualPositionComparator::bindField(const std::string& name, F field, const VariantArray& values, CondType cond,
										const CollateOpts& collate) {
	fields_.push_back(field);
	Context& ctx = ctx_.emplace_back(collate);

	ctx.cond = cond;
	ctx.cmpBool.SetValues(cond, values);
	ctx.cmpInt.SetValues(cond, values);
	ctx.cmpInt64.SetValues(cond, values);
	ctx.cmpString.SetValues(cond, values);
	ctx.cmpDouble.SetValues(cond, values);
	ctx.cmpFloat.SetValues(cond, values);
	ctx.cmpUuid.SetValues(cond, values);
	assertrx_throw(ctx_.size() == fields_.size());

	name_.append(" ").append(name);
}

bool EqualPositionComparator::Compare(const PayloadValue& pv, IdType /*rowId*/) {
	++totalCalls_;
	ConstPayload pl(payloadType_, pv);
	size_t len = INT_MAX;

	h_vector<VariantArray, 2> vals;
	size_t tagsPathIdx = 0;
	vals.reserve(fields_.size());
	for (const auto& field : fields_) {
		auto& v = vals.emplace_back();
		bool isRegularIndex = field != IndexValueType::SetByJsonPath && field < payloadType_.NumFields();
		if (isRegularIndex) {
			pl.Get(field, v);
		} else {
			assertrx_throw(tagsPathIdx < fields_.getTagsPathsLength());
			pl.GetByJsonPath(fields_.getTagsPath(tagsPathIdx++), v, KeyValueType::Undefined{});
		}
		len = std::min<size_t>(len, v.size());
	}

	bool res = false;
	for (size_t i = 0; i < len; ++i) {
		bool cmpRes = true;
		for (size_t j = 0; j < fields_.size(); ++j) {
			assertrx_throw(i < vals[j].size());
			if (!compareField(j, vals[j][i])) {
				cmpRes = false;
				break;
			}
		}

		if (cmpRes) {
			res = true;
			break;
		}
	}
	matchedCount_ += int(res);
	return res;
}

bool EqualPositionComparator::compareField(size_t field, const Variant& v) {
	return v.Type().EvaluateOneOf(
		[&](KeyValueType::Bool) { return ctx_[field].cmpBool.Compare(ctx_[field].cond, static_cast<bool>(v)); },
		[&](KeyValueType::Int) { return ctx_[field].cmpInt.Compare(ctx_[field].cond, static_cast<int>(v)); },
		[&](KeyValueType::Int64) { return ctx_[field].cmpInt64.Compare(ctx_[field].cond, static_cast<int64_t>(v)); },
		[&](KeyValueType::Double) { return ctx_[field].cmpDouble.Compare(ctx_[field].cond, static_cast<double>(v)); },
		[&](KeyValueType::Float) { return ctx_[field].cmpFloat.Compare(ctx_[field].cond, static_cast<float>(v)); },
		[&](KeyValueType::String) { return ctx_[field].cmpString.Compare(ctx_[field].cond, static_cast<p_string>(v)); },
		[&](KeyValueType::Uuid) { return ctx_[field].cmpUuid.Compare(ctx_[field].cond, Uuid{v}); },
		[](concepts::OneOf<KeyValueType::Null, KeyValueType::Undefined, KeyValueType::Composite, KeyValueType::Tuple,
						   KeyValueType::FloatVector> auto) noexcept { return false; });
}

void GroupingEqualPositionComparator::BindField(const std::string& name, const VariantArray& values, CondType cond,
												const std::string& fieldStr) {
	fieldPathPart_.emplace_back();
	equal_position_helpers::ParseStrPath(fieldStr, fieldPathPart_.back());
	for (auto& v : fieldPathPart_.back()) {
		v.tag = tm_->name2tag(v.name);
		if (v.tag.IsEmpty()) {
			throw Error(Error(errParams, "Equal position tag is empty. Name {}", v.name));
		}
	}
	Context& ctx = ctx_.emplace_back(CollateOpts{});

	ctx.cond = cond;
	ctx.cmpBool.SetValues(cond, values);
	ctx.cmpInt.SetValues(cond, values);
	ctx.cmpInt64.SetValues(cond, values);
	ctx.cmpString.SetValues(cond, values);
	ctx.cmpDouble.SetValues(cond, values);
	ctx.cmpFloat.SetValues(cond, values);
	ctx.cmpUuid.SetValues(cond, values);

	name_.append(" ").append(name);
}

bool GroupingEqualPositionComparator::Compare(const PayloadValue& pv, IdType /*rowId*/) {
	++totalCalls_;
	ConstPayload pl(payloadType_, pv);
	size_t minLevel = INT_MAX;

	h_vector<h_vector<VariantArray, 2>, 2> eqPosVals;
	eqPosVals.resize(fieldPathPart_.size());
	for (size_t i = 0; i < fieldPathPart_.size(); i++) {
		BaseEncoder<FieldsExtractorGrouping> encoder(tm_, nullptr);
		const std::span<FieldPathPart> p(fieldPathPart_[i]);
		unsigned int index;
		FieldsExtractorGroupingState state;
		state.arrayIndex = &index;
		state.values = &eqPosVals[i];
		state.path = p;
		FieldsExtractorGrouping extractor{state};
		encoder.Encode(pl, extractor);
		minLevel = std::min(minLevel, size_t(state.values->size()));
	}

	bool res = false;
	for (size_t level = 0; level < minLevel; level++) {
		unsigned cmpFieldCounter = 0;
		for (size_t f = 0; f < eqPosVals.size(); f++) {
			bool cmpVal = false;
			for (size_t v = 0; v < eqPosVals[f][level].size(); v++) {
				if (compareField(f, eqPosVals[f][level][v])) {
					cmpVal = true;
					break;
				}
			}
			if (cmpVal) {
				cmpFieldCounter++;
			} else {
				break;
			}
		}
		if (cmpFieldCounter == eqPosVals.size()) {
			res = true;
			break;
		}
	}

	matchedCount_ += int(res);
	return res;
}

bool GroupingEqualPositionComparator::compareField(size_t field, const Variant& v) {
	return v.Type().EvaluateOneOf(
		[&](KeyValueType::Bool) { return ctx_[field].cmpBool.Compare(ctx_[field].cond, static_cast<bool>(v)); },
		[&](KeyValueType::Int) { return ctx_[field].cmpInt.Compare(ctx_[field].cond, static_cast<int>(v)); },
		[&](KeyValueType::Int64) { return ctx_[field].cmpInt64.Compare(ctx_[field].cond, static_cast<int64_t>(v)); },
		[&](KeyValueType::Double) { return ctx_[field].cmpDouble.Compare(ctx_[field].cond, static_cast<double>(v)); },
		[&](KeyValueType::Float) { return ctx_[field].cmpFloat.Compare(ctx_[field].cond, static_cast<float>(v)); },
		[&](KeyValueType::String) { return ctx_[field].cmpString.Compare(ctx_[field].cond, static_cast<p_string>(v)); },
		[&](KeyValueType::Uuid) { return ctx_[field].cmpUuid.Compare(ctx_[field].cond, Uuid{v}); },
		[](concepts::OneOf<KeyValueType::Null, KeyValueType::Undefined, KeyValueType::Composite, KeyValueType::Tuple,
						   KeyValueType::FloatVector> auto) noexcept { return false; });
}

namespace equal_position_helpers {

static FieldPathPart parsePathPart(std::string_view str, unsigned int& i) {
	FieldPathPart p;
	while (i < str.size()) {
		if (isalpha(str[i]) || isdigit(str[i]) || str[i] == '_') {
			if (p.name.empty()) {
				p.name = std::string_view(&str[i], 1);
			} else {
				p.name = std::string_view(p.name.data(), p.name.size() + 1);
			}
			++i;
		} else if (str[i] == '.') {
			if (p.name.empty()) {
				throw Error(errParams, "Equal position path parse error. Name is empty");
			}
			++i;
			return p;
		} else if (str[i] == '[') {
			++i;
			if (i < str.size() && str[i] == '#') {
				if (p.flags == FieldPathPartFlags::Array) {
					throw Error(errParams, "Equal position flag '#' is already set");
				}
				p.flags = FieldPathPartFlags::Array;
				i++;
			}
			if (i >= str.size() || str[i] != ']') {
				throw Error(errParams, "Equal position path parse error");
			}
			++i;
		} else {
			throw Error(errParams, "Equal position path parse error. Incorrect symbol '{}'", str[i]);
		}
	}
	if (p.name.empty()) {
		throw Error(errParams, "Equal position path parse error. Name is empty");
	}
	return p;
}

void ParseStrPath(std::string_view str, FieldPath& path) {
	unsigned int i = 0;
	unsigned arrayFlagCounter = 0;
	while (i < str.size()) {
		FieldPathPart part = parsePathPart(str, i);
		if (part.flags == FieldPathPartFlags::Array) {
			arrayFlagCounter++;
		}
		if (arrayFlagCounter > 1) {
			throw Error(errParams, "Equal position field can contain only one array marker");
		}
		path.emplace_back(std::move(part));
	}
}
}  // namespace equal_position_helpers

}  // namespace reindexer
