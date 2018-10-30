#include "core/aggregator.h"
#include <limits>
#include "core/query/queryresults.h"

namespace reindexer {

Aggregator::Aggregator(AggType aggType, const string &name) : aggType_(aggType), name_(name) {
	switch (aggType_) {
		case AggFacet:
			facets_.reset(new fast_hash_map<Variant, int>());
			break;
		case AggMin:
			result_ = std::numeric_limits<double>::max();
			break;
		case AggMax:
			result_ = std::numeric_limits<double>::min();
			break;
		case AggAvg:
		case AggSum:
			break;
		default:
			throw Error(errParams, "Unknown aggregation type %d", int(aggType_));
	}
}

void Aggregator::Bind(PayloadType type, int fieldIdx, const TagsPath &fieldPath) {
	payloadType_ = type;
	if (fieldIdx >= 0) {
		fieldType_ = &type->Field(fieldIdx);
	} else {
		fieldPath_ = fieldPath;
	}
}

AggregationResult Aggregator::GetResult() const {
	AggregationResult ret;
	ret.name = name_;

	switch (aggType_) {
		case AggAvg:
			ret.value = double(hitCount_ == 0 ? 0 : (result_ / hitCount_));
			break;
		case AggSum:
		case AggMin:
		case AggMax:
			ret.value = result_;
			break;
		case AggFacet:
			for (auto &it : *facets_) {
				ret.facets.push_back(FacetResult(it.first.As<string>(), it.second));
			}
			break;
		default:
			abort();
	}
	return ret;
}

void Aggregator::Aggregate(const PayloadValue &data) {
	if (!fieldType_) {
		ConstPayload pl(payloadType_, data);
		VariantArray va;
		pl.GetByJsonPath(fieldPath_, va, KeyValueUndefined);
		for (const Variant &v : va) aggregate(v);
		return;
	}

	if (!fieldType_->IsArray()) {
		aggregate(PayloadFieldValue(*fieldType_, data.Ptr() + fieldType_->Offset()).Get());
		return;
	}

	PayloadFieldValue::Array *arr = reinterpret_cast<PayloadFieldValue::Array *>(data.Ptr() + fieldType_->Offset());

	uint8_t *ptr = data.Ptr() + arr->offset;
	for (int i = 0; i < arr->len; i++, ptr += fieldType_->Sizeof()) {
		aggregate(PayloadFieldValue(*fieldType_, ptr).Get());
	}
}

void Aggregator::aggregate(const Variant &v) {
	switch (aggType_) {
		case AggSum:
		case AggAvg:
			result_ += v.As<double>();
			hitCount_++;
			break;
		case AggMin:
			result_ = std::min(v.As<double>(), result_);
			break;
		case AggMax:
			result_ = std::max(v.As<double>(), result_);
			break;
		case AggFacet:
			(*facets_)[v]++;
			break;
	};
}

}  // namespace reindexer
