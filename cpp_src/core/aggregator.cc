#include "core/aggregator.h"
#include "core/payload/payloadiface.h"

namespace reindexer {

Aggregator::Aggregator(KeyValueType type, bool isArray, void *rawData, AggType aggType)
	: type_(type), isArray_(isArray), rawData_(static_cast<uint8_t *>(rawData)), aggType_(aggType) {}

void Aggregator::Bind(PayloadType type, int field) {
	offset_ = type->Field(field).Offset();
	sizeof_ = type->Field(field).ElemSizeof();
}

void Aggregator::Aggregate(const PayloadValue &data, int idx) {
	if (rawData_) {
		aggregate(rawData_ + idx * sizeof_);
		return;
	}

	if (!isArray_) {
		aggregate(data.Ptr() + offset_);
		return;
	}

	PayloadFieldValue::Array *arr = reinterpret_cast<PayloadFieldValue::Array *>(data.Ptr() + offset_);

	uint8_t *ptr = data.Ptr() + arr->offset;
	for (int i = 0; i < arr->len; i++, ptr += sizeof_) aggregate(ptr);
}

}  // namespace reindexer
