#include "core/comparator.h"
#include "core/payload/payloadiface.h"

namespace reindexer {

Comparator::Comparator(CondType cond, KeyValueType type, const KeyValues &values, bool isArray, void *rawData, int collateMode)
	: cond_(cond), type_(type), isArray_(isArray), rawData_(static_cast<uint8_t *>(rawData)), collateMode_(collateMode) {
	switch (type_) {
		case KeyValueInt:
			cmpInt.SetValues(cond, values);
			break;
		case KeyValueInt64:
			cmpInt64.SetValues(cond, values);
			break;
		case KeyValueDouble:
			cmpDouble.SetValues(cond, values);
			break;
		case KeyValueString:
			cmpString.SetValues(cond, values);
			break;
		default:
			assert(0);
	}
}

void Comparator::Bind(const PayloadType *type, int field) {
	offset_ = type->Field(field).Offset();
	sizeof_ = type->Field(field).ElemSizeof();
}

bool Comparator::Compare(const PayloadValue &data, int idx) {
	if (rawData_) return compare(rawData_ + idx * sizeof_);

	if (!isArray_) {
		return compare(data.Ptr() + offset_);
	}

	PayloadFieldValue::Array *arr = reinterpret_cast<PayloadFieldValue::Array *>(data.Ptr() + offset_);

	if (cond_ == CondEmpty) return arr->len == 0;
	if (cond_ == CondAny) return arr->len != 0;

	uint8_t *ptr = data.Ptr() + arr->offset;
	for (int i = 0; i < arr->len; i++, ptr += sizeof_)
		if (compare(ptr)) return true;

	return false;
}

}  // namespace reindexer
