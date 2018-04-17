#include "core/comparator.h"
#include "core/payload/payloadiface.h"

namespace reindexer {

Comparator::Comparator() : fields_(), cmpComposite(payloadType_, fields_) {}
Comparator::~Comparator() {}

Comparator::Comparator(CondType cond, KeyValueType type, const KeyValues &values, bool isArray, PayloadType payloadType,
					   const FieldsSet &fields, void *rawData, const CollateOpts &collateOpts)
	: cond_(cond),
	  type_(type),
	  isArray_(isArray),
	  rawData_(reinterpret_cast<uint8_t *>(rawData)),
	  collateOpts_(collateOpts),
	  payloadType_(payloadType),
	  fields_(fields),
	  cmpComposite(payloadType_, fields_) {
	if (type == KeyValueComposite) {
		assert(fields_.size() > 0);
	}
	setValues(values);
}

void Comparator::setValues(const KeyValues &values) {
	switch (type_) {
		case KeyValueInt:
			cmpInt.SetValues(cond_, values);
			break;
		case KeyValueInt64:
			cmpInt64.SetValues(cond_, values);
			break;
		case KeyValueDouble:
			cmpDouble.SetValues(cond_, values);
			break;
		case KeyValueString:
			cmpString.SetValues(cond_, values);
			break;
		case KeyValueComposite:
			cmpComposite.SetValues(cond_, values);
			break;
		default:
			assert(0);
	}
}

void Comparator::Bind(PayloadType type, int field) {
	if (type_ != KeyValueComposite) {
		offset_ = type->Field(field).Offset();
		sizeof_ = type->Field(field).ElemSizeof();
	}
}

bool Comparator::Compare(const PayloadValue &data, int idx) {
	if (rawData_) return compare(rawData_ + idx * sizeof_);

	if (type_ == KeyValueComposite) {
		return compare(&const_cast<PayloadValue &>(data));
	}

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
