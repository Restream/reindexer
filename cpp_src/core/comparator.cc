#include "core/comparator.h"
#include "core/payload/payloadiface.h"

namespace reindexer {

Comparator::Comparator() {}
Comparator::~Comparator() {}

Comparator::Comparator(CondType cond, KeyValueType type, const VariantArray &values, bool isArray, bool distinct, PayloadType payloadType,
					   const FieldsSet &fields, void *rawData, const CollateOpts &collateOpts)
	: ComparatorVars(cond, type, isArray, payloadType, fields, rawData, collateOpts),
	  cmpBool(distinct),
	  cmpInt(distinct),
	  cmpInt64(distinct),
	  cmpDouble(distinct),
	  cmpString(distinct) {
	if (type == KeyValueComposite) assert(fields_.size() > 0);
	if (cond_ == CondEq && values.size() != 1) cond_ = CondSet;
	setValues(values);
}

void Comparator::setValues(const VariantArray &values) {
	if (fields_.getTagsPathsLength() > 0) {
		cmpInt.SetValues(cond_, values);
		cmpBool.SetValues(cond_, values);
		cmpInt64.SetValues(cond_, values);
		cmpDouble.SetValues(cond_, values);
		cmpString.SetValues(cond_, values);
	} else {
		switch (type_) {
			case KeyValueBool:
				cmpBool.SetValues(cond_, values);
				break;
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
				cmpComposite.SetValues(cond_, values, *this);
				break;
			default:
				assert(0);
		}
	}
	bool isRegularIndex = fields_.size() > 0 && fields_.getTagsPathsLength() == 0 && fields_[0] < payloadType_.NumFields();
	if (isArray_ && isRegularIndex && payloadType_->Field(fields_[0]).IsArray()) {
		offset_ = payloadType_->Field(fields_[0]).Offset();
		sizeof_ = payloadType_->Field(fields_[0]).ElemSizeof();
	}
}

void Comparator::Bind(PayloadType type, int field) {
	if (type_ != KeyValueComposite) {
		offset_ = type->Field(field).Offset();
		sizeof_ = type->Field(field).ElemSizeof();
	}
}

void Comparator::BindEqualPosition(int field, const VariantArray &val, CondType cond) { cmpEqualPosition.BindField(field, val, cond); }

void Comparator::BindEqualPosition(const TagsPath &tagsPath, const VariantArray &val, CondType cond) {
	cmpEqualPosition.BindField(tagsPath, val, cond);
}

bool Comparator::Compare(const PayloadValue &data, int rowId) {
	if (cmpEqualPosition.IsBinded()) {
		return cmpEqualPosition.Compare(data, *this);
	}
	if (fields_.getTagsPathsLength() > 0) {
		// Comparing field by CJSON path (slow path)
		VariantArray rhs;
		ConstPayload(payloadType_, data).GetByJsonPath(fields_.getTagsPath(0), rhs, type_);

		if (cond_ == CondEmpty) return rhs.empty() || rhs[0].Type() == KeyValueNull;

		if ((cond_ == CondAny) && (rhs.empty() || rhs[0].Type() == KeyValueNull)) return false;

		for (const Variant &kr : rhs) {
			if (compare(kr)) return true;
		}
	} else {
		// Comparing field from payload by offset (fast path)

		// Special case: compare by composite condition. Pass pointer to PayloadValue
		if (type_ == KeyValueComposite) return compare(&data);

		// Check if we have column (rawData_), then go to fastest path with column
		if (rawData_) return compare(rawData_ + rowId * sizeof_);

		// Not array: Just compare field by offset in PayloadValue
		if (!isArray_) return compare(data.Ptr() + offset_);

		PayloadFieldValue::Array *arr = reinterpret_cast<PayloadFieldValue::Array *>(data.Ptr() + offset_);

		if (cond_ == CondEmpty) return arr->len == 0;
		if (cond_ == CondAny && arr->len == 0) return false;

		uint8_t *ptr = data.Ptr() + arr->offset;
		for (int i = 0; i < arr->len; i++, ptr += sizeof_) {
			if (compare(ptr)) return true;
		}
	}

	return false;
}

void Comparator::ExcludeDistinct(const PayloadValue &data, int rowId) {
	assert(!cmpEqualPosition.IsBinded());
	if (fields_.getTagsPathsLength() > 0) {
		// Exclude field by CJSON path (slow path)
		VariantArray rhs;
		ConstPayload(payloadType_, data).GetByJsonPath(fields_.getTagsPath(0), rhs, type_);
		for (const auto &v : rhs) excludeDistinct(v);
	} else {
		// Exclude field from payload by offset (fast path)

		assert(type_ != KeyValueComposite);

		// Check if we have column (rawData_), then go to fastest path with column
		if (rawData_) return excludeDistinct(rawData_ + rowId * sizeof_);

		// Not array: Just exclude field by offset in PayloadValue
		if (!isArray_) return excludeDistinct(data.Ptr() + offset_);

		PayloadFieldValue::Array *arr = reinterpret_cast<PayloadFieldValue::Array *>(data.Ptr() + offset_);
		uint8_t *ptr = data.Ptr() + arr->offset;
		for (int i = 0; i < arr->len; i++, ptr += sizeof_) excludeDistinct(ptr);
	}
}

}  // namespace reindexer
