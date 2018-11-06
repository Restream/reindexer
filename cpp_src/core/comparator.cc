#include "core/comparator.h"
#include "cjson/baseencoder.h"
#include "core/payload/payloadiface.h"
#include "query/querywhere.h"

namespace reindexer {

Comparator::Comparator() : fields_(), cmpComposite(payloadType_, fields_), cmpEqualPosition(payloadType_, KeyValueNull) {}
Comparator::~Comparator() {}

Comparator::Comparator(CondType cond, KeyValueType type, const VariantArray &values, bool isArray, bool distinct, PayloadType payloadType,
					   const FieldsSet &fields, void *rawData, const CollateOpts &collateOpts)
	: cond_(cond),
	  type_(type),
	  isArray_(isArray),
	  rawData_(reinterpret_cast<uint8_t *>(rawData)),
	  collateOpts_(collateOpts),
	  payloadType_(payloadType),
	  fields_(fields),
	  cmpComposite(payloadType_, fields_),
	  cmpEqualPosition(payloadType_, type),
	  dist_(distinct ? new fast_hash_set<Variant> : nullptr)
{
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
		cmpComposite.SetValues(cond_, values);
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
				cmpComposite.SetValues(cond_, values);
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

void Comparator::BindEqualPosition(int field, const VariantArray &val, CondType cond) {
	assert(isArray_);
	cmpEqualPosition.BindField(field, val, cond);
	equalPositionMode = true;
}

void Comparator::BindEqualPosition(const TagsPath &tagsPath, const VariantArray &val, CondType cond) {
	assert(isArray_);
	cmpEqualPosition.BindField(tagsPath, val, cond);
	equalPositionMode = true;
}

bool Comparator::Compare(const PayloadValue &data, int rowId) {
	if (fields_.getTagsPathsLength() > 0) {
		VariantArray rhs;
		Payload pl(payloadType_, const_cast<PayloadValue &>(data));

		pl.GetByJsonPath(fields_.getTagsPath(0), rhs, type_);
		if (cond_ == CondEmpty) {
			bool empty = true;
			for (const Variant &v : rhs) empty &= (v.Type() == KeyValueNull);
			return (rhs.size() == 0) || empty;
		}

		if (cond_ == CondAny && !dist_) {
			if (rhs.empty()) return false;

			int nullCnt = 0;
			for (const Variant &v : rhs) nullCnt += v.Type() != KeyValueNull;
			return nullCnt > 0;
		}

		if (equalPositionMode) {
			return cmpEqualPosition.Compare(data, collateOpts_);
		} else {
			for (const Variant &kr : rhs) {
				if (compare(kr) && is_unique(kr)) return true;
			}
		}
	} else {
		if (rawData_) return compare(rawData_ + rowId * sizeof_);

		if (type_ == KeyValueComposite) {
			return compare(&const_cast<PayloadValue &>(data));
		}

		if (!isArray_) {
			return compare(data.Ptr() + offset_);
		}

		PayloadFieldValue::Array *arr = reinterpret_cast<PayloadFieldValue::Array *>(data.Ptr() + offset_);

		if (cond_ == CondEmpty) return arr->len == 0;
		if (cond_ == CondAny) return arr->len != 0;

		if (equalPositionMode) {
			return cmpEqualPosition.Compare(data, collateOpts_);
		} else {
			uint8_t *ptr = data.Ptr() + arr->offset;
			for (int i = 0; i < arr->len; i++, ptr += sizeof_) {
				bool ret = compare(ptr);
				if (ret) return true;
			}
		}
	}

	return false;
}

}  // namespace reindexer
