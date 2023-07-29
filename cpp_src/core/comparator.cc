#include "core/comparator.h"
#include "core/payload/payloadiface.h"

namespace reindexer {

Comparator::Comparator(CondType cond, KeyValueType type, const VariantArray &values, bool isArray, bool distinct, PayloadType payloadType,
					   const FieldsSet &fields, void *rawData, const CollateOpts &collateOpts)
	: ComparatorVars(cond, type, isArray, std::move(payloadType), fields, rawData, collateOpts),
	  cmpBool(distinct),
	  cmpInt(distinct),
	  cmpInt64(distinct),
	  cmpDouble(distinct),
	  cmpString(distinct),
	  cmpGeom(distinct),
	  cmpUuid(distinct) {
	if (type.Is<KeyValueType::Composite>()) assertrx(fields_.size() > 0);
	switch (cond) {
		case CondEq:
			if (values.size() != 1) {
				cond_ = CondSet;
			}
			setValues(values);
			break;
		case CondSet:
		case CondAllSet:
			if (values.size() == 1) {
				cond_ = CondEq;
			}
			[[fallthrough]];
		case CondLt:
		case CondLe:
		case CondGt:
		case CondGe:
		case CondLike:
		case CondRange:
			setValues(values);
			break;
		case CondDWithin:
			cmpGeom.SetValues(values);
			break;
		case CondEmpty:
		case CondAny:
			break;
	}
}

void Comparator::setValues(const VariantArray &values) {
	if (values.size() > 0) {
		valuesType_ = values.front().Type();
	}
	if (fields_.getTagsPathsLength() > 0) {
		cmpInt.SetValues(cond_, values);
		cmpBool.SetValues(cond_, values);
		cmpInt64.SetValues(cond_, values);
		cmpDouble.SetValues(cond_, values);
		cmpString.SetValues(cond_, values, collateOpts_);
		cmpUuid.SetValues(cond_, values);
	} else {
		type_.EvaluateOneOf([&](KeyValueType::Bool) { cmpBool.SetValues(cond_, values); },
							[&](KeyValueType::Int) { cmpInt.SetValues(cond_, values); },
							[&](KeyValueType::Int64) { cmpInt64.SetValues(cond_, values); },
							[&](KeyValueType::Double) { cmpDouble.SetValues(cond_, values); },
							[&](KeyValueType::String) { cmpString.SetValues(cond_, values, collateOpts_); },
							[&](KeyValueType::Composite) { cmpComposite.SetValues(cond_, values, *this); },
							[&](KeyValueType::Uuid) { cmpUuid.SetValues(cond_, values); },
							[](OneOf<KeyValueType::Null, KeyValueType::Tuple, KeyValueType::Undefined>) noexcept {
								assertrx(0);
								abort();
							});
	}
	bool isRegularIndex = fields_.size() > 0 && fields_.getTagsPathsLength() == 0 && fields_[0] < payloadType_.NumFields();
	if (isArray_ && isRegularIndex && payloadType_->Field(fields_[0]).IsArray()) {
		offset_ = payloadType_->Field(fields_[0]).Offset();
		sizeof_ = payloadType_->Field(fields_[0]).ElemSizeof();
	}
}

void Comparator::Bind(const PayloadType &type, int field) {
	if (!type_.Is<KeyValueType::Composite>()) {
		offset_ = type->Field(field).Offset();
		sizeof_ = type->Field(field).ElemSizeof();
	}
}

void Comparator::BindEqualPosition(int field, const VariantArray &val, CondType cond) { cmpEqualPosition.BindField(field, val, cond); }

void Comparator::BindEqualPosition(const TagsPath &tagsPath, const VariantArray &val, CondType cond) {
	cmpEqualPosition.BindField(tagsPath, val, cond);
}

bool Comparator::isNumericComparison(const VariantArray &values) const {
	if (valuesType_.Is<KeyValueType::Undefined>() || values.empty()) return false;
	const KeyValueType keyType{values.front().Type()};
	return !valuesType_.IsSame(keyType) && (valuesType_.Is<KeyValueType::String>() || keyType.Is<KeyValueType::String>());
}

bool Comparator::Compare(const PayloadValue &data, int rowId) {
	if (cmpEqualPosition.IsBinded()) {
		return cmpEqualPosition.Compare(data, *this);
	}
	if (fields_.getTagsPathsLength() == 0) {
		if (cond_ == CondAllSet) clearIndividualAllSetValues();
		// Comparing field from payload by offset (fast path)

		// Special case: compare by composite condition. Pass pointer to PayloadValue
		if (type_.Is<KeyValueType::Composite>()) return compare(&data);

		// Check if we have column (rawData_), then go to fastest path with column
		if (rawData_) return compare(rawData_ + rowId * sizeof_);

		// Not array: Just compare field by offset in PayloadValue
		if (!isArray_) return compare(data.Ptr() + offset_);

		const PayloadFieldValue::Array *arr = reinterpret_cast<const PayloadFieldValue::Array *>(data.Ptr() + offset_);

		switch (cond_) {
			case CondEmpty:
				return arr->len == 0;
			case CondAny:
				if (arr->len == 0) return false;
				break;
			case CondEq:
			case CondLt:
			case CondLe:
			case CondGt:
			case CondGe:
			case CondRange:
			case CondSet:
			case CondAllSet:
			case CondLike:
			case CondDWithin:
				break;
		}

		const uint8_t *ptr = data.Ptr() + arr->offset;
		if (cond_ == CondDWithin) {
			if (arr->len != 2 || !type_.Is<KeyValueType::Double>()) throw Error(errQueryExec, "DWithin with not point data");
			return cmpGeom.Compare(Point{*reinterpret_cast<const double *>(ptr), *reinterpret_cast<const double *>(ptr + sizeof_)});
		}

		for (int i = 0; i < arr->len; ++i, ptr += sizeof_) {
			if (compare(ptr)) return true;
		}
	} else {
		VariantArray rhs;
		ConstPayload(payloadType_, data).GetByJsonPath(fields_.getTagsPath(0), rhs, type_);
		if (isNumericComparison(rhs)) {
			// Numeric comparison is not allowed
			return false;
		}
		switch (cond_) {
			case CondEmpty:
				return rhs.empty() || rhs[0].Type().Is<KeyValueType::Null>();
			case CondDWithin:
				return cmpGeom.Compare(static_cast<Point>(rhs));
			case CondAllSet:
				clearAllSetValues();
				break;
			case CondAny:
				if (rhs.empty() || rhs[0].Type().Is<KeyValueType::Null>()) return false;
				break;
			case CondEq:
			case CondLt:
			case CondLe:
			case CondGt:
			case CondGe:
			case CondRange:
			case CondSet:
			case CondLike:
				break;
		}
		for (const Variant &kr : rhs) {
			if (compare(kr)) return true;
		}
	}

	return false;
}

void Comparator::ExcludeDistinct(const PayloadValue &data, int rowId) {
	assertrx(!cmpEqualPosition.IsBinded());
	if (fields_.getTagsPathsLength() > 0) {
		// Exclude field by CJSON path (slow path)
		VariantArray rhs;
		ConstPayload(payloadType_, data).GetByJsonPath(fields_.getTagsPath(0), rhs, type_);
		if (cond_ == CondDWithin) {
			cmpGeom.ExcludeDistinct(static_cast<Point>(rhs));
		} else {
			for (const auto &v : rhs) excludeDistinct(v);
		}
	} else {
		// Exclude field from payload by offset (fast path)

		assertrx(!type_.Is<KeyValueType::Composite>());

		// Check if we have column (rawData_), then go to fastest path with column
		if (rawData_) return excludeDistinct(rawData_ + rowId * sizeof_);

		// Not array: Just exclude field by offset in PayloadValue
		if (!isArray_) return excludeDistinct(data.Ptr() + offset_);

		PayloadFieldValue::Array *arr = reinterpret_cast<PayloadFieldValue::Array *>(data.Ptr() + offset_);
		uint8_t *ptr = data.Ptr() + arr->offset;
		if (cond_ == CondDWithin) {
			if (arr->len != 2 || !type_.Is<KeyValueType::Double>()) throw Error(errQueryExec, "DWithin with not point data");
			return cmpGeom.ExcludeDistinct(Point{*reinterpret_cast<const double *>(ptr), *reinterpret_cast<const double *>(ptr + sizeof_)});
		}

		for (int i = 0; i < arr->len; i++, ptr += sizeof_) excludeDistinct(ptr);
	}
}

}  // namespace reindexer
