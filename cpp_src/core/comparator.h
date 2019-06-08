#pragma once

#include "comparatorimpl.h"
#include "compositearraycomparator.h"

namespace reindexer {

class Comparator : public ComparatorVars {
public:
	Comparator();
	Comparator(CondType cond, KeyValueType type, const VariantArray &values, bool isArray, bool distinct, PayloadType payloadType,
			   const FieldsSet &fields, void *rawData = nullptr, const CollateOpts &collateOpts = CollateOpts());
	~Comparator();

	bool Compare(const PayloadValue &lhs, int rowId);
	void Bind(PayloadType type, int field);
	void BindEqualPosition(int field, const VariantArray &val, CondType cond);
	void BindEqualPosition(const TagsPath &tagsPath, const VariantArray &val, CondType cond);

protected:
	bool compare(const Variant &kr) {
		switch (kr.Type()) {
			case KeyValueNull:
				return cond_ == CondEmpty;
			case KeyValueInt:
				return cmpInt.Compare(cond_, static_cast<int>(kr));
			case KeyValueBool:
				return cmpBool.Compare(cond_, static_cast<bool>(kr));
			case KeyValueInt64:
				return cmpInt64.Compare(cond_, static_cast<int64_t>(kr));
			case KeyValueDouble:
				return cmpDouble.Compare(cond_, static_cast<double>(kr));
			case KeyValueString:
				return cmpString.Compare(cond_, static_cast<p_string>(kr), collateOpts_);
			case KeyValueComposite:
				return cmpComposite.Compare(cond_, static_cast<const PayloadValue &>(kr), *this);
			default:
				abort();
		}
	}

	bool compare(const void *ptr) {
		switch (type_) {
			case KeyValueNull:
				return cond_ == CondEmpty;
			case KeyValueBool:
				return cmpBool.Compare(cond_, *static_cast<const bool *>(ptr));
			case KeyValueInt:
				return cmpInt.Compare(cond_, *static_cast<const int *>(ptr));
			case KeyValueInt64:
				return cmpInt64.Compare(cond_, *static_cast<const int64_t *>(ptr));
			case KeyValueDouble:
				return cmpDouble.Compare(cond_, *static_cast<const double *>(ptr));
			case KeyValueString:
				return cmpString.Compare(cond_, *static_cast<const p_string *>(ptr), collateOpts_);
			case KeyValueComposite:
				return cmpComposite.Compare(cond_, *static_cast<const PayloadValue *>(ptr), *this);
			default:
				abort();
		}
	}

	void setValues(const VariantArray &values);

	ComparatorImpl<bool> cmpBool;
	ComparatorImpl<int> cmpInt;
	ComparatorImpl<int64_t> cmpInt64;
	ComparatorImpl<double> cmpDouble;
	ComparatorImpl<key_string> cmpString;
	ComparatorImpl<PayloadValue> cmpComposite;
	CompositeArrayComparator cmpEqualPosition;
};

}  // namespace reindexer
