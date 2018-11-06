#pragma once

#include "comparatorimpl.h"
#include "compositearraycomparator.h"
#include "estl/fast_hash_set.h"

namespace reindexer {

using reindexer::lower;
class Comparator {
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
			case KeyValueComposite: {
				const PayloadValue &pl = static_cast<const PayloadValue &>(kr);
				return cmpComposite.Compare(cond_, const_cast<PayloadValue &>(pl), collateOpts_);
			}
			default:
				abort();
		}
	}

	bool compare(void *ptr) {
		switch (type_) {
			case KeyValueNull:
				return cond_ == CondEmpty;
			case KeyValueBool:
				return cmpBool.Compare(cond_, *static_cast<bool *>(ptr));
			case KeyValueInt:
				return cmpInt.Compare(cond_, *static_cast<int *>(ptr));
			case KeyValueInt64:
				return cmpInt64.Compare(cond_, *static_cast<int64_t *>(ptr));
			case KeyValueDouble:
				return cmpDouble.Compare(cond_, *static_cast<double *>(ptr));
			case KeyValueString:
				return cmpString.Compare(cond_, *static_cast<p_string *>(ptr), collateOpts_);
			case KeyValueComposite:
				return cmpComposite.Compare(cond_, *static_cast<PayloadValue *>(ptr), collateOpts_);
			default:
				abort();
		}
	}

	inline
	bool is_unique(const Variant& v) { return dist_ ? dist_->emplace(v).second : true; }

	void setValues(const VariantArray &values);

	ComparatorImpl<bool> cmpBool;
	ComparatorImpl<int> cmpInt;
	ComparatorImpl<int64_t> cmpInt64;
	ComparatorImpl<double> cmpDouble;
	ComparatorImpl<key_string> cmpString;

	CondType cond_ = CondEq;
	KeyValueType type_ = KeyValueUndefined;
	size_t offset_ = 0;
	size_t sizeof_ = 0;
	bool isArray_ = false;
	uint8_t *rawData_ = nullptr;
	CollateOpts collateOpts_;

	PayloadType payloadType_;
	FieldsSet fields_;
	ComparatorImpl<PayloadValue> cmpComposite;
	CompositeArrayComparator cmpEqualPosition;
	shared_ptr<fast_hash_set<Variant>> dist_;
	bool equalPositionMode = false;
};

}  // namespace reindexer
