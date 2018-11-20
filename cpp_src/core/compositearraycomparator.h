#pragma once

#include "comparatorimpl.h"
#include "core/payload/fieldsset.h"
#include "estl/h_vector.h"

namespace reindexer {

class CompositeArrayComparator {
public:
	CompositeArrayComparator(PayloadType pt, KeyValueType kvtype);

	void BindField(int field, const VariantArray &values, CondType condType);
	void BindField(const TagsPath &tagsPath, const VariantArray &values, CondType condType);
	bool Compare(const PayloadValue &pv, const CollateOpts &collateOpts);

private:
	bool compareField(size_t field, const Variant &v, const CollateOpts &collateOpts);

	PayloadType pt_;
	KeyValueType type_ = KeyValueNull;

	struct Context {
		CondType cond;
		ComparatorImpl<bool> cmpBool;
		ComparatorImpl<int> cmpInt;
		ComparatorImpl<int64_t> cmpInt64;
		ComparatorImpl<double> cmpDouble;
		ComparatorImpl<key_string> cmpString;
	};

	h_vector<Context, 0> ctx_;
	FieldsSet fields_;
};
}  // namespace reindexer
