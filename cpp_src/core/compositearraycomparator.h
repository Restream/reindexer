#pragma once

#include "comparatorimpl.h"
#include "core/payload/fieldsset.h"

namespace reindexer {

class EqualPositionComparator {
public:
	EqualPositionComparator() noexcept = default;

	void BindField(int field, const VariantArray &, CondType);
	void BindField(const FieldsPath &, const VariantArray &, CondType);
	bool Compare(const PayloadValue &, const ComparatorVars &);
	bool IsBinded() { return !ctx_.empty(); }

private:
	bool compareField(size_t field, const Variant &, const ComparatorVars &);
	template <typename F>
	void bindField(F field, const VariantArray &, CondType);

	struct Context {
		CondType cond;
		ComparatorImpl<bool> cmpBool;
		ComparatorImpl<int> cmpInt;
		ComparatorImpl<int64_t> cmpInt64;
		ComparatorImpl<double> cmpDouble;
		ComparatorImpl<key_string> cmpString;
		ComparatorImpl<Uuid> cmpUuid;
	};

	std::vector<Context> ctx_;
	FieldsSet fields_;
};
}  // namespace reindexer
