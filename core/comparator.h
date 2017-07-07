#pragma once

#include <unordered_set>
#include "core/keyvalue.h"
#include "core/payload.h"
#include "core/type_consts.h"

namespace reindexer {

using std::unordered_set;

template <class T>
class ComparatorImpl {
public:
	ComparatorImpl(){};
	void SetValues(CondType cond, const KeyValues &values) {
		for (auto &k : values) {
			if (cond == CondSet)
				valuesS_.emplace(static_cast<T>(static_cast<KeyRef>(k)));
			else
				values_.push_back(static_cast<T>(static_cast<KeyRef>(k)));
		}
	}

	bool Compare(CondType cond, const T &lhs) {
		const T *rhs = values_.size() > 0 ? (const T *)&values_[0] : nullptr;
		switch (cond) {
			case CondEq:
				return lhs == *rhs;
			case CondGe:
				return lhs >= *rhs;
			case CondLe:
				return lhs <= *rhs;
			case CondLt:
				return lhs < *rhs;
			case CondGt:
				return lhs > *rhs;
			case CondRange:
				return lhs >= *rhs && lhs <= (T)values_[1];
			case CondSet:
				return valuesS_.find(lhs) != valuesS_.end();
			default:
				abort();
		}
	}
	h_vector<T, 2> values_;
	unordered_set<T> valuesS_;
};

class Comparator {
public:
	Comparator(CondType cond, KeyValueType type, const KeyValues &values, bool isArray, void *rawData = nullptr);
	Comparator(){};
	~Comparator(){};
	bool Compare(const PayloadData &lhs, int idx);
	void Bind(const PayloadType *type, int field);

protected:
	bool compare(void *ptr) {
		switch (type_) {
			case KeyValueInt:
				return cmpInt.Compare(cond_, *static_cast<int *>(ptr));
			case KeyValueInt64:
				return cmpInt64.Compare(cond_, *static_cast<int64_t *>(ptr));
			case KeyValueDouble:
				return cmpDouble.Compare(cond_, *static_cast<double *>(ptr));
			case KeyValueString:
				return cmpString.Compare(cond_, *static_cast<p_string *>(ptr));
			default:
				abort();
		}
	}

	ComparatorImpl<int> cmpInt;
	ComparatorImpl<int64_t> cmpInt64;
	ComparatorImpl<double> cmpDouble;
	ComparatorImpl<p_string> cmpString;

	CondType cond_ = CondEq;
	KeyValueType type_ = KeyValueUndefined;
	size_t offset_ = 0;
	size_t sizeof_ = 0;
	bool isArray_ = false;
	uint8_t *rawData_ = nullptr;
};

}  // namespace reindexer