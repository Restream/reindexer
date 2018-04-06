#pragma once

#include <functional>
#include <unordered_set>
#include "core/index/payload_map.h"
#include "core/keyvalue/keyvalue.h"
#include "core/payload/fieldsset.h"
#include "core/type_consts.h"
#include "tools/stringstools.h"

namespace reindexer {

using reindexer::lower;
using std::unordered_set;
using std::reference_wrapper;
using std::shared_ptr;

template <class T>
class ComparatorImpl {
public:
	ComparatorImpl(){};
	void SetValues(CondType cond, const KeyValues &values) {
		if (cond == CondSet) {
			valuesS_.reset(new unordered_set<T>());
		}
		for (auto &k : values) {
			if (cond == CondSet)
				valuesS_->emplace(static_cast<T>(static_cast<KeyRef>(k)));
			else
				values_.push_back(static_cast<T>(static_cast<KeyRef>(k)));
		}
	}

	bool Compare(CondType cond, const T &lhs) {
		const T &rhs = values_[0];
		switch (cond) {
			case CondEq:
				return lhs == rhs;
			case CondGe:
				return lhs >= rhs;
			case CondLe:
				return lhs <= rhs;
			case CondLt:
				return lhs < rhs;
			case CondGt:
				return lhs > rhs;
			case CondRange:
				return lhs >= rhs && lhs <= values_[1];
			case CondSet:
				return valuesS_->find(lhs) != valuesS_->end();
			default:
				abort();
		}
	}

	bool Compare(CondType cond, const p_string &lhs, int collateMode) {
		const p_string &rhs = values_[0];
		switch (cond) {
			case CondEq:
				return collateCompare(Slice(lhs), Slice(rhs), collateMode) == 0;
			case CondGe:
				return collateCompare(Slice(lhs), Slice(rhs), collateMode) >= 0;
			case CondLe:
				return collateCompare(Slice(lhs), Slice(rhs), collateMode) <= 0;
			case CondLt:
				return collateCompare(Slice(lhs), Slice(rhs), collateMode) < 0;
			case CondGt:
				return collateCompare(Slice(lhs), Slice(rhs), collateMode) > 0;
			case CondRange:
				return collateCompare(Slice(lhs), Slice(rhs), collateMode) >= 0 &&
					   collateCompare(Slice(lhs), Slice(values_[1]), collateMode) <= 0;
			case CondSet:
				if (collateMode == CollateNone) return valuesS_->find(lhs) != valuesS_->end();
				for (auto it : *valuesS_) {
					if (!collateCompare(Slice(lhs), Slice(it), collateMode)) return true;
				}
				return false;
			default:
				abort();
		}
	}

	h_vector<T, 2> values_;
	shared_ptr<unordered_set<T>> valuesS_;
};

template <>
class ComparatorImpl<PayloadValue> {
public:
	ComparatorImpl(const PayloadType &payloadType, const FieldsSet &fields) : payloadType_(payloadType), fields_(fields) {}

	void SetValues(CondType cond, const KeyValues &values) {
		if (cond == CondSet) {
			valuesSet_.reset(new unordered_payload_set(0, hash_composite(payloadType_, fields_), equal_composite(payloadType_, fields_)));
		}
		for (auto &kv : values) {
			const PayloadValue &pv(kv);
			if (cond == CondSet) {
				valuesSet_->emplace(pv);
			} else {
				values_.push_back(pv);
			}
		}
	}

	bool Compare(CondType cond, PayloadValue &leftValue) {
		assert(!values_.empty() || !valuesSet_->empty());
		assert(fields_.size() > 0);
		PayloadValue *rightValue(&values_[0]);
		Payload lhs(payloadType_, leftValue);
		switch (cond) {
			case CondEq:
				return (lhs.Compare(*rightValue, fields_) == 0);
			case CondGe:
				return (lhs.Compare(*rightValue, fields_) >= 0);
			case CondGt:
				return (lhs.Compare(*rightValue, fields_) > 0);
			case CondLe:
				return (lhs.Compare(*rightValue, fields_) <= 0);
			case CondLt: {
				return (lhs.Compare(*rightValue, fields_) < 0);
			}
			case CondRange: {
				PayloadValue *upperValue(&values_[1]);
				return (lhs.Compare(*rightValue, fields_) >= 0) && (lhs.Compare(*upperValue, fields_) <= 0);
			}
			case CondSet:
				return valuesSet_->find(leftValue) != valuesSet_->end();
			default:
				abort();
		}
	}

	PayloadType payloadType_;
	FieldsSet fields_;
	h_vector<PayloadValue, 2> values_;
	shared_ptr<unordered_payload_set> valuesSet_;
};

class Comparator {
public:
	Comparator();
	Comparator(CondType cond, KeyValueType type, const KeyValues &values, bool isArray, PayloadType payloadType, const FieldsSet &fields,
			   void *rawData = nullptr, int collateMode = CollateNone);
	~Comparator();

	bool Compare(const PayloadValue &lhs, int idx);
	void Bind(PayloadType type, int field);

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
				return cmpString.Compare(cond_, *static_cast<p_string *>(ptr), collateMode_);
			case KeyValueComposite:
				return cmpComposite.Compare(cond_, *static_cast<PayloadValue *>(ptr));
			default:
				abort();
		}
	}

	void setValues(const KeyValues &values);

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
	int collateMode_ = CollateNone;

	PayloadType payloadType_;
	FieldsSet fields_;
	ComparatorImpl<PayloadValue> cmpComposite;
};

}  // namespace reindexer
