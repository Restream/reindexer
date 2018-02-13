#pragma once

#include "core/keyvalue/keyvalue.h"
#include "core/payload/payloadiface.h"
#include "core/type_consts.h"

namespace reindexer {

class Aggregator {
public:
	Aggregator(KeyValueType type, bool isArray, void *rawData, AggType aggType);
	Aggregator(){};
	~Aggregator(){};
	void Aggregate(const PayloadValue &lhs, int idx);
	void Bind(const PayloadType *type, int field);
	double GetResult() const {
		switch (aggType_) {
			case AggAvg:
				return hitCount_ == 0 ? 0 : (result_ / hitCount_);
			case AggSum:
				return result_;
			default:
				abort();
		}
	}

protected:
	void aggregate(void *ptr) {
		hitCount_++;
		switch (type_) {
			case KeyValueInt:
				result_ += *static_cast<int *>(ptr);
				break;
			case KeyValueInt64:
				result_ += *static_cast<int64_t *>(ptr);
				break;
			case KeyValueDouble:
				result_ += *static_cast<double *>(ptr);
				break;
			default:
				abort();
		}
	}

	KeyValueType type_ = KeyValueUndefined;
	size_t offset_ = 0;
	size_t sizeof_ = 0;
	bool isArray_ = false;
	uint8_t *rawData_ = nullptr;
	double result_ = 0;
	int hitCount_ = 0;
	AggType aggType_;
};

}  // namespace reindexer
