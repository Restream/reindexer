#pragma once

#include "core/keyvalue/variant.h"
#include "core/payload/payloadiface.h"
#include "core/type_consts.h"

namespace reindexer {

struct AggregationResult;

class Aggregator {
public:
	Aggregator(AggType aggType, const string &name);
	Aggregator() = default;
	Aggregator(Aggregator &&) = default;
	Aggregator &operator=(Aggregator &&) = default;
	Aggregator(const Aggregator &) = delete;
	Aggregator &operator=(const Aggregator &) = delete;

	void Aggregate(const PayloadValue &lhs);
	void Bind(PayloadType type, int fieldIdx, const TagsPath &fieldPath);
	AggregationResult GetResult() const;

protected:
	void aggregate(const Variant &variant);

	PayloadType payloadType_;
	// Field type for indexed field
	const PayloadFieldType *fieldType_ = nullptr;

	// Json path to field, for non indexed field
	TagsPath fieldPath_;
	double result_ = 0;
	int hitCount_ = 0;
	AggType aggType_;
	std::unique_ptr<fast_hash_map<Variant, int>> facets_;
	string name_;
};

}  // namespace reindexer
