#pragma once

#include "core/payload/payloadiface.h"
#include "core/type_consts.h"
#include "vendor/cpp-btree/btree_map.h"

namespace reindexer {

struct AggregationResult;

class Aggregator {
public:
	struct SortingEntry {
		int field;
		bool desc;
		enum { Count = -1 };
	};

	Aggregator(const PayloadType &, const FieldsSet &, AggType aggType, const h_vector<string, 1> &names,
			   const h_vector<SortingEntry, 1> &sort, size_t limit, size_t offset);
	Aggregator();
	Aggregator(Aggregator &&);
	Aggregator &operator=(Aggregator &&);
	Aggregator(const Aggregator &) = delete;
	Aggregator &operator=(const Aggregator &) = delete;
	~Aggregator();

	void Aggregate(const PayloadValue &lhs);
	AggregationResult GetResult() const;

protected:
	void aggregate(const Variant &variant);

	PayloadType payloadType_;

	FieldsSet fields_;
	double result_ = 0;
	int hitCount_ = 0;
	AggType aggType_;
	h_vector<string, 1> names_;
	class OrderBy;
	std::unique_ptr<OrderBy> comparator_;
	using FacetMap = btree::btree_map<PayloadValue, int, OrderBy>;
	std::unique_ptr<FacetMap> facets_;
	size_t limit_ = UINT_MAX;
	size_t offset_ = 0;
};

}  // namespace reindexer
