#pragma once

#include <climits>
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
	~Aggregator();

	void Aggregate(const PayloadValue &lhs);
	AggregationResult GetResult() const;

	Aggregator(const Aggregator &) = delete;
	Aggregator &operator=(const Aggregator &) = delete;
	Aggregator &operator=(Aggregator &&) = delete;

protected:
	enum Direction { Desc = -1, Asc = 1 };
	class MultifieldComparator;
	class SinglefieldComparator;
	using MultifieldMap = btree::btree_map<PayloadValue, int, MultifieldComparator>;
	using SinglefieldMap = btree::btree_map<Variant, int, SinglefieldComparator>;

	void aggregate(const Variant &variant);

	PayloadType payloadType_;
	FieldsSet fields_;
	double result_ = 0;
	int hitCount_ = 0;
	AggType aggType_;
	h_vector<string, 1> names_;
	size_t limit_ = UINT_MAX;
	size_t offset_ = 0;

	std::unique_ptr<MultifieldMap> multifieldFacets_;
	std::unique_ptr<SinglefieldMap> singlefieldFacets_;
};

}  // namespace reindexer
