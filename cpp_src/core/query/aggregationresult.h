#pragma once

#include <string>
#include "core/type_consts.h"
#include "estl/h_vector.h"
#include "estl/span.h"
#include "estl/string_view.h"
#include "tools/errors.h"

namespace reindexer {
using std::string;
class WrSerializer;

struct FacetResult {
	FacetResult(const h_vector<std::string, 1> &v, int c) : values(v), count(c) {}
	FacetResult() : count(0) {}
	h_vector<string, 1> values;
	int count;
};

struct AggregationResult {
	void GetJSON(WrSerializer &ser) const;
	Error FromJSON(span<char> json);
	AggType type = AggSum;
	h_vector<string, 1> fields;
	double value = 0;
	h_vector<FacetResult, 1> facets;

	static AggType strToAggType(string_view type);
	static string_view aggTypeToStr(AggType type);
};

};  // namespace reindexer
