#pragma once

#include <string>
#include "core/type_consts.h"
#include "estl/h_vector.h"
#include "estl/string_view.h"
#include "tools/errors.h"

namespace reindexer {
using std::string;
class WrSerializer;

struct FacetResult {
	FacetResult(const std::string &v, int c) : value(v), count(c) {}
	FacetResult() : count(0) {}
	string value;
	int count;
};

struct AggregationResult {
	void GetJSON(WrSerializer &ser) const;
	Error FromJSON(char *json);
	AggType type = AggSum;
	string field;
	double value = 0;
	h_vector<FacetResult, 1> facets;

	static AggType strToAggType(string_view type);
	static string_view aggTypeToStr(AggType type);
};

};  // namespace reindexer
