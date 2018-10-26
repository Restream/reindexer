#pragma once

#include <string>
#include "estl/h_vector.h"
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
	string name;
	double value = 0;
	h_vector<FacetResult, 1> facets;
};

};  // namespace reindexer
