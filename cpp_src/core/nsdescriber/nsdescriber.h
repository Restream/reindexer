#pragma once
#include "core/query/query.h"
#include "core/query/queryresults.h"

namespace reindexer {

using std::string;
using std::vector;

class NsDescriber {
public:
	NsDescriber(Namespace *parent) : ns_(parent) {}
	void operator()(QueryResults &result);

private:
	Namespace *ns_;
};
}  // namespace reindexer
