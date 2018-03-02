#pragma once
#include "core/query/queryresults.h"

namespace reindexer {

class Namespace;
class NsDescriber {
public:
	NsDescriber(Namespace *parent) : ns_(parent) {}
	void operator()(QueryResults &result);

private:
	Namespace *ns_;
};
}  // namespace reindexer
