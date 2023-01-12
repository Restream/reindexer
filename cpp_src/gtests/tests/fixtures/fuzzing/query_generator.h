#pragma once

#include "ns.h"

namespace reindexer {

class Query;

}  // namespace reindexer

namespace fuzzing {

class QueryGenerator {
public:
	QueryGenerator(const std::vector<Ns>& nss, std::ostream& os, RandomGenerator::ErrFactorType errorFactor)
		: namespaces_{nss}, rndGen_{os, errorFactor} {}
	reindexer::Query operator()();

private:
	const std::vector<Ns>& namespaces_;
	RandomGenerator rndGen_;
};

}  // namespace fuzzing
