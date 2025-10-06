#pragma once

#include "random_generator.h"

namespace reindexer {

class Query;

}  // namespace reindexer

namespace fuzzing {

class Ns;

class [[nodiscard]] QueryGenerator {
public:
	QueryGenerator(const std::vector<Ns>& nss, RandomGenerator::ErrFactorType errorFactor) : namespaces_{nss}, rndGen_{errorFactor} {}
	reindexer::Query operator()();

private:
	const std::vector<Ns>& namespaces_;
	RandomGenerator rndGen_;
};

}  // namespace fuzzing
