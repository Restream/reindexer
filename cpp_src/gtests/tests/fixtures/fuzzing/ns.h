#pragma once

#include "index.h"
#include "ns_scheme.h"
#include "random_generator.h"

namespace fuzzing {

class Ns {
public:
	Ns(std::string name, std::ostream&, RandomGenerator::ErrFactorType errorFactor);
	std::vector<Index>& GetIndexes() noexcept { return indexes_; }
	const std::string& GetName() const noexcept { return name_; }
	const NsScheme& GetScheme() const noexcept { return scheme_; }
	RandomGenerator& GetRandomGenerator() noexcept { return rndGen_; }
	void AddIndex(Index&, bool isSparse);
	void NewItem(reindexer::WrSerializer& ser) { scheme_.NewItem(ser, rndGen_); }
	const std::vector<Index>& GetIndexes() const noexcept { return indexes_; }
	void Dump(std::ostream&) const;

private:
	std::string name_;
	RandomGenerator rndGen_;
	NsScheme scheme_;
	std::vector<Index> indexes_;
};

}  // namespace fuzzing
