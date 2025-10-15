#pragma once

#include "ns_scheme.h"
#include "random_generator.h"

namespace fuzzing {

class Index;

class [[nodiscard]] Ns {
public:
	Ns(std::string name, RandomGenerator::ErrFactorType errorFactor);
	const std::vector<Index>& GetIndexes() const& noexcept { return indexes_; }
	std::vector<Index>& GetIndexes() & noexcept { return indexes_; }
	const std::vector<Index>& GetIndexes() const&& = delete;
	const std::string& GetName() const noexcept { return name_; }
	const NsScheme& GetScheme() const noexcept { return scheme_; }
	RandomGenerator& GetRandomGenerator() noexcept { return rndGen_; }
	void AddIndexToScheme(const Index&, size_t indexNumber);
	void NewItem(reindexer::WrSerializer& ser) { scheme_.NewItem(ser, rndGen_, indexes_); }
	void Dump(std::ostream&) const;

private:
	std::string name_;
	RandomGenerator rndGen_;
	NsScheme scheme_;
	std::vector<Index> indexes_;
};

}  // namespace fuzzing
