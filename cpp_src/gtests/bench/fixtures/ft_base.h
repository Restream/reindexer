#pragma once

#include <random>
#include "tools/errors.h"

namespace reindexer::bench {

class [[nodiscard]] FullTextBase {
protected:
	FullTextBase(size_t maxWord1Count = 0) noexcept : maxWord1Count_{maxWord1Count} {}
	Error Initialize();
	int RndInt(int min, int max) { return randomGenerator_(randomEngine_, std::uniform_int_distribution<int>::param_type{min, max}); }
	size_t RndIndexOf(const auto& container) { return RndInt(0, container.size() - 1); }
	const auto& RndFrom(const auto& container) { return container.at(RndIndexOf(container)); }
	const std::string& RndWord1() & { return RndFrom(words1_); }
	size_t Words1Count() const { return words1_.size(); }
	std::string MakeTypoWord();
	std::wstring GetRandomUTF16WordByLength(size_t minLen = 4);
	std::string CreatePhrase();
	std::string MakePrefixWord();
	std::string MakeSuffixWord();

private:
	Error readDictFile(const std::string& fileName, std::vector<std::string>& words);

	std::vector<std::string> words1_;
	size_t maxWord1Count_{0};
	std::mt19937 randomEngine_{1};
	std::uniform_int_distribution<int> randomGenerator_{};
};

}  // namespace reindexer::bench
