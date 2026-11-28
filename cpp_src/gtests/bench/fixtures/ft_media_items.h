#pragma once

#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

#include "base_fixture.h"
#include "estl/fast_hash_set.h"
#include "ft_base.h"

namespace reindexer_benchmarks {

class [[nodiscard]] FullTextMediaItems : private BaseFixture, private FullTextBase {
public:
	FullTextMediaItems(Reindexer* db, std::string_view name, size_t maxItems);

	reindexer::Error Initialize() override;
	void RegisterAllCases();

private:
	enum class [[nodiscard]] QueryTerms { One, Two };

	reindexer::Item MakeItem(benchmark::State&) override;

	void Insert(benchmark::State& state);
	void Build(benchmark::State& state);
	void SelectOneWord(benchmark::State& state);
	void SelectTwoWords(benchmark::State& state);
	void select(benchmark::State& state, QueryTerms queryTerms);
	reindexer::Error resetFtCache() noexcept;

	void rememberWord(const std::string& word);
	std::string makeText(size_t wordsCount, int id);
	std::vector<std::string> makeWords(size_t wordsCount);

	static constexpr size_t kFrequentWordsCount = 16;
	static constexpr size_t kInsertBatchSize = 10'000;
	static constexpr size_t kSelectThreads = 16;
	static constexpr size_t kSelectIterations = 2'000;

	const std::string kIndexName_ = "search_data";
	size_t cacheResetCount_ = 0;  // Incremented after each successful update of the document used to reset the FT cache.
	reindexer::fast_hash_set<std::string> generatedWords_;	// Distinct source words recorded while generating documents.
	std::vector<std::string> frequentWords_;				// Common words used to generate large posting lists.
	std::vector<std::string> queryWords_;  // Distinct source words in their original spelling, sorted for the query streams.
};

}  // namespace reindexer_benchmarks
