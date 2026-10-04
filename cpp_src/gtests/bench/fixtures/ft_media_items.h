#pragma once

#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

#include "base_fixture.h"
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

	std::string makeText(size_t wordsCount, int id);
	std::vector<std::string> makeWords(size_t wordsCount);

	static constexpr size_t kQueryWordsCount = 16;
	static constexpr size_t kInsertBatchSize = 10'000;
	static constexpr size_t kSelectThreads = 16;
	static constexpr double kSelectMinTime = 10.0;

	const std::string kIndexName_ = "search_data";
	std::vector<std::string> queryWords_;
};

}  // namespace reindexer_benchmarks
