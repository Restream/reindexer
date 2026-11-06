#pragma once

#include "base_fixture.h"

namespace reindexer_benchmarks {

class [[nodiscard]] ApiTvArithmetic : protected BaseFixture {
public:
	~ApiTvArithmetic() override = default;
	ApiTvArithmetic(Reindexer* db, std::string_view name, size_t maxItems);

	void RegisterAllCases();
	reindexer::Error Initialize() override;

private:
	reindexer::Item MakeItem(benchmark::State&) override;
	reindexer::Item makeItem(std::string_view ns);

	void Insert(State& state);
	void WarmUpIndexes(State& state);
	void waitForOptimization(std::string_view ns) const;
	void registerWithTotals(const std::string& name, const reindexer::Query& q);

	std::string sparseNs_;
	NamespaceDef sparseNsDef_;
};

}  // namespace reindexer_benchmarks
