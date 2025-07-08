#pragma once

#include "reindexertestapi.h"

class HybridTest : public ::testing::TestWithParam<reindexer::VectorMetric> {
protected:
	static constexpr std::string_view kNsName = "hybrid_ns";
	static constexpr std::string_view kFieldNameId = "id";
	static constexpr std::string_view kFieldNameFt = "ft";
	static constexpr std::string_view kFieldNameIP = "ip";
	static constexpr std::string_view kFieldNameCos = "cos";
	static constexpr std::string_view kFieldNameL2 = "l2";

	void SetUp() override;
	reindexer::Item newItem(int id);

	ReindexerTestApi<reindexer::Reindexer> rt;
};
