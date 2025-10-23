#pragma once

#include "reindexertestapi.h"

class [[nodiscard]] HybridTest : public ::testing::TestWithParam<reindexer::VectorMetric> {
protected:
#if defined(REINDEX_WITH_TSAN) || defined(REINDEX_WITH_ASAN)
	constexpr static size_t kDimension = 32;
	constexpr static size_t kMaxElements = 500;
#else
	constexpr static size_t kDimension = 512;
	constexpr static size_t kMaxElements = 2'000;
#endif

protected:
	static constexpr std::string_view kNsName = "hybrid_ns";
	static constexpr std::string_view kFieldNameId = "id";
	static constexpr std::string_view kFieldNameFt = "ft";
	static constexpr std::string_view kFieldNameIP = "ip";
	static constexpr std::string_view kFieldNameCos = "cos";
	static constexpr std::string_view kFieldNameL2 = "l2";

	void SetUp() override;
	reindexer::Item newItem(int id);
	void check(const reindexer::Query&) const;
	std::string checkFailed(const reindexer::Query&) const;
	void checkFailed(const reindexer::Query&, std::string_view expectErr) const;
	void checkFailedRegex(const reindexer::Query&, std::string_view expectErrRegex) const;
	reindexer::Query makeHybridQuery();
	reindexer::Query makeFtQuery() const;
	reindexer::Query makeKnnQuery() const;
	std::string rndReranker() const;

	ReindexerTestApi<reindexer::Reindexer> rt;

	struct {
		std::string_view name;
		reindexer::KnnSearchParams params;
	} constexpr static knnFields_[]{{kFieldNameIP, reindexer::HnswSearchParams{}.K(kMaxElements / 4).Ef(kMaxElements / 4)},
									{kFieldNameCos, reindexer::IvfSearchParams{}.K(kMaxElements / 4).NProbe(5)},
									{kFieldNameL2, reindexer::IvfSearchParams{}.K(kMaxElements / 4).NProbe(5)}};
	size_t currentKnnField_ = 0;
};
