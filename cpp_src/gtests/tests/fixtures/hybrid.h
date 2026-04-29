#pragma once

#include "reindexertestapi.h"

class [[nodiscard]] HybridTest : public ::testing::TestWithParam<reindexer::VectorMetric> {
protected:
	enum [[nodiscard]] IsArray : bool { Array = true, Scalar = false };
#if defined(REINDEX_WITH_TSAN) || defined(REINDEX_WITH_ASAN) || defined(RX_WITH_STDLIB_DEBUG)
	template <IsArray isArray>
	constexpr static size_t kDimension = isArray ? 8 : 32;
	constexpr static size_t kMaxElements = 500;
#else
	template <IsArray isArray>
	constexpr static size_t kDimension = isArray ? 64 : 512;
	constexpr static size_t kMaxElements = 2'000;
#endif

protected:
	static constexpr std::string_view kNsName = "hybrid_ns";
	static constexpr std::string_view kFieldNameId = "id";
	static constexpr std::string_view kFieldNameFt = "ft";
	static constexpr std::string_view kFieldNameIP = "ip";
	static constexpr std::string_view kFieldNameCos = "cos";
	static constexpr std::string_view kFieldNameL2 = "l2";
	static constexpr std::string_view kFieldNameIPArray = "ip_array";
	static constexpr std::string_view kFieldNameCosArray = "cos_array";
	static constexpr std::string_view kFieldNameL2Array = "l2_array";

	void SetUp() override;
	reindexer::Item newItem(int id);
	void check(const reindexer::Query&) const;
	std::string checkFailed(const reindexer::Query&) const;
	void checkFailed(const reindexer::Query&, std::string_view expectErr) const;
	void checkFailedRegex(const reindexer::Query&, std::string_view expectErrRegex) const;
	template <IsArray>
	reindexer::Query makeHybridQuery();
	reindexer::Query makeFtQuery() const;
	template <IsArray>
	reindexer::Query makeKnnQuery() const;
	template <IsArray>
	std::string rndReranker() const;

	ReindexerTestApi<reindexer::Reindexer> rt;

	template <IsArray>
	void TestQueries();
	template <IsArray>
	void TestMerge();

	struct {
		std::string_view nameScalar;
		std::string_view nameArray;
		reindexer::KnnSearchParams params;
	} constexpr static knnFields_[]{
		{kFieldNameIP, kFieldNameIPArray, reindexer::HnswSearchParams{}.K(kMaxElements / 4).Ef(kMaxElements / 4)},
		{kFieldNameCos, kFieldNameCosArray, reindexer::IvfSearchParams{}.K(kMaxElements / 4).NProbe(5)},
		{kFieldNameL2, kFieldNameL2Array, reindexer::IvfSearchParams{}.K(kMaxElements / 4).NProbe(5)}};
	size_t currentKnnField_ = 0;
};
