#include "gtests/tests/fixtures/float_vector_index.h"
#include "gtests/tests/gtest_cout.h"
#include "gtests/tools.h"
#include "tools/fsops.h"

namespace reindexer_tests {

using reindexer::Query;
using reindexer::VectorMetric;

constexpr auto kFieldNameId = "id";
constexpr auto kFieldHnsw = "hnsw";
constexpr auto kFieldBf = "vec_bf";
constexpr auto kHash01pct = "hash01pct";
constexpr auto kHash1pct = "hash1pct";
constexpr auto kHash10pct = "hash10pct";
constexpr auto kScoreTree = "score_tree";
constexpr auto kGroupTreeInt = "group_tree_int";
constexpr auto kGroupTreeStr = "group_tree_str";

#if defined(RX_WITH_STDLIB_DEBUG) || defined(REINDEX_WITH_TSAN) || defined(REINDEX_WITH_ASAN)
constexpr bool kIsRelease = false;
constexpr size_t kDimension = 64;
constexpr size_t kMaxElements = 2'000;
#else
constexpr bool kIsRelease = true;
constexpr size_t kDimension = 768;
constexpr size_t kMaxElements = 10'000;
#endif

constexpr size_t kTxBatchSize = 1'000;

struct [[nodiscard]] RecallScenario {
	const char* name;
	std::function<void(Query&)> applyFilters;
	size_t limit;
	size_t offset;
	float minRecall = 0.9f;
};

static float calcRecall(const std::unordered_set<int>& candidate, const std::unordered_set<int>& reference) {
	if (reference.empty()) {
		return 1.f;
	}
	size_t hits = 0;
	for (const int id : candidate) {
		hits += reference.count(id);
	}
	return float(hits) / float(reference.size());
}

static std::unordered_set<int> collectIds(const reindexer::QueryResults& qr) {
	std::unordered_set<int> ids;
	ids.reserve(qr.Count());
	for (auto it = qr.begin(); it != qr.end(); ++it) {
		const auto& item = it.GetItemRefRanked();
		ids.insert(item.NotRanked().Id().ToNumber());
	}
	return ids;
}

static void fillItem(reindexer::Item& item, int id, std::array<float, kDimension>& vecBuf) {
	reindexer_tests_tools::rndFloatVector(vecBuf);

	item[kFieldNameId] = id;
	item[kHash01pct] = id % 1000;
	item[kHash1pct] = id % 100;
	item[kHash10pct] = id % 10;
	item[kScoreTree] = id;
	item[kGroupTreeInt] = id % 5;
	item[kGroupTreeStr] = std::string("g") + std::to_string(id % 5);
	item[kFieldHnsw] = reindexer::ConstFloatVectorView{vecBuf};
	item[kFieldBf] = reindexer::ConstFloatVectorView{vecBuf};
}

static std::vector<RecallScenario> scenarios(VectorMetric metric) {
	return {
		//////////////////////////////////////////////////// hash tree scenarios ////////////////////////////////////////////////////
		{
			"hash01pct_eq0",
			[](Query& q) { q.Where(kHash01pct, CondEq, 0); },
			10,
			0,
		},
		{
			"hash1pct_eq0",
			[](Query& q) { q.Where(kHash1pct, CondEq, 0); },
			50,
			0,
		},
		{
			"hash10pct_eq0",
			[](Query& q) { q.Where(kHash10pct, CondEq, 0); },
			100,
			0,
		},
		{
			"tree_score_10pct",
			[](Query& q) { q.Where(kScoreTree, CondGt, int(kMaxElements * 9 / 10)); },
			50,
			0,
			metric == VectorMetric::L2 ? 0.9f : 0.8f,
		},
		{
			"tree_group_int_20pct",
			[](Query& q) { q.Where(kGroupTreeInt, CondEq, 0); },
			100,
			0,
			metric == VectorMetric::L2 ? 0.9f : 0.8f,
		},
		{
			"hash10pct_and_tree_score_1pct",
			[](Query& q) { q.Where(kHash10pct, CondEq, 0).Where(kScoreTree, CondGt, int(kMaxElements * 9 / 10)); },
			10,
			0,
		},
		{
			"tree_score_50pct",
			[](Query& q) { q.Where(kScoreTree, CondGt, int(kMaxElements / 2)); },
			100,
			0,
			metric == VectorMetric::L2 ? 0.75f : 0.5f,
		},
		//////////////////////////////////////////////////// pagination scenarios ////////////////////////////////////////////////////
		// needed << 800, ~10% post-filter (hash10pct)
		{
			"pag_limit20_offset5_hash10pct",
			[](Query& q) { q.Where(kHash10pct, CondEq, 0); },
			20,
			5,
			metric == VectorMetric::L2 ? 0.75f : 0.5f,
		},
		{
			"pag_limit20_offset0_hash10pct",
			[](Query& q) { q.Where(kHash10pct, CondEq, 0); },
			20,
			0,
			metric == VectorMetric::L2 ? 0.75f : 0.5f,
		},
		{
			"pag_limit50_offset20_hash10pct",
			[](Query& q) { q.Where(kHash10pct, CondEq, 0); },
			50,
			20,
			0.8f,
		},
		{
			"pag_limit100_offset0_hash10pct",
			[](Query& q) { q.Where(kHash10pct, CondEq, 0); },
			100,
			0,
		},
		{
			"pag_limit500_offset0_hash10pct",
			[](Query& q) { q.Where(kHash10pct, CondEq, 0); },
			500,
			0,
		},
		// needed = 800 (batch clamp boundary)
		{
			"pag_limit800_offset0_hash10pct",
			[](Query& q) { q.Where(kHash10pct, CondEq, 0); },
			800,
			0,
		},
		// needed > 800, soft ~70% post-filter (score_tree)
		{
			"pag_limit801_offset0_score70pct",
			[](Query& q) { q.Where(kScoreTree, CondGt, int(kMaxElements * 3 / 10)); },
			801,
			0,
		},
		{
			"pag_limit1000_offset0_score70pct",
			[](Query& q) { q.Where(kScoreTree, CondGt, int(kMaxElements * 3 / 10)); },
			1000,
			0,
		},
		{
			"pag_limit2000_offset0_score70pct",
			[](Query& q) { q.Where(kScoreTree, CondGt, int(kMaxElements * 3 / 10)); },
			2000,
			0,
		},
		// ~1% post-filter (hash1pct), small pages only
		{
			"pag_limit10_offset0_hash1pct",
			[](Query& q) { q.Where(kHash1pct, CondEq, 0); },
			10,
			0,
		},
		{
			"pag_limit50_offset5_hash1pct",
			[](Query& q) { q.Where(kHash1pct, CondEq, 0); },
			50,
			5,
		},
		{
			"pag_limit80_offset10_hash1pct",
			[](Query& q) { q.Where(kHash1pct, CondEq, 0); },
			80,
			10,
		},
		//////////////////////////////////////////////////// tree string scenarios ////////////////////////////////////////////////////
		{
			"tree_str_group_20pct",
			[](Query& q) { q.Where(kGroupTreeStr, CondEq, std::string("g0")); },
			25,
			0,
			metric == VectorMetric::L2 ? 0.5f : 0.4f,
		},
		{
			"tree_str_and_hash10pct_2pct",
			[](Query& q) { q.Where(kGroupTreeStr, CondEq, std::string("g0")).Where(kHash10pct, CondEq, 0); },
			25,
			0,
			metric == VectorMetric::L2 ? 0.75f : 0.65f,
		},
		{
			"tree_str_and_tree_score_10pct",
			[](Query& q) { q.Where(kGroupTreeStr, CondEq, std::string("g1")).Where(kScoreTree, CondGt, int(kMaxElements * 9 / 10)); },
			25,
			0,
		},
		{
			"tree_str_pag_limit100_offset0",
			[](Query& q) { q.Where(kGroupTreeStr, CondEq, std::string("g0")); },
			100,
			0,
			0.8f,
		},
		{
			"tree_str_pag_limit500_offset50",
			[](Query& q) { q.Where(kGroupTreeStr, CondEq, std::string("g0")); },
			500,
			50,
		},
		{
			"tree_str_pag_limit800_offset100",
			[](Query& q) { q.Where(kGroupTreeStr, CondEq, std::string("g0")); },
			800,
			100,
		},
		//////////////////////////////////////////////////// inner join scenarios ////////////////////////////////////////////////////
		// Self-join on PK: joined side adds an extra post-filter without KNN in the right-hand query.
		{
			"join_hash10pct_group0",
			[](Query& q) {
				const auto& ns = q.NsName();
				q.Where(kHash10pct, CondEq, 0).InnerJoin(kFieldNameId, kFieldNameId, CondEq, Query{ns}.Where(kGroupTreeInt, CondEq, 0));
			},
			50,
			0,
			metric == VectorMetric::L2 ? 0.85f : 0.8f,
		},
		{
			"join_hash10pct_score_10pct",
			[](Query& q) {
				const auto& ns = q.NsName();
				q.Where(kHash10pct, CondEq, 0)
					.InnerJoin(kFieldNameId, kFieldNameId, CondEq, Query{ns}.Where(kScoreTree, CondGt, int(kMaxElements * 9 / 10)));
			},
			25,
			0,
		},
		{
			"join_hash1pct_group0",
			[](Query& q) {
				const auto& ns = q.NsName();
				q.Where(kHash1pct, CondEq, 0).InnerJoin(kFieldNameId, kFieldNameId, CondEq, Query{ns}.Where(kGroupTreeInt, CondEq, 0));
			},
			50,
			0,
			metric == VectorMetric::L2 ? 0.9f : 0.8f,
		},
		{
			"join_pag_limit20_offset5_hash10pct",
			[](Query& q) {
				const auto& ns = q.NsName();
				q.Where(kHash10pct, CondEq, 0).InnerJoin(kFieldNameId, kFieldNameId, CondEq, Query{ns}.Where(kGroupTreeInt, CondEq, 0));
			},
			20,
			5,
			metric == VectorMetric::L2 ? 0.75f : 0.5f,
		},
		{
			"join_hash10pct_and_score_50pct",
			[](Query& q) {
				const auto& ns = q.NsName();
				q.Where(kHash10pct, CondEq, 0)
					.Where(kScoreTree, CondGt, int(kMaxElements / 2))
					.InnerJoin(kFieldNameId, kFieldNameId, CondEq, Query{ns}.Where(kGroupTreeStr, CondEq, std::string("g0")));
			},
			50,
			0,
			metric == VectorMetric::L2 ? 0.75f : 0.65f,
		},
	};
}

class [[nodiscard]] StreamingKnn : public FloatVector {
protected:
	void SetUp() override {
		auto dir = reindexer::fs::JoinPath(reindexer::fs::GetTempDir(), "/StreamingKnnTest");
		std::ignore = reindexer::fs::RmDirAll(dir);
		rt.reindexer = std::make_shared<reindexer::Reindexer>();
		rt.Connect("builtin://" + dir);
	}

	void setupNamespace(VectorMetric metric, std::string_view nsName) {
		using reindexer::IndexOpts;

		auto fvBfOpts = reindexer::FloatVectorIndexOpts{}
							.SetDimension(kDimension)
							.SetStartSize(kMaxElements)
							.SetMetric(metric)
							.SetMultithreading(reindexer::MultithreadingMode::MultithreadTransactions);
		auto fvHnswOpts = reindexer::FloatVectorIndexOpts{fvBfOpts}.SetM(16).SetEfConstruction(200);

		rt.OpenNamespace(nsName);
		rt.DefineNamespaceDataset(nsName, {
											  IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts{}.PK(), 0},
											  IndexDeclaration{kHash01pct, "hash", "int", IndexOpts{}, 0},
											  IndexDeclaration{kHash1pct, "hash", "int", IndexOpts{}, 0},
											  IndexDeclaration{kHash10pct, "hash", "int", IndexOpts{}, 0},
											  IndexDeclaration{kScoreTree, "tree", "int", IndexOpts{}, 0},
											  IndexDeclaration{kGroupTreeInt, "tree", "int", IndexOpts{}, 0},
											  IndexDeclaration{kGroupTreeStr, "tree", "string", IndexOpts{}, 0},
											  IndexDeclaration{kFieldHnsw, kFieldHnsw, "float_vector",
															   IndexOpts{}.Array(false).SetFloatVector(IndexHnsw, fvHnswOpts), 0},
											  IndexDeclaration{kFieldBf, kFieldBf, "float_vector",
															   IndexOpts{}.Array(false).SetFloatVector(IndexVectorBruteforce, fvBfOpts), 0},
										  });

		std::array<float, kDimension> vecBuf{};
		size_t itemId = 0;
		while (itemId < kMaxElements) {
			auto tx = rt.NewTransaction(nsName);
			size_t cnt = 0;
			for (; cnt < kTxBatchSize; ++cnt) {
				if (itemId >= kMaxElements) {
					break;
				}
				auto txItem = tx.NewItem();
				fillItem(txItem, int(itemId), vecBuf);
				const auto err = tx.Insert(std::move(txItem));
				ASSERT_TRUE(err.ok());
				++itemId;
			}
			const auto qr = rt.CommitTransaction(tx);
			ASSERT_EQ(qr.Count(), cnt);
		}
	}

	enum class [[nodiscard]] ScenarioResult {
		Success,
		Failure,
		Retry,
	};
	static constexpr int kMaxRetries = 3;

	ScenarioResult runTestScenario(std::string_view nsName, const RecallScenario& scenario, int retryCount) {
		const auto makeFilteredQuery = [&]() {
			Query q{nsName};
			scenario.applyFilters(q);
			return q;
		};

		std::array<float, kDimension> queryBuf{};
		float avgRecall = 0.f;
		const int count = 10;
		for (int i = 0; i < count; ++i) {
			reindexer_tests_tools::rndFloatVector(queryBuf);
			const reindexer::ConstFloatVectorView query{queryBuf};

			auto ref = rt.Select(makeFilteredQuery()
									 .WhereKNN(kFieldBf, query, reindexer::BruteForceSearchParams{}.K(kMaxElements))
									 .Limit(scenario.limit)
									 .Offset(scenario.offset)
									 .WithRank());

			auto streaming = rt.Select(makeFilteredQuery()
										   .WhereKNN(kFieldHnsw, query, reindexer::HnswSearchParams{})
										   .Limit(scenario.limit)
										   .Offset(scenario.offset)
										   .WithRank());

			const auto refIds = collectIds(ref);
			const auto streamIds = collectIds(streaming);

			if (refIds.empty()) {
				EXPECT_FALSE(refIds.empty()) << scenario.name;
				return ScenarioResult::Failure;
			}

			if (kIsRelease) {
				EXPECT_EQ(ref.Count(), scenario.limit) << scenario.name;
				EXPECT_LE(std::abs(float(ref.Count()) - float(streaming.Count())) / ref.Count(), 0.1f) << scenario.name;
			}
			avgRecall += calcRecall(streamIds, refIds);
		}
		avgRecall /= count;

		TEST_COUT << scenario.name << ": recall=" << avgRecall << ", minRecall=" << scenario.minRecall << " limit=" << scenario.limit
				  << " offset=" << scenario.offset << std::endl;

		if (avgRecall >= scenario.minRecall) {
			return ScenarioResult::Success;
		} else if (avgRecall >= 0.9 * scenario.minRecall && retryCount < kMaxRetries - 1) {
			return ScenarioResult::Retry;
		} else {
			EXPECT_GE(avgRecall, scenario.minRecall) << scenario.name;
			return ScenarioResult::Failure;
		}
	}
};

TEST_P(StreamingKnn, SelectivityRecallTest) try {
	auto metric = GetParam();
	const std::string nsName = std::string("fv_streaming_recall_") + std::string(VectorMetricToStr(metric));
	setupNamespace(metric, nsName);
	for (const auto& scenario : scenarios(metric)) {
		ScenarioResult result = ScenarioResult::Failure;
		for (int i = 0; i < kMaxRetries; ++i) {
			result = runTestScenario(nsName, scenario, i);
			if (result != ScenarioResult::Retry) {
				break;
			}
		}
		EXPECT_EQ(result, ScenarioResult::Success);
	}
}
CATCH_AND_ASSERT

INSTANTIATE_TEST_SUITE_P(, StreamingKnn,
						 ::testing::Values(reindexer::VectorMetric::L2, reindexer::VectorMetric::InnerProduct,
										   reindexer::VectorMetric::Cosine),
						 [](const auto& info) { return std::string(VectorMetricToStr(info.param)); });

}  // namespace reindexer_tests
