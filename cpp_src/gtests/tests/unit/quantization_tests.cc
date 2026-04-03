#include "core/system_ns_names.h"
#include "fmt/printf.h"
#include "gtests/tests/fixtures/quantization_helpers.h"
#include "reindexertestapi.h"
#include "tools/fsops.h"
#include "tools/scope_guard.h"

namespace sq8_test {

template <typename MetricT>
void DataSamplerBaseTestBody() {
	const size_t kHnswRandomSeed = 100;

	MetricT space(kDimension);
	const auto map = std::make_unique<hnswlib::HierarchicalNSWST>(&space, kHNSWInitSize, kM, kEfConstruction, kHnswRandomSeed,
																  reindexer::ReplaceDeleted_True);

	std::vector<std::vector<float>> points(kHNSWInitSize);

	for (size_t i = 0; i < kHNSWInitSize; ++i) {
		points[i] = MakePoint();
		map->template addPoint<hnswlib::DummyLocker>(points[i].data(), i);
	}

	const int partialSampleSize = 0.25 * points.size();

	auto partialIndexes = hnswlib::HNSWView<hnswlib::HierarchicalNSWST>::GetSampleIndexes(partialSampleSize, map->cur_element_count);
	auto samples = hnswlib::HNSWView(*map, partialIndexes);

	const auto dim = map->fstdistfunc_.Dims();
	int cnt = 0;
	for (auto& sample : samples) {
		for (const auto& comp : sample) {
			ASSERT_EQ(comp, points[partialIndexes[cnt / dim]][cnt % dim]);
			++cnt;
		}
	}
	ASSERT_EQ(cnt, partialSampleSize * dim);
}

TEST(Quantization, DataSamplerTest_L2) { DataSamplerBaseTestBody<hnswlib::L2Space>(); }
TEST(Quantization, DataSamplerTest_IP) { DataSamplerBaseTestBody<hnswlib::InnerProductSpace>(); }
TEST(Quantization, DataSamplerTest_Cosine) { DataSamplerBaseTestBody<hnswlib::CosineSpace>(); }

class [[nodiscard]] QuantizationApi : public ::testing::Test {
protected:
	void SetUp() override {
		auto dir = reindexer::fs::JoinPath(reindexer::fs::GetTempDir(), "/QuantizationTest");
		std::ignore = reindexer::fs::RmDirAll(dir);
		api.reindexer = std::make_shared<reindexer::Reindexer>();
		api.Connect("builtin://" + dir);
	}

	ReindexerTestApi<reindexer::Reindexer> api;
};

template <reindexer::VectorMetric Metric>
void SaveLoadTestBody(auto& api) {
	constexpr static auto kNsName = "hnsw_quantization_save_load_test_ns";

	const size_t K = 20;
	const size_t ef = 0.05 * kHNSWInitSize;

	auto checkFloatVectorValues = [&api] {
		auto qr = api.Select(reindexer::Query(kNsName).Sort("id", false).SelectAllFields());
		ASSERT_EQ(qr.Count(), kHNSWInitSize);

		for (auto it = qr.begin(); it != qr.end(); ++it) {
			auto item = (*it).GetItem();
			auto fv = item[kHnswIndexName].template As<reindexer::ConstFloatVectorView>().Span();
			auto fvQ = item[kHnswIndexNameQ].template As<reindexer::ConstFloatVectorView>().Span();

			ASSERT_EQ(fvQ.size(), kDimension);
			ASSERT_EQ(fv.size(), kDimension);

			for (size_t i = 0; i < kDimension; ++i) {
				ASSERT_TRUE(reindexer::fp::EqualWithinULPs(fv[i], fvQ[i]));
			}
		}
	};

	InitNsAndIndexes<Metric>(
		api, kNsName, kHNSWInitSize,
		hnswlib::QuantizationConfig{.quantile = kQuantile, .sampleSize = kHNSWInitSize / 2, .quantizationThreshold = kHNSWInitSize});

	const auto queryPoints = MakeQueryPoints();
	WaitQuantization(api, kNsName);

	CheckRecallRate(api, kNsName, queryPoints, K, ef);
	checkFloatVectorValues();

	api.CloseNamespace(kNsName);
	api.OpenNamespace(kNsName);

	CheckRecallRate(api, kNsName, queryPoints, K, ef);
	checkFloatVectorValues();
}

TEST_F(QuantizationApi, SaveLoadTest_L2) { SaveLoadTestBody<reindexer::VectorMetric::L2>(api); }
TEST_F(QuantizationApi, SaveLoadTest_IP) { SaveLoadTestBody<reindexer::VectorMetric::InnerProduct>(api); }
TEST_F(QuantizationApi, SaveLoadTest_Cosine) { SaveLoadTestBody<reindexer::VectorMetric::Cosine>(api); }

static void SetTxSizeToAlwaysCopy(ReindexerTestApi<reindexer::Reindexer>& api, int64_t v) {
	auto updated = api.Update(
		reindexer::Query(reindexer::kConfigNamespace).Set("namespaces[*].tx_size_to_always_copy", v).Where("type", CondEq, "namespaces"));
	ASSERT_GT(updated, 0);
}

template <reindexer::VectorMetric Metric>
void IndexQuantizingDropIndexTestBody(auto& api) {
	constexpr static auto kNsName = "hnsw_quantization_drop_idx_test_ns";

	const size_t K = 20;
	const size_t ef = 0.05 * kHNSWInitSize;

	InitNsAndIndexes<Metric>(
		api, kNsName, kHNSWInitSize,
		hnswlib::QuantizationConfig{.quantile = kQuantile, .sampleSize = kHNSWInitSize / 2, .quantizationThreshold = kHNSWInitSize});

	const auto queryPoints = MakeQueryPoints();
	WaitQuantization(api, kNsName);

	CheckRecallRate(api, kNsName, queryPoints, K, ef);

	api.DropIndex(kNsName, kHnswIndexNameQ);

	// Check that all float vector values have been copied back to items
	auto qr = api.Select(reindexer::Query(kNsName).Sort("id", false).SelectAllFields());
	ASSERT_EQ(qr.Count(), kHNSWInitSize);

	for (auto it = qr.begin(); it != qr.end(); ++it) {
		auto item = (*it).GetItem();
		auto fv = item[kHnswIndexName].template As<reindexer::ConstFloatVectorView>().Span();
		auto fvQ = YAML::Load(std::string{item.GetJSON()})[kHnswIndexNameQ];

		ASSERT_EQ(fvQ.Type(), YAML::NodeType::Sequence);
		ASSERT_EQ(fv.size(), kDimension);

		int i = 0;
		for (const auto& val : fvQ) {
			ASSERT_TRUE(reindexer::fp::EqualWithinULPs(fv[i], val.as<float>()))
				<< fmt::format("i = {}; fv[i] = {};  fvQ[i] = {}", i, fv[i], val.as<float>()) << std::endl;
			++i;
		}
		ASSERT_EQ(i, kDimension);
	}
}

TEST_F(QuantizationApi, IndexQuantizingDropIndexTest_L2) { IndexQuantizingDropIndexTestBody<reindexer::VectorMetric::L2>(api); }
TEST_F(QuantizationApi, IndexQuantizingDropIndexTest_IP) { IndexQuantizingDropIndexTestBody<reindexer::VectorMetric::InnerProduct>(api); }
TEST_F(QuantizationApi, IndexQuantizingDropIndexTest_Cosine) { IndexQuantizingDropIndexTestBody<reindexer::VectorMetric::Cosine>(api); }

template <reindexer::VectorMetric Metric>
static void ResetQuantizationByFastUpdate(auto& api, std::string_view nsName, MultithreadingMode mode) {
	// Reset quantization. Index udpate must be executed by the "fast-update" branch with updating only the quantization config
	api.UpdateIndex(nsName, reindexer::IndexDef{kHnswIndexNameQ,
												{kHnswIndexNameQ},
												IndexHnsw,
												IndexOpts{}.SetFloatVector(IndexHnsw, FloatVectorIndexOpts{}
																						  .SetDimension(kDimension)
																						  .SetStartSize(kHNSWMaxSize)
																						  .SetM(kM)
																						  .SetEfConstruction(kEfConstruction)
																						  .SetMultithreading(mode)
																						  .SetMetric(Metric))});
}

template <reindexer::VectorMetric Metric>
void IndexQuantizingSimpleTestBody(auto& api) {
	constexpr static auto kNsName = "hnsw_quantization_simple_test_ns";

	const size_t K = 20;
	const size_t ef = 0.05 * kHNSWInitSize;

	InitNsAndIndexes<Metric>(
		api, kNsName, kHNSWInitSize,
		hnswlib::QuantizationConfig{.quantile = kQuantile, .sampleSize = kHNSWInitSize / 2, .quantizationThreshold = kHNSWInitSize + 1});

	const auto queryPoints = MakeQueryPoints();

	ASSERT_FALSE(GetQuantizationStatus(api, kNsName));

	auto item = newItemWithVector(api, kNsName, kHNSWInitSize);
	api.Upsert(kNsName, item);

	WaitQuantization(api, kNsName);
	CheckRecallRate(api, kNsName, queryPoints, K, ef);

	// It is necessary to verify the correctness of the serialization/deserialization of deleted points in the hnsw graph.
	const auto delStep = 5;
	for (size_t i = 0; i < kHNSWInitSize; i += delStep) {
		auto item = api.NewItem(kNsName);
		const auto err = item.FromJSON(fmt::sprintf(R"({ "id": %d })", i + std::rand() % delStep));
		if (!err.ok()) {
			throw err;
		}

		api.Delete(kNsName, item);
	}

	CheckRecallRate(api, kNsName, queryPoints, K, ef);

	// Check copying transaction
	SetTxSizeToAlwaysCopy(api, 1);
	auto guard = reindexer::MakeScopeGuard([&api] { SetTxSizeToAlwaysCopy(api, 100'000); });
	auto tx = api.NewTransaction(kNsName);
	for (size_t i = 0; i < kHNSWInitSize / 5; ++i) {
		auto err = tx.Upsert(newTxItemWithVector(tx, kHNSWInitSize + i));
		ASSERT_TRUE(err.ok()) << err.what();
	}
	std::ignore = api.CommitTransaction(tx);
	CheckRecallRate(api, kNsName, queryPoints, K, ef);

	// Check recall rates after dequantization
	ResetQuantizationByFastUpdate<Metric>(api, kNsName, MultithreadingMode::SingleThread);

	WaitDequantization(api, kNsName);
	CheckRecallRate(api, kNsName, queryPoints, K, ef);
}

TEST_F(QuantizationApi, IndexQuantizingSimpleTest_L2) { IndexQuantizingSimpleTestBody<reindexer::VectorMetric::L2>(api); }
TEST_F(QuantizationApi, IndexQuantizingSimpleTest_IP) { IndexQuantizingSimpleTestBody<reindexer::VectorMetric::InnerProduct>(api); }
TEST_F(QuantizationApi, IndexQuantizingSimpleTest_Cosine) { IndexQuantizingSimpleTestBody<reindexer::VectorMetric::Cosine>(api); }

template <reindexer::VectorMetric Metric>
void IndexQuantizingConcurrentTestBody(auto& api) {
	constexpr static auto kNsName = "hnsw_quantization_concurrent_test_ns";
	const size_t qThreshold = 1.5 * kHNSWInitSize;
	const auto kMaxSize = kHNSWMaxSize + kHNSWInitSize;	 // for initiate resizeIndex call

	const size_t K = 20;

	InitNsAndIndexes<Metric, MultithreadingMode::MultithreadTransactions>(
		api, kNsName, kHNSWInitSize,
		hnswlib::QuantizationConfig{.quantile = kQuantile, .sampleSize = kHNSWInitSize, .quantizationThreshold = qThreshold});

	std::atomic_uint32_t inserted = kHNSWInitSize;

	auto insertTh = std::thread([&] {
		for (size_t id = kHNSWInitSize; id < kMaxSize; ++id) {
			auto item = newItemWithVector(api, kNsName, id);
			api.Upsert(kNsName, item);
			inserted.fetch_add(1, std::memory_order_acq_rel);
			if (auto cnt = inserted.load(std::memory_order_relaxed); cnt % 100 == 0) {
				TEST_COUT << fmt::format("Inserted {} items\n", cnt);
			}
		}
	});

	auto deleteTh = std::thread([&] {
		while (inserted.load(std::memory_order_acquire) < kMaxSize) {
			auto item = api.NewItem(kNsName);
			const auto err = item.FromJSON(fmt::sprintf(R"({ "id": %d })", std::rand() % inserted.load(std::memory_order_acquire)));
			if (!err.ok()) {
				throw err;
			}

			api.Delete(kNsName, item);

			std::this_thread::sleep_for(std::chrono::milliseconds(std::rand() % 100));
			std::this_thread::yield();
		}
	});

	const auto dequantizedCnt = kMaxSize - kHNSWInitSize / 2;

	auto checkQuantizationTh = std::thread([&] {
		uint32_t cnt;
		while (cnt = inserted.load(std::memory_order_acquire), cnt < dequantizedCnt && !GetQuantizationStatus(api, kNsName)) {
			std::this_thread::sleep_for(std::chrono::milliseconds(10));
			std::this_thread::yield();
		}
		ASSERT_TRUE(GetQuantizationStatus(api, kNsName) && cnt >= qThreshold);
		TEST_COUT << "QUANTIZED" << std::endl;
	});

	auto checkDequantizationTh = std::thread([&] {
		uint32_t cnt;
		while (cnt = inserted.load(std::memory_order_acquire), cnt <= dequantizedCnt) {
			std::this_thread::sleep_for(std::chrono::milliseconds(100));
			std::this_thread::yield();
		}

		ASSERT_TRUE(GetQuantizationStatus(api, kNsName));

		ResetQuantizationByFastUpdate<Metric>(api, kNsName, MultithreadingMode::MultithreadTransactions);

		while (cnt = inserted.load(std::memory_order_acquire), (cnt < kMaxSize && GetQuantizationStatus(api, kNsName))) {
			std::this_thread::sleep_for(std::chrono::milliseconds(10));
			std::this_thread::yield();
			continue;
		}

		ASSERT_FALSE(GetQuantizationStatus(api, kNsName));
		TEST_COUT << "DEQUANTIZED" << std::endl;
	});

	auto readFn = [&] {
		size_t cnt = 0;
		while (cnt = inserted.load(std::memory_order_acquire), cnt < kMaxSize) {
			CheckRecallRate(api, kNsName, MakeQueryPoints(), K, 0.05 * cnt);
			std::this_thread::yield();
		}
	};

	auto readTh1 = std::thread(readFn);
	auto readTh2 = std::thread(readFn);

	insertTh.join();
	checkQuantizationTh.join();
	checkDequantizationTh.join();
	deleteTh.join();
	readTh1.join();
	readTh2.join();
}

TEST_F(QuantizationApi, IndexQuantizingConcurrentTest_L2) { IndexQuantizingConcurrentTestBody<reindexer::VectorMetric::L2>(api); }
TEST_F(QuantizationApi, IndexQuantizingConcurrentTest_IP) { IndexQuantizingConcurrentTestBody<reindexer::VectorMetric::InnerProduct>(api); }
TEST_F(QuantizationApi, IndexQuantizingConcurrentTest_Cosine) { IndexQuantizingConcurrentTestBody<reindexer::VectorMetric::Cosine>(api); }

template <reindexer::VectorMetric Metric>
void SearchWithRadiusTestBody(auto& api) {
	constexpr static auto kNsName = "hnsw_quantization_search_with_radius_test_ns";

	const size_t K = 100;
	const size_t ef = 1.2 * K;

	InitNsAndIndexes<Metric>(
		api, kNsName, kHNSWMaxSize,
		hnswlib::QuantizationConfig{.quantile = kQuantile, .sampleSize = kHNSWInitSize, .quantizationThreshold = kHNSWMaxSize});

	WaitQuantization(api, kNsName);

	std::unordered_map<int, float> res, resQ;

	float avgRankVariance = 0;

	auto test = [&](auto& queryPoint, std::optional<float> refRank) {
		auto params = !refRank ? reindexer::HnswSearchParams{}.Ef(ef).K(K) : reindexer::HnswSearchParams{}.Ef(ef).Radius(refRank);
		auto qr =
			api.Select(reindexer::Query(kNsName).WhereKNN(kHnswIndexName, reindexer::ConstFloatVectorView{queryPoint}, params).WithRank());
		auto qrQ =
			api.Select(reindexer::Query(kNsName).WhereKNN(kHnswIndexNameQ, reindexer::ConstFloatVectorView{queryPoint}, params).WithRank());

		auto it = qr.begin();
		auto itQ = qrQ.begin();

		res.clear();
		resQ.clear();
		while (it != qr.end() && itQ != qrQ.end()) {
			auto item = YAML::Load(std::string{(*it).GetItem().GetJSON()});
			auto itemQ = YAML::Load(std::string{(*itQ).GetItem().GetJSON()});

			res.emplace(item["id"].template as<int>(), (*it).GetItemRefRanked().Rank().Value());
			resQ.emplace(itemQ["id"].template as<int>(), (*itQ).GetItemRefRanked().Rank().Value());

			++it;
			++itQ;
		}

		int recallCounter = 0;
		float variance = 0;
		for (auto& [id, rank] : resQ) {
			if (res.contains(id)) {
				variance += std::abs(res[id] - rank) / res[id];
				++recallCounter;
			}
		}
		EXPECT_GT(recallCounter, 0);

		avgRankVariance += variance / recallCounter;

		if (!refRank) {
			EXPECT_EQ(it, qr.end());
			EXPECT_EQ(itQ, qrQ.end());
			refRank = (*(qr.begin() + qr.Count() / 2)).GetItemRefRanked().Rank().Value();
		}

		return refRank;
	};

	const auto queryPoints = MakeQueryPoints();
	for (auto& queryPoint : queryPoints) {
		auto refRank = test(queryPoint, std::nullopt);
		std::ignore = test(queryPoint, refRank);
	}

	avgRankVariance /= queryPoints.size();
	if (kIsRelease) {
		ASSERT_LE(avgRankVariance, 0.06f);
	}
}

TEST_F(QuantizationApi, SearchWithRadiusTest_L2) { SearchWithRadiusTestBody<reindexer::VectorMetric::L2>(api); }
TEST_F(QuantizationApi, SearchWithRadiusTest_IP) { SearchWithRadiusTestBody<reindexer::VectorMetric::InnerProduct>(api); }
TEST_F(QuantizationApi, SearchWithRadiusTest_Cosine) { SearchWithRadiusTestBody<reindexer::VectorMetric::Cosine>(api); }
}  // namespace sq8_test
