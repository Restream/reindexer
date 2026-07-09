#include "core/index/float_vector/float_vector_index.h"
#include "core/index/float_vector/hnswlib/hnswalg.h"
#include "core/system_ns_names.h"
#include "fmt/printf.h"
#include "gtests/tests/fixtures/quantization_helpers.h"
#include "gtests/tools.h"
#include "reindexertestapi.h"
#include "tools/fsops.h"
#include "tools/scope_guard.h"

namespace reindexer_tests {

using reindexer::MultithreadingMode;
using reindexer::IndexOpts;
using reindexer::FloatVectorIndexOpts;

namespace sq8_test {

template <reindexer::VectorMetric metric>
void DataSamplerBaseTestBody() {
	const size_t kHnswRandomSeed = 100;

	const auto map = std::make_unique<hnswlib::HierarchicalNSWImpl<float, hnswlib::Synchronization::None>>(
		metric, kDimension, kHNSWInitSize, kM, kEfConstruction, kHnswRandomSeed, reindexer::ReplaceDeleted_True);

	std::vector<std::vector<float>> points(kHNSWInitSize);

	for (size_t i = 0; i < kHNSWInitSize; ++i) {
		points[i] = MakePoint();
		map->AddPointNoLock(points[i].data(), i);
	}

	const int partialSampleSize = 0.25 * points.size();

	auto partialIndexes = hnswlib::HNSWView<std::nullptr_t>::GetSampleIndexes(partialSampleSize, map->cur_element_count);
	auto samples = hnswlib::HNSWView(*map, partialIndexes);

	const auto dim = map->fstdistfunc_.Dim();
	int cnt = 0;
	for (auto& sample : samples) {
		for (const auto& comp : sample) {
			ASSERT_EQ(comp, points[partialIndexes[cnt / dim]][cnt % dim]);
			++cnt;
		}
	}
	ASSERT_EQ(cnt, partialSampleSize * dim);
}

TEST(Quantization, DataSamplerTest_L2) { DataSamplerBaseTestBody<reindexer::VectorMetric::L2>(); }
TEST(Quantization, DataSamplerTest_IP) { DataSamplerBaseTestBody<reindexer::VectorMetric::InnerProduct>(); }
TEST(Quantization, DataSamplerTest_Cosine) { DataSamplerBaseTestBody<reindexer::VectorMetric::Cosine>(); }

class [[nodiscard]] HnswTestWriter final : public hnswlib::IWriter {
public:
	void PutVarUInt(uint32_t v) override { ser_.PutVarUint(v); }
	void PutVarUInt(uint64_t v) override { ser_.PutVarUint(v); }
	void PutVarInt(int64_t v) override { ser_.PutVarint(v); }
	void PutVarInt(int32_t v) override { ser_.PutVarint(v); }
	void PutFloat(float v) override { ser_.PutFloat(v); }
	void PutVString(std::string_view slice) override { ser_.PutVString(slice); }
	void AppendPKByID(hnswlib::labeltype label) override {
		const auto fvId = reindexer::FloatVectorId::FromNumber(label);
		ser_.PutVariant(reindexer::VariantArray{reindexer::Variant{fvId.RowId().ToNumber()}}[0]);
	}

	std::string_view Slice() const noexcept { return ser_.Slice(); }

private:
	reindexer::WrSerializer ser_;
};

class [[nodiscard]] HnswTestReader final : public hnswlib::IReader {
public:
	HnswTestReader(std::string_view data, std::span<const reindexer::h_vector<reindexer::FloatVector, 1>> vectorData) noexcept
		: ser_{data}, getVectorData_(vectorData, sizeof(float) * kDimension) {}

	uint64_t GetVarUInt() override { return ser_.GetVarUInt(); }
	int64_t GetVarInt() override { return ser_.GetVarint(); }
	float GetFloat() override { return ser_.GetFloat(); }
	std::string_view GetVString() override { return ser_.GetVString(); }
	hnswlib::labeltype ReadPkEncodedData(float* destBuf) override {
		reindexer::Variant key;
		key = ser_.GetVariant();
		const reindexer::IdType itemID = getVectorData_(std::move(key), 0, destBuf);
		EXPECT_TRUE(itemID.IsValid());
		return reindexer::FloatVectorId{itemID, 0}.AsNumber();
	}
	bool WithQuantizer() const override { return true; }

private:
	reindexer::Serializer ser_;
	const reindexer::FloatVectorIndexRawDataInserter getVectorData_;
};

template <reindexer::VectorMetric metric, hnswlib::Synchronization sync>
void HnswHashStorageBaseTestBody() {
	using namespace reindexer;
	using HnswT = hnswlib::HierarchicalNSWImpl<float, sync>;
	using QuantizedHnswT = hnswlib::HierarchicalNSWImpl<uint8_t, sync>;

	const size_t kHnswRandomSeed = 100;
	constexpr size_t kPointsCount = 100;
	const auto vectorHash = [](const FloatVector& point) {
		auto res = ConstFloatVectorView(point).Hash();
		EXPECT_GT(res, 0);
		return res;
	};
	const auto emptyHash = ConstFloatVectorView{}.Hash();

	auto map = std::make_unique<HnswT>(metric, kDimension, kPointsCount + 1, kM, kEfConstruction, kHnswRandomSeed, ReplaceDeleted_True);

	std::unordered_map<FloatVectorId, FloatVector> points;
	std::vector<h_vector<FloatVector, 1>> vectorsData(kPointsCount + 1);

	for (size_t i = 0; i < kPointsCount; ++i) {
		const auto [it, inserted] = points.insert({FloatVectorId{IdType::FromNumber(i), 0}, FloatVector(MakePoint())});
		ASSERT_TRUE(inserted);
		vectorsData[i].emplace_back(it->second);
	}

	if constexpr (sync == hnswlib::Synchronization::OnInsertions) {
		const size_t kNumThreads = 5;
		const auto kBatchSize = kPointsCount / kNumThreads;
		const size_t kNumBatches = (kPointsCount + kBatchSize - 1) / kBatchSize;

		std::vector<std::thread> threads;
		threads.reserve(kNumThreads);

		std::atomic<size_t> nextBatch{0};
		for (size_t i = 0; i < kNumThreads; ++i) {
			threads.emplace_back([&]() {
				while (true) {
					size_t batchId = nextBatch.fetch_add(1, std::memory_order_relaxed);
					if (batchId >= kNumBatches) {
						break;
					}

					size_t begin = batchId * kBatchSize;
					size_t end = std::min(begin + kBatchSize, kPointsCount);
					for (size_t i = begin; i < end; ++i) {
						auto label = FloatVectorId{IdType::FromNumber(i), 0};
						map->AddPointConcurrent(points[label].RawData(), label.AsNumber());
					}
				}
			});
		}

		for (auto& thread : threads) {
			thread.join();
		}
	} else {
		for (const auto& [fvId, vec] : points) {
			map->AddPointNoLock(vec.RawData(), fvId.AsNumber());
		}
	}

	for (const auto& [fvId, vec] : points) {
		ASSERT_EQ(map->GetHash(fvId.AsNumber()), vectorHash(points[fvId])) << fmt::format("rowId = {}", fvId.RowId().ToNumber());
	}

	const auto deletedFvId = FloatVectorId{IdType::FromNumber(0), 0};
	map->MarkDelete(deletedFvId.AsNumber());
	ASSERT_EQ(map->GetHash(deletedFvId.AsNumber()), emptyHash);

	const auto replacementLabel = FloatVectorId{IdType::FromNumber(kPointsCount), 0};
	ASSERT_EQ(map->GetHash(replacementLabel.AsNumber()), emptyHash);

	points[replacementLabel] = FloatVector(MakePoint());
	vectorsData[kPointsCount].emplace_back(points[replacementLabel]);

	map->AddPointNoLock(points[replacementLabel].RawData(), replacementLabel.AsNumber());
	ASSERT_EQ(map->GetHash(replacementLabel.AsNumber()), vectorHash(points[replacementLabel]));

	auto checkHashes = [&](const auto& hnsw) {
		for (const auto& [fvId, vec] : points) {
			ASSERT_EQ(hnsw.GetHash(fvId.AsNumber()), fvId == deletedFvId ? emptyHash : vectorHash(vec));
		}
	};

	HnswT copied(*map, map->MaxElements() + 1);
	checkHashes(copied);

	hnswlib::QuantizationConfig config{.quantile = kQuantile, .sampleSize = kPointsCount / 2, .quantizationThreshold = 1};
	QuantizedHnswT quantized(*map, map->MaxElements() + 1, std::optional(config));
	checkHashes(quantized);

	QuantizedHnswT quantizedCopy(quantized, quantized.MaxElements() + 1);
	checkHashes(quantizedCopy);

	auto resizeTest = [&checkHashes](auto& hnsw) {
		hnsw.ResizeIndex(hnsw.MaxElements() + 1);
		checkHashes(hnsw);
	};

	resizeTest(*map);
	resizeTest(quantized);

	auto reloadTest = [&](auto& hsnw) {
		auto quantizingParams = hsnw.quantizer_ ? std::optional{hsnw.quantizer_->Params()} : std::nullopt;

		HnswTestWriter writer;
		map->SaveIndex(writer, 0);
		auto reader = HnswTestReader(writer.Slice(), vectorsData);
		auto reloaded =
			std::decay_t<decltype(hsnw)>(reader, metric, kDimension, kHnswRandomSeed, ReplaceDeleted_True, std::move(quantizingParams));

		checkHashes(reloaded);
	};

	reloadTest(*map);
	reloadTest(quantized);
}

TEST(Quantization, HnswHashStorageTest_L2) { HnswHashStorageBaseTestBody<reindexer::VectorMetric::L2, hnswlib::Synchronization::None>(); }
TEST(Quantization, HnswHashStorageTest_IP) {
	HnswHashStorageBaseTestBody<reindexer::VectorMetric::InnerProduct, hnswlib::Synchronization::None>();
}
TEST(Quantization, HnswHashStorageTest_Cosine) {
	HnswHashStorageBaseTestBody<reindexer::VectorMetric::Cosine, hnswlib::Synchronization::None>();
}

TEST(Quantization, HnswHashStorageConcurrentTest_L2) {
	HnswHashStorageBaseTestBody<reindexer::VectorMetric::L2, hnswlib::Synchronization::OnInsertions>();
}
TEST(Quantization, HnswHashStorageConcurrentTest_IP) {
	HnswHashStorageBaseTestBody<reindexer::VectorMetric::InnerProduct, hnswlib::Synchronization::OnInsertions>();
}
TEST(Quantization, HnswHashStorageConcurrentTest_Cosine) {
	HnswHashStorageBaseTestBody<reindexer::VectorMetric::Cosine, hnswlib::Synchronization::OnInsertions>();
}

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

	const auto emptyIds = InitNsAndIndexes<Metric>(
		api, kNsName, kHNSWInitSize,
		hnswlib::QuantizationConfig{
			.quantile = kQuantile, .sampleSize = kHNSWInitSize / 2, .quantizationThreshold = kHNSWInitSize - kInitEmptyItemsNum});

	auto checkFloatVectorValues = [&api, &emptyIds] {
		auto qr = api.Select(reindexer::Query(kNsName).Sort("id", false).SelectAllFields());
		ASSERT_EQ(qr.Count(), kHNSWInitSize);

		for (auto it = qr.begin(); it != qr.end(); ++it) {
			auto item = (*it).GetItem();
			auto fv = item[kHnswIndexName].template As<reindexer::ConstFloatVectorView>().Span();
			auto fvQ = item[kHnswIndexNameQ].template As<reindexer::ConstFloatVectorView>().Span();

			const auto empty = emptyIds.contains(item["id"].template As<int>());
			ASSERT_EQ(fvQ.size(), empty ? 0 : kDimension);
			ASSERT_EQ(fv.size(), empty ? 0 : kDimension);
			for (size_t i = 0; !empty && i < kDimension; ++i) {
				ASSERT_TRUE(reindexer::fp::EqualWithinULPs(fv[i], fvQ[i]));
			}
		}
	};

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

	const auto emptyIds = InitNsAndIndexes<Metric>(
		api, kNsName, kHNSWInitSize,
		hnswlib::QuantizationConfig{
			.quantile = kQuantile, .sampleSize = kHNSWInitSize / 2, .quantizationThreshold = kHNSWInitSize - kInitEmptyItemsNum});

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

		const auto empty = emptyIds.contains(item["id"].template As<int>());
		ASSERT_EQ(fvQ.Type(), empty ? YAML::NodeType::Undefined : YAML::NodeType::Sequence);
		ASSERT_EQ(fv.size(), empty ? 0 : kDimension);

		int i = 0;
		for (const auto& val : fvQ) {
			ASSERT_TRUE(reindexer::fp::EqualWithinULPs(fv[i], val.as<float>()))
				<< fmt::format("i = {}; fv[i] = {};  fvQ[i] = {}", i, fv[i], val.as<float>()) << std::endl;
			++i;
		}
		ASSERT_EQ(i, empty ? 0 : kDimension);
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

	const auto emptyIds = InitNsAndIndexes<Metric>(
		api, kNsName, kHNSWInitSize,
		hnswlib::QuantizationConfig{
			.quantile = kQuantile, .sampleSize = kHNSWInitSize / 2, .quantizationThreshold = kHNSWInitSize + 1 - kInitEmptyItemsNum});

	// The check only makes sense if the kInitEmptyItemsNum has been reached.
	if (emptyIds.size() == kInitEmptyItemsNum) {
		ASSERT_FALSE(GetQuantizationStatus(api, kNsName));
		auto item = newItemWithVectors(api, kNsName, kHNSWInitSize);
		api.Upsert(kNsName, item);
	}

	WaitQuantization(api, kNsName);

	const auto queryPoints = MakeQueryPoints();

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
		auto err = tx.Upsert(newTxItemWithVectors(tx, kHNSWInitSize + i));
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
	const size_t qThreshold = 1.5 * (kHNSWInitSize - kInitEmptyItemsNum);
	const auto kMaxSize = kHNSWMaxSize + kHNSWInitSize;	 // for initiate resizeIndex call

	const size_t K = 20;

	std::ignore = InitNsAndIndexes<Metric, MultithreadingMode::MultithreadTransactions>(
		api, kNsName, kHNSWInitSize,
		hnswlib::QuantizationConfig{.quantile = kQuantile, .sampleSize = kHNSWInitSize, .quantizationThreshold = qThreshold});

	std::atomic_uint32_t inserted = kHNSWInitSize;

	auto insertTh = std::thread([&] {
		constexpr size_t kEmptyNewItemsLim = 0.2 * kHNSWMaxSize;
		size_t emptyCnt = 0;
		for (size_t id = kHNSWInitSize; id < kMaxSize; ++id) {
			const bool empty = rand() % 10 == 0 && emptyCnt++ < kEmptyNewItemsLim;
			auto item = empty ? newItemWithoutVectors(api, kNsName, id) : newItemWithVectors(api, kNsName, id);
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

#if RX_WITH_STDLIB_DEBUG
TEST_F(QuantizationApi, IndexQuantizingConcurrentTest_RND) {
	auto metric = reindexer_tests_tools::randMetric();
	TestCout() << fmt::format("Running test for '{}'-metric", reindexer::VectorMetricToStr(metric)) << std::endl;
	switch (metric) {
		case reindexer::VectorMetric::Cosine:
			IndexQuantizingConcurrentTestBody<reindexer::VectorMetric::Cosine>(api);
			break;
		case reindexer::VectorMetric::L2:
			IndexQuantizingConcurrentTestBody<reindexer::VectorMetric::L2>(api);
			break;
		case reindexer::VectorMetric::InnerProduct:
			IndexQuantizingConcurrentTestBody<reindexer::VectorMetric::InnerProduct>(api);
			break;
		default:
			std::abort();
	}
}
#else	// !RX_WITH_STDLIB_DEBUG
TEST_F(QuantizationApi, IndexQuantizingConcurrentTest_L2) { IndexQuantizingConcurrentTestBody<reindexer::VectorMetric::L2>(api); }
TEST_F(QuantizationApi, IndexQuantizingConcurrentTest_IP) { IndexQuantizingConcurrentTestBody<reindexer::VectorMetric::InnerProduct>(api); }
TEST_F(QuantizationApi, IndexQuantizingConcurrentTest_Cosine) { IndexQuantizingConcurrentTestBody<reindexer::VectorMetric::Cosine>(api); }
#endif	// RX_WITH_STDLIB_DEBUG

template <reindexer::VectorMetric Metric>
void SearchWithRadiusTestBody(auto& api) {
	constexpr static auto kNsName = "hnsw_quantization_search_with_radius_test_ns";

	const size_t K = 100;
	const size_t ef = 1.2 * K;

	std::ignore = InitNsAndIndexes<Metric>(
		api, kNsName, kHNSWMaxSize,
		hnswlib::QuantizationConfig{
			.quantile = kQuantile, .sampleSize = kHNSWInitSize, .quantizationThreshold = kHNSWMaxSize - kInitEmptyItemsNum});

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

#if RX_WITH_STDLIB_DEBUG
TEST_F(QuantizationApi, SearchWithRadiusTest_RND) {
	auto metric = reindexer_tests_tools::randMetric();
	TestCout() << fmt::format("Running test for '{}'-metric", VectorMetricToStr(metric)) << std::endl;
	switch (metric) {
		case reindexer::VectorMetric::Cosine:
			SearchWithRadiusTestBody<reindexer::VectorMetric::Cosine>(api);
			break;
		case reindexer::VectorMetric::L2:
			SearchWithRadiusTestBody<reindexer::VectorMetric::L2>(api);
			break;
		case reindexer::VectorMetric::InnerProduct:
			SearchWithRadiusTestBody<reindexer::VectorMetric::InnerProduct>(api);
			break;
		default:
			std::abort();
	}
}
#else	// !RX_WITH_STDLIB_DEBUG
TEST_F(QuantizationApi, SearchWithRadiusTest_L2) { SearchWithRadiusTestBody<reindexer::VectorMetric::L2>(api); }
TEST_F(QuantizationApi, SearchWithRadiusTest_IP) { SearchWithRadiusTestBody<reindexer::VectorMetric::InnerProduct>(api); }
TEST_F(QuantizationApi, SearchWithRadiusTest_Cosine) { SearchWithRadiusTestBody<reindexer::VectorMetric::Cosine>(api); }
#endif	// RX_WITH_STDLIB_DEBUG

}  // namespace sq8_test

}  // namespace reindexer_tests
