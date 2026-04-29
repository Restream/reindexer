#pragma once

#include <gtest/gtest.h>
#include <unordered_set>
#include "core/cjson/jsonbuilder.h"
#include "core/index/float_vector/hnswlib/hnswlib.h"
#include "core/indexdef.h"
#include "core/query/query.h"
#include "gtests/tests/gtest_cout.h"
#include "yaml-cpp/yaml.h"

namespace sq8_test {

#if defined(REINDEX_WITH_TSAN) || defined(REINDEX_WITH_ASAN) || defined(_GLIBCXX_DEBUG) || defined(RX_WITH_STDLIB_DEBUG)
static constexpr inline bool kIsRelease = false;
#else
static constexpr inline bool kIsRelease = true;
#endif

constexpr static inline std::optional<float> kQuantile = kIsRelease ? std::nullopt : std::optional(0.995f);

constexpr static inline size_t kDimension = kIsRelease ? 768 : 20;
constexpr static inline size_t kHNSWInitSize = kIsRelease ? 1200 : 500;
constexpr static inline size_t kHNSWMaxSize = 3 * kHNSWInitSize;
constexpr static inline size_t kEfConstruction = 200;

constexpr static inline size_t kM = 16;

constexpr static inline auto kHnswIndexName = "hnsw_index";
constexpr static inline auto kHnswIndexNameQ = "quantized_hnsw_index";
constexpr static inline auto kBfIndexName = "bf_index";

constexpr static inline size_t kInitEmptyItemsNum = 0.2 * kHNSWInitSize;

static inline std::vector<float> MakePoint(float multiplier = 1.f) {
	static thread_local std::random_device rd;
	static thread_local std::mt19937 gen(rd());
	static thread_local std::normal_distribution<> nd(0, 0.25);

	std::vector<float> point(kDimension);
	for (size_t i = 0; i < kDimension; ++i) {
		point[i] = multiplier * nd(gen);
	}
	return point;
}

static inline std::vector<std::vector<float>> MakeQueryPoints() {
	constexpr static int size = 100;

	std::vector<std::vector<float>> res;
	res.reserve(size);
	for (int i = 0; i < size; ++i) {
		res.emplace_back(MakePoint());
	}
	return res;
}

void CheckRecallRate(auto& api, std::string_view nsName, const auto& queryPoints, int k, int ef, float expectedRecall = 0.8f,
					 float expectedRecallDiff = kIsRelease ? 0.05 : 0.1, std::string_view hnswIndexName = kHnswIndexName,
					 std::string_view hnswIndexNameQ = kHnswIndexNameQ, std::string_view bfIndexName = kBfIndexName) {
	const int cnt = queryPoints.size();
	float recall = 0;
	float recallQ = 0;

	auto calcRecall = [](auto& lhs, auto& rhs) {
		float res = 0;
		for (auto id : rhs) {
			res += lhs.contains(id);
		}
		res /= lhs.size();

		return res;
	};

	std::unordered_set<int> resBf, resHnsw, resHnswQ;
	for (int i = 0; i < cnt; ++i) {
		resBf.clear();
		resHnsw.clear();
		resHnswQ.clear();

		auto qrBf = api.Select(reindexer::Query(nsName).WhereKNN(bfIndexName, reindexer::ConstFloatVectorView{queryPoints[i]},
																 reindexer::BruteForceSearchParams{}.K(k)));
		auto qrHnsw = api.Select(reindexer::Query(nsName).WhereKNN(hnswIndexName, reindexer::ConstFloatVectorView{queryPoints[i]},
																   reindexer::HnswSearchParams{}.K(k).Ef(ef)));
		auto qrHnswQ = api.Select(reindexer::Query(nsName).WhereKNN(hnswIndexNameQ, reindexer::ConstFloatVectorView{queryPoints[i]},
																	reindexer::HnswSearchParams{}.K(k).Ef(ef)));

		EXPECT_EQ(qrBf.Count(), qrHnsw.Count());
		EXPECT_EQ(qrBf.Count(), qrHnswQ.Count());

		auto itBf = qrBf.begin();
		auto itHnsw = qrHnsw.begin();
		auto itHnswQ = qrHnswQ.begin();

		while (itBf != qrBf.end()) {
			auto itemBf = YAML::Load(std::string{(*itBf).GetItem().GetJSON()});
			auto itemHnsw = YAML::Load(std::string{(*itHnsw).GetItem().GetJSON()});
			auto itemHnswQ = YAML::Load(std::string{(*itHnswQ).GetItem().GetJSON()});

			resBf.emplace(itemBf["id"].template as<int>());
			resHnsw.emplace(itemHnsw["id"].template as<int>());
			resHnswQ.emplace(itemHnswQ["id"].template as<int>());

			++itBf;
			++itHnsw;
			++itHnswQ;
		}

		EXPECT_EQ(itHnsw, qrHnsw.end());
		EXPECT_EQ(itHnswQ, qrHnswQ.end());

		recall += calcRecall(resBf, resHnsw) / cnt;
		recallQ += calcRecall(resBf, resHnswQ) / cnt;
	}

	TestCout() << fmt::format("recall = {}; recallQ = {}\n", recall, recallQ);
	EXPECT_GE(recall, expectedRecall);
	EXPECT_GE(recallQ, expectedRecall);

	EXPECT_NEAR(recall, recallQ, expectedRecallDiff);
}

inline reindexer::WrSerializer newItemWithVectorsJson(int id) {
	reindexer::WrSerializer ser;
	{
		reindexer::JsonBuilder json(ser);
		json.Put("id", id);

		const auto point = MakePoint();
		json.Array(kHnswIndexName, std::span{point});
		json.Array(kHnswIndexNameQ, std::span{point});
		json.Array(kBfIndexName, std::span{point});
	}
	return ser;
}

inline reindexer::WrSerializer newItemWithoutVectors(int id) {
	reindexer::WrSerializer ser;
	{
		reindexer::JsonBuilder json(ser);
		json.Put("id", id);
	}
	return ser;
}

auto deserializeItem(auto item, int id, bool empty) {
	auto ser = empty ? newItemWithoutVectors(id) : newItemWithVectorsJson(id);
	const auto err = item.FromJSON(ser.Slice());
	if (!err.ok()) {
		throw err;
	}
	return item;
}

auto newItemWithoutVectors(auto& api, std::string_view nsName, int id) { return deserializeItem(api.NewItem(nsName), id, true); }
auto newItemWithVectors(auto& api, std::string_view nsName, int id) { return deserializeItem(api.NewItem(nsName), id, false); }

auto newTxItemWithVectors(auto& tx, int id) {
	auto item = tx.NewItem();
	if (!item.Status().ok()) {
		throw item.Status();
	}
	return deserializeItem(std::move(item), id, false);
}

template <reindexer::VectorMetric Metric, MultithreadingMode mode = MultithreadingMode::SingleThread>
[[nodiscard]] auto InitNsAndIndexes(auto& api, std::string_view nsName, size_t size, hnswlib::QuantizationConfig quantizationConfig) {
	auto fvBfOpts = FloatVectorIndexOpts{}.SetDimension(kDimension).SetStartSize(kHNSWMaxSize).SetMetric(Metric).SetMultithreading(mode);
	auto fvHnswOpts = FloatVectorIndexOpts{fvBfOpts}.SetM(kM).SetEfConstruction(kEfConstruction);

	api.OpenNamespace(nsName);

	api.AddIndex(nsName, reindexer::IndexDef{"id", {"id"}, IndexIntHash, IndexOpts{}.PK()});
	api.AddIndex(nsName,
				 reindexer::IndexDef{kHnswIndexName, {kHnswIndexName}, IndexHnsw, IndexOpts{}.SetFloatVector(IndexHnsw, fvHnswOpts)});
	api.AddIndex(nsName, reindexer::IndexDef{
							 kHnswIndexNameQ,
							 {kHnswIndexNameQ},
							 IndexHnsw,
							 IndexOpts{}.SetFloatVector(IndexHnsw, fvHnswOpts.SetQuantizationConfig(std::move(quantizationConfig)))});
	api.AddIndex(nsName,
				 reindexer::IndexDef{
					 kBfIndexName, {kBfIndexName}, IndexVectorBruteforce, IndexOpts{}.SetFloatVector(IndexVectorBruteforce, fvBfOpts)});

	std::unordered_set<size_t> emptyIds;
	for (size_t i = 0; i < size; ++i) {
		const bool empty = rand() % 10 == 0 && emptyIds.size() < kInitEmptyItemsNum;
		if (empty) {
			emptyIds.insert(i);
		}
		auto item = empty ? newItemWithoutVectors(api, nsName, i) : newItemWithVectors(api, nsName, i);
		api.Upsert(nsName, item);
	}
	return emptyIds;
}

bool GetQuantizationStatus(auto& api, std::string_view nsName, std::string_view indexName = kHnswIndexNameQ) {
	auto qr = api.Select(reindexer::Query("#memstats").Where("name", CondEq, nsName));
	EXPECT_EQ(qr.Count(), 1);
	auto item = YAML::Load(std::string{(*qr.begin()).GetItem().GetJSON()});

	const auto indexes = item["indexes"];
	for (auto& index : indexes) {
		if (index["name"].as<std::string>() == indexName) {
			return index["is_quantized"].as<bool>();
		}
	}

	EXPECT_TRUE(false) << fmt::format("info about index {} not found in #memstats", indexName);
	return false;
}

void waitQuantizationStatus(auto& api, std::string_view nsName, bool isWaitQuantization, std::string_view indexName = kHnswIndexNameQ) {
	auto now = std::chrono::milliseconds(0);
	const auto pause = std::chrono::milliseconds(50);
	constexpr auto kMaxSyncTime = std::chrono::seconds(10);
	while (GetQuantizationStatus(api, nsName, indexName) != isWaitQuantization) {
		now += pause;
		ASSERT_TRUE(now < kMaxSyncTime) << fmt::format("Wait {}quantization is too long", isWaitQuantization ? "" : "de");
		std::this_thread::sleep_for(pause);
	}
}
void WaitQuantization(auto& api, std::string_view nsName, std::string_view indexName = kHnswIndexNameQ) {
	waitQuantizationStatus(api, nsName, true, indexName);
}
void WaitDequantization(auto& api, std::string_view nsName, std::string_view indexName = kHnswIndexNameQ) {
	waitQuantizationStatus(api, nsName, false, indexName);
}

enum class [[nodiscard]] TestSyncType { Online, WAL, Force };

}  // namespace sq8_test
