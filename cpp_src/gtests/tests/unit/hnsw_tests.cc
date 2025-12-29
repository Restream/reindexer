#include "gtest/gtest.h"
#include "gtests/tests/gtest_cout.h"

#include "core/index/float_vector/hnswlib/hnswlib.h"
#include "tools/serializer.h"

using namespace reindexer;

#ifdef REINDEX_WITH_TSAN
static constexpr bool kWithTSAN = true;
#else
static constexpr bool kWithTSAN = false;
#endif

constexpr static size_t kDimension = kWithTSAN ? 10 : 100;
constexpr static size_t kHNSWInitSize = kWithTSAN ? 500 : 1000;
constexpr static size_t kHNSWMaxSize = 3 * kHNSWInitSize;
constexpr static size_t kEfConstruction = 200;

constexpr static size_t kM = 16;
constexpr static size_t kHnswRandomSeed = 100;

static float randFloat() {
#if 0
	static std::random_device rd;
	static std::mt19937 gen(rd());
	static std::normal_distribution<> nd(0, 0.25);
	return nd(gen);
#else
	return (rand() % 2 ? -1.f : 1.f) * (rand() % 10'000) / 10'000;
#endif
}

std::vector<float> makePoint(float multiplier = 1.f) {
	std::vector<float> point(kDimension);
	for (size_t i = 0; i < kDimension; ++i) {
		point[i] = multiplier * randFloat();
	}
	return point;
}

static auto MakeEmptyMap(hnswlib::SpaceInterface* space) {
	return std::make_unique<hnswlib::HierarchicalNSWST>(space, kHNSWMaxSize, kM, kEfConstruction, kHnswRandomSeed, ReplaceDeleted_True);
}

template <typename MetricT>
static auto CreateHNSWGraph() {
	auto map = MakeEmptyMap(std::make_unique<MetricT>(kDimension).get());

	std::vector<std::vector<float>> points(kHNSWInitSize);

	for (size_t i = 0; i < kHNSWInitSize; ++i) {
		points[i] = makePoint();
		map->template addPoint<hnswlib::DummyLocker>(points[i].data(), i);
	}

	return std::make_pair(std::move(map), std::move(points));
}

template <typename MetricT>
class HNSWTestGraphSingleton {
public:
	static auto Map() noexcept {
		return std::make_unique<hnswlib::HierarchicalNSWST>(instance().space_.get(), *instance().map_, instance().map_->max_elements_);
	}
	static auto& Points() noexcept { return instance().points_; }

private:
	static const HNSWTestGraphSingleton& instance() noexcept {
		static HNSWTestGraphSingleton instance;
		return instance;
	}

	HNSWTestGraphSingleton() { std::tie(map_, points_) = CreateHNSWGraph<MetricT>(); }
	std::unique_ptr<hnswlib::HierarchicalNSWST> map_;
	std::vector<std::vector<float>> points_;
	std::unique_ptr<MetricT> space_ = std::make_unique<MetricT>(kDimension);
};

template <typename MetricT>
void DataSamplerBaseTestBody() {
	const auto& map = HNSWTestGraphSingleton<MetricT>::Map();
	const auto& points = HNSWTestGraphSingleton<MetricT>::Points();
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

TEST(HNSW, DataSamplerTest_L2) { DataSamplerBaseTestBody<hnswlib::L2Space>(); }
TEST(HNSW, DataSamplerTest_IP) { DataSamplerBaseTestBody<hnswlib::InnerProductSpace>(); }
TEST(HNSW, DataSamplerTest_Cosine) { DataSamplerBaseTestBody<hnswlib::CosineSpace>(); }

static float SimilarityQuantizationMetric(auto&& origRes, auto& quantizedRes) {
	const int size = std::min(quantizedRes.size(), origRes.size());

	EXPECT_GT(size, 0);

	std::vector<hnswlib::tableint> orig;
	std::unordered_map<hnswlib::tableint, int> quantizedPositions;
	orig.reserve(origRes.size());
	quantizedPositions.reserve(origRes.size());

	int cnt = 0;
	while (!origRes.empty() && !quantizedRes.empty()) {
		orig.emplace_back(origRes.top().second);
		quantizedPositions[quantizedRes.top().second] = cnt++;

		origRes.pop();
		quantizedRes.pop();
	}

	auto qMultiplier = [&](int i) {
		if (!quantizedPositions.contains(orig[i])) {
			return 0;
		} else if (std::abs(i - quantizedPositions[orig[i]]) <= 1) {
			return size;
		} else {
			return size - std::abs(i - quantizedPositions[orig[i]]);
		}
	};

	float res = 0;
	for (int i = 0; i < size; ++i) {
		res += (size - i) * qMultiplier(i);
	}
	res /= size;

	float norm = size * (size + 1) / 2.f;
	res /= norm;

	return res;
}
template <typename MetricT>
static std::string MetricName() {
	if constexpr (std::is_same_v<MetricT, hnswlib::L2Space>) {
		return "L2";
	}
	if constexpr (std::is_same_v<MetricT, hnswlib::InnerProductSpace>) {
		return "InnerProduct";
	}
	if constexpr (std::is_same_v<MetricT, hnswlib::CosineSpace>) {
		return "Cosine";
	}
}

template <typename MetricT>
void BaseQuantizeHNSWTestBody(const float quantile) {
	const auto map = HNSWTestGraphSingleton<MetricT>::Map();
	// graph without requantizing
	const auto mapSq8 = HNSWTestGraphSingleton<MetricT>::Map();
	// graph with requantizing
	const auto mapSq8R = HNSWTestGraphSingleton<MetricT>::Map();
	// graph with requantizing with nonlinear correction in metric calclulation
	const auto mapSq8RCorr = HNSWTestGraphSingleton<MetricT>::Map();

	const size_t sampleSize = 0.25 * map->cur_element_count;
	auto sq8Config = hnswlib::QuantizingConfig{
		.quantile = quantile, .sampleSize = sampleSize, .nonLinearCorrection = hnswlib::Sq8NonLinearCorrection::Disabled};

	mapSq8->template Quantize<hnswlib::DummyLocker>(sq8Config);
	mapSq8R->template Quantize<hnswlib::DummyLocker>(sq8Config);
	sq8Config.nonLinearCorrection = hnswlib::Sq8NonLinearCorrection::Enabled;
	mapSq8RCorr->template Quantize<hnswlib::DummyLocker>(sq8Config);

	auto countOutliers = [](const auto& point) {
		return std::count_if(point.begin(), point.end(), [](auto v) { return std::abs(v) > 1.f; });
	};

	const auto addPointInMaps = [&](int label, float multiplier = 1.f, size_t* outliersCounter = nullptr) {
		auto point = makePoint(multiplier);
		if (outliersCounter) {
			*outliersCounter += countOutliers(point);
		}
		map->template addPoint<hnswlib::DummyLocker>(point.data(), label);
		mapSq8->template addPoint<hnswlib::DummyLocker>(point.data(), label);
		mapSq8R->template addPoint<hnswlib::DummyLocker>(point.data(), label);
		mapSq8RCorr->template addPoint<hnswlib::DummyLocker>(point.data(), label);
	};

	for (int i = kHNSWInitSize; i < int(2 * kHNSWInitSize); ++i) {
		// 0.95 - to avoid extra outliers
		addPointInMaps(i, 0.95 * quantile);
	};

	EXPECT_TRUE(!mapSq8R->quantizer_->NeedRequantize());
	EXPECT_TRUE(!mapSq8RCorr->quantizer_->NeedRequantize());

	const float kCompMultiplier = [&quantile]() {
		if (reindexer::fp::EqualWithinULPs(quantile, 1.f)) {
			return 1.05f;
		} else if (reindexer::fp::EqualWithinULPs(quantile, 0.99f)) {
			return 1.05f;
		} else if (reindexer::fp::EqualWithinULPs(quantile, 0.95f)) {
			return 1.2f;
		}
		return 1.f;
	}();

	enum class RequantizeState { Before, After };
	auto test = [&](RequantizeState requantized) {
		float avgSq8Metric = 0;
		float avgSq8MetricR = 0;
		[[maybe_unused]] float avgSq8MetricRCorr = 0;

		const int count = 100;
		const int KnnN = 0.05 * map->cur_element_count;
		const int ef = 2 * KnnN;
		for (int i = 0; i < count; ++i) {
			auto queryPoint = makePoint(requantized == RequantizeState::Before ? 1.f : kCompMultiplier);

			auto res = map->searchKnn(queryPoint.data(), KnnN, ef);
			auto resSq = mapSq8->searchKnn(queryPoint.data(), KnnN, ef);
			auto resSqR = mapSq8R->searchKnn(queryPoint.data(), KnnN, ef);
			auto resSqRCorr = mapSq8RCorr->searchKnn(queryPoint.data(), KnnN, ef);

			avgSq8Metric += SimilarityQuantizationMetric(decltype(res){res}, resSq) / count;
			avgSq8MetricR += SimilarityQuantizationMetric(decltype(res){res}, resSqR) / count;
			avgSq8MetricRCorr += SimilarityQuantizationMetric(res, resSqRCorr) / count;
		}

		auto yellowB = requantized == RequantizeState::After ? "\e[0;33m" : "";
		auto yellowE = requantized == RequantizeState::After ? "\e[0m" : "";
		TestCout() << fmt::format("{}-metric; Search with ef = {}; metric = {}; metricR = {}{}{}; metricRCorr = {}{}{};\n",
								  MetricName<MetricT>(), ef, avgSq8Metric, yellowB, avgSq8MetricR, yellowE, yellowB, avgSq8MetricRCorr,
								  yellowE);

		switch (requantized) {
			case RequantizeState::Before: {
				EXPECT_GT(avgSq8Metric, 0.88);
				EXPECT_GT(avgSq8MetricR, 0.85);

				if constexpr (!kWithTSAN) {
					EXPECT_GT(avgSq8MetricRCorr, 0.88);
				}
				break;
			}
			case RequantizeState::After: {
				EXPECT_GT(avgSq8MetricR, quantile > 0.95f ? 0.9 : 0.8);

				if constexpr (!kWithTSAN) {
					EXPECT_GT(avgSq8MetricRCorr, quantile > 0.95f ? 0.9 : 0.8);
				}

				// not often, but sporadically failed for good values of avgSq8Metric
				// EXPECT_TRUE(avgSq8MetricR > avgSq8Metric);
				// EXPECT_TRUE(avgSq8MetricRCorr > avgSq8Metric);
				if (avgSq8Metric < 0.9) {
					EXPECT_TRUE(avgSq8MetricR > avgSq8Metric);
					if constexpr (!kWithTSAN) {
						EXPECT_TRUE(avgSq8MetricRCorr > avgSq8Metric);
					}
				}

				// TODO It is necessary to compare avgSq8MetricRCorr and avgSq8MetricR

				break;
			}
			default:
				ASSERT_TRUE(false);
		}
	};

	test(RequantizeState::Before);

	size_t outliersCounter = 0;

	int counter = 2 * kHNSWInitSize;
	while (counter < int(kHNSWMaxSize) && (!mapSq8R->quantizer_->NeedRequantize() || !mapSq8RCorr->quantizer_->NeedRequantize())) {
		addPointInMaps(counter++, kCompMultiplier, &outliersCounter);
	}

	auto outliersPct = [&map, curHNSWSize = counter - kHNSWInitSize](size_t outliers) {
		return float(outliers) / (curHNSWSize * map->fstdistfunc_.Dims());
	};

	ASSERT_NEAR(reindexer::fp::EqualWithinULPs(quantile, 1.f) ? 0.01f : (1.f - quantile), outliersPct(outliersCounter),
				kWithTSAN ? 0.007f : 0.005f);
	TestCout() << fmt::format("Inserted after quantizing - {}, outliersPct = {}\n", counter - 2 * kHNSWInitSize,
							  outliersPct(outliersCounter));
	EXPECT_GT(counter, 0.1f * (2 * kHNSWInitSize))
		<< fmt::format("Inserted after quantizing - {}, outliersPct = {}\n", counter - 2 * kHNSWInitSize, outliersPct(outliersCounter));

	EXPECT_TRUE(mapSq8R->quantizer_->NeedRequantize());
	EXPECT_TRUE(mapSq8RCorr->quantizer_->NeedRequantize());

	mapSq8R->template Requantize<hnswlib::DummyLocker>();
	mapSq8RCorr->template Requantize<hnswlib::DummyLocker>();

	while (counter < int(kHNSWMaxSize)) {
		addPointInMaps(counter++, kCompMultiplier);
	}

	test(RequantizeState::After);
}

class HNSW_P : public ::testing::TestWithParam<float> {};

TEST_P(HNSW_P, BaseQuantizeTest_L2) { BaseQuantizeHNSWTestBody<hnswlib::L2Space>(GetParam()); }
TEST_P(HNSW_P, BaseQuantizeTest_IP) { BaseQuantizeHNSWTestBody<hnswlib::InnerProductSpace>(GetParam()); }
TEST_P(HNSW_P, BaseQuantizeTest_Cosine) { BaseQuantizeHNSWTestBody<hnswlib::CosineSpace>(GetParam()); }

INSTANTIATE_TEST_SUITE_P(, HNSW_P, ::testing::Values(1.f, 0.99f, 0.95f), [](const auto& info) {
	if (reindexer::fp::EqualWithinULPs(info.param, 1.f)) {
		return "1_quantile";
	} else if (reindexer::fp::EqualWithinULPs(info.param, 0.99f)) {
		return "99_quantile";
	} else if (reindexer::fp::EqualWithinULPs(info.param, 0.95f)) {
		return "95_quantile";
	} else {
		assert(false);
		std::abort();
	}
});

static std::vector<std::vector<float>> MakeQueryPoints() {
	constexpr static int size = 100;

	std::vector<std::vector<float>> res;
	res.reserve(size);
	for (int i = 0; i < size; ++i) {
		res.emplace_back(makePoint());
	}
	return res;
}

static void CompareKnnResults(auto& lhs, auto& rhs, const auto& queryPoints, float expectedMetric) {
	const int KnnN = 0.05 * lhs->cur_element_count;
	const int ef = 2 * KnnN;
	const int cnt = queryPoints.size();
	float metric = 0;

	for (int i = 0; i < cnt; ++i) {
		auto lhsRes = lhs->searchKnn(queryPoints[i].data(), KnnN, ef);
		auto rhsRes = rhs->searchKnn(queryPoints[i].data(), KnnN, ef);

		metric += SimilarityQuantizationMetric(lhsRes, rhsRes) / cnt;
	}

	TestCout() << fmt::format("metric = {};\n", metric);
	ASSERT_GE(metric, expectedMetric);
}

class [[nodiscard]] TestWriter final : public hnswlib::IWriter {
public:
	explicit TestWriter(WrSerializer& ser) noexcept : ser_{ser} {}

	void PutVarUInt(uint32_t v) override { ser_.PutVarUint(v); }
	void PutVarUInt(uint64_t v) override { ser_.PutVarUint(v); }
	void PutVarInt(int64_t v) override { ser_.PutVarint(v); }
	void PutVarInt(int32_t v) override { ser_.PutVarint(v); }
	void PutFloat(float v) override { ser_.PutFloat(v); }
	void PutVString(std::string_view slice) override { ser_.PutVString(slice); }
	void AppendPKByID(hnswlib::labeltype label) override { ser_.PutVariant(Variant{IdType(label)}); }

private:
	WrSerializer& ser_;
};

template <typename MetricT>
class [[nodiscard]] TestReader final : public hnswlib::IReader {
public:
	explicit TestReader(std::string_view data) noexcept : ser_{data} {}

	uint64_t GetVarUInt() override { return ser_.GetVarUInt(); }
	int64_t GetVarInt() override { return ser_.GetVarint(); }
	float GetFloat() override { return ser_.GetFloat(); }
	std::string_view GetVString() override { return ser_.GetVString(); }

	uint8_t Version() override { return 2; }

	hnswlib::labeltype ReadPkEncodedData(char* dest) override {
		int id{ser_.GetVariant()};
		std::memcpy(dest, HNSWTestGraphSingleton<MetricT>::Points()[id].data(), kDimension * sizeof(float));
		return id;
	}

private:
	Serializer ser_;
};

template <typename MetricT>
void SaveLoadTestBody() {
	const static auto space = std::make_unique<MetricT>(kDimension);
	const auto origMap = HNSWTestGraphSingleton<MetricT>::Map();
	const auto mapSq8 = HNSWTestGraphSingleton<MetricT>::Map();
	const static std::atomic_int32_t cancel = 0;

	auto queryPoints = MakeQueryPoints();
	auto test = [&queryPoints](auto& map) {
		WrSerializer wser;
		TestWriter writer(wser);
		map->saveIndex(writer, cancel);

		TestReader<MetricT> reader(wser.Slice());
		auto loadedMap = MakeEmptyMap(space.get());
		loadedMap->loadIndex(reader, space.get());

		CompareKnnResults(map, loadedMap, queryPoints, 0.99f);
		return loadedMap;
	};

	hnswlib::QuantizingConfig sq8Config;
	sq8Config.sampleSize = 0.25 * origMap->cur_element_count;
	mapSq8->template Quantize<hnswlib::DummyLocker>(sq8Config);

	TestCout() << "Check save/load orig HSNW graph" << std::endl;
	std::ignore = test(origMap);
	TestCout() << "Check save/load quantized HSNW-graph" << std::endl;
	auto mapSq8reload = test(mapSq8);
	TestCout() << "Compare origin and reloaded quantized HSNW-graph" << std::endl;
	CompareKnnResults(origMap, mapSq8reload, queryPoints, kWithTSAN ? 0.9f : 0.95f);
}

TEST(HNSW, SaveLoadTest_L2) { SaveLoadTestBody<hnswlib::L2Space>(); }
TEST(HNSW, SaveLoadTest_IP) { SaveLoadTestBody<hnswlib::InnerProductSpace>(); }
TEST(HNSW, SaveLoadTest_Cosine) { SaveLoadTestBody<hnswlib::CosineSpace>(); }

template <typename MetricT>
void QuantizingPreparedAdvanceGraphTestBody() {
	const static auto space = std::make_unique<MetricT>(kDimension);
	const auto map = HNSWTestGraphSingleton<MetricT>::Map();
	const auto mapSq8 = HNSWTestGraphSingleton<MetricT>::Map();

	auto queryPoints = MakeQueryPoints();

	hnswlib::QuantizingConfig sq8Config;
	sq8Config.sampleSize = 0.25 * map->cur_element_count;
	mapSq8->template Quantize<hnswlib::DummyLocker>(sq8Config);

	mapSq8->visited_list_pool_.reset(new hnswlib::VisitedListPool(1, mapSq8->max_elements_));

	TestCout() << "Comparison orig and inplace quantizing HSNW graph" << std::endl;
	CompareKnnResults(map, mapSq8, queryPoints, kWithTSAN ? 0.9f : 0.95f);

	TestCout() << "Comparison orig and prepared in advance for quantization HSNW graph" << std::endl;
	const auto preparedSq8Map = map->template CopyForQuantize<hnswlib::DummyLocker>();
	preparedSq8Map->template Quantize<hnswlib::DummyLocker>(*map, sq8Config);

	CompareKnnResults(map, preparedSq8Map, queryPoints, kWithTSAN ? 0.9f : 0.95f);

	TestCout() << "Comparison inplace quantized and prepared in advance for quantization HSNW graphs" << std::endl;
	CompareKnnResults(mapSq8, preparedSq8Map, queryPoints, kWithTSAN ? 0.9f : 0.95f);
}

TEST(HNSW, QuantizingPreparedAdvanceGraphTest_L2) { QuantizingPreparedAdvanceGraphTestBody<hnswlib::L2Space>(); }
TEST(HNSW, QuantizingPreparedAdvanceGraphTest_IP) { QuantizingPreparedAdvanceGraphTestBody<hnswlib::InnerProductSpace>(); }
TEST(HNSW, QuantizingPreparedAdvanceGraphTest_Cosine) { QuantizingPreparedAdvanceGraphTestBody<hnswlib::CosineSpace>(); }

template <typename MetricT>
void BruteForceCheckTestBody() {
	const auto hnsw = HNSWTestGraphSingleton<MetricT>::Map();
	const auto bf = std::make_unique<hnswlib::BruteforceSearch>(std::make_unique<MetricT>(kDimension).get(), kHNSWMaxSize);

	hnswlib::labeltype label = 0;
	for (const auto& point : HNSWTestGraphSingleton<MetricT>::Points()) {
		bf->addPoint(point.data(), label++);
	}

	CompareKnnResults(hnsw, bf, MakeQueryPoints(), kWithTSAN ? 0.9f : 0.95f);
}

TEST(HNSW, BruteForceCheckTest_L2) { BruteForceCheckTestBody<hnswlib::L2Space>(); }
TEST(HNSW, BruteForceCheckTest_IP) { BruteForceCheckTestBody<hnswlib::InnerProductSpace>(); }
TEST(HNSW, BruteForceCheckTest_Cosine) { BruteForceCheckTestBody<hnswlib::CosineSpace>(); }

#if 0 
// Test for verifying metrics with nonlinear additive terms for vectors with normally distributed components
template <typename MetricT>
void QuantizeHNSWWithQuantilesTestBody(hnswlib::Sq8NonLinearCorrection corr) {
	const auto map = HNSWTestGraphSingleton<MetricT>::Map();

	const auto mapSq8 = HNSWTestGraphSingleton<MetricT>::Map();
	const auto mapSq8_2sigma = HNSWTestGraphSingleton<MetricT>::Map();
	const auto mapSq8_3sigma = HNSWTestGraphSingleton<MetricT>::Map();

	const int sampleSize = (0.25 * mapSq8->cur_element_count);

	mapSq8->template Quantize<hnswlib::DummyLocker>(1.f, sampleSize, corr);
	mapSq8_2sigma->template Quantize<hnswlib::DummyLocker>(.95f, sampleSize, corr);
	mapSq8_3sigma->template Quantize<hnswlib::DummyLocker>(.99f, sampleSize, corr);

	auto getQueryPoint = [] {
		std::vector<float> res(kDimension);
		for (size_t i = 0; i < kDimension; ++i) {
			res[i] = randFloat();
		}
		return res;
	};

	float avgQuantizationMetric = 0;
	float avgQuantizationMetric_2sigma = 0;
	float avgQuantizationMetric_3sigma = 0;

	const int count = 10;
	const int KnnN = 0.05 * kHNSWSize;
	const int ef = 1.5 * KnnN;
	for (int i = 0; i < count; ++i) {
		auto queryPoint = getQueryPoint();

		auto res = map->searchKnn(queryPoint.data(), KnnN, ef);
		auto resSq = mapSq8->searchKnn(queryPoint.data(), KnnN, ef);
		auto resSq_2sigma = mapSq8_2sigma->searchKnn(queryPoint.data(), KnnN, ef);
		auto resSq_3sigma = mapSq8_3sigma->searchKnn(queryPoint.data(), KnnN, ef);

		avgQuantizationMetric += SimilarityQuantizationMetric(decltype(res){res}, resSq) / count;
		avgQuantizationMetric_2sigma += SimilarityQuantizationMetric(decltype(res){res}, resSq_2sigma) / count;
		avgQuantizationMetric_3sigma += SimilarityQuantizationMetric(res, resSq_3sigma) / count;
	}

	for (auto [quantile, metricValue] : {std::pair{"100", avgQuantizationMetric}, std::pair{"99", avgQuantizationMetric_3sigma},
										 std::pair{"95", avgQuantizationMetric_2sigma}}) {
		TestCout() << fmt::format("{}-metric; Quantile - {}%; metric = {}\n", MetricName<MetricT>(), quantile, metricValue);
	}
}

TEST(HNSW, QuantizeWithQuantilesTest_L2) { QuantizeHNSWWithQuantilesTestBody<hnswlib::L2Space>(hnswlib::Sq8NonLinearCorrection::Disabled); }
TEST(HNSW, QuantizeWithQuantilesTest_IP) {
	QuantizeHNSWWithQuantilesTestBody<hnswlib::InnerProductSpace>(hnswlib::Sq8NonLinearCorrection::Disabled);
}
TEST(HNSW, QuantizeWithQuantilesTest_Cosine) {
	QuantizeHNSWWithQuantilesTestBody<hnswlib::CosineSpace>(hnswlib::Sq8NonLinearCorrection::Disabled);
}

TEST(HNSW, QuantizeWithQuantilesTest_L2_corr) {
	QuantizeHNSWWithQuantilesTestBody<hnswlib::L2Space>(hnswlib::Sq8NonLinearCorrection::Enabled);
}
TEST(HNSW, QuantizeWithQuantilesTest_IP_corr) {
	QuantizeHNSWWithQuantilesTestBody<hnswlib::InnerProductSpace>(hnswlib::Sq8NonLinearCorrection::Enabled);
}
TEST(HNSW, QuantizeWithQuantilesTest_Cosine_corr) {
	QuantizeHNSWWithQuantilesTestBody<hnswlib::CosineSpace>(hnswlib::Sq8NonLinearCorrection::Enabled);
}
#endif
