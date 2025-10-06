#include "gtest/gtest.h"
#include "gtests/tests/gtest_cout.h"

#include "core/enums.h"
#include "core/index/float_vector/hnswlib/hnswlib.h"

using namespace reindexer;
using hnswlib::ScalarQuantizeType;

constexpr static size_t kDimension = 100;
constexpr static size_t kHNSWSize = 1'000;
constexpr static size_t kEfConstruction = 200;

static float randFloat() { return (rand() % 2 ? -1.f : 1.f) * (rand() % 10'000) / 10'000; }
static float randFloat(int from, int size) { return from + float(rand() % (1000 * size)) / 1000; }

static std::vector<float> VectorWithComponentDispersion() {
	std::vector<float> res;
	res.reserve(kDimension);

	const int from = -5'000;
	const int to = 5'000;
	const int step = (to - from) / kDimension;
	for (size_t i = 0; i < kDimension; ++i) {
		res.emplace_back(randFloat(from + i * step, step));
	}

	return res;
}

class HNSWSq8Test : public ::testing::TestWithParam<ScalarQuantizeType> {};

template <typename MetricT>
auto CreateHNSWGraph(size_t size, const bool withComponentDispersion = false) {
	constexpr static size_t kM = 16;
	constexpr static size_t kHnswRandomSeed = 100;

	auto space = std::make_unique<MetricT>(kDimension);
	auto map = std::make_unique<hnswlib::HierarchicalNSWST>(space.get(), size, kM, kEfConstruction, kHnswRandomSeed, ReplaceDeleted_True);

	std::vector<std::vector<float>> points(size);

	for (size_t i = 0; i < size; ++i) {
		if (withComponentDispersion) {
			points[i] = VectorWithComponentDispersion();
		} else {
			points[i].reserve(kDimension);
			for (size_t j = 0; j < kDimension; ++j) {
				points[i].emplace_back(randFloat());
			}
		}
	}

	for (size_t i = 0; i < size; ++i) {
		map->template addPoint<hnswlib::DummyLocker>(points[i].data(), hnswlib::labeltype(i));
	}

	return std::make_pair(std::move(map), std::move(points));
}

template <typename MetricT, bool withComponentDispersion = false>
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

	HNSWTestGraphSingleton() { std::tie(map_, points_) = CreateHNSWGraph<MetricT>(kHNSWSize, withComponentDispersion); }
	std::unique_ptr<hnswlib::HierarchicalNSWST> map_;
	std::vector<std::vector<float>> points_;
	std::unique_ptr<MetricT> space_ = std::make_unique<MetricT>(kDimension);
};

template <typename MetricT>
void DataGettersBaseTestBody(ScalarQuantizeType Sq8type) {
	const auto& map = HNSWTestGraphSingleton<MetricT>::Map();
	const auto& points = HNSWTestGraphSingleton<MetricT>::Points();
	const int partialSamleSize = 0.25 * points.size();

	auto partialIndexes = hnswlib::HNSWView<hnswlib::HierarchicalNSWST>::GetParitalSampleIndexes(partialSamleSize, map->cur_element_count);
	auto samples = hnswlib::HNSWView(Sq8type, *map, partialIndexes);

	const auto dim = map->fstdistfunc_.Dims();
	if (Sq8type == ScalarQuantizeType::Full) {
		int cnt = 0;
		for (const auto& sample : samples) {
			for (const auto& comp : sample) {
				ASSERT_EQ(comp, points[cnt / dim][cnt % dim]) << fmt::format("cnt={}", cnt);
				++cnt;
			}
		}
		ASSERT_EQ(cnt, map->cur_element_count * dim);
	} else if (Sq8type == ScalarQuantizeType::Partial) {
		int cnt = 0;
		for (auto& sample : samples) {
			for (const auto& comp : sample) {
				ASSERT_EQ(comp, points[partialIndexes[cnt / dim]][cnt % dim]);
				++cnt;
			}
		}
		ASSERT_EQ(cnt, partialSamleSize * dim);
	} else if (Sq8type == ScalarQuantizeType::Component) {
		int compIdx = 0;
		for (const auto& sample : samples) {
			int i = 0;
			for (const auto& comp : sample) {
				(void)comp;
				ASSERT_EQ(comp, points[i][compIdx]) << fmt::format("i={}, compIdx = {}", i, compIdx);
				++i;
			}
			ASSERT_EQ(i, map->cur_element_count) << fmt::format("compIdx = {}", compIdx);
			++compIdx;
		}
		ASSERT_EQ(compIdx, dim);
	}
}

TEST_P(HNSWSq8Test, DataGettersTest_L2) { DataGettersBaseTestBody<hnswlib::L2Space>(GetParam()); }
TEST_P(HNSWSq8Test, DataGettersTest_IP) { DataGettersBaseTestBody<hnswlib::InnerProductSpace>(GetParam()); }
TEST_P(HNSWSq8Test, DataGettersTest_Cosine) { DataGettersBaseTestBody<hnswlib::CosineSpace>(GetParam()); }

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

static std::string ScalarQuntizeTypeName(ScalarQuantizeType param) {
	switch (param) {
		case ScalarQuantizeType::Full:
			return "Full";
		case ScalarQuantizeType::Partial:
			return "Partial";
		case ScalarQuantizeType::Component:
			return "Component";
		default:
			assert(false);
			std::abort();
	}
}

template <typename MetricT>
void QuantizeHNSWTestBody(ScalarQuantizeType quantizeType) {
	const auto map = HNSWTestGraphSingleton<MetricT>::Map();
	const auto mapSq8 = HNSWTestGraphSingleton<MetricT>::Map();

	const int partialSamleSize = (0.25 * mapSq8->cur_element_count);

	mapSq8->template Quantize<hnswlib::DummyLocker>(quantizeType, 1.f, partialSamleSize);

	auto getQueryPoint = [] {
		std::vector<float> res(kDimension);
		for (size_t i = 0; i < kDimension; ++i) {
			res[i] = randFloat();
		}
		return res;
	};

	auto test = [&](float efMultiplier) {
		float avgQuantizationMetric = 0;

		const int count = 100;
		const int KnnN = 0.05 * kHNSWSize;
		const int ef = 1.5 * KnnN;
		for (int i = 0; i < count; ++i) {
			auto queryPoint = getQueryPoint();
			auto res = map->searchKnn(queryPoint.data(), KnnN, efMultiplier * ef);
			auto resSq = mapSq8->searchKnn(queryPoint.data(), KnnN, efMultiplier * ef);
			avgQuantizationMetric += SimilarityQuantizationMetric(res, resSq) / count;
		}
		TestCout() << fmt::format("{}-metric; QuntizeType - {}; Search with ef = {} * {}; metric = {}\n", MetricName<MetricT>(),
								  ScalarQuntizeTypeName(quantizeType), efMultiplier, ef, avgQuantizationMetric);

		ASSERT_GT(avgQuantizationMetric, 0.9);
	};

	test(1);
	test(1.3);
	test(1.5);
	test(2);
}

TEST_P(HNSWSq8Test, QuantizeTest_L2) { QuantizeHNSWTestBody<hnswlib::L2Space>(GetParam()); }
TEST_P(HNSWSq8Test, QuantizeTest_IP) { QuantizeHNSWTestBody<hnswlib::InnerProductSpace>(GetParam()); }
TEST_P(HNSWSq8Test, QuantizeTest_Cosine) { QuantizeHNSWTestBody<hnswlib::CosineSpace>(GetParam()); }

template <typename MetricT>
void QuantizeComponentBasedTestBody() {
	const auto map = HNSWTestGraphSingleton<MetricT, true>::Map();
	const auto mapSq8PFull = HNSWTestGraphSingleton<MetricT, true>::Map();
	const auto mapSq8Partial = HNSWTestGraphSingleton<MetricT, true>::Map();
	const auto mapSq8Component = HNSWTestGraphSingleton<MetricT, true>::Map();

	const int partialSamleSize = (0.25 * mapSq8Partial->cur_element_count);

	mapSq8PFull->template Quantize<hnswlib::DummyLocker>(ScalarQuantizeType::Full);
	mapSq8Partial->template Quantize<hnswlib::DummyLocker>(ScalarQuantizeType::Partial, 1.f, partialSamleSize);
	mapSq8Component->template Quantize<hnswlib::DummyLocker>(ScalarQuantizeType::Component);

	float avgFullQntMetric = 0;
	float avgPartialQntMetric = 0;
	float avgComponentQntMetric = 0;

	const int count = 10;
	const int KnnN = 0.05 * kHNSWSize;
	const int ef = 1.5 * KnnN;

	for (int i = 0; i < count; ++i) {
		auto queryPoint = VectorWithComponentDispersion();
		auto res = map->searchKnn(queryPoint.data(), KnnN, ef);
		auto resSqFull = mapSq8PFull->searchKnn(queryPoint.data(), KnnN, ef);
		auto resSqPartial = mapSq8Partial->searchKnn(queryPoint.data(), KnnN, ef);
		auto resSqComp = mapSq8Component->searchKnn(queryPoint.data(), KnnN, ef);

		avgFullQntMetric += SimilarityQuantizationMetric(decltype(res){res}, resSqFull) / count;
		avgPartialQntMetric += SimilarityQuantizationMetric(decltype(res){res}, resSqPartial) / count;
		avgComponentQntMetric += SimilarityQuantizationMetric(res, resSqComp) / count;
	}
	TestCout() << fmt::format("{}-metric; Quantizing metric values: Full - {}; Partial - {}; Component - {}\n", MetricName<MetricT>(),
							  avgFullQntMetric, avgPartialQntMetric, avgComponentQntMetric);

	ASSERT_GT(avgComponentQntMetric, 0.9);
	ASSERT_GT(avgComponentQntMetric, avgFullQntMetric);
	ASSERT_GT(avgComponentQntMetric, avgPartialQntMetric);
}

TEST(HNSW, QuantizeTest_L2) { QuantizeComponentBasedTestBody<hnswlib::L2Space>(); }
TEST(HNSW, QuantizeTest_IP) { QuantizeComponentBasedTestBody<hnswlib::InnerProductSpace>(); }

INSTANTIATE_TEST_SUITE_P(, HNSWSq8Test,
						 ::testing::Values(ScalarQuantizeType::Full, ScalarQuantizeType::Partial, ScalarQuantizeType::Component),
						 [](const auto& info) {
							 switch (info.param) {
								 case ScalarQuantizeType::Full:
									 return "Full";
								 case ScalarQuantizeType::Partial:
									 return "Partial";
								 case ScalarQuantizeType::Component:
									 return "Component";
								 default:
									 assert(false);
									 std::abort();
							 }
						 });
