#include <gtest/gtest.h>

#include "gtests/tests/fixtures/quantization_helpers.h"

#include "core/index/float_vector/hnswlib/bruteforce.h"
#include "core/index/float_vector/hnswlib/hnsw.h"

using namespace reindexer;

namespace reindexer_tests {

class [[nodiscard]] HnswStreamingSearchTest : public ::testing::TestWithParam<VectorMetric> {
	static constexpr auto synchronization = hnswlib::Synchronization::OnInsertions;
	using pair_t = std::pair<float, hnswlib::labeltype>;

protected:
	using MinHeapQueue = hnswlib::PriorityQueue<pair_t, std::vector<pair_t>, std::greater<pair_t>>;
	void SetUp() override {
		using namespace sq8_test;
		constexpr size_t kSize = kIsRelease ? 10'000 : kHNSWMaxSize;

		hnsw =
			std::make_unique<hnswlib::HierarchicalNSW<synchronization>>(IsArray_False, GetParam(), kDimension, kSize, kM, kEfConstruction);

		bf = std::make_unique<hnswlib::BruteforceSearch>(GetParam(), kDimension, kSize);

		const size_t kNumThreads = 5;
		const auto kBatchSize = kSize / kNumThreads;
		const size_t kNumBatches = (kSize + kBatchSize - 1) / kBatchSize;

		std::vector<std::thread> threads;
		threads.reserve(kNumThreads);

		std::vector<std::vector<float>> points;
		points.reserve(kSize);
		for (size_t label = 0; label < kSize; ++label) {
			auto& point = points.emplace_back(MakePoint());
			bf->AddPointNoLock(ConstFloatVectorView{point}, FloatVectorId{IdType::FromNumber(label), 0});
		}

		std::atomic<size_t> nextBatch{0};
		for (size_t i = 0; i < kNumThreads; ++i) {
			threads.emplace_back([&]() {
				while (true) {
					size_t batchId = nextBatch.fetch_add(1, std::memory_order_relaxed);
					if (batchId >= kNumBatches) {
						break;
					}

					size_t begin = batchId * kBatchSize;
					size_t end = std::min(begin + kBatchSize, kSize);
					for (size_t i = begin; i < end; ++i) {
						hnsw->AddPointConcurrent(ConstFloatVectorView{points[i]}, FloatVectorId{IdType::FromNumber(i), 0});
					}
				}
			});
		}

		for (auto& thread : threads) {
			thread.join();
		}
	}

	void Quantize() {
		hnsw->Quantize(hnswlib::QuantizationConfig{
			.quantile = std::nullopt, .sampleSize = sq8_test::kHNSWMaxSize / 5, .quantizationThreshold = sq8_test::kHNSWMaxSize});
		hnsw->SwitchMapOnQuantized();

		ASSERT_TRUE(hnsw->IsQuantized());
	}

	static float RecallRate(MinHeapQueue hnswQueue, MinHeapQueue bfQueue) {
		std::unordered_set<hnswlib::labeltype> hnswLabels, bfLabels;
		while (!hnswQueue.empty() && !bfQueue.empty()) {
			EXPECT_TRUE(hnswLabels.insert(hnswQueue.top().second).second);
			EXPECT_TRUE(bfLabels.insert(bfQueue.top().second).second);
			hnswQueue.pop();
			bfQueue.pop();
		}
		EXPECT_TRUE(hnswQueue.empty() && bfQueue.empty());

		const auto recall = calcRecall(hnswLabels, bfLabels);
		std::cout << fmt::format("Recall@{}: {}\n", bfLabels.size(), recall);

		return recall;
	}

	static float RecallRate(std::vector<hnswlib::SearchResultQueue> hnswStream, MinHeapQueue bfQueue) {
		std::unordered_set<hnswlib::labeltype> hnswLabels, bfLabels;
		float recall = 0.f;
		for (auto& queue : hnswStream) {
			for (size_t i = 0; i < queue.size() && !bfQueue.empty(); i++) {
				EXPECT_TRUE(bfLabels.insert(bfQueue.top().second).second);
				bfQueue.pop();
			}
			while (!queue.empty()) {
				EXPECT_TRUE(hnswLabels.insert(queue.top().second).second);
				queue.pop();
			}
			recall = calcRecall(hnswLabels, bfLabels);
			TEST_COUT << fmt::format("Intermediate Streaming Recall@{}: {}\n", bfLabels.size(), recall);
		}
		TEST_COUT << fmt::format("Final Streaming Recall@{}: {}\n", bfLabels.size(), recall);

		return recall;
	}

	static float calcRecall(const std::unordered_set<hnswlib::labeltype>& candidate,
							const std::unordered_set<hnswlib::labeltype>& reference) {
		float res = 0;
		for (auto id : candidate) {
			res += reference.contains(id);
		}
		return res / reference.size();
	}

	static MinHeapQueue ToMinHeapQueue(hnswlib::SearchResultQueue knnResult) {
		MinHeapQueue result;
		while (!knnResult.empty()) {
			result.push(knnResult.top());
			knnResult.pop();
		}
		return result;
	}

	std::unique_ptr<hnswlib::HierarchicalNSW<synchronization>> hnsw;
	std::unique_ptr<hnswlib::BruteforceSearch> bf;
};

TEST_P(HnswStreamingSearchTest, CompareRecallRatesTest) {
	const size_t k = 500;
	const size_t batchSize = 50;
	const size_t kMaxBatches = k / batchSize + 1;
	const size_t efBatch = 100;

	const auto query = sq8_test::MakePoint();
	auto test = [&] {
		std::optional<float> normL2 = std::nullopt;
		const float* queryData = query.data();
		std::array<float, sq8_test::kDimension> normalizedStorage;

		if (GetParam() == VectorMetric::Cosine && hnsw->IsQuantized()) {
			normL2 = 1.f / ann::NormalizeCopyVector(query.data(), int32_t(sq8_test::kDimension), normalizedStorage.data());
			queryData = normalizedStorage.data();
		}

		auto session = hnsw->BeginStreamingSearch(queryData, normL2, hnswlib::StreamingSearchOptions{.ef = efBatch});
		std::vector<hnswlib::PriorityQueue<std::pair<float, hnswlib::labeltype>>> batches;
		batches.reserve(kMaxBatches);
		for (;;) {
			auto batch = hnsw->ContinueStreamingSearch(session, batchSize);
			if (!batch.results.empty()) {
				batches.emplace_back(std::move(batch.results));
			}
			if (batch.exhausted || batches.size() >= kMaxBatches) {
				break;
			}
		}

		const size_t ef = 1.1 * k;
		MinHeapQueue hnswRes = ToMinHeapQueue(hnsw->SearchKnn(queryData, normL2, k, ef));
		MinHeapQueue bfRes = ToMinHeapQueue(bf->SearchKnn(queryData, std::nullopt, k));

		EXPECT_EQ(hnswRes.size(), k);
		EXPECT_EQ(bfRes.size(), k);

		const float refRecallRate = RecallRate(hnswRes, bfRes);
		const float streamingRecallRate = RecallRate(batches, bfRes);
		if (streamingRecallRate < refRecallRate) {
			EXPECT_NEAR(refRecallRate, streamingRecallRate, 0.1);
		}
	};

	test();
	Quantize();
	std::cout << "===HNSW quantized===\n";
	test();
}

INSTANTIATE_TEST_SUITE_P(, HnswStreamingSearchTest,
						 ::testing::Values(reindexer::VectorMetric::L2, reindexer::VectorMetric::InnerProduct,
										   reindexer::VectorMetric::Cosine),
						 [](const auto& info) {
							 switch (info.param) {
								 case reindexer::VectorMetric::L2:
									 return "L2";
								 case reindexer::VectorMetric::InnerProduct:
									 return "InnerProduct";
								 case reindexer::VectorMetric::Cosine:
									 return "Cosine";
								 default:
									 assert(false);
									 std::abort();
							 }
						 });

}  // namespace reindexer_tests