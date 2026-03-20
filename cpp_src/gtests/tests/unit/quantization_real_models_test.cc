#include <fstream>

#if defined(__GNUC__) && ((__GNUC__ == 12) || (__GNUC__ == 13)) && defined(REINDEX_WITH_ASAN)
// regex header is broken in GCC 12.0-13.3 with ASAN
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#include <regex>
#pragma GCC diagnostic pop
#else  // REINDEX_WITH_ASAN
#include <regex>
#endif	// REINDEX_WITH_ASAN
#include "gtests/tests/fixtures/quantization_helpers.h"
#include "reindexertestapi.h"
#include "tools/fsops.h"

const size_t kDataSize = 10'001;
const size_t kQueriesSize = 40'000;
const size_t kQueriesSampleSize = 10'000;

struct [[nodiscard]] ModelParams {
	int dim;
	float expectedRecall = 0.9f;
	float expectedRecallDiff = 0.05f;
};

const std::map<std::string, ModelParams> kModelsParams{
	{"e5", {.dim = 1024}},
	{"jina", {.dim = 1024}},
	{"vk", {.dim = 768}},
	{"rubert", {.dim = 768, .expectedRecall = 0.8f, .expectedRecallDiff = 0.1f}},
};

std::vector<std::vector<float>> loadEmbeddings(const std::string& filename, const ModelParams& params,
											   std::optional<std::vector<hnswlib::tableint>> sampleIndexes = std::nullopt) {
	std::ifstream f(filename, std::ios::binary);

	if (!f) {
		throw std::runtime_error("Cannot open file");
	}

	char magic[6];
	f.read(magic, 6);

	if (std::string(magic, 6) != "\x93NUMPY") {
		throw std::runtime_error("Not a npy file");
	}

	char version[2];
	f.read(version, 2);

	uint16_t header_len;
	f.read(reinterpret_cast<char*>(&header_len), 2);
	std::string header(header_len, ' ');
	f.read(header.data(), header_len);

	std::regex shape_regex(R"(\((\d+),\s*(\d+)\))");
	std::smatch match;

	if (!std::regex_search(header, match, shape_regex)) {
		throw std::runtime_error("Cannot parse shape");
	}

	const size_t kSize = std::stoul(match[1]);
	const size_t kDim = std::stoul(match[2]);

	EXPECT_TRUE(kSize == kDataSize || kSize == kQueriesSize);
	EXPECT_EQ(kDim, params.dim);

	std::vector<std::vector<float>> res;
	res.reserve(kSize);
	size_t sampleIdx = 0;
	for (size_t i = 0; i < kSize; ++i) {
		std::vector<float> vec(kDim);
		f.read(reinterpret_cast<char*>(vec.data()), kDim * sizeof(float));

		if (sampleIndexes) {
			if (sampleIdx >= sampleIndexes->size()) {
				break;
			}

			if ((*sampleIndexes)[sampleIdx] != i) {
				continue;
			}
		}

		res.emplace_back(std::move(vec));
		++sampleIdx;
	}

	return res;
}

static void Init(auto& api, std::string_view nsName, size_t dim) {
	auto bfOpts = FloatVectorIndexOpts{}
					  .SetStartSize(kDataSize)
					  .SetMetric(reindexer::VectorMetric::Cosine)
					  .SetMultithreading(MultithreadingMode::MultithreadTransactions);
	auto hnswOpts = FloatVectorIndexOpts{bfOpts}.SetM(16).SetEfConstruction(200);

	api.DropNamespace(nsName);
	api.OpenNamespace(nsName);

	hnswlib::QuantizationConfig config;
	config.sampleSize = kDataSize / 5;
	config.quantizationThreshold = kDataSize;

	api.AddIndex(nsName, reindexer::IndexDef{"id", {"id"}, IndexIntHash, IndexOpts{}.PK()});

	api.AddIndex(nsName,
				 reindexer::IndexDef{
					 "hnsw", {"hnsw"}, IndexHnsw, IndexOpts{}.SetFloatVector(IndexHnsw, FloatVectorIndexOpts{hnswOpts}.SetDimension(dim))});
	api.AddIndex(
		nsName, reindexer::IndexDef{
					"hnsw_q",
					{"hnsw_q"},
					IndexHnsw,
					IndexOpts{}.SetFloatVector(IndexHnsw, FloatVectorIndexOpts{hnswOpts}.SetDimension(dim).SetQuantizationConfig(config))});
	api.AddIndex(nsName,
				 reindexer::IndexDef{"bf",
									 {"bf"},
									 IndexVectorBruteforce,
									 IndexOpts{}.SetFloatVector(IndexVectorBruteforce, FloatVectorIndexOpts{bfOpts}.SetDimension(dim))});
}

static void CheckRecallRate(auto& api, std::string_view nsName, const auto& queryPoints, const ModelParams& modelParams) {
	const size_t k = 20;
	const size_t ef = 100;
	sq8_test::CheckRecallRate(api, nsName, queryPoints, k, ef, modelParams.expectedRecall, modelParams.expectedRecallDiff, "hnsw", "hnsw_q",
							  "bf");
}

TEST(Quantization, RealModelsTest) {
	constexpr auto getSampleIndexes = hnswlib::HNSWView<hnswlib::HierarchicalNSWMT>::GetSampleIndexes;
	const auto kNsName = "items_embeddings";

	const auto kEmbeddingsPathEnv = std::getenv("RX_TEST_EMBEDDINGS_PATH");
	if (!kEmbeddingsPathEnv) {
		GTEST_SKIP();
	}

	const std::string kEmbeddingsPath(kEmbeddingsPathEnv);
	ReindexerTestApi<reindexer::Reindexer> api;
	api.reindexer = std::make_shared<reindexer::Reindexer>();
	api.Connect(fmt::format("builtin://{}", reindexer::fs::JoinPath(kEmbeddingsPath, "sq8_embedding", "db")));

	api.OpenNamespace(kNsName);

	for (const auto& [model, params] : kModelsParams) {
		auto itemsEmbeddings = loadEmbeddings(reindexer::fs::JoinPath(kEmbeddingsPath, model, "items.npy"), params);
		ASSERT_EQ(itemsEmbeddings.size(), kDataSize);
		TEST_COUT << fmt::format("{}-embeded items loaded", model) << std::endl;
		auto queriesEmbeddings = loadEmbeddings(reindexer::fs::JoinPath(kEmbeddingsPath, model, "queries.npy"), params,
												getSampleIndexes(kQueriesSampleSize, kQueriesSize));
		ASSERT_EQ(queriesEmbeddings.size(), kQueriesSampleSize);
		TEST_COUT << fmt::format("{}-embeded queries loaded", model) << std::endl;

		Init(api, kNsName, params.dim);

		const size_t kTxBatchSize = 1000;

		size_t itemId = 0;
		while (itemId < kDataSize) {
			auto tx = api.NewTransaction(kNsName);
			size_t cnt = 0;
			for (; cnt < kTxBatchSize; ++cnt) {
				if (itemId >= kDataSize) {
					break;
				}
				auto txItem = tx.NewItem();
				txItem["id"] = int64_t(itemId);
				auto view = reindexer::ConstFloatVectorView{itemsEmbeddings[itemId]};
				txItem["hnsw"] = view;
				txItem["hnsw_q"] = view;
				txItem["bf"] = view;
				auto err = tx.Insert(std::move(txItem));
				ASSERT_TRUE(err.ok());

				++itemId;
			}

			auto qr = api.CommitTransaction(tx);
			ASSERT_EQ(qr.Count(), cnt);
		}

		TEST_COUT << "Data inserted" << std::endl;

		sq8_test::WaitQuantization(api, kNsName, "hnsw_q");
		TEST_COUT << "Indexes quantized" << std::endl;

		const size_t kNumCheckRecalRateThreads = 4;
		const auto kQueriesSampleBatchSize = kQueriesSampleSize / kNumCheckRecalRateThreads;

		std::atomic<size_t> nextBatch{0};
		const size_t numBatches = (kQueriesSampleSize + kQueriesSampleBatchSize - 1) / kQueriesSampleBatchSize;

		auto worker = [&]() {
			while (true) {
				size_t batchId = nextBatch.fetch_add(1, std::memory_order_relaxed);
				if (batchId >= numBatches) {
					break;
				}

				size_t begin = batchId * kQueriesSampleBatchSize;
				size_t end = std::min(begin + kQueriesSampleBatchSize, kQueriesSampleSize);
				CheckRecallRate(api, kNsName, std::span(queriesEmbeddings.begin() + begin, queriesEmbeddings.begin() + end), params);
			}
		};

		std::vector<std::thread> threads;
		threads.reserve(kNumCheckRecalRateThreads);
		for (size_t i = 0; i < kNumCheckRecalRateThreads; ++i) {
			threads.emplace_back(worker);
		}
		for (auto& th : threads) {
			th.join();
		}
	}
}