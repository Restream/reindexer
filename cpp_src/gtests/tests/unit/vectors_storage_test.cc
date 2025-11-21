#include "float_vector_index.h"
#include "gtests/tests/gtest_cout.h"
#include "gtests/tools.h"
#include "tools/fsops.h"

using reindexer::Query;
using reindexer::ConstFloatVectorView;
using reindexer::ConstFloatVector;
using reindexer::KnnSearchParams;
using reindexer::VectorMetric;
using reindexer::Reindexer;

class [[nodiscard]] VectorStorageApi : public virtual ::testing::Test {
protected:
	void SetUp() override {
		const std::string kDir = getTestStoragePath();
		const std::string kDsn = getDSN();
		deletedIds_.clear();
		rt.reindexer.reset();
		std::ignore = reindexer::fs::RmDirAll(kDir);
		rt.reindexer = std::make_shared<Reindexer>();
		rt.Connect(kDsn);
		rt.OpenNamespace(kNsName, StorageOpts().Enabled().CreateIfMissing());
	}
	void TearDown() override {}

	struct [[nodiscard]] JsonResults {
		std::vector<std::string> items;
		std::vector<reindexer::RankT> ranks;
	};

	static inline void rndFloatVector(std::vector<float>& buf, size_t dim) {
		if (rand() % 10 == 0) {
			buf.resize(0);
		} else {
			return rndFloatVectorNotEmpty(buf, dim);
		}
	}

	static inline void rndFloatVectorNotEmpty(std::vector<float>& buf, size_t dim) {
		buf.resize(dim);
		for (float& v : buf) {
			v = randBin<float>(-10, 10);
		}
	}

	void upsertItems(int from, int to, size_t HNSWSTDims, size_t HNSWMTDims, size_t IVFDims, size_t IVFUnbuiltDims, size_t BFDims) {
		assert(to > from);
		std::vector<float> bufHNSWST, bufHNSWSTCosine, bufHNSWMT, bufIVF, bufIVFCosine, bufIVFUnbuilt, bufIVFCosineUnbuilt, bufBF,
			bufBFCosine;

		for (int id = from; id < to; ++id) {
			auto item = rt.NewItem(kNsName);
			item[kFieldNameId] = id;
			rndFloatVector(bufHNSWST, HNSWSTDims);
			rndFloatVector(bufHNSWSTCosine, HNSWSTDims);
			rndFloatVector(bufHNSWMT, HNSWMTDims);
			rndFloatVector(bufIVF, IVFDims);
			rndFloatVector(bufIVFCosine, IVFDims);
			rndFloatVector(bufIVFUnbuilt, IVFUnbuiltDims);
			rndFloatVector(bufIVFCosineUnbuilt, IVFUnbuiltDims);
			rndFloatVector(bufBF, BFDims);
			rndFloatVector(bufBFCosine, BFDims);
			item[kHNSWSTIdxName] = ConstFloatVectorView{bufHNSWST};
			item[kHNSWSTCosineIdxName] = ConstFloatVectorView{bufHNSWSTCosine};
			item[kHNSWMTIdxName] = ConstFloatVectorView{bufHNSWMT};
			item[kIVFIdxName] = ConstFloatVectorView{bufIVF};
			item[kIVFUnbuiltIdxName] = ConstFloatVectorView{bufIVFUnbuilt};
			item[kIVFCosineIdxName] = ConstFloatVectorView{bufIVF};
			item[kIVFCosineUnbuiltIdxName] = ConstFloatVectorView{bufIVFUnbuilt};
			item[kBFIdxName] = ConstFloatVectorView{bufBF};
			item[kBFCosineIdxName] = ConstFloatVectorView{bufBFCosine};

			rt.Upsert(kNsName, item);
		}
	}
	void deleteItem(int id) {
		auto item = rt.NewItem(kNsName);
		item[kFieldNameId] = id;
		rt.Delete(kNsName, item);
		deletedIds_.emplace(id);
	}
	std::string getTestStoragePath() {
		using reindexer::fs::JoinPath;
		const std::string testSetName = ::testing::UnitTest::GetInstance()->current_test_info()->test_case_name();
		const std::string testName = ::testing::UnitTest::GetInstance()->current_test_info()->name();
		return JoinPath(reindexer::fs::GetTempDir(), JoinPath(testSetName, testName));
	}
	std::string getDSN() {
		const std::string kDir = getTestStoragePath();
		return "builtin://" + kDir;
	}
	JsonResults selectToJsonResults(const Query& q) {
		SCOPED_TRACE(fmt::format("Query: {}", q.GetSQL()));
		auto qr = rt.Select(q);
		JsonResults jsons;
		for (auto& it : qr) {
			auto item = it.GetItem();
			const auto id = item["id"].As<int>();
			const auto found = deletedIds_.find(id);
			std::string_view json;
			try {
				json = item.GetJSON();
			} catch (const reindexer::Error& err) {
				SCOPED_TRACE(err.what());
				EXPECT_TRUE(false);
			}
			SCOPED_TRACE(fmt::format("JSON: {}", json));
			EXPECT_EQ(found, deletedIds_.end()) << "Deleted id found in results: " << id;
			jsons.items.emplace_back(json);
			if (it.IsRanked()) {
				auto ref = it.GetItemRefRanked();
				jsons.ranks.emplace_back(ref.Rank());
			} else {
				jsons.ranks.emplace_back();
			}
		}
		return jsons;
	}
	std::vector<JsonResults> selectToJsonResults(std::span<const Query> qs) {
		std::vector<JsonResults> jsons;
		jsons.reserve(qs.size());
		for (auto& q : qs) {
			jsons.emplace_back(selectToJsonResults(q));
		}
		return jsons;
	}
	JsonResults requestAllData() {
		const static auto kRequestAllQ = Query(kNsName).Sort(kFieldNameId, false).SelectAllFields();
		return selectToJsonResults(kRequestAllQ);
	}
	void reloadStorage() {
		rt.reindexer.reset();
		rt.reindexer = std::make_shared<Reindexer>();
		rt.Connect(getDSN());
	}
	void validateResults(const JsonResults& expected, const JsonResults& actual) {
		ASSERT_EQ(expected.items.size(), actual.items.size());
		if (expected.items.size() != actual.items.size()) {
			TestCout() << "Expected:" << std::endl;
			for (size_t i = 0; i < expected.items.size(); ++i) {
				TestCout() << expected.items[i] << std::endl;
				TestCout() << "Rank: " << expected.ranks[i].Value() << std::endl;
			}
			TestCout() << std::endl;
			TestCout() << "Actual:" << std::endl;
			for (size_t i = 0; i < actual.items.size(); ++i) {
				TestCout() << actual.items[i] << std::endl;
				TestCout() << "Rank: " << actual.ranks[i].Value() << std::endl;
			}
			TestCout() << std::endl;
			ASSERT_TRUE(false);
		}
		for (size_t j = 0; j < expected.items.size(); ++j) {
			EXPECT_EQ(expected.items[j], actual.items[j]);
		}
	}
	std::vector<Query> prepareANNQueries(size_t count, std::string_view idx, size_t dims, const KnnSearchParams& param) {
		std::vector<Query> ret;
		ret.reserve(count);
		std::vector<float> buf;

		[[maybe_unused]] size_t counter = 0;
		while (ret.size() != count) {
			assertrx(counter++ < count * 10);  // in case of infinite loop

			rndFloatVectorNotEmpty(buf, dims);
			auto query = Query{kNsName}.WhereKNN(idx, ConstFloatVector{buf}, param).WithRank().SelectAllFields();
			auto qr = rt.Select(query);
			EXPECT_GT(qr.Count(), 0);
			bool hasDuplicatedRanks = false;
			auto prevRank = qr.begin().GetItemRefRanked().Rank();
			for (auto it = qr.begin() + 1; it != qr.end(); ++it) {
				if (prevRank == it.GetItemRefRanked().Rank()) {
					hasDuplicatedRanks = true;
					break;
				}
				prevRank = it.GetItemRefRanked().Rank();
			}
			if (hasDuplicatedRanks) {
				// Avoiding query results with duplicated ranks for simplier further validation
				continue;
			}

			ret.emplace_back(std::move(query));
		}
		return ret;
	}

	constexpr static std::string_view kFieldNameId = "id";
	constexpr static std::string_view kHNSWSTIdxName = "hnsw_st_idx";
	constexpr static std::string_view kHNSWSTCosineIdxName = "hnsw_st_cosine_idx";
	constexpr static std::string_view kHNSWMTIdxName = "hnsw_mt_idx";
	constexpr static std::string_view kIVFIdxName = "ivf_idx";
	constexpr static std::string_view kIVFUnbuiltIdxName = "ivf_unbuilt_idx";
	constexpr static std::string_view kIVFCosineIdxName = "ivf_cosine_idx";
	constexpr static std::string_view kIVFCosineUnbuiltIdxName = "ivf_cosine_unbuilt_idx";
	constexpr static std::string_view kBFIdxName = "bf_idx";
	constexpr static std::string_view kBFCosineIdxName = "bf_cosine_idx";
	constexpr static std::string_view kNsName = "vector_storage_reload_ns";

	ReindexerTestApi<Reindexer> rt;
	std::unordered_set<int> deletedIds_;
};

TEST_F(VectorStorageApi, FloatStorageReload) try {
	using namespace reindexer;

	constexpr static auto kDataCount = 5000;
	constexpr static auto kHNSWSTDims = 32;
	constexpr static auto kHNSWMTDims = 40;
	constexpr static auto kIVFDims = 24;
	constexpr static auto kIVFCentroids = kDataCount / 80;
	static_assert(kIVFCentroids > 4, "Expecting some reasonable centroids count");
	constexpr static auto kIVFUnbuiltDims = 28;
	constexpr static auto kIVFUnbuiltCentroids = kDataCount / 10;
	static_assert(kIVFUnbuiltCentroids > 4, "Expecting some reasonable centroids count");
	constexpr static auto kBFDims = 48;
	constexpr static auto kQueriesCount = 20;
	constexpr std::string_view kNoIdx = "none";

	struct [[nodiscard]] IndexParams {
		std::string_view name;
		size_t dims;
		KnnSearchParams params;
		std::vector<Query> queries;
	};

	// Create indexes
	std::vector<IndexParams> indexes = {{kHNSWSTIdxName, kHNSWSTDims, HnswSearchParams{}.K(20).Ef(40), {}},
										{kHNSWSTCosineIdxName, kHNSWSTDims, HnswSearchParams{}.K(17).Ef(35), {}},
										{kHNSWMTIdxName, kHNSWMTDims, HnswSearchParams{}.K(15).Ef(25), {}},
										{kIVFIdxName, kIVFDims, IvfSearchParams{}.K(20).NProbe(8), {}},
										{kIVFUnbuiltIdxName, kIVFUnbuiltDims, IvfSearchParams{}.K(23).NProbe(6), {}},
										{kIVFCosineIdxName, kIVFDims, IvfSearchParams{}.K(21).NProbe(6), {}},
										{kIVFCosineUnbuiltIdxName, kIVFUnbuiltDims, IvfSearchParams{}.K(17).NProbe(7), {}},
										{kBFIdxName, kBFDims, BruteForceSearchParams{}.K(25), {}},
										{kBFCosineIdxName, kBFDims, BruteForceSearchParams{}.K(22), {}}};
	rt.DefineNamespaceDataset(
		kNsName,
		{
			IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts{}.PK(), 0},
			IndexDeclaration{kHNSWSTIdxName, "hnsw", "float_vector",
							 IndexOpts{}.SetFloatVector(IndexHnsw, FloatVectorIndexOpts{}
																	   .SetDimension(kHNSWSTDims)
																	   .SetStartSize(kDataCount)
																	   .SetM(16)
																	   .SetEfConstruction(50)
																	   .SetMetric(VectorMetric::L2)),
							 0},
			IndexDeclaration{kHNSWSTCosineIdxName, "hnsw", "float_vector",
							 IndexOpts{}.SetFloatVector(IndexHnsw, FloatVectorIndexOpts{}
																	   .SetDimension(kHNSWSTDims)
																	   .SetStartSize(kDataCount)
																	   .SetM(16)
																	   .SetEfConstruction(60)
																	   .SetMetric(VectorMetric::Cosine)),
							 0},
			IndexDeclaration{kHNSWMTIdxName, "hnsw", "float_vector",
							 IndexOpts{}.SetFloatVector(IndexHnsw, FloatVectorIndexOpts{}
																	   .SetDimension(kHNSWMTDims)
																	   .SetStartSize(kDataCount)
																	   .SetM(24)
																	   .SetEfConstruction(80)
																	   .SetMetric(VectorMetric::InnerProduct)
																	   .SetMultithreading(MultithreadingMode::MultithreadTransactions)),
							 0},
			IndexDeclaration{
				kIVFIdxName, "ivf", "float_vector",
				IndexOpts{}.SetFloatVector(
					IndexIvf, FloatVectorIndexOpts{}.SetDimension(kIVFDims).SetNCentroids(kIVFCentroids).SetMetric(VectorMetric::L2)),
				0},
			IndexDeclaration{kIVFUnbuiltIdxName, "ivf", "float_vector",
							 IndexOpts{}.SetFloatVector(IndexIvf, FloatVectorIndexOpts{}
																	  .SetDimension(kIVFUnbuiltDims)
																	  .SetNCentroids(kIVFUnbuiltCentroids)
																	  .SetMetric(VectorMetric::InnerProduct)),
							 0},
			IndexDeclaration{
				kIVFCosineIdxName, "ivf", "float_vector",
				IndexOpts{}.SetFloatVector(
					IndexIvf, FloatVectorIndexOpts{}.SetDimension(kIVFDims).SetNCentroids(kIVFCentroids).SetMetric(VectorMetric::Cosine)),
				0},
			IndexDeclaration{kIVFCosineUnbuiltIdxName, "ivf", "float_vector",
							 IndexOpts{}.SetFloatVector(IndexIvf, FloatVectorIndexOpts{}
																	  .SetDimension(kIVFUnbuiltDims)
																	  .SetNCentroids(kIVFUnbuiltCentroids)
																	  .SetMetric(VectorMetric::Cosine)),
							 0},
			IndexDeclaration{kBFIdxName, "vec_bf", "float_vector",
							 IndexOpts{}.SetFloatVector(
								 IndexVectorBruteforce,
								 FloatVectorIndexOpts{}.SetDimension(kBFDims).SetStartSize(kDataCount).SetMetric(VectorMetric::L2)),
							 0},
			IndexDeclaration{kBFCosineIdxName, "vec_bf", "float_vector",
							 IndexOpts{}.SetFloatVector(
								 IndexVectorBruteforce,
								 FloatVectorIndexOpts{}.SetDimension(kBFDims).SetStartSize(kDataCount).SetMetric(VectorMetric::Cosine)),
							 0},
		});

	// Prepare data
	upsertItems(0, kDataCount, kHNSWSTDims, kHNSWMTDims, kIVFDims, kIVFUnbuiltDims, kBFDims);
	for (int id = 0; id < kDataCount; id += 7) {
		deleteItem(id);
	}

	// Prepare test queries
	for (auto& idx : indexes) {
		idx.queries = prepareANNQueries(kQueriesCount, idx.name, idx.dims, idx.params);
	}

	std::unordered_map<std::string_view, std::vector<JsonResults>> selectResults;
	selectResults[kNoIdx].emplace_back(requestAllData());
	for (auto& idx : indexes) {
		selectResults[idx.name] = selectToJsonResults({idx.queries.data(), idx.queries.size()});
		ASSERT_GT(selectResults[idx.name].size(), 0) << "Expecting non-empty results for " << idx.name;
	}
	const auto stats = rt.GetReplicationState(kNsName);

	// Reload storage and validate results again
	reloadStorage();

	{
		auto& expected = selectResults[kNoIdx][0];
		auto actual = requestAllData();
		ASSERT_EQ(actual.items.size(), kDataCount - deletedIds_.size());
		validateResults(expected, actual);
	}
	const auto statsReloaded = rt.GetReplicationState(kNsName);
	EXPECT_EQ(stats.dataCount, statsReloaded.dataCount);
	EXPECT_EQ(stats.dataHash, statsReloaded.dataHash);
	EXPECT_EQ(stats.updateUnixNano, statsReloaded.updateUnixNano);

	for (auto& idx : indexes) {
		SCOPED_TRACE(fmt::format("Current idx: {}", idx.name));
		const auto& expected = selectResults[idx.name];
		const auto actual = selectToJsonResults({idx.queries.data(), idx.queries.size()});
		ASSERT_EQ(expected.size(), actual.size());

		for (size_t i = 0; i < expected.size(); ++i) {
			SCOPED_TRACE(fmt::format("Current query: {}", idx.queries[i].GetSQL()));
			validateResults(expected[i], actual[i]);
		}
	}
}
CATCH_AND_ASSERT
