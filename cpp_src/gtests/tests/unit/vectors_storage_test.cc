#include <unordered_set>
#include "core/cjson/jsonbuilder.h"
#include "core/query/query.h"
#include "core/reindexer.h"
#include "core/tag_name_index.h"
#include "gtests/tests/fixtures/reindexer_api.h"
#include "gtests/tests/gtest_cout.h"
#include "gtests/tools.h"
#include "tools/fsops.h"
#include "tools/serilize/wrserializer.h"

using reindexer::Query;
using reindexer::ConstFloatVectorView;
using reindexer::ConstFloatVector;
using reindexer::KnnSearchParams;
using reindexer::VectorMetric;
using reindexer::Reindexer;

class [[nodiscard]] VectorStorageApi : public virtual ::testing::Test {
protected:
	enum [[nodiscard]] IsArray : bool { Array = true, Scalar = false };
	void SetUp() override {
		const std::string kDir = getTestStoragePath();
		const std::string kDsn = getDSN();
		deletedIds_.clear();
		rt.reindexer.reset();
		std::ignore = reindexer::fs::RmDirAll(kDir);
		rt.reindexer = std::make_shared<Reindexer>();
		rt.Connect(kDsn);
		rt.OpenNamespace(kNsName<Array>, StorageOpts().Enabled().CreateIfMissing());
		rt.OpenNamespace(kNsName<Scalar>, StorageOpts().Enabled().CreateIfMissing());
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

	template <IsArray>
	void upsertItems(int from, int to, size_t HNSWSTDims, size_t HNSWMTDims, size_t IVFDims, size_t IVFUnbuiltDims, size_t BFDims);

	void jsonArrayField(reindexer::JsonBuilder& json, std::string_view fieldName, size_t dim) {
		std::vector<float> buf;
		auto arr = json.Array(fieldName);
		for (size_t i = 0, s = rand() % 10; i < s; ++i) {
			rndFloatVector(buf, dim);
			arr.Array(reindexer::TagName::Empty(), std::span<const float>{buf});
		}
	}
	template <IsArray isArray>
	void deleteItem(int id) {
		auto item = rt.NewItem(kNsName<isArray>);
		item[kFieldNameId] = id;
		rt.Delete(kNsName<isArray>, item);
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
	template <IsArray isArray>
	JsonResults requestAllData() {
		const static auto kRequestAllQ = Query(kNsName<isArray>).Sort(kFieldNameId, false).SelectAllFields();
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
	template <IsArray isArray>
	std::vector<Query> prepareANNQueries(size_t count, std::string_view idx, size_t dims, const KnnSearchParams& param) {
		std::vector<Query> ret;
		ret.reserve(count);
		std::vector<float> buf;

		[[maybe_unused]] size_t counter = 0;
		while (ret.size() != count) {
			assertrx(counter++ < count * 10);  // in case of infinite loop

			rndFloatVectorNotEmpty(buf, dims);
			auto query = Query{kNsName<isArray>}.WhereKNN(idx, ConstFloatVector{buf}, param).WithRank().SelectAllFields();
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

	template <IsArray>
	void TestFloatStorageReload();

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
	template <IsArray isArray>
	constexpr static std::string_view kNsName = isArray ? "vector_storage_reload_ns_array" : "vector_storage_reload_ns";

	ReindexerTestApi<Reindexer> rt;
	std::unordered_set<int> deletedIds_;
};

template <>
void VectorStorageApi::upsertItems<VectorStorageApi::Scalar>(int from, int to, size_t HNSWSTDims, size_t HNSWMTDims, size_t IVFDims,
															 size_t IVFUnbuiltDims, size_t BFDims) {
	assert(to > from);
	std::vector<float> bufHNSWST, bufHNSWSTCosine, bufHNSWMT, bufIVF, bufIVFCosine, bufIVFUnbuilt, bufIVFCosineUnbuilt, bufBF, bufBFCosine;

	for (int id = from; id < to; ++id) {
		auto item = rt.NewItem(kNsName<Scalar>);
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

		rt.Upsert(kNsName<Scalar>, item);
	}
}
template <>
void VectorStorageApi::upsertItems<VectorStorageApi::Array>(int from, int to, size_t HNSWSTDims, size_t HNSWMTDims, size_t IVFDims,
															size_t IVFUnbuiltDims, size_t BFDims) {
	assert(to > from);
	for (int id = from; id < to; ++id) {
		reindexer::WrSerializer ser;
		{
			reindexer::JsonBuilder json(ser);
			json.Put(kFieldNameId, id);
			jsonArrayField(json, kHNSWSTIdxName, HNSWSTDims);
			jsonArrayField(json, kHNSWSTCosineIdxName, HNSWSTDims);
			jsonArrayField(json, kHNSWMTIdxName, HNSWMTDims);
			jsonArrayField(json, kIVFIdxName, IVFDims);
			jsonArrayField(json, kIVFUnbuiltIdxName, IVFUnbuiltDims);
			jsonArrayField(json, kIVFCosineIdxName, IVFDims);
			jsonArrayField(json, kIVFCosineUnbuiltIdxName, IVFUnbuiltDims);
			jsonArrayField(json, kBFIdxName, BFDims);
			jsonArrayField(json, kBFCosineIdxName, BFDims);
		}
		auto item = rt.NewItem(kNsName<Array>);
		const auto err = item.FromJSON(ser.Slice());
		ASSERT_TRUE(err.ok()) << err.what();
		rt.Upsert(kNsName<Array>, item);
	}
}
template <VectorStorageApi::IsArray isArray>
void VectorStorageApi::TestFloatStorageReload() try {
	using namespace reindexer;

	constexpr static auto kDataCount = isArray ? 2000 : 5000;
	constexpr static auto kHNSWSTDims = isArray ? 8 : 32;
	constexpr static auto kHNSWMTDims = isArray ? 8 : 40;
	constexpr static auto kIVFDims = isArray ? 6 : 24;
	constexpr static auto kIVFCentroids = isArray ? (kDataCount / 40) : (kDataCount / 80);
	static_assert(kIVFCentroids > 4, "Expecting some reasonable centroids count");
	constexpr static auto kIVFUnbuiltDims = isArray ? 7 : 28;
	constexpr static auto kIVFUnbuiltCentroids = kDataCount / 10;
	static_assert(kIVFUnbuiltCentroids > 4, "Expecting some reasonable centroids count");
	constexpr static auto kBFDims = isArray ? 20 : 48;
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
		kNsName<isArray>,
		{
			IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts{}.PK(), 0},
			IndexDeclaration{kHNSWSTIdxName, "hnsw", "float_vector",
							 IndexOpts{}.Array(isArray).SetFloatVector(IndexHnsw, FloatVectorIndexOpts{}
																					  .SetDimension(kHNSWSTDims)
																					  .SetStartSize(kDataCount)
																					  .SetM(16)
																					  .SetEfConstruction(50)
																					  .SetMetric(VectorMetric::L2)),
							 0},
			IndexDeclaration{kHNSWSTCosineIdxName, "hnsw", "float_vector",
							 IndexOpts{}.Array(isArray).SetFloatVector(IndexHnsw, FloatVectorIndexOpts{}
																					  .SetDimension(kHNSWSTDims)
																					  .SetStartSize(kDataCount)
																					  .SetM(16)
																					  .SetEfConstruction(60)
																					  .SetMetric(VectorMetric::Cosine)),
							 0},
			IndexDeclaration{
				kHNSWMTIdxName, "hnsw", "float_vector",
				IndexOpts{}.Array(isArray).SetFloatVector(IndexHnsw, FloatVectorIndexOpts{}
																		 .SetDimension(kHNSWMTDims)
																		 .SetStartSize(kDataCount)
																		 .SetM(24)
																		 .SetEfConstruction(80)
																		 .SetMetric(VectorMetric::InnerProduct)
																		 .SetMultithreading(MultithreadingMode::MultithreadTransactions)),
				0},
			IndexDeclaration{
				kIVFIdxName, "ivf", "float_vector",
				IndexOpts{}.Array(isArray).SetFloatVector(
					IndexIvf, FloatVectorIndexOpts{}.SetDimension(kIVFDims).SetNCentroids(kIVFCentroids).SetMetric(VectorMetric::L2)),
				0},
			IndexDeclaration{kIVFUnbuiltIdxName, "ivf", "float_vector",
							 IndexOpts{}.Array(isArray).SetFloatVector(IndexIvf, FloatVectorIndexOpts{}
																					 .SetDimension(kIVFUnbuiltDims)
																					 .SetNCentroids(kIVFUnbuiltCentroids)
																					 .SetMetric(VectorMetric::InnerProduct)),
							 0},
			IndexDeclaration{
				kIVFCosineIdxName, "ivf", "float_vector",
				IndexOpts{}.Array(isArray).SetFloatVector(
					IndexIvf, FloatVectorIndexOpts{}.SetDimension(kIVFDims).SetNCentroids(kIVFCentroids).SetMetric(VectorMetric::Cosine)),
				0},
			IndexDeclaration{kIVFCosineUnbuiltIdxName, "ivf", "float_vector",
							 IndexOpts{}.Array(isArray).SetFloatVector(IndexIvf,
																	   FloatVectorIndexOpts{}
																		   .SetDimension(kIVFUnbuiltDims)
																		   .SetNCentroids(kIVFUnbuiltCentroids)
																		   .SetMetric(VectorMetric::Cosine)),
							 0},
			IndexDeclaration{kBFIdxName, "vec_bf", "float_vector",
							 IndexOpts{}.Array(isArray).SetFloatVector(
								 IndexVectorBruteforce,
								 FloatVectorIndexOpts{}.SetDimension(kBFDims).SetStartSize(kDataCount).SetMetric(VectorMetric::L2)),
							 0},
			IndexDeclaration{kBFCosineIdxName, "vec_bf", "float_vector",
							 IndexOpts{}.Array(isArray).SetFloatVector(
								 IndexVectorBruteforce,
								 FloatVectorIndexOpts{}.SetDimension(kBFDims).SetStartSize(kDataCount).SetMetric(VectorMetric::Cosine)),
							 0},
		});

	// Prepare data
	upsertItems<isArray>(0, kDataCount, kHNSWSTDims, kHNSWMTDims, kIVFDims, kIVFUnbuiltDims, kBFDims);
	for (int id = 0; id < kDataCount; id += 7) {
		deleteItem<isArray>(id);
	}

	// Prepare test queries
	for (auto& idx : indexes) {
		idx.queries = prepareANNQueries<isArray>(kQueriesCount, idx.name, idx.dims, idx.params);
	}

	std::unordered_map<std::string_view, std::vector<JsonResults>> selectResults;
	selectResults[kNoIdx].emplace_back(requestAllData<isArray>());
	for (auto& idx : indexes) {
		selectResults[idx.name] = selectToJsonResults({idx.queries.data(), idx.queries.size()});
		ASSERT_GT(selectResults[idx.name].size(), 0) << "Expecting non-empty results for " << idx.name;
	}
	const auto stats = rt.GetReplicationState(kNsName<isArray>);

	// Reload storage and validate results again
	reloadStorage();

	{
		auto& expected = selectResults[kNoIdx][0];
		auto actual = requestAllData<isArray>();
		ASSERT_EQ(actual.items.size(), kDataCount - deletedIds_.size());
		validateResults(expected, actual);
	}
	const auto statsReloaded = rt.GetReplicationState(kNsName<isArray>);
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

TEST_F(VectorStorageApi, FloatStorageReloadScalar) { TestFloatStorageReload<Scalar>(); }
TEST_F(VectorStorageApi, FloatStorageReloadArray) { TestFloatStorageReload<Array>(); }
