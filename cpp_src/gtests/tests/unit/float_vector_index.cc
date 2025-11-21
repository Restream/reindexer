#include "gtests/tests/fixtures/float_vector_index.h"
#include <gmock/gmock.h>
#include <gtest/gtest-param-test.h>
#include <random>
#include <thread>
#include "core/cjson/jsonbuilder.h"
#include "gtests/tests/gtest_cout.h"
#include "gtests/tools.h"
#include "tools/errors.h"
#include "tools/fsops.h"
#include "vendor/fmt/ranges.h"

using namespace std::string_view_literals;
static constexpr auto kFieldNameId = "id";
static constexpr std::string_view kFieldNameRegular = "regular_field"sv;

void FloatVector::SetUp() {
	auto dir = reindexer::fs::JoinPath(reindexer::fs::GetTempDir(), "/FloatVectorTest");
	std::ignore = reindexer::fs::RmDirAll(dir);
	rt.reindexer = std::make_shared<reindexer::Reindexer>();
	rt.Connect("builtin://" + dir);
}

static void checkOrdering(const reindexer::QueryResults& qr, reindexer::VectorMetric metric, std::optional<float> radius = std::nullopt) {
	IdType lastId = 0;
	reindexer::WrSerializer ser;
	switch (metric) {
		case reindexer::VectorMetric::L2: {
			reindexer::RankT lastRank(std::numeric_limits<reindexer::RankT>::min());
			for (auto& i : qr) {
				const auto& item = i.GetItemRefRanked();
				const auto curRank = item.Rank();
				const auto curId = item.NotRanked().Id();
				EXPECT_GE(curRank, lastRank);
				if (!(lastRank < curRank) && !(lastRank > curRank)) {
					EXPECT_LE(lastId, curId);
				}
				if (radius) {
					EXPECT_LT(curRank.Value(), *radius);
				}
				lastRank = curRank;
				lastId = curId;

				// Get JSON just to check, that it is possible
				ser.Reset();
				auto err = i.GetJSON(ser, false);
				EXPECT_TRUE(err.ok()) << err.what();
			}
		} break;
		case reindexer::VectorMetric::Cosine:
		case reindexer::VectorMetric::InnerProduct: {
			reindexer::RankT lastRank(std::numeric_limits<decltype(reindexer::RankT{}.Value())>::max());
			for (auto& i : qr) {
				const auto& item = i.GetItemRefRanked();
				const auto curRank = item.Rank();
				const auto curId = item.NotRanked().Id();
				EXPECT_LE(curRank, lastRank);
				if (!(lastRank < curRank) && !(lastRank > curRank)) {
					EXPECT_LE(lastId, curId);
				}
				if (metric == reindexer::VectorMetric::Cosine) {
					EXPECT_LE(curRank.Value(), 1.0);
					EXPECT_GE(curRank.Value(), -1.0);
				}
				if (radius) {
					EXPECT_GT(curRank.Value(), *radius);
				}
				lastRank = curRank;
				lastId = curId;

				// Get JSON just to check, that it is possible
				ser.Reset();
				auto err = i.GetJSON(ser, false);
				EXPECT_TRUE(err.ok()) << err.what();
			}
		} break;
	}
}

size_t FloatVector::deleteSomeItems(std::string_view nsName, int maxElements, std::unordered_set<int>& emptyVectors) {
	const auto step = std::max(maxElements / 100, 3);
	size_t deleted = 0;
	int prev = -1;
	for (int i = rand() % step; i < maxElements; i += rand() % step) {
		auto item = rt.NewItem(nsName);
		item["id"] = i;
		rt.Delete(nsName, item);
		if (i != prev) {
			++deleted;
			prev = i;
		}
		emptyVectors.erase(i);
	}
	return deleted;
}

void FloatVector::rebuildCentroids() {
	rt.UpsertJSON("#config",
				  R"json({"type":"action","action":{"command":"rebuild_ivf_index", "namespace":"*", "index":"*", "data_part": 0.5}})json");
}

void FloatVector::validateIndexValueInItem(std::string_view ns, std::string_view field, std::string_view json,
										   std::span<const float> expected) {
	SCOPED_TRACE(json);
	SCOPED_TRACE(fmt::format("expected: [{}]", fmt::join(expected, ", ")));
	auto item = rt.NewItem(ns);
	auto err = item.FromJSON(json);
	ASSERT_TRUE(err.ok()) << err.what();
	auto resSpan = item[field].Get<reindexer::ConstFloatVectorView>().Span();
	ASSERT_TRUE(std::equal(resSpan.begin(), resSpan.end(), expected.begin(), expected.end()));
}

void FloatVector::validateIndexValueInQueryResults(std::string_view field, const reindexer::QueryResults& qr,
												   std::span<const float> expected) {
	reindexer::WrSerializer ser;
	for (auto& it : qr) {
		ser.Reset();
		auto err = it.GetJSON(ser, false);
		ASSERT_TRUE(err.ok()) << err.whatStr();
		ASSERT_EQ(qr.GetNamespaces().size(), 1);
		validateIndexValueInItem(qr.GetNamespaces()[0], field, ser.Slice(), expected);
	}
}

template <typename ParamT, size_t kDims>
void FloatVector::checkEmptyIndexSelection(std::string_view ns, std::string_view vectorIndex) {
	const auto k = rand() % 100 + 10;
	const float radius = float(rand() % 20) / 10.;
	std::array<float, kDims> buf;
	::rndFloatVector(buf);

	auto addEf = [&](ParamT&& param) {
		if constexpr (std::is_same_v<ParamT, reindexer::HnswSearchParams>) {
			if (param.K().has_value()) {
				return param.Ef(2 * k);
			}
		}
		return param;
	};

	auto qr = rt.Select(reindexer::Query{ns}.WhereKNN(vectorIndex, reindexer::ConstFloatVectorView{buf}, addEf(ParamT{}.K(k))));
	EXPECT_EQ(qr.Count(), 0);
	qr = rt.Select(reindexer::Query{ns}.WhereKNN(vectorIndex, reindexer::ConstFloatVectorView{buf}, addEf(ParamT{}.Radius(radius))));
	EXPECT_EQ(qr.Count(), 0);
	qr = rt.Select(reindexer::Query{ns}.WhereKNN(vectorIndex, reindexer::ConstFloatVectorView{buf}, addEf(ParamT{}.K(k).Radius(radius))));
	EXPECT_EQ(qr.Count(), 0);
}

template <size_t Dimension>
reindexer::Item FloatVector::newItem(std::string_view nsName, std::string_view fieldName, int id, std::unordered_set<int>& emptyVectors) {
	if (rand() % 2) {
		return newItemDirect<Dimension>(nsName, fieldName, id, emptyVectors);
	} else {
		return newItemFromJson<Dimension>(nsName, fieldName, id, emptyVectors);
	}
}

template <size_t Dimension>
reindexer::Item FloatVector::newItemDirect(std::string_view nsName, std::string_view fieldName, int id,
										   std::unordered_set<int>& emptyVectors) {
	auto item = rt.NewItem(nsName);
	item[kFieldNameId] = id;
	if (rand() % 10 != 0) {
		std::array<float, Dimension> buf;
		::rndFloatVector(buf);
		item[fieldName] = reindexer::ConstFloatVectorView{buf};
	} else {
		if (rand() % 2 == 0) {
			item[fieldName] = reindexer::ConstFloatVectorView{};
		}
		emptyVectors.insert(id);
	}
	return item;
}

template <size_t Dimension>
reindexer::Item FloatVector::newItemFromJson(std::string_view nsName, std::string_view fieldName, int id,
											 std::unordered_set<int>& emptyVectors) {
	reindexer::WrSerializer ser;
	{
		reindexer::JsonBuilder json(ser);
		json.Put(kFieldNameId, id);
		if (rand() % 10 != 0) {
			std::array<float, Dimension> buf;
			::rndFloatVector(buf);
			json.Array(fieldName, std::span<const float>(buf));
		} else {
			switch (rand() % 3) {
				case 0:
					json.AddArray(fieldName);
					break;
				case 1:
					json.Null(fieldName);
					break;
				default:
					break;
			}
			emptyVectors.insert(id);
		}
	}
	auto item = rt.NewItem(nsName);
	const auto err = item.FromJSON(ser.Slice());
	if (!err.ok()) {
		throw err;
	}
	return item;
}

template <size_t Dimension>
void FloatVector::upsertItems(std::string_view nsName, std::string_view fieldName, int startId, int endId,
							  std::unordered_set<int>& emptyVectors, HasIndex hasIndex) {
	assert(startId < endId);
	for (int id = startId; id < endId; ++id) {
		auto item = hasIndex == HasIndex::Yes ? newItem<Dimension>(nsName, fieldName, id, emptyVectors)
											  : newItemFromJson<Dimension>(nsName, fieldName, id, emptyVectors);
		rt.Upsert(nsName, item);
	}
}

enum class [[nodiscard]] QueryWith : size_t { OnlyK, KandR, OnlyR };

template <size_t Dimension, typename SearchParamGetterT>
void FloatVector::runMultithreadQueries(size_t threads, size_t queriesPerThread, std::string_view nsName, std::string_view fieldName,
										const SearchParamGetterT& getKNNParam) {
	std::vector<std::thread> threadsStorage;
	std::optional<size_t> k = rand() % 500 + 10;
	threadsStorage.reserve(threads);
	for (size_t i = 0; i < threads; ++i) {
		threadsStorage.emplace_back([&, this] {
			std::array<float, Dimension> buf;
			for (size_t j = 0; j < queriesPerThread; ++j) {
				::rndFloatVector(buf);
				std::optional<float> radius = std::nullopt;
				size_t prevQrCount = 0;
				for (auto queryCase : {QueryWith::OnlyK, QueryWith::KandR, QueryWith::OnlyR}) {
					auto params = getKNNParam(queryCase == QueryWith::OnlyR ? std::nullopt : k, radius);
					if constexpr (std::is_same_v<decltype(params), reindexer::HnswSearchParams>) {
						params.Ef(*k);
					}

					const auto result =
						rt.Select(reindexer::Query{nsName}.WhereKNN(fieldName, reindexer::ConstFloatVectorView{buf}, std::move(params)));

					if (queryCase == QueryWith::OnlyR) {
						EXPECT_EQ(prevQrCount, result.Count());
					}
					checkOrdering(result, GetParam(), radius);
					if (!radius) {
						radius = (result.begin() + result.Count() / 2).GetItemRefRanked().Rank().Value();
					}
					prevQrCount = result.Count();
				}
			}
		});
	}
	for (auto& th : threadsStorage) {
		th.join();
	}
}

static void checkIndexMemstat(ReindexerTestApi<reindexer::Reindexer>& rx, std::string_view ns, std::string_view vectorIndex, size_t dims,
							  size_t vectors, const std::unordered_set<int>& emptyVectors) {
	auto qr = rx.Select(reindexer::Query("#memstats").Where("name", CondEq, ns));
	ASSERT_EQ(qr.Count(), 1);
	auto memstats = qr.begin().GetItem(false);
	auto json = memstats.GetJSON();
	vectors -= emptyVectors.size();
	auto expectedRegex = fmt::format(
		R"(\{{"uniq_keys_count":{},"data_size":{},"indexing_struct_size":[1-9][0-9]+,"vectors_keeper_size":[1-9][0-9]+,"is_built":true,"name":"{}"\}})",
		vectors + (emptyVectors.empty() ? 0 : 1), vectors * dims * sizeof(float), vectorIndex);
	ASSERT_THAT(json, testing::ContainsRegex(expectedRegex));
}

TEST_P(FloatVector, HnswIndex) try {
	constexpr static auto kNsName = "hnsw_ns"sv;
	constexpr static auto kFieldNameHnsw = "hnsw"sv;
#if defined(REINDEX_WITH_TSAN) || defined(REINDEX_WITH_ASAN)
	constexpr static size_t kDimension = 64;
	constexpr static size_t kMaxElements = 1'000;
#else
	constexpr static size_t kDimension = 1'024;
	constexpr static size_t kMaxElements = 3'000;
#endif
	constexpr static size_t kM = 16;
	constexpr static size_t kEfConstruction = 200;

	rt.OpenNamespace(kNsName);
	rt.DefineNamespaceDataset(kNsName, {
										   IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts{}.PK(), 0},
										   IndexDeclaration{kFieldNameHnsw, "hnsw", "float_vector",
															IndexOpts{}.SetFloatVector(IndexHnsw, FloatVectorIndexOpts{}
																									  .SetDimension(kDimension)
																									  .SetStartSize(100)
																									  .SetM(kM)
																									  .SetEfConstruction(kEfConstruction)
																									  .SetMetric(GetParam())),
															0},
									   });

	checkEmptyIndexSelection<reindexer::HnswSearchParams, kDimension>(kNsName, kFieldNameHnsw);

	std::unordered_set<int> emptyVectors;
	upsertItems<kDimension>(kNsName, kFieldNameHnsw, 0, kMaxElements, emptyVectors);
	auto res = rt.UpdateQR(reindexer::Query(kNsName).Set("non_idx", 12345).Where(kFieldNameId, CondEq, 1));
	ASSERT_EQ(res.Count(), 1);
	const auto deleted = deleteSomeItems(kNsName, kMaxElements, emptyVectors);
	runMultithreadQueries<kDimension>(4, 20, kNsName, kFieldNameHnsw, [&](std::optional<size_t> k, std::optional<float> radius) {
		return reindexer::HnswSearchParams{}.K(k).Radius(radius);
	});
	checkIndexMemstat(rt, kNsName, kFieldNameHnsw, kDimension, kMaxElements - deleted, emptyVectors);
}
CATCH_AND_ASSERT

TEST_F(FloatVector, HnswIndexMTRace) try {
	constexpr static auto kNsName = "hnsw_mt_race_ns"sv;
	constexpr static auto kFieldNameHnsw = "hnsw"sv;
	constexpr static size_t kDimension = 8;
	constexpr static size_t kM = 16;
	constexpr static size_t kSelectThreads = 2;
#if defined(REINDEX_WITH_ASAN) || defined(REINDEX_WITH_TSAN) || defined(RX_WITH_STDLIB_DEBUG)
	constexpr static size_t kTransactions = 5;
	constexpr static size_t kEfConstruction = 100;
#else
	constexpr static size_t kTransactions = 20;
	constexpr static size_t kEfConstruction = 200;
#endif

	rt.OpenNamespace(kNsName);
	rt.DefineNamespaceDataset(
		kNsName,
		{
			IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts{}.PK(), 0},
			IndexDeclaration{kFieldNameHnsw, "hnsw", "float_vector",
							 IndexOpts{}.SetFloatVector(IndexHnsw, FloatVectorIndexOpts{}
																	   .SetDimension(kDimension)
																	   .SetMultithreading(MultithreadingMode::MultithreadTransactions)
																	   .SetM(kM)
																	   .SetEfConstruction(kEfConstruction)
																	   .SetMetric(reindexer::VectorMetric::Cosine)),
							 0},
		});

	unsigned id = 0;
	std::unordered_map<IdType, std::string> expectedData;

	std::atomic_bool terminate{false};
	std::vector<std::thread> selectThreads;
	selectThreads.reserve(kSelectThreads);
	auto selectFn = [this, &terminate] {
		while (!terminate) {
			std::array<float, kDimension> buf;
			::rndFloatVector(buf);
			auto k = rand() % 100 + 10;
			const auto result = rt.Select(reindexer::Query{kNsName}.WhereKNN(kFieldNameHnsw, reindexer::ConstFloatVectorView{buf},
																			 reindexer::HnswSearchParams{}.K(k).Ef(k)));
			checkOrdering(result, reindexer::VectorMetric::Cosine);
			std::this_thread::sleep_for(std::chrono::milliseconds(10));
		}
	};

	// Deletes + updates + upserts
	for (size_t i = 0; i < kTransactions; ++i) {
		if (i == 1) {
			for (size_t j = 0; j < kSelectThreads; ++j) {
				selectThreads.emplace_back(selectFn);
			}
		}

		auto tx = rt.NewTransaction(kNsName);
		std::unordered_set<int> emptyVectors;
		int totalNewItems = 0;
		for (size_t j = 0; j < unsigned(rand() % 50 + 250); ++j) {
			if (id < 10 || rand() % 10) {
				// Insert new item
				auto item = newItem<kDimension>(kNsName, kFieldNameHnsw, id, emptyVectors);
				std::string json(item.GetJSON());
				expectedData[id] = std::move(json);
				auto err = tx.Upsert(std::move(item));
				ASSERT_TRUE(err.ok()) << err.what();
				++id;
				++totalNewItems;
			}
			if (id > 10 && rand() % 8 == 0) {
				// Update existing id
				auto updID = rand() % id;
				auto item = newItem<kDimension>(kNsName, kFieldNameHnsw, updID, emptyVectors);
				std::string json(item.GetJSON());
				if (auto found = expectedData.find(updID); found != expectedData.end()) {
					found->second = std::move(json);
				}
				auto err = tx.Update(std::move(item));
				ASSERT_TRUE(err.ok()) << err.what();
			}
			if (id > 10 && rand() % 8 == 0) {
				// Delete existing id
				auto delID = rand() % id;
				auto item = newItem<kDimension>(kNsName, kFieldNameHnsw, delID, emptyVectors);
				if (expectedData.erase(delID)) {
					--totalNewItems;
				}
				auto err = tx.Delete(std::move(item));
				ASSERT_TRUE(err.ok()) << err.what();
				--totalNewItems;
			}
			if (rand() % 20 == 0) {
				// Delete non existing id
				auto delID = id + rand() % 1000 + 1000;
				auto item = newItem<kDimension>(kNsName, kFieldNameHnsw, delID, emptyVectors);
				ASSERT_EQ(expectedData.erase(delID), 0);
				auto err = tx.Delete(std::move(item));
				ASSERT_TRUE(err.ok()) << err.what();
			}
		}
		while (totalNewItems < 201) {
			// Insert more items to force multithreading insertion
			auto item = newItem<kDimension>(kNsName, kFieldNameHnsw, id, emptyVectors);
			std::string json(item.GetJSON());
			expectedData[id] = std::move(json);
			auto err = tx.Insert(std::move(item));
			ASSERT_TRUE(err.ok()) << err.what();
			++id;
			++totalNewItems;
		}
		std::ignore = rt.CommitTransaction(tx);
		std::this_thread::sleep_for(std::chrono::milliseconds(50));
	}

	std::this_thread::sleep_for(std::chrono::seconds(3));
	for (auto& th : selectThreads) {
		terminate = true;
		th.join();
	}

	auto qr = rt.Select(reindexer::Query(kNsName).SelectAllFields());
	EXPECT_EQ(qr.Count(), expectedData.size());
	for (auto& it : qr) {
		auto item = it.GetItem(false);
		auto id = item["id"].As<int>();
		auto json = item.GetJSON();
		auto found = expectedData.find(id);
		EXPECT_NE(found, expectedData.end()) << "Item with unexpected ID in namespace: " << json;
		EXPECT_EQ(found->second, json) << "Unexpected item's content in namespace";
		expectedData.erase(found);
	}
	EXPECT_EQ(expectedData.size(), 0);
	for (auto& item : expectedData) {
		TestCout() << "Missing item: " << item.second << std::endl;
	}
}
CATCH_AND_ASSERT

TEST_P(FloatVector, VecBruteforceIndex) try {
	constexpr static auto kNsName = "vec_bf_ns"sv;
	constexpr static auto kFieldNameVec = "vec"sv;
#if defined(REINDEX_WITH_TSAN) || defined(REINDEX_WITH_ASAN)
	constexpr static size_t kDimension = 64;
	constexpr static size_t kMaxElements = 5'000;
#else
	constexpr static size_t kDimension = 1'024;
	constexpr static size_t kMaxElements = 10'000;
#endif

	rt.OpenNamespace(kNsName);
	rt.DefineNamespaceDataset(
		kNsName, {
					 IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts{}.PK(), 0},
					 IndexDeclaration{kFieldNameVec, "vec_bf", "float_vector",
									  IndexOpts{}.SetFloatVector(
										  IndexVectorBruteforce,
										  FloatVectorIndexOpts{}.SetDimension(kDimension).SetStartSize(100).SetMetric(GetParam())),
									  0},
				 });

	checkEmptyIndexSelection<reindexer::BruteForceSearchParams, kDimension>(kNsName, kFieldNameVec);

	std::unordered_set<int> emptyVectors;
	upsertItems<kDimension>(kNsName, kFieldNameVec, 0, kMaxElements, emptyVectors);
	const auto deleted = deleteSomeItems(kNsName, kMaxElements, emptyVectors);
	runMultithreadQueries<kDimension>(4, 20, kNsName, kFieldNameVec, [&](std::optional<size_t> k, std::optional<float> radius) {
		return reindexer::BruteForceSearchParams{}.K(k).Radius(radius);
	});
	checkIndexMemstat(rt, kNsName, kFieldNameVec, kDimension, kMaxElements - deleted, emptyVectors);
}
CATCH_AND_ASSERT

TEST_P(FloatVector, IvfIndex) try {
	constexpr static auto kNsName = "ivf_ns"sv;
	constexpr static auto kFieldNameIvf = "ivf"sv;
#if defined(REINDEX_WITH_TSAN) || defined(REINDEX_WITH_ASAN)
	constexpr static size_t kDimension = 64;
	constexpr static size_t kMaxElements = 5'000;
#else
	constexpr static size_t kDimension = 1'024;
	constexpr static size_t kMaxElements = 10'000;
#endif
	std::array<float, kDimension> buf;

	rt.OpenNamespace(kNsName);
	rt.DefineNamespaceDataset(
		kNsName,
		{
			IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts{}.PK(), 0},
			IndexDeclaration{
				kFieldNameIvf, "ivf", "float_vector",
				IndexOpts{}.SetFloatVector(
					IndexIvf, FloatVectorIndexOpts{}.SetDimension(kDimension).SetNCentroids(kMaxElements / 50).SetMetric(GetParam())),
				0},
		});

	checkEmptyIndexSelection<reindexer::IvfSearchParams, kDimension>(kNsName, kFieldNameIvf);

	std::unordered_set<int> emptyVectors;
	upsertItems<kDimension>(kNsName, kFieldNameIvf, 0, kMaxElements, emptyVectors);
	const auto deleted = deleteSomeItems(kNsName, kMaxElements, emptyVectors);
	::rndFloatVector(buf);
	for (size_t iter = 0; iter < 2; ++iter) {
		size_t k = rand() % kMaxElements + 10;
		std::optional<float> radius = std::nullopt;
		for ([[maybe_unused]] auto _ : {0, 1}) {
			const auto result = rt.Select(reindexer::Query{kNsName}.WhereKNN(kFieldNameIvf, reindexer::ConstFloatVectorView{buf},
																			 reindexer::IvfSearchParams{}.K(k).Radius(radius).NProbe(32)));
			checkOrdering(result, GetParam(), radius);
			radius = (result.begin() + result.Count() / 2).GetItemRefRanked().Rank().Value();
		}
		checkIndexMemstat(rt, kNsName, kFieldNameIvf, kDimension, kMaxElements - deleted, emptyVectors);
		if (iter == 0) {
			rebuildCentroids();
		}
	}
	runMultithreadQueries<kDimension>(4, 20, kNsName, kFieldNameIvf, [&](std::optional<size_t> k, std::optional<float> radius) {
		return reindexer::IvfSearchParams{}.K(k).Radius(radius).NProbe(32);
	});
}
CATCH_AND_ASSERT

TEST_F(FloatVector, HnswIndexUpdateQuery) try {
	constexpr static auto kNsName = "hnsw_ns"sv;
	constexpr static auto kFieldNameHnsw = "hnsw"sv;

	rt.OpenNamespace(kNsName);
	rt.DefineNamespaceDataset(
		kNsName, {
					 IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts{}.PK(), 0},
					 IndexDeclaration{kFieldNameHnsw, kFieldNameHnsw, "float_vector",
									  IndexOpts{}.SetFloatVector(
										  IndexHnsw, FloatVectorIndexOpts{}.SetDimension(8).SetM(16).SetEfConstruction(200).SetMetric(
														 reindexer::VectorMetric::L2)),
									  0},
				 });

	rt.UpsertJSON(kNsName, R"json({"id":0})json");
	rt.UpsertJSON(kNsName, R"json({"id":1,"hnsw":null})json");
	rt.UpsertJSON(kNsName, R"json({"id":2,"hnsw":[]})json");
	std::array<float, 8> buf;
	::rndFloatVector(buf);
	const reindexer::ConstFloatVectorView setVec{buf};
	const auto updateQuery = reindexer::Query(kNsName).Set(kFieldNameHnsw, setVec);
	SCOPED_TRACE(updateQuery.GetSQL());
	auto res = rt.UpdateQR(updateQuery);
	EXPECT_EQ(res.Count(), 3);
	validateIndexValueInQueryResults(kFieldNameHnsw, res, setVec.Span());

	res = rt.Select(reindexer::Query(kNsName).SelectAllFields());
	EXPECT_EQ(res.Count(), 3);
	validateIndexValueInQueryResults("hnsw", res, setVec.Span());
}
CATCH_AND_ASSERT

TEST_F(FloatVector, HnswIndexItemSet) try {
	constexpr static auto kNsName = "hnsw_ns"sv;
	constexpr static auto kFieldNameHnsw = "hnsw"sv;

	rt.OpenNamespace(kNsName);
	rt.DefineNamespaceDataset(
		kNsName, {
					 IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts{}.PK(), 0},
					 IndexDeclaration{kFieldNameHnsw, kFieldNameHnsw, "float_vector",
									  IndexOpts{}.SetFloatVector(
										  IndexHnsw, FloatVectorIndexOpts{}.SetDimension(8).SetM(16).SetEfConstruction(200).SetMetric(
														 reindexer::VectorMetric::L2)),
									  0},
				 });

	constexpr std::string_view kJSONs[] = {R"json({"id":0})json", R"json({"id":1,"hnsw":null})json", R"json({"id":2,"hnsw":[]})json"};

	std::array<float, 8> buf;
	::rndFloatVector(buf);
	const reindexer::ConstFloatVectorView vec{buf};

	for (auto& json : kJSONs) {
		SCOPED_TRACE(json);
		auto item = rt.NewItem(kNsName);
		auto err = item.FromJSON(json);
		ASSERT_TRUE(err.ok()) << err.what();
		item[kFieldNameHnsw] = vec;
		validateIndexValueInItem(kNsName, kFieldNameHnsw, item.GetJSON(), vec.Span());
	}
}
CATCH_AND_ASSERT

TEST_F(FloatVector, ParseDslIndexDef) try {
	using namespace std::string_literals;
	const auto indexDef = reindexer::IndexDef::FromJSON(R"json(
{
	"name":"hnsw",
	"json_paths":["hnsw"]
	"field_type":"float_vector",
	"index_type":"hnsw",
	"is_pk":false,
	"is_array":false,
	"is_dense":false,
	"is_sparse":false,
	"collate_mode":"none",
	"sort_order_letters":"",
	"expire_after":0,
	"config":{
		"dimension":2048,
		"metric":"l2",
		"start_size":100,
		"ef_construction":200,
		"m":16,
	}
}
)json"sv);
	ASSERT_TRUE(indexDef) << indexDef.error().what();
}
CATCH_AND_ASSERT

std::pair<reindexer::FloatVector, std::string> FloatVector::rndFloatVectorForSerializers(bool withSpace) {
	constexpr static size_t kDimension = 2'048;
	std::array<float, kDimension> buf;
	::rndFloatVector(buf);
	reindexer::WrSerializer ser;
	for (auto it = buf.begin(); it != buf.end(); ++it) {
		if (it != buf.begin()) {
			ser << ',';
			if (withSpace) {
				ser << ' ';
			}
		}
		ser << *it;
	}
	return {reindexer::FloatVector{buf}, std::string{ser.Slice()}};
}

TEST_F(FloatVector, DslQuery) try {
	using namespace std::string_view_literals;
	using reindexer::Query;
	using reindexer::KnnSearchParams;
	const auto [vec, vecStr] = rndFloatVectorForSerializers(false);
	struct {
		Query query;
		std::string dsl;
	} testData[]{
		{Query("ns"sv).WhereKNN("hnsw"sv, vec.View(), reindexer::HnswSearchParams{}.K(6'101).Ef(100'000)).SelectAllFields(),
		 R"json({"namespace":"ns","limit":-1,"offset":0,"req_total":"disabled","explain":false,"type":"select","select_with_rank":false,"select_filter":["*","vectors()"],"select_functions":[],"sort":[],"filters":[{"op":"and","cond":"knn","field":"hnsw","value":[)json" +
			 vecStr + R"json(],"params":{"k":6101,"ef":100000}}],"merge_queries":[],"aggregations":[]})json"},
		{Query("ns"sv).WhereKNN("bf"sv, vec.View(), reindexer::BruteForceSearchParams{}.K(8'317).Radius(1.0)).Select("vectors()"),
		 R"json({"namespace":"ns","limit":-1,"offset":0,"req_total":"disabled","explain":false,"type":"select","select_with_rank":false,"select_filter":["vectors()"],"select_functions":[],"sort":[],"filters":[{"op":"and","cond":"knn","field":"bf","value":[)json" +
			 vecStr + R"json(],"params":{"k":8317,"radius":1.0}}],"merge_queries":[],"aggregations":[]})json"},
		{Query("ns"sv).WhereKNN("ivf"sv, vec.View(), reindexer::IvfSearchParams{}.K(5'125).NProbe(5)).Select({"ivf", "vectors()"}),
		 R"json({"namespace":"ns","limit":-1,"offset":0,"req_total":"disabled","explain":false,"type":"select","select_with_rank":false,"select_filter":["ivf","vectors()"],"select_functions":[],"sort":[],"filters":[{"op":"and","cond":"knn","field":"ivf","value":[)json" +
			 vecStr + R"json(],"params":{"k":5125,"nprobe":5}}],"merge_queries":[],"aggregations":[]})json"}};

	for (const auto& [query, expectedDsl] : testData) {
		const auto generatedDsl = query.GetJSON();
		EXPECT_EQ(generatedDsl, expectedDsl);
		const auto parsedQuery = Query::FromJSON(expectedDsl);
		EXPECT_EQ(query, parsedQuery);
	}
}
CATCH_AND_ASSERT

TEST_F(FloatVector, SqlQuery) try {
	using namespace std::string_view_literals;
	using reindexer::Query;
	using reindexer::KnnSearchParams;
	const auto [vec, vecStr] = rndFloatVectorForSerializers(true);
	struct {
		Query query;
		std::string sql;
	} testData[]{{Query("ns"sv).WhereKNN("hnsw"sv, vec.View(), reindexer::HnswSearchParams{}.K(4'291).Ef(100'000)).SelectAllFields(),
				  "SELECT *, vectors() FROM ns WHERE KNN(hnsw, [" + vecStr + "], k=4291, ef=100000)"},
				 {Query("ns"sv).WhereKNN("bf"sv, vec.View(), reindexer::BruteForceSearchParams{}.K(8'184).Radius(1.2f)).Select("vectors()"),
				  "SELECT vectors() FROM ns WHERE KNN(bf, [" + vecStr + "], k=8184, radius=1.2)"},
				 {Query("ns"sv).WhereKNN("ivf"sv, vec.View(), reindexer::IvfSearchParams{}.K(823).NProbe(5)).Select({"hnsw", "vectors()"}),
				  "SELECT hnsw, vectors() FROM ns WHERE KNN(ivf, [" + vecStr + "], k=823, nprobe=5)"}};
	for (const auto& [query, expectedSql] : testData) {
		const auto generatedSql = query.GetSQL();
		EXPECT_EQ(generatedSql, expectedSql);
		const auto parsedQuery = Query::FromSQL(expectedSql);
		EXPECT_EQ(parsedQuery, query) << "original: " << expectedSql << "\nparsed: " << parsedQuery.GetSQL();
	}
}
CATCH_AND_ASSERT

TEST_P(FloatVector, Queries) try {
	constexpr static auto kNsName = "fv_queries_ns"sv;
	constexpr static auto kFieldNameBool = "bool"sv;
	const static std::string kFieldNameIvf = "ivf";
#if defined(REINDEX_WITH_TSAN) || defined(REINDEX_WITH_ASAN)
	constexpr static size_t kDimension = 64;
	constexpr static size_t kMaxElements = 5'000;
#else
	constexpr static size_t kDimension = 1'024;
	constexpr static size_t kMaxElements = 10'000;
#endif
	std::array<float, kDimension> buf;
	const auto params = reindexer::IvfSearchParams{}.K(kMaxElements).NProbe(32);

	rt.OpenNamespace(kNsName);
	rt.DefineNamespaceDataset(
		kNsName,
		{
			IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts{}.PK(), 0},
			IndexDeclaration{kFieldNameBool, "-", "bool", IndexOpts{}, 0},
			IndexDeclaration{
				kFieldNameIvf, "ivf", "float_vector",
				IndexOpts{}.SetFloatVector(
					IndexIvf, FloatVectorIndexOpts{}.SetDimension(kDimension).SetNCentroids(kMaxElements / 50).SetMetric(GetParam())),
				0},
		});

	size_t i = 0;
	std::unordered_set<int> emptyVectors;
	for (; i < kMaxElements / 3; ++i) {
		auto item = newItem<kDimension>(kNsName, kFieldNameIvf, i, emptyVectors);
		item[kFieldNameBool] = bool(rand() % 2);
		rt.Upsert(kNsName, item);
	}
	for (; i < 2 * kMaxElements / 3; ++i) {
		auto item = newItem<kDimension>(kNsName, kFieldNameIvf, i, emptyVectors);
		item[kFieldNameBool] = bool(rand() % 2);
		rt.Upsert(kNsName, item);
	}
	for (; i < kMaxElements; ++i) {
		auto item = newItem<kDimension>(kNsName, kFieldNameIvf, i, emptyVectors);
		item[kFieldNameBool] = bool(rand() % 2);
		rt.Upsert(kNsName, item);
	}
	::rndFloatVector(buf);
	auto result = rt.Select(
		reindexer::Query{kNsName}.Where(kFieldNameId, CondGt, 5).WhereKNN(kFieldNameIvf, reindexer::ConstFloatVectorView{buf}, params));
	checkOrdering(result, GetParam());
	::rndFloatVector(buf);
	result = rt.Select(reindexer::Query{kNsName}
						   .Where(kFieldNameBool, CondEq, true)
						   .OpenBracket()
						   .Where(kFieldNameId, CondGt, 5)
						   .WhereKNN(kFieldNameIvf, reindexer::ConstFloatVectorView{buf}, params)
						   .CloseBracket());
	checkOrdering(result, GetParam());
	::rndFloatVector(buf);
	result =
		rt.Select(reindexer::Query{kNsName}.WhereKNN(kFieldNameIvf, reindexer::ConstFloatVectorView{buf}, params).Sort("rank()", false));
	::rndFloatVector(buf);
	result = rt.Select(reindexer::Query{kNsName}
						   .WhereKNN(kFieldNameIvf, reindexer::ConstFloatVectorView{buf}, params)
						   .Sort("rank(" + kFieldNameIvf + ')', false));
	::rndFloatVector(buf);
	std::map<IdType, reindexer::RankT> ranks;
	result = rt.Select(reindexer::Query{kNsName}.WhereKNN(kFieldNameIvf, reindexer::ConstFloatVectorView{buf}, params).WithRank());
	for (auto& i : result) {
		const auto& item = i.GetItemRefRanked();
		ranks[item.NotRanked().Id()] = item.Rank();
	}
	result = rt.Select(reindexer::Query{kNsName}
						   .Where(kFieldNameBool, CondEq, true)
						   .OpenBracket()
						   .Where(kFieldNameId, CondGt, 5)
						   .WhereKNN(kFieldNameIvf, reindexer::ConstFloatVectorView{buf}, params)
						   .CloseBracket()
						   .WithRank());
	for (auto& i : result) {
		const auto& item = i.GetItemRefRanked();
		const auto it = ranks.find(item.NotRanked().Id());
		if (it == ranks.end()) {
			EXPECT_NE(it, ranks.end()) << item.NotRanked().Id() << ' ' << item.Rank().Value();
		} else {
			EXPECT_EQ(it->second, item.Rank()) << item.NotRanked().Id();
		}
	}

	::rndFloatVector(buf);
	result = rt.Select(reindexer::Query{kNsName}
						   .Where(kFieldNameId, CondLt, kMaxElements / 2.0)
						   .WhereKNN(kFieldNameIvf, reindexer::ConstFloatVectorView{buf}, params)
						   .Merge(reindexer::Query{kNsName}
									  .Where(kFieldNameId, CondGt, kMaxElements / 2.0)
									  .WhereKNN(kFieldNameIvf, reindexer::ConstFloatVectorView{buf}, params)));
}
CATCH_AND_ASSERT

TEST_F(FloatVector, DeleteIndex) try {
	constexpr static auto kNsName = "fv_delete_index_ns"sv;
	constexpr static auto kFieldNameBool = "bool"sv;
	constexpr static auto kFieldNameIvf = "ivf"sv;
	constexpr static size_t kDimension = 10;
	constexpr static size_t kItemCount = 10;

	rt.OpenNamespace(kNsName);
	rt.DefineNamespaceDataset(
		kNsName,
		{
			IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts{}.PK(), 0},
			IndexDeclaration{kFieldNameBool, "hash", "int", IndexOpts{}, 0},
			IndexDeclaration{
				kFieldNameIvf, "ivf", "float_vector",
				IndexOpts{}.SetFloatVector(
					IndexIvf,
					FloatVectorIndexOpts{}.SetDimension(kDimension).SetNCentroids(5).SetMetric(reindexer::VectorMetric::InnerProduct)),
				0},
		});

	std::unordered_set<int> emptyVectors;
	for (size_t i = 0; i < kItemCount; ++i) {
		auto item = newItem<kDimension>(kNsName, kFieldNameIvf, i, emptyVectors);
		item[kFieldNameBool] = rand() % 100;
		rt.Upsert(kNsName, item);
	}

	rt.DropIndex(kNsName, kFieldNameBool);
	rt.DropIndex(kNsName, kFieldNameIvf);
}
CATCH_AND_ASSERT

static std::string indexName(size_t idxNumber) {
	constexpr static const char* kIndexNameBase = "fv_";
	return kIndexNameBase + std::to_string(idxNumber);
}

constexpr static const char* kObjectName = "object";

static std::string fieldName(size_t idxNumber) {
	constexpr static const char* kFieldNameBase = "fv_field_";
	return kFieldNameBase + std::to_string(idxNumber);
}

static std::string fieldPath(size_t idxNumber) {
	if (idxNumber % 2 == 0) {
		std::string prefix = kObjectName;
		if (idxNumber % 4 == 0) {
			prefix += '.';
			prefix += kObjectName;
		}
		return prefix + '.' + fieldName(idxNumber);
	} else {
		return fieldName(idxNumber);
	}
}

template <size_t Dimension>
reindexer::FloatVector FloatVector::rndFloatVector() {
	std::array<float, Dimension> buf;
	if (rand() % 10 != 0) {
		::rndFloatVector(buf);
		return reindexer::FloatVector{reindexer::ConstFloatVectorView{buf}};
	} else {
		return {};
	}
}

template <size_t Dimension>
reindexer::Item FloatVector::newItem(std::string_view nsName, size_t fieldsCount, std::vector<std::vector<reindexer::FloatVector>>& items) {
	const int id = items.size();
	items.emplace_back();
	return newItem<Dimension>(nsName, fieldsCount, id, items);
}

static void putJsonField(reindexer::JsonBuilder& json, std::string_view fieldName, reindexer::ConstFloatVectorView value) {
	if (!value.IsEmpty() || rand() % 3 == 0) {
		json.Array(fieldName, value.Span());
	} else if (rand() % 2 == 0) {
		json.Null(fieldName);
	}
}

static int regularFieldValue(int id) noexcept { return id + 100; }

template <size_t Dimension>
reindexer::Item FloatVector::newItem(std::string_view nsName, size_t fieldsCount, int id,
									 std::vector<std::vector<reindexer::FloatVector>>& items) {
	assertrx(size_t(id) < items.size());
	auto& vectors = items[id];
	vectors.clear();
	vectors.reserve(fieldsCount);
	for (size_t i = 0; i < fieldsCount; ++i) {
		vectors.emplace_back(rndFloatVector<Dimension>());
	}
	reindexer::WrSerializer ser;
	{
		reindexer::JsonBuilder json(ser);
		json.Put(kFieldNameId, id);
		json.Put(kFieldNameRegular, regularFieldValue(id));
		for (size_t i = 1; i < fieldsCount; i += 2) {
			putJsonField(json, fieldName(i), vectors[i].View());
		}
		{
			auto obj = json.Object(kObjectName);
			for (size_t i = 2; i < fieldsCount; i += 4) {
				putJsonField(obj, fieldName(i), vectors[i].View());
			}
			{
				auto nestedObj = obj.Object(kObjectName);
				for (size_t i = 0; i < fieldsCount; i += 4) {
					putJsonField(nestedObj, fieldName(i), vectors[i].View());
				}
			}
		}
	}
	auto resultItem = rt.NewItem(nsName);
	const auto err = resultItem.FromJSON(ser.Slice());
	if (!err.ok()) {
		throw err;
	}
	return resultItem;
}

static void checkItemAfterUpdate(reindexer::Item& item, int expectedId, const size_t indexesCount,
								 const std::vector<std::vector<reindexer::FloatVector>>& vectors) {
	const auto id = item[kFieldNameId].As<int>();
	ASSERT_EQ(id, expectedId);
	ASSERT_EQ(item[kFieldNameRegular].As<int>(), regularFieldValue(id));
	ASSERT_LT(id, vectors.size());
	const std::vector<reindexer::FloatVector>& currentData = vectors[id];
	const auto json = item.GetJSON();

	gason::JsonParser parser;
	auto parsedJson = parser.Parse(json);

	ASSERT_JSON_FIELD_INT_EQ(parsedJson, kFieldNameId, id);
	ASSERT_JSON_FIELD_INT_EQ(parsedJson, kFieldNameRegular, regularFieldValue(id));

	for (size_t j = 0; j < indexesCount; ++j) {
		ASSERT_JSON_FIELD_ARRAY_EQ(parsedJson, fieldPath(j), currentData[j].Span());
		const auto foundSpan = item[indexName(j)].As<reindexer::ConstFloatVectorView>().Span();
		const auto expectedSpan = currentData[j].Span();
		ASSERT_EQ(foundSpan.size(), expectedSpan.size());
		for (size_t i = 0, s = foundSpan.size(); i < s; ++i) {
			ASSERT_EQ(foundSpan[i], expectedSpan[i]);
		}
	}
}

static void checkQRAfterUpdate(const reindexer::QueryResults& qr, int expectedId, const size_t indexesCount,
							   const std::vector<std::vector<reindexer::FloatVector>>& vectors) {
	ASSERT_EQ(qr.Count(), 1);
	for (auto it : qr) {
		ASSERT_TRUE(it.Status().ok()) << it.Status().what();
		auto item = it.GetItem();
		checkItemAfterUpdate(item, expectedId, indexesCount, vectors);
	}
}

static void checkItemAfterDelete(reindexer::Item& item, int expectedId, const size_t indexesCount) {
	const auto id = item[kFieldNameId].As<int>();
	ASSERT_EQ(id, expectedId);
	const auto json = item.GetJSON();
	gason::JsonParser parser;
	auto parsedJson = parser.Parse(json);
	ASSERT_JSON_FIELD_INT_EQ(parsedJson, kFieldNameId, id);
	ASSERT_JSON_FIELD_IS_NULL(parsedJson, kFieldNameRegular);

	for (size_t j = 0; j < indexesCount; ++j) {
		ASSERT_JSON_FIELD_IS_NULL(parsedJson, fieldPath(j));
	}
}

reindexer::Item FloatVector::itemForDelete(std::string_view nsName, int id) {
	reindexer::WrSerializer ser;
	{
		reindexer::JsonBuilder json(ser);
		json.Put(kFieldNameId, id);
	}
	auto item = rt.NewItem(nsName);
	const auto err = item.FromJSON(ser.Slice());
	if (!err.ok()) {
		throw err;
	}
	return item;
}

TEST_F(FloatVector, SelectFilters) try {
	constexpr static auto kNsName = "fv_select_filters_ns"sv;
	constexpr static size_t kDimension = 5;
	constexpr static size_t kMaxElements = 1'000;
	constexpr static size_t kIndexesCount = 20;

	std::vector<std::pair<IndexType, IndexOpts>> indexOpts{
		{IndexIvf, IndexOpts{}.SetFloatVector(IndexIvf, FloatVectorIndexOpts{}
															.SetDimension(kDimension)
															.SetNCentroids(kMaxElements / 3)
															.SetMetric(reindexer::VectorMetric::InnerProduct))},
		{IndexHnsw,
		 IndexOpts{}.SetFloatVector(
			 IndexHnsw,
			 FloatVectorIndexOpts{}.SetDimension(kDimension).SetM(16).SetEfConstruction(50).SetMetric(reindexer::VectorMetric::L2))},
		{IndexVectorBruteforce,
		 IndexOpts{}.SetFloatVector(IndexVectorBruteforce,
									FloatVectorIndexOpts{}.SetDimension(kDimension).SetMetric(reindexer::VectorMetric::Cosine))}};

	rt.OpenNamespace(kNsName);
	rt.DefineNamespaceDataset(kNsName, {IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts{}.PK(), 0},
										{IndexDeclaration{kFieldNameRegular, "hash", "int", IndexOpts{}, 0}}});
	for (size_t i = 0; i < kIndexesCount; ++i) {
		const auto& idxOpts = indexOpts[rand() % indexOpts.size()];
		rt.AddIndex(kNsName, reindexer::IndexDef{indexName(i), {fieldPath(i)}, idxOpts.first, idxOpts.second});
	}

	std::vector<std::vector<reindexer::FloatVector>> vectors;

	vectors.reserve(kMaxElements);

	for (size_t i = 0; i < kMaxElements; ++i) {
		auto item = newItem<kDimension>(kNsName, kIndexesCount, vectors);
		rt.Upsert(kNsName, item);
	}

	static std::random_device rd;
	static std::mt19937 g(rd());
	for (int i = 0; i < 20; ++i) {
		// select
		std::vector<size_t> selectFieldsNumbers;
		selectFieldsNumbers.reserve(kIndexesCount);
		for (size_t j = 0; j < kIndexesCount; ++j) {
			selectFieldsNumbers.emplace_back(j);
		}
		std::shuffle(selectFieldsNumbers.begin(), selectFieldsNumbers.end(), g);
		const size_t fieldsCountInFilter = rand() % (kIndexesCount + 1);
		selectFieldsNumbers.erase(selectFieldsNumbers.begin() + fieldsCountInFilter, selectFieldsNumbers.end());
		const bool allVector = rand() % 10 == 0;
		auto query = reindexer::Query{kNsName};
		if (allVector) {
			query.Select("vectors()");
		}
		for (auto j : selectFieldsNumbers) {
			query.Select(fieldPath(j));
		}
		const auto result = rt.Select(query);
		for (auto& it : result) {
			ASSERT_TRUE(it.Status().ok()) << it.Status().what();
			auto item = it.GetItem();
			const auto id = item[kFieldNameId].As<int>();
			ASSERT_LT(id, vectors.size());
			ASSERT_EQ(item[kFieldNameRegular].As<int>(), regularFieldValue(id));
			const std::vector<reindexer::FloatVector>& currentData = vectors[id];
			const auto json = item.GetJSON();

			gason::JsonParser parser;
			auto parsedJson = parser.Parse(json);

			ASSERT_JSON_FIELD_INT_EQ(parsedJson, kFieldNameId, id);
			ASSERT_JSON_FIELD_INT_EQ(parsedJson, kFieldNameRegular, regularFieldValue(id));

			std::unordered_set<size_t> returnedFields;
			if (allVector) {
				for (size_t j = 0; j < kIndexesCount; ++j) {
					returnedFields.insert(j);
				}
			} else {
				for (size_t j : selectFieldsNumbers) {
					returnedFields.insert(j);
				}
			}

			for (size_t j = 0; j < kIndexesCount; ++j) {
				if (returnedFields.count(j) == 0) {
					ASSERT_JSON_FIELD_ABSENT_OR_IS_NULL(parsedJson, fieldPath(j));
					EXPECT_THROW([[maybe_unused]] reindexer::Variant _{item[indexName(j)]}, reindexer::Error);
				} else {
					ASSERT_JSON_FIELD_ARRAY_EQ(parsedJson, fieldPath(j), currentData[j].Span());
					const auto foundSpan = item[indexName(j)].As<reindexer::ConstFloatVectorView>().Span();
					const auto expectedSpan = currentData[j].Span();
					ASSERT_EQ(foundSpan.size(), expectedSpan.size());
					for (size_t i = 0, s = foundSpan.size(); i < s; ++i) {
						ASSERT_EQ(foundSpan[i], expectedSpan[i]);
					}
				}
			}
		}

		// update
		{
			const auto id = rand() % kMaxElements;
			auto item = newItem<kDimension>(kNsName, kIndexesCount, id, vectors);
			rt.Upsert(kNsName, item);
			checkItemAfterUpdate(item, id, kIndexesCount, vectors);
		}
		{
			const auto id = rand() % kMaxElements;
			auto item = newItem<kDimension>(kNsName, kIndexesCount, id, vectors);
			reindexer::QueryResults qr;
			rt.Upsert(kNsName, item, qr);
			checkItemAfterUpdate(item, id, kIndexesCount, vectors);
			checkQRAfterUpdate(qr, id, kIndexesCount, vectors);
		}
		{
			const auto id = rand() % kMaxElements;
			auto item = newItem<kDimension>(kNsName, kIndexesCount, id, vectors);
			rt.Update(kNsName, item);
			checkItemAfterUpdate(item, id, kIndexesCount, vectors);
		}
		{
			const auto id = rand() % kMaxElements;
			auto item = newItem<kDimension>(kNsName, kIndexesCount, id, vectors);
			reindexer::QueryResults qr;
			rt.Update(kNsName, item, qr);
			checkItemAfterUpdate(item, id, kIndexesCount, vectors);
			checkQRAfterUpdate(qr, id, kIndexesCount, vectors);
		}
		{
			const int id = rand() % kMaxElements;
			const auto fldIdx = rand() % kIndexesCount;
			vectors[id][fldIdx] = rndFloatVector<kDimension>();
			const reindexer::ConstFloatVectorView vectView(vectors[id][fldIdx]);

			auto query = reindexer::Query(kNsName)
							 .Set(rand() % 2 == 0 ? fieldPath(fldIdx) : indexName(fldIdx), vectView)
							 .Where(kFieldNameId, CondEq, id);
			reindexer::QueryResults qr;
			rt.Update(query, qr);
			checkQRAfterUpdate(qr, id, kIndexesCount, vectors);
		}
	}

	// delete
	std::vector<int> idsForDelete;
	idsForDelete.reserve(kMaxElements);
	for (size_t i = 0; i < kMaxElements; ++i) {
		idsForDelete.emplace_back(i);
	}
	std::shuffle(idsForDelete.begin(), idsForDelete.end(), g);
	static_assert(kMaxElements > 60);
	idsForDelete.erase(idsForDelete.begin() + 60, idsForDelete.end());
	for (int id : idsForDelete) {
		switch (rand() % 3) {
			case 0: {
				auto item = itemForDelete(kNsName, id);
				rt.Delete(kNsName, item);
				checkItemAfterDelete(item, id, kIndexesCount);
			} break;
			case 1: {
				auto item = itemForDelete(kNsName, id);
				reindexer::QueryResults qr;
				rt.Delete(kNsName, item, qr);
				checkItemAfterDelete(item, id, kIndexesCount);
				checkQRAfterUpdate(qr, id, kIndexesCount, vectors);
			} break;
			default: {
				reindexer::QueryResults qr;
				rt.Delete(reindexer::Query(kNsName).Where(kFieldNameId, CondEq, id), qr);
				checkQRAfterUpdate(qr, id, kIndexesCount, vectors);
			} break;
		}
	}
}
CATCH_AND_ASSERT

TEST_P(FloatVector, UpdateIndex) try {
	constexpr static auto kNsName = "float_vector_index_ns"sv;
	static const std::string kFieldNameFloatVector = "float_vector";
	constexpr static size_t kStartSize = 100;
#if defined(REINDEX_WITH_TSAN) || defined(REINDEX_WITH_ASAN)
	constexpr static size_t kDimension = 32;
#else
	constexpr static size_t kDimension = 1'024;
#endif
	constexpr static size_t kMaxElementsInStep = 100;
	constexpr static size_t kK = kMaxElementsInStep * 5;
	struct {
		reindexer::IndexDef indexDef;
		reindexer::KnnSearchParams searchParams;
	} paramsOfIndexes[]{
		{{kFieldNameFloatVector,
		  {kFieldNameFloatVector},
		  IndexHnsw,
		  IndexOpts{}.SetFloatVector(IndexHnsw, FloatVectorIndexOpts{}
													.SetDimension(kDimension)
													.SetStartSize(kStartSize)
													.SetM(16)
													.SetEfConstruction(200)
													.SetMetric(GetParam()))},
		 reindexer::HnswSearchParams{}.K(kK).Ef(kK * 2)},
		{{kFieldNameFloatVector,
		  {kFieldNameFloatVector},
		  IndexVectorBruteforce,
		  IndexOpts{}.SetFloatVector(IndexVectorBruteforce,
									 FloatVectorIndexOpts{}.SetDimension(kDimension).SetStartSize(kStartSize).SetMetric(GetParam()))},
		 reindexer::BruteForceSearchParams{}.K(kK)},
		{{kFieldNameFloatVector,
		  {kFieldNameFloatVector},
		  IndexIvf,
		  IndexOpts{}.SetFloatVector(IndexIvf, FloatVectorIndexOpts{}
												   .SetDimension(kDimension)
												   .SetNCentroids(std::max<size_t>(kMaxElementsInStep / 30, 2))
												   .SetMetric(GetParam()))},
		 reindexer::IvfSearchParams{}.K(kK).NProbe(std::max<size_t>(kK / 200, 2))},
		{{kFieldNameFloatVector,
		  {kFieldNameFloatVector},
		  IndexIvf,
		  IndexOpts{}.SetFloatVector(
			  IndexIvf, FloatVectorIndexOpts{}.SetDimension(kDimension).SetNCentroids(kMaxElementsInStep * 2).SetMetric(GetParam()))},
		 reindexer::IvfSearchParams{}.K(kK).NProbe(std::max<size_t>(kK / 200, 2))},
	};
	const auto rndIndexParams = [&] { return paramsOfIndexes[rand() % (sizeof(paramsOfIndexes) / sizeof(paramsOfIndexes[0]))]; };
	rt.OpenNamespace(kNsName);
	rt.DefineNamespaceDataset(kNsName, {IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts{}.PK(), 0}});
	int currMaxId = 0;
	std::unordered_set<int> emptyVectors;
	auto updateSomeData = [&, lastMaxId = 0](HasIndex hasIndex) mutable {
		lastMaxId = currMaxId;
		currMaxId += (rand() % kMaxElementsInStep + 1);
		upsertItems<kDimension>(kNsName, kFieldNameFloatVector, lastMaxId, currMaxId, emptyVectors, hasIndex);
		deleteSomeItems(kNsName, currMaxId, emptyVectors);
	};
	auto testQueryNoIndex = [&] {
		auto result = rt.Select(reindexer::Query{kNsName}.Where(kFieldNameId, CondEq, rand() % currMaxId));
		result = rt.Select(reindexer::Query{kNsName}
							   .Strict(StrictModeNone)
							   .Where(kFieldNameId, CondLt, rand() % currMaxId)
							   .Where(kFieldNameFloatVector, CondLt, randBin<float>(-1'000'000, 1'000'000)));
	};
	auto testQueryIndexed = [&](const reindexer::KnnSearchParams& searchParams) {
		auto result = rt.Select(reindexer::Query{kNsName}.Where(kFieldNameId, CondEq, rand() % currMaxId));
		std::array<float, kDimension> buf;
		::rndFloatVector(buf);
		result = rt.Select(reindexer::Query{kNsName}.WhereKNN(kFieldNameFloatVector, reindexer::ConstFloatVectorView{buf}, searchParams));
	};
	updateSomeData(HasIndex::No);
	testQueryNoIndex();
	while (size_t(currMaxId) < kMaxElementsInStep * 40) {
		{
			reindexer::WrSerializer ser;
			const auto& idxParams = rndIndexParams();
			idxParams.indexDef.GetJSON(ser);
			SCOPED_TRACE(ser.Slice());
			rt.AddIndex(kNsName, idxParams.indexDef);
			testQueryIndexed(idxParams.searchParams);
			updateSomeData(HasIndex::Yes);
			testQueryIndexed(idxParams.searchParams);
		}
		{
			reindexer::WrSerializer ser;
			const auto& idxParams = rndIndexParams();
			idxParams.indexDef.GetJSON(ser);
			SCOPED_TRACE(ser.Slice());
			rt.UpdateIndex(kNsName, idxParams.indexDef);
			testQueryIndexed(idxParams.searchParams);
			updateSomeData(HasIndex::Yes);
			testQueryIndexed(idxParams.searchParams);
		}
		{
			rt.DropIndex(kNsName, kFieldNameFloatVector);
			testQueryNoIndex();
			updateSomeData(HasIndex::No);
			testQueryNoIndex();
		}
	}
}
CATCH_AND_ASSERT

TEST_F(FloatVector, WhereCondIsNullIsNotNull) try {
	const static std::string kNsName = "float_vector_cond_ns";
	constexpr static size_t kDimension = 32;
	constexpr static size_t kMaxElements = 1'000;

	std::vector<std::pair<IndexType, IndexOpts>> indexOpts{
		{IndexHnsw,
		 IndexOpts{}.SetFloatVector(
			 IndexHnsw,
			 FloatVectorIndexOpts{}.SetDimension(kDimension).SetM(16).SetEfConstruction(50).SetMetric(reindexer::VectorMetric::L2))},
		{IndexVectorBruteforce,
		 IndexOpts{}.SetFloatVector(IndexVectorBruteforce,
									FloatVectorIndexOpts{}.SetDimension(kDimension).SetMetric(reindexer::VectorMetric::Cosine))},
		{IndexIvf, IndexOpts{}.SetFloatVector(IndexIvf, FloatVectorIndexOpts{}
															.SetDimension(kDimension)
															.SetNCentroids(kMaxElements / 3)
															.SetMetric(reindexer::VectorMetric::InnerProduct))}};
	std::string nsName, idxName;
	std::unordered_set<int> emptyVectors;
	for (const auto& opts : indexOpts) {
		idxName = indexName(opts.first);
		nsName = kNsName + idxName;
		rt.OpenNamespace(nsName);
		rt.DefineNamespaceDataset(nsName, {IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts{}.PK(), 0}});
		rt.AddIndex(nsName, reindexer::IndexDef{idxName, {idxName}, opts.first, opts.second});

		emptyVectors.clear();
		upsertItems<kDimension>(nsName, idxName, 0, kMaxElements, emptyVectors);

		auto qr = rt.Select(reindexer::Query(nsName).Where(idxName, CondEmpty, reindexer::VariantArray{}));
		ASSERT_TRUE(qr.Count() > 0) << idxName;
		ASSERT_EQ(emptyVectors.size(), qr.Count()) << idxName;
		const auto deletedCount = deleteSomeItems(nsName, kMaxElements, emptyVectors);
		ASSERT_TRUE(deletedCount > 0) << idxName;
		qr = rt.Select(reindexer::Query(nsName).Where(idxName, CondEmpty, reindexer::VariantArray{}));
		const auto isNullCount = qr.Count();
		ASSERT_TRUE(isNullCount > 0) << idxName;
		for (auto& it : qr) {
			auto item = it.GetItem(false);
			auto id = item["id"].As<int>();
			ASSERT_TRUE(emptyVectors.end() != emptyVectors.find(id)) << idxName;
		}
		ASSERT_EQ(emptyVectors.size(), isNullCount) << idxName;
		qr = rt.Select(reindexer::Query(nsName).Where(idxName, CondAny, reindexer::VariantArray{}));
		for (auto& it : qr) {
			auto item = it.GetItem(false);
			auto id = item["id"].As<int>();
			ASSERT_TRUE(emptyVectors.end() == emptyVectors.find(id)) << idxName;
		}
		ASSERT_EQ(qr.Count(), kMaxElements - isNullCount - deletedCount) << idxName;
	}
}
CATCH_AND_ASSERT

TEST_F(FloatVector, KeeperQRIterateAfterDropIndex) try {
	constexpr static auto kNsName = "fv_delete_index_ns_keeper"sv;
	constexpr static auto kFieldNameIvf = "ivf"sv;
	constexpr static size_t kDimension = 10;
	constexpr static size_t kItemCount = 10;
	constexpr auto kSleepTime = std::chrono::milliseconds(500);

	rt.OpenNamespace(kNsName);
	rt.DefineNamespaceDataset(
		kNsName,
		{
			IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts{}.PK(), 0},
			IndexDeclaration{
				kFieldNameIvf, "ivf", "float_vector",
				IndexOpts{}.SetFloatVector(
					IndexIvf,
					FloatVectorIndexOpts{}.SetDimension(kDimension).SetNCentroids(5).SetMetric(reindexer::VectorMetric::InnerProduct)),
				0},
		});

	std::unordered_set<int> emptyVectors;
	for (size_t i = 0; i < kItemCount; ++i) {
		auto item = newItem<kDimension>(kNsName, kFieldNameIvf, i, emptyVectors);
		rt.Upsert(kNsName, item);
	}

	auto res = rt.Select(reindexer::Query(kNsName).SelectAllFields());
	EXPECT_EQ(res.Count(), 10);

	rt.DropIndex(kNsName, kFieldNameIvf);

	std::this_thread::sleep_for(kSleepTime);

	// iterate after drop index
	reindexer::WrSerializer ser;
	for (auto& it : res) {
		ser.Reset();
		auto err = it.GetJSON(ser, false);
		ASSERT_TRUE(err.ok()) << err.whatStr();
		ASSERT_EQ(res.GetNamespaces().size(), 1);
	}
}
CATCH_AND_ASSERT

// TODO delete this after #2220
TEST_F(FloatVector, CheckPKDuringUpdateFVIndex) try {
	constexpr static auto kNsName = "FV_PK_check_ns"sv;
	constexpr static auto kFvField = "fv";

	rt.OpenNamespace(kNsName);
	rt.DefineNamespaceDataset(kNsName, {
										   IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts{}.PK(), 0},
									   });

	// Add item
	{
		reindexer::WrSerializer ser;
		{
			reindexer::JsonBuilder json(ser);
			json.Put(kFieldNameId, 0);
			json.Array(kFvField, {1.2, 3.4});
		}
		rt.UpsertJSON(kNsName, ser.Slice());
	}

	rt.DropIndex(kNsName, kFieldNameId);
	auto err = rt.reindexer->AddIndex(
		kNsName, reindexer::IndexDef{kFvField,
									 {kFvField},
									 IndexVectorBruteforce,
									 IndexOpts{}.SetFloatVector(IndexVectorBruteforce, FloatVectorIndexOpts{}.SetDimension(2).SetMetric(
																						   reindexer::VectorMetric::Cosine))});
	ASSERT_FALSE(err.ok());
	ASSERT_EQ(err.whatStr(),
			  fmt::format("Cannot add index '{}' in namespace '{}'. The namespace does not have PK index", kFvField, kNsName));

	rt.AddIndex(kNsName, reindexer::IndexDef{kFieldNameId, {kFieldNameId}, "hash", "int", IndexOpts{}.PK()});

	rt.AddIndex(kNsName,
				reindexer::IndexDef{kFvField,
									{kFvField},
									IndexVectorBruteforce,
									IndexOpts{}.SetFloatVector(IndexVectorBruteforce, FloatVectorIndexOpts{}.SetDimension(2).SetMetric(
																						  reindexer::VectorMetric::Cosine))});

	err = rt.reindexer->DropIndex(kNsName, reindexer::IndexDef{kFieldNameId});
	ASSERT_FALSE(err.ok());
	ASSERT_EQ(err.whatStr(), fmt::format("Cannot remove PK index '{}' from namespace '{}': the namespace contains float vector index '{}'",
										 kFieldNameId, kNsName, kFvField));
}
CATCH_AND_ASSERT

INSTANTIATE_TEST_SUITE_P(, FloatVector,
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
