#include "gtests/tests/fixtures/float_vector_index.h"
#include <gmock/gmock.h>
#include <gtest/gtest-param-test.h>
#include <random>
#include <ranges>
#include <thread>
#include "core/cjson/jsonbuilder.h"
#include "gtests/tests/gtest_cout.h"
#include "gtests/tools.h"
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
	reindexer::IdType lastId = reindexer::IdType::Zero();
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

void FloatVector::deleteSomeItems(std::string_view nsName, int maxElements, std::unordered_set<int>& emptyVectors,
								  std::unordered_map<int, size_t>& vectorsCount) {
	const auto step = std::max(maxElements / 100, 3);
	for (int i = rand() % step; i < maxElements; i += rand() % step) {
		auto item = rt.NewItem(nsName);
		item["id"] = i;
		rt.Delete(nsName, item);
		vectorsCount.erase(i);
		emptyVectors.erase(i);
	}
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

template <size_t Dimension, FloatVector::IsArray isArray>
reindexer::Item FloatVector::newItem(std::string_view nsName, std::string_view fieldName, int id, std::unordered_set<int>& emptyVectors,
									 std::unordered_map<int, size_t>& vectorsCount) {
	if (rand() % 2) {
		return newItemDirect<Dimension, isArray>(nsName, fieldName, id, emptyVectors, vectorsCount);
	} else {
		return newItemFromJson<Dimension, isArray>(nsName, fieldName, id, emptyVectors, vectorsCount);
	}
}

template <size_t Dimension, FloatVector::IsArray isArray>
reindexer::Item FloatVector::newItemDirect(std::string_view nsName, std::string_view fieldName, int id,
										   std::unordered_set<int>& emptyVectors, std::unordered_map<int, size_t>& vectorsCount) {
	auto item = rt.NewItem(nsName);
	item[kFieldNameId] = id;
	std::array<float, Dimension> buf;
	if (isArray && (rand() % 10 != 0)) {
		std::vector<reindexer::FloatVector> vectors;
		const auto arraySize = rand() % 16;
		vectors.resize(arraySize);
		for (int i = 0; i < arraySize; ++i) {
			if (rand() % 10 != 0) {
				::rndFloatVector(buf);
				vectors.emplace_back(buf);
				++vectorsCount[id];
			} else {
				vectors.emplace_back();
				emptyVectors.insert(id);
			}
		}
		if (arraySize == 0) {
			emptyVectors.insert(id);
		}
		item[fieldName] = vectors;
	} else {
		if (rand() % 10 != 0) {
			::rndFloatVector(buf);
			item[fieldName] = reindexer::ConstFloatVectorView{buf};
			++vectorsCount[id];
		} else {
			if (rand() % 2 == 0) {
				item[fieldName] = reindexer::ConstFloatVectorView{};
			}
			emptyVectors.insert(id);
		}
	}
	return item;
}

template <size_t Dimension>
void FloatVector::newVectorFromJson(reindexer::JsonBuilder& json, std::string_view fieldName, int id, std::unordered_set<int>& emptyVectors,
									std::unordered_map<int, size_t>& vectorsCount) {
	if (rand() % 10 != 0) {
		std::array<float, Dimension> buf;
		::rndFloatVector(buf);
		json.Array(fieldName, std::span<const float>(buf));
		++vectorsCount[id];
	} else {
		if (rand() % 2 == 1) {
			json.AddArray(fieldName);
		} else {
			json.Null(fieldName);
		}
		emptyVectors.insert(id);
	}
}

template <size_t Dimension, FloatVector::IsArray isArray>
reindexer::Item FloatVector::newItemFromJson(std::string_view nsName, std::string_view fieldName, int id,
											 std::unordered_set<int>& emptyVectors, std::unordered_map<int, size_t>& vectorsCount) {
	reindexer::WrSerializer ser;
	{
		reindexer::JsonBuilder json(ser);
		json.Put(kFieldNameId, id);
		if (isArray && (rand() % 10 != 0)) {
			const auto arraySize = rand() % 16;
			auto array = json.Array(fieldName);
			for (int i = 0; i < arraySize; ++i) {
				newVectorFromJson<Dimension>(array, {}, id, emptyVectors, vectorsCount);
			}
		} else {
			newVectorFromJson<Dimension>(json, fieldName, id, emptyVectors, vectorsCount);
		}
	}
	auto item = rt.NewItem(nsName);
	const auto err = item.FromJSON(ser.Slice());
	if (!err.ok()) {
		throw err;
	}
	return item;
}

template <size_t Dimension, FloatVector::IsArray isArray>
void FloatVector::upsertItems(std::string_view nsName, std::string_view fieldName, int startId, int endId,
							  std::unordered_set<int>& emptyVectors, std::unordered_map<int, size_t>& vectorsCount, HasIndex hasIndex) {
	assert(startId < endId);
	for (int id = startId; id < endId; ++id) {
		auto item = hasIndex == HasIndex::Yes ? newItem<Dimension, isArray>(nsName, fieldName, id, emptyVectors, vectorsCount)
											  : newItemFromJson<Dimension, isArray>(nsName, fieldName, id, emptyVectors, vectorsCount);
		rt.Upsert(nsName, item);
	}
}

enum class [[nodiscard]] QueryWith : size_t { OnlyK, KandR, OnlyR };

template <size_t Dimension, FloatVector::IsArray isArray, typename SearchParamGetterT>
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

					if (!isArray && queryCase == QueryWith::OnlyR) {
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
							  const std::unordered_set<int>& emptyVectors, const std::unordered_map<int, size_t>& vectorsCount) {
	auto qr = rx.Select(reindexer::Query("#memstats").Where("name", CondEq, ns));
	ASSERT_EQ(qr.Count(), 1);
	auto memstats = qr.begin().GetItem(false);
	auto json = memstats.GetJSON();
	const auto count = std::accumulate(vectorsCount.begin(), vectorsCount.end(), 0, [](auto c, const auto& vc) { return c + vc.second; });
	auto expectedRegex = fmt::format(
		R"(\{{"uniq_keys_count":{},"data_size":{},"indexing_struct_size":[1-9][0-9]+,"vectors_keeper_size":[1-9][0-9]+,"is_built":true,("is_quantized":false,)*"name":"{}"\}})",
		count + (emptyVectors.empty() ? 0 : 1), count * dims * sizeof(float), vectorIndex);
	ASSERT_THAT(json, testing::ContainsRegex(expectedRegex));
}

template <FloatVector::IsArray isArray>
void FloatVector::TestHnswIndex() try {
	constexpr static auto kNsName = isArray ? "hnsw_array_ns"sv : "hnsw_scalar_ns";
	constexpr static auto kFieldNameHnsw = "hnsw"sv;
#if defined(RX_WITH_STDLIB_DEBUG)
	constexpr static size_t kDimension = isArray ? 4 : 16;
	constexpr static size_t kMaxElements = 100;
#elif defined(REINDEX_WITH_TSAN) || defined(REINDEX_WITH_ASAN)
	constexpr static size_t kDimension = isArray ? 8 : 64;
	constexpr static size_t kMaxElements = 300;
#else
	constexpr static size_t kDimension = isArray ? 128 : 1'024;
	constexpr static size_t kMaxElements = 3'000;
#endif
	constexpr static size_t kM = 16;
	constexpr static size_t kEfConstruction = 200;

	rt.OpenNamespace(kNsName);
	rt.DefineNamespaceDataset(
		kNsName, {
					 IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts{}.PK(), 0},
					 IndexDeclaration{kFieldNameHnsw, "hnsw", "float_vector",
									  IndexOpts{}.Array(isArray).SetFloatVector(IndexHnsw, FloatVectorIndexOpts{}
																							   .SetDimension(kDimension)
																							   .SetStartSize(100)
																							   .SetM(kM)
																							   .SetEfConstruction(kEfConstruction)
																							   .SetMetric(GetParam())),
									  0},
				 });

	checkEmptyIndexSelection<reindexer::HnswSearchParams, kDimension>(kNsName, kFieldNameHnsw);

	std::unordered_set<int> emptyVectors;
	std::unordered_map<int, size_t> vectorsCount;
	upsertItems<kDimension, isArray>(kNsName, kFieldNameHnsw, 0, kMaxElements, emptyVectors, vectorsCount);
	auto res = rt.UpdateQR(reindexer::Query(kNsName).Set("non_idx", 12345).Where(kFieldNameId, CondEq, 1));
	ASSERT_EQ(res.Count(), 1);
	deleteSomeItems(kNsName, kMaxElements, emptyVectors, vectorsCount);
	runMultithreadQueries<kDimension, isArray>(4, 20, kNsName, kFieldNameHnsw, [&](std::optional<size_t> k, std::optional<float> radius) {
		return reindexer::HnswSearchParams{}.K(k).Radius(radius);
	});
	checkIndexMemstat(rt, kNsName, kFieldNameHnsw, kDimension, emptyVectors, vectorsCount);
}
CATCH_AND_ASSERT

TEST_P(FloatVector, HnswArrayIndex) { TestHnswIndex<Array>(); }
TEST_P(FloatVector, HnswScalarIndex) { TestHnswIndex<Scalar>(); }

template <FloatVector::IsArray isArray>
void FloatVector::TestHnswIndexMTRace() try {
	constexpr static auto kNsName = isArray ? "hnsw_array_mt_race_ns"sv : "hnsw_scalar_mt_race_ns"sv;
	constexpr static auto kFieldNameHnsw = "hnsw"sv;
	constexpr static size_t kDimension = isArray ? 4 : 8;
	constexpr static size_t kM = 16;
	constexpr static size_t kSelectThreads = 2;
#if defined(RX_WITH_STDLIB_DEBUG)
	constexpr static size_t kTransactions = 4;
	constexpr static size_t kEfConstruction = 50;
#elif defined(REINDEX_WITH_ASAN) || defined(REINDEX_WITH_TSAN)
	constexpr static size_t kTransactions = 5;
	constexpr static size_t kEfConstruction = 100;
#else
	constexpr static size_t kTransactions = 20;
	constexpr static size_t kEfConstruction = 200;
#endif

	rt.OpenNamespace(kNsName);
	rt.DefineNamespaceDataset(kNsName,
							  {
								  IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts{}.PK(), 0},
								  IndexDeclaration{kFieldNameHnsw, "hnsw", "float_vector",
												   IndexOpts{}.Array(isArray).SetFloatVector(
													   IndexHnsw, FloatVectorIndexOpts{}
																	  .SetDimension(kDimension)
																	  .SetMultithreading(MultithreadingMode::MultithreadTransactions)
																	  .SetM(kM)
																	  .SetEfConstruction(kEfConstruction)
																	  .SetMetric(reindexer::VectorMetric::Cosine)),
												   0},
							  });

	unsigned id = 0;
	std::unordered_map<reindexer::IdType, std::string> expectedData;

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
		std::unordered_map<int, size_t> vectorsCount;
		int totalNewItems = 0;
		for (size_t j = 0; j < unsigned(rand() % 50 + 250); ++j) {
			if (id < 10 || rand() % 10) {
				// Insert new item
				auto item = newItem<kDimension, isArray>(kNsName, kFieldNameHnsw, id, emptyVectors, vectorsCount);
				std::string json(item.GetJSON());
				expectedData[reindexer::IdType::FromNumber(id)] = std::move(json);
				auto err = tx.Upsert(std::move(item));
				ASSERT_TRUE(err.ok()) << err.what();
				++id;
				++totalNewItems;
			}
			if (id > 10 && rand() % 8 == 0) {
				// Update existing id
				auto updID = rand() % id;
				auto item = newItem<kDimension, isArray>(kNsName, kFieldNameHnsw, updID, emptyVectors, vectorsCount);
				std::string json(item.GetJSON());
				if (auto found = expectedData.find(reindexer::IdType::FromNumber(updID)); found != expectedData.end()) {
					found->second = std::move(json);
				}
				auto err = tx.Update(std::move(item));
				ASSERT_TRUE(err.ok()) << err.what();
			}
			if (id > 10 && rand() % 8 == 0) {
				// Delete existing id
				auto delID = rand() % id;
				auto item = newItem<kDimension, isArray>(kNsName, kFieldNameHnsw, delID, emptyVectors, vectorsCount);
				if (expectedData.erase(reindexer::IdType::FromNumber(delID))) {
					--totalNewItems;
				}
				auto err = tx.Delete(std::move(item));
				ASSERT_TRUE(err.ok()) << err.what();
				--totalNewItems;
			}
			if (rand() % 20 == 0) {
				// Delete non existing id
				auto delID = id + rand() % 1000 + 1000;
				auto item = newItem<kDimension, isArray>(kNsName, kFieldNameHnsw, delID, emptyVectors, vectorsCount);
				ASSERT_EQ(expectedData.erase(reindexer::IdType::FromNumber(delID)), 0);
				auto err = tx.Delete(std::move(item));
				ASSERT_TRUE(err.ok()) << err.what();
			}
		}
		while (totalNewItems < 201) {
			// Insert more items to force multithreading insertion
			auto item = newItem<kDimension, isArray>(kNsName, kFieldNameHnsw, id, emptyVectors, vectorsCount);
			std::string json(item.GetJSON());
			expectedData[reindexer::IdType::FromNumber(id)] = std::move(json);
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
		auto found = expectedData.find(reindexer::IdType::FromNumber(id));
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

TEST_F(FloatVector, TestHnswArrayIndexMTRace) { TestHnswIndexMTRace<Array>(); }
TEST_F(FloatVector, TestHnswScalarIndexMTRace) { TestHnswIndexMTRace<Scalar>(); }

template <FloatVector::IsArray isArray>
void FloatVector::TestVecBruteforceIndex() try {
	constexpr static auto kNsName = isArray ? "vec_bf_array_ns"sv : "vec_bf_scalar_ns"sv;
	constexpr static auto kFieldNameVec = "vec"sv;
#if defined(RX_WITH_STDLIB_DEBUG)
	constexpr static size_t kDimension = isArray ? 4 : 16;
	constexpr static size_t kMaxElements = 300;
#elif defined(REINDEX_WITH_TSAN) || defined(REINDEX_WITH_ASAN)
	constexpr static size_t kDimension = isArray ? 8 : 64;
	constexpr static size_t kMaxElements = 1'000;
#else
	constexpr static size_t kDimension = isArray ? 128 : 1'024;
	constexpr static size_t kMaxElements = 10'000;
#endif

	rt.OpenNamespace(kNsName);
	rt.DefineNamespaceDataset(
		kNsName, {
					 IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts{}.PK(), 0},
					 IndexDeclaration{kFieldNameVec, "vec_bf", "float_vector",
									  IndexOpts{}.Array(isArray).SetFloatVector(
										  IndexVectorBruteforce,
										  FloatVectorIndexOpts{}.SetDimension(kDimension).SetStartSize(100).SetMetric(GetParam())),
									  0},
				 });

	checkEmptyIndexSelection<reindexer::BruteForceSearchParams, kDimension>(kNsName, kFieldNameVec);

	std::unordered_set<int> emptyVectors;
	std::unordered_map<int, size_t> vectorsCount;
	upsertItems<kDimension, isArray>(kNsName, kFieldNameVec, 0, kMaxElements, emptyVectors, vectorsCount);
	deleteSomeItems(kNsName, kMaxElements, emptyVectors, vectorsCount);
	runMultithreadQueries<kDimension, isArray>(4, 20, kNsName, kFieldNameVec, [&](std::optional<size_t> k, std::optional<float> radius) {
		return reindexer::BruteForceSearchParams{}.K(k).Radius(radius);
	});
	checkIndexMemstat(rt, kNsName, kFieldNameVec, kDimension, emptyVectors, vectorsCount);
}
CATCH_AND_ASSERT

TEST_P(FloatVector, VecBruteforceArrayIndex) { TestVecBruteforceIndex<Array>(); }
TEST_P(FloatVector, VecBruteforceScalarIndex) { TestVecBruteforceIndex<Scalar>(); }

template <FloatVector::IsArray isArray>
void FloatVector::TestIvfIndex() try {
	constexpr static auto kNsName = isArray ? "ivf_array_ns"sv : "ivf_scalar_ns"sv;
	constexpr static auto kFieldNameIvf = "ivf"sv;
#if defined(REINDEX_WITH_TSAN) || defined(REINDEX_WITH_ASAN) || defined(RX_WITH_STDLIB_DEBUG)
	constexpr static size_t kDimension = isArray ? 8 : 64;
	constexpr static size_t kMaxElements = isArray ? 300 : 1'200;
#else
	constexpr static size_t kDimension = isArray ? 256 : 1'024;
	constexpr static size_t kMaxElements = 10'000;
#endif
	std::array<float, kDimension> buf;
	constexpr size_t kNCentroids = isArray ? (kMaxElements / 20) : (kMaxElements / 60);

	rt.OpenNamespace(kNsName);
	rt.DefineNamespaceDataset(
		kNsName, {
					 IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts{}.PK(), 0},
					 IndexDeclaration{
						 kFieldNameIvf, "ivf", "float_vector",
						 IndexOpts{}.Array(isArray).SetFloatVector(
							 IndexIvf, FloatVectorIndexOpts{}.SetDimension(kDimension).SetNCentroids(kNCentroids).SetMetric(GetParam())),
						 0},
				 });

	checkEmptyIndexSelection<reindexer::IvfSearchParams, kDimension>(kNsName, kFieldNameIvf);

	std::unordered_set<int> emptyVectors;
	std::unordered_map<int, size_t> vectorsCount;
	upsertItems<kDimension, isArray>(kNsName, kFieldNameIvf, 0, kMaxElements, emptyVectors, vectorsCount);
	deleteSomeItems(kNsName, kMaxElements, emptyVectors, vectorsCount);
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
		checkIndexMemstat(rt, kNsName, kFieldNameIvf, kDimension, emptyVectors, vectorsCount);
		if (iter == 0) {
			rebuildCentroids();
		}
	}
	runMultithreadQueries<kDimension, isArray>(4, 20, kNsName, kFieldNameIvf, [&](std::optional<size_t> k, std::optional<float> radius) {
		return reindexer::IvfSearchParams{}.K(k).Radius(radius).NProbe(32);
	});
}
CATCH_AND_ASSERT

TEST_P(FloatVector, IvfArrayIndex) { TestIvfIndex<Array>(); }
TEST_P(FloatVector, IvfScalarIndex) { TestIvfIndex<Scalar>(); }

template <FloatVector::IsArray isArray>
void FloatVector::TestHnswIndexUpdateQuery() try {
	constexpr static auto kNsName = isArray ? "hnsw_array_ns"sv : "hnsw_scalar_ns"sv;
	constexpr static auto kFieldNameHnsw = "hnsw"sv;

	rt.OpenNamespace(kNsName);
	rt.DefineNamespaceDataset(
		kNsName, {
					 IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts{}.PK(), 0},
					 IndexDeclaration{kFieldNameHnsw, kFieldNameHnsw, "float_vector",
									  IndexOpts{}.Array(isArray).SetFloatVector(
										  IndexHnsw, FloatVectorIndexOpts{}.SetDimension(8).SetM(16).SetEfConstruction(200).SetMetric(
														 reindexer::VectorMetric::L2)),
									  0},
				 });

	rt.UpsertJSON(kNsName, R"json({"id":0})json");
	rt.UpsertJSON(kNsName, R"json({"id":1,"hnsw":null})json");
	rt.UpsertJSON(kNsName, R"json({"id":2,"hnsw":[]})json");
	if constexpr (isArray) {
		rt.UpsertJSON(kNsName, R"json({"id":3,"hnsw":[null]})json");
		rt.UpsertJSON(kNsName, R"json({"id":4,"hnsw":[[]]})json");
	}
	constexpr int itemsCount = isArray ? 5 : 3;
	std::array<float, 8> buf;
	::rndFloatVector(buf);
	const reindexer::ConstFloatVectorView setVec{buf};
	const auto updateQuery = reindexer::Query(kNsName).Set(kFieldNameHnsw, setVec);
	SCOPED_TRACE(updateQuery.GetSQL());
	auto res = rt.UpdateQR(updateQuery);
	EXPECT_EQ(res.Count(), itemsCount);
	validateIndexValueInQueryResults(kFieldNameHnsw, res, setVec.Span());

	res = rt.Select(reindexer::Query(kNsName).SelectAllFields());
	EXPECT_EQ(res.Count(), itemsCount);
	validateIndexValueInQueryResults("hnsw", res, setVec.Span());
}
CATCH_AND_ASSERT

TEST_F(FloatVector, HnswArrayIndexUpdateQuery) { TestHnswIndexUpdateQuery<Array>(); }
TEST_F(FloatVector, HnswScalarIndexUpdateQuery) { TestHnswIndexUpdateQuery<Scalar>(); }

template <FloatVector::IsArray isArray>
void FloatVector::HnswIndexItemSet() try {
	constexpr static auto kNsName = isArray ? "hnsw_array_ns"sv : "hnsw_scalar_ns"sv;
	constexpr static auto kFieldNameHnsw = "hnsw"sv;

	rt.OpenNamespace(kNsName);
	rt.DefineNamespaceDataset(
		kNsName, {
					 IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts{}.PK(), 0},
					 IndexDeclaration{kFieldNameHnsw, kFieldNameHnsw, "float_vector",
									  IndexOpts{}.Array(isArray).SetFloatVector(
										  IndexHnsw, FloatVectorIndexOpts{}.SetDimension(8).SetM(16).SetEfConstruction(200).SetMetric(
														 reindexer::VectorMetric::L2)),
									  0},
				 });

	std::vector<std::string_view> JSONs = {R"json({"id":0})json", R"json({"id":1,"hnsw":null})json", R"json({"id":2,"hnsw":[]})json"};
	if constexpr (isArray == Array) {
		JSONs.emplace_back(R"json({"id":3,"hnsw":[null]})json");
		JSONs.emplace_back(R"json({"id":4,"hnsw":[[]]})json");
	}

	std::array<float, 8> buf;
	::rndFloatVector(buf);
	const reindexer::ConstFloatVectorView vec{buf};

	for (auto& json : JSONs) {
		SCOPED_TRACE(json);
		auto item = rt.NewItem(kNsName);
		auto err = item.FromJSON(json);
		ASSERT_TRUE(err.ok()) << err.what();
		item[kFieldNameHnsw] = vec;
		validateIndexValueInItem(kNsName, kFieldNameHnsw, item.GetJSON(), vec.Span());
	}
}
CATCH_AND_ASSERT

TEST_F(FloatVector, HnswArrayIndexItemSet) { HnswIndexItemSet<Array>(); }
TEST_F(FloatVector, HnswScalarIndexItemSet) { HnswIndexItemSet<Scalar>(); }

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

template <IndexType>
struct QueryIndexParams;

template <>
struct [[nodiscard]] QueryIndexParams<IndexType::IndexIvf> {
	QueryIndexParams(size_t maxElements, size_t dimension, reindexer::VectorMetric metric) {
		std::ignore = searchParams.K(maxElements).NProbe(32);
		opts.SetDimension(dimension).SetNCentroids(maxElements / 50).SetMetric(metric);
	}
	std::string name = "ivf";
	reindexer::IvfSearchParams searchParams;
	FloatVectorIndexOpts opts;
};

template <>
struct [[nodiscard]] QueryIndexParams<IndexType::IndexHnsw> {
	QueryIndexParams(size_t maxElements, size_t dimension, reindexer::VectorMetric metric) {
		std::ignore = searchParams.K(maxElements).Ef(maxElements * 2);
		opts.SetDimension(dimension).SetMetric(metric).SetEfConstruction(5).SetStartSize(maxElements / 2).SetM(8);
	}
	std::string name = "hnsw";
	reindexer::HnswSearchParams searchParams;
	FloatVectorIndexOpts opts;
};

template <>
struct [[nodiscard]] QueryIndexParams<IndexType::IndexVectorBruteforce> {
	QueryIndexParams(size_t maxElements, size_t dimension, reindexer::VectorMetric metric) {
		std::ignore = searchParams.K(maxElements);
		opts.SetDimension(dimension).SetMetric(metric).SetStartSize(maxElements);
	}
	std::string name = "vec_bf";
	reindexer::BruteForceSearchParams searchParams;
	FloatVectorIndexOpts opts;
};

template <FloatVector::IsArray isArray, IndexType indexType>
void FloatVector::TestQueries() try {
	constexpr static auto kNsName = isArray ? "fv_array_queries_ns"sv : "fv_scalar_queries_ns"sv;
	constexpr static auto kFieldNameBool = "bool"sv;
#if defined(RX_WITH_STDLIB_DEBUG)
	constexpr static size_t kDimension = isArray ? 4 : 16;
	constexpr static size_t kMaxElements = 300;
#elif defined(REINDEX_WITH_TSAN) || defined(REINDEX_WITH_ASAN)
	constexpr static size_t kDimension = isArray ? 8 : 64;
	constexpr static size_t kMaxElements = 1'000;
#else
	constexpr static size_t kDimension = isArray ? 128 : 1'024;
	constexpr static size_t kMaxElements = 10'000;
#endif
	std::array<float, kDimension> buf;
	const auto indexParams = QueryIndexParams<indexType>(kMaxElements, kDimension, GetParam());
	const auto searchParams = indexParams.searchParams;
	const auto& fieldName = indexParams.name;

	rt.OpenNamespace(kNsName);
	rt.DefineNamespaceDataset(kNsName, {
										   IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts{}.PK(), 0},
										   IndexDeclaration{kFieldNameBool, "-", "bool", IndexOpts{}, 0},
										   IndexDeclaration{fieldName, fieldName, "float_vector",
															IndexOpts{}.Array(isArray).SetFloatVector(indexType, indexParams.opts), 0},
									   });

	size_t i = 0;
	std::unordered_set<int> emptyVectors;
	std::unordered_map<int, size_t> vectorsCount;
	for (; i < kMaxElements / 3; ++i) {
		auto item = newItem<kDimension, isArray>(kNsName, fieldName, i, emptyVectors, vectorsCount);
		item[kFieldNameBool] = bool(rand() % 2);
		rt.Upsert(kNsName, item);
	}
	for (; i < 2 * kMaxElements / 3; ++i) {
		auto item = newItem<kDimension, isArray>(kNsName, fieldName, i, emptyVectors, vectorsCount);
		item[kFieldNameBool] = bool(rand() % 2);
		rt.Upsert(kNsName, item);
	}
	for (; i < kMaxElements; ++i) {
		auto item = newItem<kDimension, isArray>(kNsName, fieldName, i, emptyVectors, vectorsCount);
		item[kFieldNameBool] = bool(rand() % 2);
		rt.Upsert(kNsName, item);
	}
	::rndFloatVector(buf);
	auto result = rt.Select(
		reindexer::Query{kNsName}.Where(kFieldNameId, CondGt, 5).WhereKNN(fieldName, reindexer::ConstFloatVectorView{buf}, searchParams));
	checkOrdering(result, GetParam());
	::rndFloatVector(buf);
	result = rt.Select(reindexer::Query{kNsName}
						   .Where(kFieldNameBool, CondEq, true)
						   .OpenBracket()
						   .Where(kFieldNameId, CondGt, 5)
						   .WhereKNN(fieldName, reindexer::ConstFloatVectorView{buf}, searchParams)
						   .CloseBracket());
	checkOrdering(result, GetParam());
	::rndFloatVector(buf);
	result =
		rt.Select(reindexer::Query{kNsName}.WhereKNN(fieldName, reindexer::ConstFloatVectorView{buf}, searchParams).Sort("rank()", false));
	::rndFloatVector(buf);
	result = rt.Select(reindexer::Query{kNsName}
						   .WhereKNN(fieldName, reindexer::ConstFloatVectorView{buf}, searchParams)
						   .Sort("rank(" + fieldName + ')', false));
	::rndFloatVector(buf);
	std::map<reindexer::IdType, reindexer::RankT> ranks;
	result = rt.Select(reindexer::Query{kNsName}.WhereKNN(fieldName, reindexer::ConstFloatVectorView{buf}, searchParams).WithRank());
	for (auto& i : result) {
		const auto& item = i.GetItemRefRanked();
		ranks[item.NotRanked().Id()] = item.Rank();
	}
	result = rt.Select(reindexer::Query{kNsName}
						   .Where(kFieldNameBool, CondEq, true)
						   .OpenBracket()
						   .Where(kFieldNameId, CondGt, 5)
						   .WhereKNN(fieldName, reindexer::ConstFloatVectorView{buf}, searchParams)
						   .CloseBracket()
						   .WithRank());
	for (auto& i : result) {
		const auto& item = i.GetItemRefRanked();
		const auto it = ranks.find(item.NotRanked().Id());
		if (it == ranks.end()) {
			EXPECT_NE(it, ranks.end()) << item.NotRanked().Id().ToNumber() << ' ' << item.Rank().Value();
		} else {
			EXPECT_EQ(it->second, item.Rank()) << item.NotRanked().Id().ToNumber();
		}
	}

	::rndFloatVector(buf);
	result = rt.Select(reindexer::Query{kNsName}
						   .Where(kFieldNameId, CondLt, kMaxElements / 2.0)
						   .WhereKNN(fieldName, reindexer::ConstFloatVectorView{buf}, searchParams)
						   .Merge(reindexer::Query{kNsName}
									  .Where(kFieldNameId, CondGt, kMaxElements / 2.0)
									  .WhereKNN(fieldName, reindexer::ConstFloatVectorView{buf}, searchParams)));
}
CATCH_AND_ASSERT

TEST_P(FloatVector, TestQueriesArrayIvf) { TestQueries<Array, IndexIvf>(); }
TEST_P(FloatVector, TestQueriesScalarIvf) { TestQueries<Scalar, IndexIvf>(); }
TEST_P(FloatVector, TestQueriesArrayHnsw) { TestQueries<Array, IndexHnsw>(); }
TEST_P(FloatVector, TestQueriesScalarHnsw) { TestQueries<Scalar, IndexHnsw>(); }
TEST_P(FloatVector, TestQueriesArrayBruteforce) { TestQueries<Array, IndexVectorBruteforce>(); }
TEST_P(FloatVector, TestQueriesScalarBruteforce) { TestQueries<Scalar, IndexVectorBruteforce>(); }

template <FloatVector::IsArray isArray>
void FloatVector::TestDeleteIndex() try {
	constexpr static auto kNsName = isArray ? "fv_array_delete_index_ns"sv : "fv_scalar_delete_index_ns"sv;
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
				IndexOpts{}.Array(isArray).SetFloatVector(
					IndexIvf,
					FloatVectorIndexOpts{}.SetDimension(kDimension).SetNCentroids(5).SetMetric(reindexer::VectorMetric::InnerProduct)),
				0},
		});

	std::unordered_set<int> emptyVectors;
	std::unordered_map<int, size_t> vectorsCount;
	for (size_t i = 0; i < kItemCount; ++i) {
		auto item = newItem<kDimension, isArray>(kNsName, kFieldNameIvf, i, emptyVectors, vectorsCount);
		item[kFieldNameBool] = rand() % 100;
		rt.Upsert(kNsName, item);
	}

	rt.DropIndex(kNsName, kFieldNameBool);
	rt.DropIndex(kNsName, kFieldNameIvf);
}
CATCH_AND_ASSERT

TEST_F(FloatVector, DeleteArrayIndex) { TestDeleteIndex<Array>(); }
TEST_F(FloatVector, DeleteScalarIndex) { TestDeleteIndex<Scalar>(); }

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

template <size_t Dimension, FloatVector::IsArray isArray>
reindexer::Item FloatVector::newItem(std::string_view nsName, size_t fieldsCount,
									 std::vector<std::vector<std::vector<reindexer::FloatVector>>>& items) {
	const int id = items.size();
	items.emplace_back();
	return newItem<Dimension, isArray>(nsName, fieldsCount, id, items);
}

template <bool isArray>
static void putJsonField(reindexer::JsonBuilder& json, std::string_view fieldName, std::vector<reindexer::FloatVector>& values);

template <>
void putJsonField<false>(reindexer::JsonBuilder& json, std::string_view fieldName, std::vector<reindexer::FloatVector>& values) {
	assertrx(values.size() == 1);
	if (!values[0].IsEmpty() || rand() % 3 == 0) {
		json.Array(fieldName, values[0].Span());
	} else if (rand() % 2 == 0) {
		json.Null(fieldName);
	}
}

template <>
void putJsonField<true>(reindexer::JsonBuilder& json, std::string_view fieldName, std::vector<reindexer::FloatVector>& values) {
	if (values.empty()) {
		switch (rand() % 3) {
			case 0:
				json.Null(fieldName);
				return;
			case 1:
				return;
			default:
				break;
		}
	}
	auto array = json.Array(fieldName);
	for (const auto& v : values) {
		if (!v.IsEmpty() || rand() % 2 == 0) {
			array.Array(reindexer::TagName::Empty(), v.Span());
		} else {
			array.Null(reindexer::TagName::Empty());
		}
	}
}

template <size_t Dimension, FloatVector::IsArray isArray>
std::vector<reindexer::FloatVector> FloatVector::rndFVField() {
	std::vector<reindexer::FloatVector> result;
	if constexpr (!isArray) {
		result.emplace_back(rndFloatVector<Dimension>());
	} else {
		const unsigned size = rand() % 5;
		result.reserve(size);
		for (unsigned i = 0; i < size; ++i) {
			result.emplace_back(rndFloatVector<Dimension>());
		}
	}
	return result;
}

static int regularFieldValue(int id) noexcept { return id + 100; }

template <size_t Dimension, FloatVector::IsArray isArray>
reindexer::Item FloatVector::newItem(std::string_view nsName, size_t fieldsCount, int id,
									 std::vector<std::vector<std::vector<reindexer::FloatVector>>>& items) {
	assertrx(size_t(id) < items.size());
	auto& vectors = items[id];
	vectors.clear();
	vectors.reserve(fieldsCount);
	for (size_t i = 0; i < fieldsCount; ++i) {
		vectors.emplace_back(rndFVField<Dimension, isArray>());
	}
	reindexer::WrSerializer ser;
	{
		reindexer::JsonBuilder json(ser);
		json.Put(kFieldNameId, id);
		json.Put(kFieldNameRegular, regularFieldValue(id));
		for (size_t i = 1; i < fieldsCount; i += 2) {
			putJsonField<isArray>(json, fieldName(i), vectors[i]);
		}
		{
			auto obj = json.Object(kObjectName);
			for (size_t i = 2; i < fieldsCount; i += 4) {
				putJsonField<isArray>(obj, fieldName(i), vectors[i]);
			}
			{
				auto nestedObj = obj.Object(kObjectName);
				for (size_t i = 0; i < fieldsCount; i += 4) {
					putJsonField<isArray>(nestedObj, fieldName(i), vectors[i]);
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

template <bool isArray>
static void checkFieldValue(gason::JsonNode& parsedJson, const reindexer::Item&, const std::string& fieldPath, const std::string& indexName,
							const std::vector<reindexer::FloatVector>& currentData);

template <>
void checkFieldValue<false>(gason::JsonNode& parsedJson, const reindexer::Item& item, const std::string& fieldPath,
							const std::string& indexName, const std::vector<reindexer::FloatVector>& currentData) {
	assertrx(currentData.size() == 1);
	ASSERT_JSON_FIELD_ARRAY_EQ(parsedJson, fieldPath, currentData[0].Span());
	const auto foundSpan = item[indexName].As<reindexer::ConstFloatVectorView>().Span();
	const auto expectedSpan = currentData[0].Span();
	ASSERT_EQ(foundSpan.size(), expectedSpan.size());
	for (size_t i = 0, s = foundSpan.size(); i < s; ++i) {
		ASSERT_EQ(foundSpan[i], expectedSpan[i]);
	}
}

template <>
void checkFieldValue<true>(gason::JsonNode& parsedJson, const reindexer::Item& item, const std::string& fieldPath,
						   const std::string& indexName, const std::vector<reindexer::FloatVector>& expectedData) {
	if (expectedData.empty()) {
		ASSERT_JSON_FIELD_IS_NULL(parsedJson, fieldPath)
	} else {
		const auto jsonField = findJsonField(parsedJson, fieldPath);
		if (jsonField.isNull() || (jsonField.isArray() && (begin(jsonField) == end(jsonField)))) {
			if (!expectedData.empty()) {
				ASSERT_TRUE(expectedData.size() == 1 && expectedData[0].IsEmpty());
			}
		} else if (jsonField.isArray() && begin(jsonField) != end(jsonField) && begin(jsonField)->isNumber()) {
			ASSERT_EQ(expectedData.size(), 1);
			ASSERT_JSON_ARRAY_EQ(jsonField, fieldPath + '[' + std::to_string(0) + ']', expectedData[0].Span());
		} else {
			size_t i = 0;
			for (auto arrayItem : jsonField) {
				ASSERT_LT(i, expectedData.size());
				if (expectedData[i].IsEmpty()) {
					ASSERT_JSON_IS_NULL(arrayItem.value, fieldPath + '[' + std::to_string(i) + ']')
				} else {
					ASSERT_JSON_ARRAY_EQ(arrayItem, fieldPath + '[' + std::to_string(i) + ']', expectedData[i].Span());
				}
				++i;
			}
			EXPECT_EQ(i, expectedData.size());
		}
	}
	const reindexer::VariantArray itemArray(item[indexName]);
	ASSERT_EQ(itemArray.size(), expectedData.size());
	for (size_t i = 0; i < expectedData.size(); ++i) {
		const auto foundSpan = itemArray[i].As<reindexer::ConstFloatVectorView>().Span();
		const auto expectedSpan = expectedData[i].Span();
		ASSERT_EQ(foundSpan.size(), expectedSpan.size());
		for (size_t i = 0, s = foundSpan.size(); i < s; ++i) {
			ASSERT_EQ(foundSpan[i], expectedSpan[i]);
		}
	}
}

template <bool isArray>
static void checkItemAfterUpdate(reindexer::Item& item, int expectedId, const size_t indexesCount,
								 const std::vector<std::vector<std::vector<reindexer::FloatVector>>>& vectors) {
	const auto id = item[kFieldNameId].As<int>();
	ASSERT_EQ(id, expectedId);
	ASSERT_EQ(item[kFieldNameRegular].As<int>(), regularFieldValue(id));
	ASSERT_LT(id, vectors.size());
	const auto json = item.GetJSON();

	gason::JsonParser parser;
	auto parsedJson = parser.Parse(json);

	ASSERT_JSON_FIELD_INT_EQ(parsedJson, kFieldNameId, id);
	ASSERT_JSON_FIELD_INT_EQ(parsedJson, kFieldNameRegular, regularFieldValue(id));

	const auto& currentVectors = vectors[id];
	assertrx(currentVectors.size() == indexesCount);
	for (size_t j = 0; j < indexesCount; ++j) {
		checkFieldValue<isArray>(parsedJson, item, fieldPath(j), indexName(j), currentVectors[j]);
	}
}

template <bool isArray>
static void checkQRAfterUpdate(const reindexer::QueryResults& qr, int expectedId, const size_t indexesCount,
							   const std::vector<std::vector<std::vector<reindexer::FloatVector>>>& vectors) {
	ASSERT_EQ(qr.Count(), 1);
	for (auto it : qr) {
		ASSERT_TRUE(it.Status().ok()) << it.Status().what();
		auto item = it.GetItem();
		checkItemAfterUpdate<isArray>(item, expectedId, indexesCount, vectors);
	}
}

template <FloatVector::IsArray isArray>
void FloatVector::TestSelectFilters() try {
	constexpr static auto kNsName = isArray ? "fv_array_select_filters_ns"sv : "fv_scalar_select_filters_ns"sv;
	constexpr static size_t kDimension = 5;
	constexpr static size_t kMaxElements = 1'000;
	constexpr static size_t kIndexesCount = 20;

	std::vector<std::pair<IndexType, IndexOpts>> indexOpts{
		{IndexIvf, IndexOpts{}.Array(isArray).SetFloatVector(IndexIvf, FloatVectorIndexOpts{}
																		   .SetDimension(kDimension)
																		   .SetNCentroids(kMaxElements / 3)
																		   .SetMetric(reindexer::VectorMetric::InnerProduct))},
		{IndexHnsw,
		 IndexOpts{}.Array(isArray).SetFloatVector(
			 IndexHnsw,
			 FloatVectorIndexOpts{}.SetDimension(kDimension).SetM(16).SetEfConstruction(50).SetMetric(reindexer::VectorMetric::L2))},
		{IndexVectorBruteforce,
		 IndexOpts{}.Array(isArray).SetFloatVector(
			 IndexVectorBruteforce, FloatVectorIndexOpts{}.SetDimension(kDimension).SetMetric(reindexer::VectorMetric::Cosine))}};

	rt.OpenNamespace(kNsName);
	rt.DefineNamespaceDataset(kNsName, {IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts{}.PK(), 0},
										{IndexDeclaration{kFieldNameRegular, "hash", "int", IndexOpts{}, 0}}});
	for (size_t i = 0; i < kIndexesCount; ++i) {
		const auto& idxOpts = indexOpts[rand() % indexOpts.size()];
		rt.AddIndex(kNsName, reindexer::IndexDef{indexName(i), {fieldPath(i)}, idxOpts.first, idxOpts.second});
	}

	std::vector<std::vector<std::vector<reindexer::FloatVector>>> vectors;

	vectors.reserve(kMaxElements);

	for (size_t i = 0; i < kMaxElements; ++i) {
		auto item = newItem<kDimension, isArray>(kNsName, kIndexesCount, vectors);
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
			const auto& currentData = vectors[id];
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
					ASSERT_JSON_FIELD_ABSENT_OR_IS_NULL(parsedJson, fieldPath(j) + (isArray ? "[0]" : ""));
					EXPECT_THROW([[maybe_unused]] reindexer::Variant _{item[indexName(j)]}, reindexer::Error);
				} else {
					checkFieldValue<isArray>(parsedJson, item, fieldPath(j), indexName(j), currentData[j]);
				}
			}
		}

		// update
		{
			const auto id = rand() % kMaxElements;
			auto item = newItem<kDimension, isArray>(kNsName, kIndexesCount, id, vectors);
			rt.Upsert(kNsName, item);
			checkItemAfterUpdate<isArray>(item, id, kIndexesCount, vectors);
		}
		{
			const auto id = rand() % kMaxElements;
			auto item = newItem<kDimension, isArray>(kNsName, kIndexesCount, id, vectors);
			reindexer::QueryResults qr;
			rt.Upsert(kNsName, item, qr);
			checkItemAfterUpdate<isArray>(item, id, kIndexesCount, vectors);
			checkQRAfterUpdate<isArray>(qr, id, kIndexesCount, vectors);
		}
		{
			const auto id = rand() % kMaxElements;
			auto item = newItem<kDimension, isArray>(kNsName, kIndexesCount, id, vectors);
			rt.Update(kNsName, item);
			checkItemAfterUpdate<isArray>(item, id, kIndexesCount, vectors);
		}
		{
			const auto id = rand() % kMaxElements;
			auto item = newItem<kDimension, isArray>(kNsName, kIndexesCount, id, vectors);
			reindexer::QueryResults qr;
			rt.Update(kNsName, item, qr);
			checkItemAfterUpdate<isArray>(item, id, kIndexesCount, vectors);
			checkQRAfterUpdate<isArray>(qr, id, kIndexesCount, vectors);
		}
		{
			const int id = rand() % kMaxElements;
			const auto fldIdx = rand() % kIndexesCount;
			vectors[id][fldIdx] = rndFVField<kDimension, isArray>();
			const auto& itemVectors(vectors[id][fldIdx]);

			auto query = reindexer::Query(kNsName).Where(kFieldNameId, CondEq, id);
			if (itemVectors.size() == 1 && rand() % 3 != 0) {
				query.Set(rand() % 2 == 0 ? fieldPath(fldIdx) : indexName(fldIdx), itemVectors[0].View());
			} else {
				reindexer::VariantArray views;
				views.reserve(itemVectors.size());
				std::ranges::copy(std::views::transform(itemVectors, [](const auto& v) { return reindexer::Variant{v.View()}; }),
								  std::back_inserter(views));
				query.Set(rand() % 2 == 0 ? fieldPath(fldIdx) : indexName(fldIdx), views);
			}
			reindexer::QueryResults qr;
			rt.Update(query, qr);
			ASSERT_TRUE(qr.begin().Status().ok()) << qr.begin().Status().what();
			checkQRAfterUpdate<isArray>(qr, id, kIndexesCount, vectors);
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
				checkQRAfterUpdate<isArray>(qr, id, kIndexesCount, vectors);
			} break;
			default: {
				reindexer::QueryResults qr;
				rt.Delete(reindexer::Query(kNsName).Where(kFieldNameId, CondEq, id), qr);
				checkQRAfterUpdate<isArray>(qr, id, kIndexesCount, vectors);
			} break;
		}
	}
}
CATCH_AND_ASSERT

TEST_F(FloatVector, TestSelectFiltersArray) { TestSelectFilters<Array>(); }
TEST_F(FloatVector, TestSelectFiltersScalar) { TestSelectFilters<Scalar>(); }

template <FloatVector::IsArray isArray>
void FloatVector::TestUpdateIndex(reindexer::VectorMetric metric) try {
	constexpr static auto kNsName = isArray ? "float_vector_index_array_ns"sv : "float_vector_index_scalar_ns"sv;
	static const std::string kFieldNameFloatVector = "float_vector";
	constexpr static size_t kStartSize = 100;
	constexpr static size_t kMaxElementsInStep = 100;
	constexpr static size_t kK = kMaxElementsInStep * 5;
#if defined(RX_WITH_STDLIB_DEBUG)
	constexpr static size_t kDimension = isArray ? 4 : 16;
#elif defined(REINDEX_WITH_TSAN) || defined(REINDEX_WITH_ASAN)
	constexpr static size_t kDimension = isArray ? 8 : 32;
#else
	constexpr static size_t kDimension = isArray ? 128 : 1'024;
#endif
	constexpr static size_t kTotalIDs = isArray ? (kMaxElementsInStep * 10) : kMaxElementsInStep * 40;

	struct {
		reindexer::IndexDef indexDef;
		reindexer::KnnSearchParams searchParams;
	} paramsOfIndexes[]{
		{{kFieldNameFloatVector,
		  {kFieldNameFloatVector},
		  IndexHnsw,
		  IndexOpts{}.Array(isArray).SetFloatVector(
			  IndexHnsw,
			  FloatVectorIndexOpts{}.SetDimension(kDimension).SetStartSize(kStartSize).SetM(16).SetEfConstruction(200).SetMetric(metric))},
		 reindexer::HnswSearchParams{}.K(kK).Ef(kK * 2)},
		{{kFieldNameFloatVector,
		  {kFieldNameFloatVector},
		  IndexVectorBruteforce,
		  IndexOpts{}.Array(isArray).SetFloatVector(
			  IndexVectorBruteforce, FloatVectorIndexOpts{}.SetDimension(kDimension).SetStartSize(kStartSize).SetMetric(metric))},
		 reindexer::BruteForceSearchParams{}.K(kK)},
		{{kFieldNameFloatVector,
		  {kFieldNameFloatVector},
		  IndexIvf,
		  IndexOpts{}.Array(isArray).SetFloatVector(IndexIvf, FloatVectorIndexOpts{}
																  .SetDimension(kDimension)
																  .SetNCentroids(std::max<size_t>(kMaxElementsInStep / 30, 2))
																  .SetMetric(metric))},
		 reindexer::IvfSearchParams{}.K(kK).NProbe(std::max<size_t>(kK / 200, 2))},
		{{kFieldNameFloatVector,
		  {kFieldNameFloatVector},
		  IndexIvf,
		  IndexOpts{}.Array(isArray).SetFloatVector(
			  IndexIvf, FloatVectorIndexOpts{}.SetDimension(kDimension).SetNCentroids(kMaxElementsInStep * 2).SetMetric(metric))},
		 reindexer::IvfSearchParams{}.K(kK).NProbe(std::max<size_t>(kK / 200, 2))},
	};
	const auto rndIndexParams = [&] { return paramsOfIndexes[rand() % (sizeof(paramsOfIndexes) / sizeof(paramsOfIndexes[0]))]; };
	rt.OpenNamespace(kNsName);
	rt.DefineNamespaceDataset(kNsName, {IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts{}.PK(), 0}});
	int currMaxId = 0;
	std::unordered_set<int> emptyVectors;
	std::unordered_map<int, size_t> vectorsCount;
	auto updateSomeData = [&, lastMaxId = 0](HasIndex hasIndex) mutable {
		lastMaxId = currMaxId;
		currMaxId += (rand() % kMaxElementsInStep + 1);
		upsertItems<kDimension, isArray>(kNsName, kFieldNameFloatVector, lastMaxId, currMaxId, emptyVectors, vectorsCount, hasIndex);
		deleteSomeItems(kNsName, currMaxId, emptyVectors, vectorsCount);
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
	while (size_t(currMaxId) < kTotalIDs) {
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

TEST_F(FloatVector, UpdateArrayIndex) {
	auto metric = randMetric();
	TestCout() << fmt::format("Running test for '{}'-metric", VectorMetricToStr(metric)) << std::endl;
	TestUpdateIndex<Array>(metric);
}
TEST_F(FloatVector, UpdateScalarIndex) {
	auto metric = randMetric();
	TestCout() << fmt::format("Running test for '{}'-metric", VectorMetricToStr(metric)) << std::endl;
	TestUpdateIndex<Scalar>(metric);
}

// TODO add array version in #2231
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
	std::unordered_map<int, size_t> vectorsCount;
	for (const auto& opts : indexOpts) {
		idxName = indexName(opts.first);
		nsName = kNsName + idxName;
		rt.OpenNamespace(nsName);
		rt.DefineNamespaceDataset(nsName, {IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts{}.PK(), 0}});
		rt.AddIndex(nsName, reindexer::IndexDef{idxName, {idxName}, opts.first, opts.second});

		emptyVectors.clear();
		vectorsCount.clear();
		upsertItems<kDimension, Scalar>(nsName, idxName, 0, kMaxElements, emptyVectors, vectorsCount);

		auto qr = rt.Select(reindexer::Query(nsName).Where(idxName, CondEmpty, reindexer::VariantArray{}));
		ASSERT_TRUE(qr.Count() > 0) << idxName;
		ASSERT_EQ(emptyVectors.size(), qr.Count()) << idxName;
		size_t deletedCount = vectorsCount.size() + emptyVectors.size();
		deleteSomeItems(nsName, kMaxElements, emptyVectors, vectorsCount);
		deletedCount -= vectorsCount.size();
		deletedCount -= emptyVectors.size();
		ASSERT_TRUE(deletedCount > 0) << idxName;
		qr = rt.Select(reindexer::Query(nsName).Where(idxName, CondEmpty, reindexer::VariantArray{}));
		const auto isNullCount = qr.Count();
		ASSERT_EQ(isNullCount, emptyVectors.size()) << idxName;
		for (auto& it : qr) {
			auto item = it.GetItem(false);
			auto id = item["id"].As<int>();
			ASSERT_TRUE(emptyVectors.end() != emptyVectors.find(id)) << idxName;
		}
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

template <FloatVector::IsArray isArray>
void FloatVector::TestKeeperQRIterateAfterDropIndex() try {
	constexpr static auto kNsName = isArray ? "fv_delete_index_array_ns_keeper"sv : "fv_delete_index_scalar_ns_keeper"sv;
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
				IndexOpts{}.Array(isArray).SetFloatVector(
					IndexIvf,
					FloatVectorIndexOpts{}.SetDimension(kDimension).SetNCentroids(5).SetMetric(reindexer::VectorMetric::InnerProduct)),
				0},
		});

	std::unordered_set<int> emptyVectors;
	std::unordered_map<int, size_t> vectorsCount;
	for (size_t i = 0; i < kItemCount; ++i) {
		auto item = newItem<kDimension, isArray>(kNsName, kFieldNameIvf, i, emptyVectors, vectorsCount);
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

TEST_F(FloatVector, KeeperQRIterateAfterDropArrayIndex) { TestKeeperQRIterateAfterDropIndex<Array>(); }
TEST_F(FloatVector, KeeperQRIterateAfterDropScalarIndex) { TestKeeperQRIterateAfterDropIndex<Scalar>(); }

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
