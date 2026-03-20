#include <thread>
#include "core/id_type.h"
#include "core/system_ns_names.h"
#include "estl/condition_variable.h"
#include "estl/lock.h"
#include "estl/mutex.h"
#include "tools/fsops.h"
#include "transaction_api.h"

#if defined(__GNUC__) && !defined(__clang__) && defined(REINDEX_WITH_TSAN)
// GCC-only workaround for a known false-positive -Warray-bounds warning
// triggered by std::thread when building with ThreadSanitizer.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Warray-bounds"
#endif

TEST_F(TransactionApi, ConcurrencyTest) {
	using reindexer::fs::GetTempDir;
	using reindexer::fs::JoinPath;

	const std::string kDir = JoinPath(GetTempDir(), "TransactionApi/ConcurrencyTest");
	const std::string kDsn = "builtin://" + kDir;
	rt.reindexer.reset();
	std::ignore = reindexer::fs::RmDirAll(kDir);

	std::unique_ptr<Reindexer> rx = std::make_unique<Reindexer>();
	Error err = rx->Connect(kDsn);
	ASSERT_TRUE(err.ok()) << err.what();

	rt.EnablePerfStats(*rx);

	OpenNamespace(*rx);
	SetTxCopyConfigs(*rx);

	const std::array<DataRange, 5> ranges = {{{0, 1000, "initial"},
											  {1000, 49000, "first_writer"},
											  {49000, 55000, "second_writer"},
											  {55000, 130000, "third_writer"},
											  {130000, 190000, "fourth_writer"}}};

#if !defined(RX_WITH_STDLIB_DEBUG) && !defined(REINDEX_WITH_TSAN)
	const size_t smallPortion = 100;
	const size_t mediumPortion = 1000;
	const size_t bigPortion = 15000;
	const size_t hugePortion = 30000;
	const auto selectLimit = reindexer::IdType::Max().ToNumber();
#else	// defined(RX_WITH_STDLIB_DEBUG) || defined(REINDEX_WITH_TSAN)
	const size_t smallPortion = 100;
	const size_t mediumPortion = 1000;
	const size_t bigPortion = 3000;
	const size_t hugePortion = 10000;
	const int selectLimit = 6000;
#endif	// defined(RX_WITH_STDLIB_DEBUG) || defined(REINDEX_WITH_TSAN)

	reindexer::condition_variable cond;
	reindexer::mutex mtx;
	std::atomic<unsigned> totalTxs{0}, expectedTotalItems{unsigned(ranges[0].till - ranges[0].from)};

	AddDataToNsTx(*rx, ranges[0].from, ranges[0].till, ranges[0].data);
	++totalTxs;
	std::vector<std::thread> readThreads;
	std::vector<std::thread> writeThreads;
	std::atomic<bool> stop{false};

	auto selectFn = [&] {
		while (!stop.load()) {
			SelectData(*rx, GetItemsCount(*rx), selectLimit);
			std::this_thread::sleep_for(std::chrono::milliseconds(50));
		}
	};

	readThreads.emplace_back(selectFn);
	readThreads.emplace_back(selectFn);

	writeThreads.emplace_back(
		[&](int id) {
			reindexer::unique_lock lck(mtx);
			cond.wait(lck);
			lck.unlock();
			size_t from = ranges[id].from;
			for (size_t i = 0; i < 3; ++i) {
				size_t portion = GetPortion(from, bigPortion, ranges[id].till);
				AddDataToNsTx(*rx, from, bigPortion, ranges[id].data);
				expectedTotalItems += portion;
				++totalTxs;
				from += portion;
				std::this_thread::yield();
				portion = GetPortion(from, mediumPortion, ranges[id].till);
				AddDataToNsTx(*rx, from, mediumPortion, ranges[id].data);
				expectedTotalItems += portion;
				++totalTxs;
				from += portion;
				std::this_thread::yield();
			}
		},
		1);
	writeThreads.emplace_back(
		[&](int id) {
			reindexer::unique_lock lck(mtx);
			cond.wait(lck);
			lck.unlock();
			size_t from = ranges[id].from;
			size_t till = ranges[id].till;
			size_t portion = 0;
			for (size_t pos = from; pos < till; pos += portion) {
				portion = GetPortion(from, smallPortion, till);
				AddDataToNsTx(*rx, pos, portion, ranges[id].data);
				expectedTotalItems += portion;
				++totalTxs;
				std::this_thread::yield();
			}
		},
		2);
	writeThreads.emplace_back(
		[&](int id) {
			reindexer::unique_lock lck(mtx);
			cond.wait(lck);
			lck.unlock();
			size_t from = ranges[id].from;
			for (size_t i = 0; i < 5; ++i) {
				size_t portion = GetPortion(from, bigPortion, ranges[id].till);
				AddDataToNsTx(*rx, from, portion, ranges[id].data);
				expectedTotalItems += portion;
				++totalTxs;
				from += portion;
				std::this_thread::yield();
			}
		},
		3);
	writeThreads.emplace_back(
		[&](int id) {
			reindexer::unique_lock lck(mtx);
			cond.wait(lck);
			lck.unlock();
			size_t from = ranges[id].from;
			for (size_t i = 0; i < 2; ++i) {
				size_t portion = GetPortion(from, hugePortion, ranges[id].till);
				AddDataToNsTx(*rx, from, portion, ranges[id].data);
				expectedTotalItems += portion;
				++totalTxs;
				from += portion;
				std::this_thread::yield();
			}
		},
		4);

	std::this_thread::sleep_for(std::chrono::milliseconds(200));
	cond.notify_all();
	for (auto& it : writeThreads) {
		it.join();
	}
	stop = true;
	for (auto& it : readThreads) {
		it.join();
	}

	auto ValidateData = [&] {
		QueryResults qr;
		err = rx->Select(Query(default_namespace), qr);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_GT(expectedTotalItems.load(), hugePortion);
		ASSERT_EQ(qr.Count(), expectedTotalItems.load());
#if !defined(RX_WITH_STDLIB_DEBUG) && !defined(REINDEX_WITH_TSAN)
		ASSERT_EQ(qr.Count(), ranges.back().till);
#endif	// !defined(RX_WITH_STDLIB_DEBUG) && !defined(REINDEX_WITH_TSAN)
		for (auto it : qr) {
			auto item = it.GetItem(false);
			int id = static_cast<size_t>(item[kFieldId].As<int>());
			bool idIsCorrect = false;
			for (auto& it : ranges) {
				if (id >= it.from && id < it.till) {
					auto data = item[kFieldData].As<std::string>();
					auto data1 = item[kFieldData1].As<std::string>();
					auto data2 = item[kFieldData2].As<std::string>();

					ASSERT_EQ(data, fmt::format("{}_{}_{}", kFieldData, it.data, id));
					ASSERT_EQ(data1, fmt::format("{}_{}_{}", kFieldData1, it.data, id));
					ASSERT_EQ(data2, fmt::format("{}_{}_{}", kFieldData2, it.data, id));
					idIsCorrect = true;
					break;
				}
			}
			ASSERT_TRUE(idIsCorrect) << id;
		}
	};
	auto ValidateStats = [&] {
		// Basic sanity check
		auto stats = GetTxPerfStats(*rx, default_namespace);
		EXPECT_EQ(stats.totalCount, totalTxs.load());
		EXPECT_GT(stats.totalCopyCount, 1);	 // Expecting at least 2 copies
		EXPECT_LT(stats.totalCopyCount, stats.totalCount);
		EXPECT_EQ(stats.minStepsCount, smallPortion);
		EXPECT_EQ(stats.maxStepsCount, hugePortion);
		EXPECT_GT(stats.avgStepsCount, stats.minStepsCount);
		EXPECT_LT(stats.avgStepsCount, stats.maxStepsCount);

		EXPECT_GT(stats.minPrepareTimeUs, 0);
		EXPECT_GT(stats.avgPrepareTimeUs, stats.minPrepareTimeUs);
		EXPECT_GT(stats.maxPrepareTimeUs, stats.avgPrepareTimeUs);

		EXPECT_GT(stats.minCommitTimeUs, 0);
		EXPECT_GT(stats.avgCommitTimeUs, stats.minCommitTimeUs);
		EXPECT_GT(stats.maxCommitTimeUs, stats.avgCommitTimeUs);

		EXPECT_GT(stats.minCopyTimeUs, 0);
		EXPECT_GT(stats.avgCopyTimeUs, stats.minCopyTimeUs);
		EXPECT_GT(stats.maxCopyTimeUs, stats.avgCopyTimeUs);
	};

	ValidateData();
	ValidateStats();

	// Check data after storage reload
	rx = std::make_unique<Reindexer>();
	err = rx->Connect(kDsn);
	ASSERT_TRUE(err.ok()) << err.what();
	ValidateData();
}

TEST_F(TransactionApi, IndexesOptimizeTest) {
	// Perform select to trick ns copy heuristic - it expecting at least one select query
	auto qr = rt.Select(Query(default_namespace));
	ASSERT_EQ(0, qr.Count());
	// Add 15000 items to ns. With default settings this should call
	// transaction with ns copy & atomic change
	AddDataToNsTx(*rt.reindexer, 0, 15000, "data");
	// Fetch optimization state
	qr = rt.Select(Query(reindexer::kMemStatsNamespace).Where("name", CondEq, default_namespace));
	ASSERT_EQ(1, qr.Count());

	// Ensure, that ns indexes is in optimized state immediately after tx done
	bool optimization_completed = qr.begin().GetItem(false)["optimization_completed"].Get<bool>();
	ASSERT_EQ(true, optimization_completed);
}

#if defined(__GNUC__) && !defined(__clang__) && defined(REINDEX_WITH_TSAN)
#pragma GCC diagnostic pop
#endif

TEST_F(TransactionApi, ConcurrentTwoFloatVectorUpdateTest) {
#if defined(REINDEX_WITH_TSAN) || defined(REINDEX_WITH_ASAN) || defined(_GLIBCXX_DEBUG)
	static constexpr bool kIsRelease = false;
#else
	static constexpr bool kIsRelease = true;
#endif
	static const size_t kDataSize = kIsRelease ? 10'326 : 1'326;
	static const size_t kDim = kIsRelease ? 1000 : 10;
	static const size_t kTxBatchSize = kIsRelease ? 1000 : 200;
	static const std::string kIndexName1 = "hnsw1";
	static const std::string kIndexName2 = "hnsw2";

	auto addIndex = [this](const std::string& indexName) {
		rt.AddIndex(
			default_namespace,
			reindexer::IndexDef{indexName,
								{indexName},
								IndexHnsw,
								IndexOpts{}.SetFloatVector(IndexHnsw, FloatVectorIndexOpts{}
																		  .SetStartSize(kDataSize)
																		  .SetMetric(reindexer::VectorMetric::Cosine)
																		  .SetMultithreading(MultithreadingMode::MultithreadTransactions)
																		  .SetM(16)
																		  .SetEfConstruction(200)
																		  .SetDimension(kDim))});
	};

	addIndex(kIndexName1);
	addIndex(kIndexName2);

	auto points = [] {
		std::vector<reindexer::FloatVector> res;
		res.reserve(kDataSize);

		for (size_t i = 0; i < kDataSize; ++i) {
			auto point = reindexer::FloatVector::CreateNotInitialized(reindexer::FloatVectorDimension(kDim));
			for (size_t j = 0; j < kDim; ++j) {
				point.RawData()[j] = float(rand() % 10'000) / 10'000;
			}
			res.emplace_back(std::move(point));
		}
		return res;
	}();

	const size_t kNumTxThreads = 3;
	std::atomic<size_t> nextBatch{0};
	const size_t numBatches = (kDataSize + kTxBatchSize - 1) / kTxBatchSize;

	auto worker = [&]() {
		while (true) {
			size_t batchId = nextBatch.fetch_add(1, std::memory_order_relaxed);
			if (batchId >= numBatches) {
				break;
			}

			size_t begin = batchId * kTxBatchSize;
			size_t end = std::min(begin + kTxBatchSize, kDataSize);

			auto tx = rt.NewTransaction(default_namespace);
			size_t cnt = 0;

			for (size_t itemId = begin; itemId < end; ++itemId) {
				auto txItem = tx.NewItem();
				txItem["id"] = int64_t(itemId);
				auto view = points[itemId].View();
				txItem[kIndexName1] = view;
				txItem[kIndexName2] = view;

				ASSERT_TRUE(tx.Insert(std::move(txItem)).ok());
				++cnt;
			}

			auto qr = rt.CommitTransaction(tx);
			ASSERT_EQ(qr.Count(), cnt);
		}
	};

	std::vector<std::thread> threads;
	threads.reserve(kNumTxThreads);
	for (size_t i = 0; i < kNumTxThreads; ++i) {
		threads.emplace_back(worker);
	}
	for (auto& th : threads) {
		th.join();
	}

	auto qr = rt.Select(Query(default_namespace).Sort("id", false).SelectAllFields());

	ASSERT_EQ(qr.Count(), kDataSize);

	int id = 0;
	for (auto it = qr.begin(); it != qr.end(); ++it) {
		auto item = (*it).GetItem();
		auto fv = item[kIndexName1].template As<reindexer::ConstFloatVectorView>().Span();
		auto fvQ = item[kIndexName2].template As<reindexer::ConstFloatVectorView>().Span();

		ASSERT_EQ(fvQ.size(), kDim);
		ASSERT_EQ(fv.size(), kDim);

		auto point = points[id].Span();
		for (size_t i = 0; i < kDim; ++i) {
			ASSERT_TRUE(reindexer::fp::EqualWithinULPs(fv[i], point[i]));
			ASSERT_TRUE(reindexer::fp::EqualWithinULPs(fvQ[i], point[i]));
		}
		++id;
	}
}
