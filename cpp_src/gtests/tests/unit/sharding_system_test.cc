#include "core/cjson/jsonbuilder.h"
#include "estl/condition_variable.h"
#include "estl/lock.h"
#include "estl/mutex.h"
#include "gtests/tests/gtest_cout.h"
#include "sharding_system_api.h"

using namespace reindexer;

TEST_F(ShardingSystemApi, Reconnect) {
	Init();
	std::shared_ptr<client::Reindexer> rx = getNode(0)->api.reindexer;
	for (size_t shard = 1; shard < kShards; ++shard) {
		for (size_t clusterNodeId = 0; clusterNodeId < kNodesInCluster; ++clusterNodeId) {
			const auto server = shard * kNodesInCluster + clusterNodeId;
			ASSERT_TRUE(StopByIndex(server));
			if (clusterNodeId) {
				AwaitOnlineReplicationStatus(server - 1);
			}
			Error err;
			const std::string location = "key" + std::to_string(shard);
			bool succeed = false;
			for (size_t i = 0; i < 20; ++i) {  // FIXME: Max retries count should be 1
				client::QueryResults qr;
				err = rx->Select(Query(default_namespace).Where(kFieldLocation, CondEq, location), qr);
				if (err.ok()) {	 // First request may get an error, because disconnect event may not be handled yet
					ASSERT_EQ(qr.Count(), 40) << "; shard = " << shard << "; node = " << clusterNodeId;
					succeed = true;
					break;
				}
				std::this_thread::sleep_for(std::chrono::milliseconds(2));
			}
			ASSERT_TRUE(succeed) << err.what() << "; shard = " << shard << "; node = " << clusterNodeId;

			client::QueryResults qr;
			const std::string newValue = "most probably updated";
			err = rx->Update(Query(default_namespace).Set(kFieldData, newValue).Where(kFieldLocation, CondEq, location), qr);
			ASSERT_TRUE(err.ok()) << err.what() << "; shard = " << shard << "; node = " << clusterNodeId;
			ASSERT_EQ(qr.Count(), 40) << "; shard = " << shard << "; node = " << clusterNodeId;
			StartByIndex(server);
		}
	}
}

TEST_F(ShardingSystemApi, ReconnectTimeout) {
	constexpr auto kTimeout = std::chrono::seconds(2);
	InitShardingConfig cfg;
	cfg.disableNetworkTimeout = true;
	Init(std::move(cfg));
	std::shared_ptr<client::Reindexer> rx = getNode(0)->api.reindexer;
	const std::string newValue = "most probably updated";
	// Stop half of the clusters' nodes
	for (size_t shard = 1; shard < kShards; ++shard) {
		for (size_t clusterNodeId = 0; clusterNodeId < kNodesInCluster / 2; ++clusterNodeId) {
			ASSERT_TRUE(StopByIndex(shard, clusterNodeId));
		}
	}
	for (size_t shard = 1; shard < kShards; ++shard) {
		// Stop one more of the clusters' nodes. Now there is no consensus
		const auto server = shard * kNodesInCluster + kNodesInCluster / 2;
		ASSERT_TRUE(StopByIndex(server));
		const std::string location = "key" + std::to_string(shard);
		client::QueryResults qr;
		auto err = rx->WithTimeout(kTimeout).Update(
			Query(default_namespace).Set(kFieldData, newValue).Where(kFieldLocation, CondEq, location), qr);
		if (err.code() != errTimeout && err.code() != errNetwork && err.code() != errUpdateReplication) {
			ASSERT_TRUE(false) << err.what() << "(" << err.code() << ")" << "; shard = " << shard;
		}
	}

	for (size_t shard = 1; shard < kShards; ++shard) {
		const std::string location = "key" + std::to_string(shard);
		client::QueryResults qr;
		auto err = rx->WithTimeout(kTimeout).Update(
			Query(default_namespace).Set(kFieldData, newValue).Where(kFieldLocation, CondEq, location), qr);
		ASSERT_EQ(err.code(), errTimeout) << err.what() << "; shard = " << shard;
	}
}

TEST_F(ShardingSystemApi, MultithreadedReconnect) {
	const size_t kValidThreadsCount = 5;
	Init();
	const auto& kDSN = getNode(0)->kRPCDsn;
	auto upsertItemF = [&](int index, client::Reindexer& rx) {
		client::Item item = rx.NewItem(default_namespace);
		EXPECT_TRUE(item.Status().ok());

		const std::string key = std::string("key" + std::to_string(kShards));

		WrSerializer wrser;
		reindexer::JsonBuilder jsonBuilder(wrser, ObjType::TypeObject);
		jsonBuilder.Put(kFieldId, index);
		jsonBuilder.Put(kFieldLocation, key);
		jsonBuilder.Put(kFieldData, RandString());
		jsonBuilder.End();

		Error err = item.FromJSON(wrser.Slice());
		EXPECT_TRUE(err.ok()) << err.what();
		return rx.Upsert(default_namespace, item);
	};

	struct [[nodiscard]] RxWithStatus {
		std::shared_ptr<client::Reindexer> client = std::make_shared<client::Reindexer>();
		std::atomic<int> errors = 0;
	};

	std::vector<RxWithStatus> rxClients(kValidThreadsCount);
	for (auto& rx : rxClients) {
		auto err = rx.client->Connect(kDSN);
		ASSERT_TRUE(err.ok()) << err.what();
		err = rx.client->Status();
		ASSERT_TRUE(err.ok()) << err.what();
	}

	for (size_t shard = 1; shard < kShards; ++shard) {
		const std::string key = "key" + std::to_string(shard);
		// The last node won't be stopped because (in this case)
		// there will be no other nodes to reconnect to.
		TestCout() << "Shard: " << shard << std::endl;
		std::vector<std::thread> anyResultThreads;
		std::atomic<bool> stop = {false};
		for (size_t i = 0; i < 3; ++i) {
			anyResultThreads.emplace_back(std::thread([&, index = i]() {
				client::Reindexer rx;
				auto err = rx.Connect(kDSN);
				ASSERT_TRUE(err.ok()) << err.what();
				while (!stop) {
					err = upsertItemF(index, rx);
					(void)err;	// ignore; Errors are expected
					std::this_thread::sleep_for(std::chrono::milliseconds(10));
				}
			}));
			anyResultThreads.emplace_back(std::thread([&]() {
				client::Reindexer rx;
				auto err = rx.Connect(kDSN);
				ASSERT_TRUE(err.ok()) << err.what();
				while (!stop) {
					client::QueryResults qr;
					err = rx.Select(Query(default_namespace).Where(kFieldLocation, CondEq, key), qr);
					if (err.ok()) {
						ASSERT_TRUE(qr.Count() == 40) << qr.Count();
					}
					qr = client::QueryResults();
					err = rx.Select(Query(default_namespace), qr);
					if (err.ok()) {
						ASSERT_GE(qr.Count(), 40 * kShards);
					}
					std::this_thread::sleep_for(std::chrono::milliseconds(10));
				}
			}));
		}

		for (size_t clusterNodeId = 0; clusterNodeId < kNodesInCluster; ++clusterNodeId) {
			const auto server = shard * kNodesInCluster + clusterNodeId;
			TestCout() << "Stopping: " << server << std::endl;
			ASSERT_TRUE(StopByIndex(server));
			if (clusterNodeId) {
				AwaitOnlineReplicationStatus(server - 1);
			}
			std::vector<std::thread> alwaysValidThreads;
			// std::atomic<int> errorsCount = {0};
			constexpr size_t kMaxErrors = 5;  // FIXME: Max errors should be 1
			for (size_t i = 0; i < kValidThreadsCount; ++i) {
				alwaysValidThreads.emplace_back(std::thread([&, currShard = shard, index = i, idx = i]() {
					auto& rx = rxClients[idx];
					bool succeed = false;
					Error err;
					for (size_t j = 0; j < kMaxErrors; ++j) {
						err = upsertItemF(index, *rx.client);
						if (err.ok()) {
							succeed = true;
							break;
						}
						// ASSERT_LE(++errorsCount, kMaxErrors) << err.what() << "; shard = " << currShard << "; location = " << key;
					}
					ASSERT_TRUE(succeed) << err.what() << "; shard = " << currShard << "; location = " << key;
				}));
				alwaysValidThreads.emplace_back(std::thread([&, currShard = shard, idx = i]() {
					auto& rx = rxClients[idx];
					bool succeed = false;
					Error err;
					for (size_t j = 0; j < kMaxErrors; ++j) {
						client::QueryResults qr;
						const auto query = Query(default_namespace).Where(kFieldLocation, CondEq, key);
						err = rx.client->Select(query, qr);
						if (err.ok()) {
							ASSERT_EQ(qr.Count(), 40) << "; shard = " << currShard;
							succeed = true;
							break;
						}
						// ASSERT_LE(++errorsCount, kMaxErrors) << err.what() << "; shard = " << currShard << "; location = " << key;
					}
					ASSERT_TRUE(succeed) << err.what() << "; shard = " << currShard;
				}));
			}
			for (auto& th : alwaysValidThreads) {
				th.join();
			}
			// ASSERT_LE(errorsCount, std::max(kMaxErrors, kValidThreadsCount)) << "Too many network errors: " << errorsCount.load();
			StartByIndex(server);
		}
		stop = true;
		for (auto& th : anyResultThreads) {
			th.join();
		}
	}
}

TEST_F(ShardingSystemApi, Shutdown) {
	Init();

	std::vector<std::thread> threads;
	std::atomic<int> counter{0};
	std::atomic<bool> done = {false};
	constexpr auto kSleepTime = std::chrono::milliseconds(10);

	auto addItemFn = [this, &counter, &done, kSleepTime](size_t shardId, size_t node) noexcept {
		while (!done) {
			auto err = AddRow(default_namespace, shardId, node, counter++);
			(void)err;	// ignored; Errors are expected here
			std::this_thread::sleep_for(kSleepTime);
		}
	};
	for (size_t j = 0; j < kNodesInCluster; ++j) {
		for (size_t i = 0; i < kShards; ++i) {
			threads.emplace_back(addItemFn, i, j);
		}
	}
	std::this_thread::sleep_for(std::chrono::milliseconds(200));
	for (size_t j = 0; j < kNodesInCluster; ++j) {
		for (size_t i = 0; i < kShards; ++i) {
			TestCout() << "Stopping shard " << i << ", node " << j << std::endl;
			ASSERT_TRUE(StopSC(i, j));
		}
	}
	done = true;
	for (auto& th : threads) {
		th.join();
	}
}

TEST_F(ShardingSystemApi, AwaitShards) {
	const std::string kNewNs = "new_namespace";
	constexpr size_t kThreads = 5;
	InitShardingConfig cfg;
	cfg.rowsInTableOnShard = 0;
	cfg.additionalNss = {{kNewNs, false}};
	cfg.nodesInCluster = 1;
	Init(std::move(cfg));

	reindexer::mutex mtx;
	bool ready = false;
	reindexer::condition_variable cv;
	const std::vector<std::string> kNamespaces = {default_namespace, kNewNs};

	for (size_t shard = 1; shard < kShards; ++shard) {
		Stop(shard);
	}

	std::shared_ptr<client::Reindexer> rx = getNode(0)->api.reindexer;
	std::vector<std::thread> tds;
	tds.reserve(kThreads);
	for (size_t i = 0; i < kThreads; ++i) {
		tds.emplace_back([&] {
			unique_lock lck(mtx);
			cv.wait(lck, [&ready] { return ready; });
			lck.unlock();
			for (auto& ns : kNamespaces) {
				auto err = rx->OpenNamespace(ns);
				ASSERT_TRUE(err.ok()) << "Namespace: " << ns << "; " << err.what();
			}
		});
	}

	unique_lock lck(mtx);
	ready = true;
	lck.unlock();
	cv.notify_all();
	std::this_thread::sleep_for(std::chrono::milliseconds(200));
	for (size_t shard = 1; shard < kShards; ++shard) {
		Start(shard);
	}
	for (auto& th : tds) {
		th.join();
	}

	std::vector<NamespaceDef> nsDefs;
	for (size_t shard = 0; shard < kShards; ++shard) {
		rx = getNode(shard)->api.reindexer;
		nsDefs.clear();
		auto err = rx->WithShardId(ShardingKeyType::ProxyOff, false).EnumNamespaces(nsDefs, EnumNamespacesOpts().OnlyNames().HideSystem());
		ASSERT_TRUE(err.ok()) << "shard: " << shard << "; " << err.what();
		ValidateNamespaces(shard, kNamespaces, nsDefs);
	}
}

TEST_F(ShardingSystemApi, AwaitShardsTimeout) {
	const std::string kNewNs = "new_namespace";
	constexpr size_t kThreads = 2;	// TODO: Add more threads, when multiple connect will be used in sharding proxy

	InitShardingConfig cfg;
	cfg.additionalNss = {{kNewNs, false}};
	cfg.awaitTimeout = std::chrono::seconds(3);
	Init(std::move(cfg));

	for (size_t shard = 1; shard < kShards; ++shard) {
		Stop(shard);
	}

	std::atomic<bool> done = false;
	std::shared_ptr<client::Reindexer> rx = getNode(0)->api.reindexer;
	std::vector<std::thread> tds;
	tds.reserve(kThreads);
	for (size_t i = 0; i < kThreads; ++i) {
		tds.emplace_back([&] {
			auto err = rx->OpenNamespace(kNewNs);
			ASSERT_FALSE(err.ok());
			ASSERT_EQ(err.code(), errTimeout);
		});
	}
	std::thread cancelingThread = std::thread([&] {
		auto time = std::chrono::seconds(30);
		constexpr auto kStep = std::chrono::seconds(1);
		while (!done && time.count() > 0) {
			time -= kStep;
			std::this_thread::sleep_for(kStep);
		}
		if (!done) {
			Stop(0);
			ASSERT_TRUE(false) << "Timeout expired";
		}
	});

	for (auto& th : tds) {
		th.join();
	}
	done = true;
	cancelingThread.join();

	std::vector<NamespaceDef> nsDefs;
	for (size_t shard = 0; shard < kShards; ++shard) {
		rx = getNode(shard)->api.reindexer;
		nsDefs.clear();
		auto err = rx->WithShardId(ShardingKeyType::ProxyOff, false).EnumNamespaces(nsDefs, EnumNamespacesOpts().OnlyNames().HideSystem());
		ASSERT_TRUE(err.ok()) << "shard: " << shard << "; " << err.what();
		ValidateNamespaces(shard, {default_namespace}, nsDefs);
	}
}
