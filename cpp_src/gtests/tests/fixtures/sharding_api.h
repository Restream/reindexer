#pragma once

#include <fstream>
#include "clusterization_api.h"
#include "core/cjson/jsonbuilder.h"
#include "tools/fsops.h"

struct InitShardingConfig {
	struct Namespace {
		std::string name;
		bool withData = false;
	};

	int shards = 3;
	int nodesInCluster = 3;
	int rowsInTableOnShard = 40;
	bool disableNetworkTimeout = false;
	bool createAdditionalIndexes = true;
	std::vector<Namespace> additionalNss;
	std::chrono::seconds awaitTimeout = std::chrono::seconds(30);
	fast_hash_map<int, std::string>* insertedItemsById = nullptr;
	int nodeIdInThread = -1;  // Allows to run one of the nodes in thread, instead fo process
	uint8_t strlen = 0;		  // Strings len in items, which will be created during Fill()
};

class ShardingApi : public ReindexerApi {
public:
	void Init(InitShardingConfig c = InitShardingConfig()) {
		std::vector<InitShardingConfig::Namespace> namespaces = std::move(c.additionalNss);
		namespaces.emplace_back(InitShardingConfig::Namespace{default_namespace, true});
		kShards = c.shards;
		kNodesInCluster = c.nodesInCluster;
		svc_.resize(kShards);
		for (auto& cluster : svc_) {
			cluster.resize(kNodesInCluster);
		}
		auto getHostDsn = [&](size_t id) {
			return "cproto://127.0.0.1:" + std::to_string(GetDefaults().defaultRpcPort + id) + "/shard" + std::to_string(id);
		};
		config_.shardsAwaitingTimeout = c.awaitTimeout;
		size_t id = 0;
		std::string nsList("\n");
		config_.namespaces.clear();
		config_.namespaces.resize(namespaces.size());
		for (size_t i = 0; i < namespaces.size(); ++i) {
			config_.namespaces[i].ns = namespaces[i].name;
			config_.namespaces[i].index = kFieldLocation;
			config_.namespaces[i].defaultShard = 0;

			nsList += "  - ";
			nsList += namespaces[i].name;
			nsList += '\n';
		}
		config_.shards.clear();
		for (size_t shard = 0; shard < kShards; ++shard) {
			config_.shards[shard].reserve(kNodesInCluster);
			for (size_t node = 0; node < kNodesInCluster; ++node) {
				config_.shards[shard].emplace_back(getHostDsn(id++));
			}
			reindexer::cluster::ShardingConfig::Key key;
			key.values.emplace_back(string("key") + std::to_string(shard));
			key.values.emplace_back(string("key") + std::to_string(shard) + "_" + std::to_string(shard));
			key.shardId = shard;
			for (size_t nsId = 0; nsId < namespaces.size(); ++nsId) {
				config_.namespaces[nsId].keys.emplace_back(key);
			}
		}

		int syncThreadsCount = 2;
		int maxSyncCount = rand() % 3;
		std::chrono::milliseconds resyncTimeout = std::chrono::milliseconds(3000);
		TestCout() << "Cluster's max_sync_count was chosen randomly: " << maxSyncCount << std::endl;

		// clang-format off
			const std::string baseClusterConf =
			"app_name: rx_node\n"
			"namespaces: []\n"// + nsList +
			"sync_threads: " + std::to_string(syncThreadsCount) + "\n"
			"enable_compression: true\n"
			"online_updates_timeout_sec: 20\n"
			"sync_timeout_sec: 60\n"
			"syncs_per_thread: " + std::to_string(maxSyncCount) + "\n"
			"retry_sync_interval_msec: " + std::to_string(resyncTimeout.count()) + "\n"
			"nodes:\n";
		// clang-format on
		for (size_t shard = 0; shard < kShards; ++shard) {
			std::string clusterConf = baseClusterConf;
			const size_t startId = shard * kNodesInCluster;
			for (size_t i = startId; i < startId + kNodesInCluster; ++i) {
				// clang-format off
				clusterConf.append(
				"  -\n"
					"    dsn: " + fmt::format("cproto://127.0.0.1:{}/shard{}", GetDefaults().defaultRpcPort + i,i) + "\n"
					"    server_id: " + std::to_string(i) + "\n");
				// clang-format on
			}

			config_.thisShardId = shard;
			config_.proxyConnCount = 4;
			config_.proxyConnThreads = 3;
			config_.proxyConnConcurrency = 8;
			for (size_t idx = startId, node = 0; idx < startId + kNodesInCluster; ++idx, ++node) {
				const std::string replConf = "cluster_id: " + std::to_string(shard) +
											 "\n"
											 "server_id: " +
											 std::to_string(idx);

				std::string pathToDb =
					fs::JoinPath(GetDefaults().baseTestsetDbPath, "shard" + std::to_string(shard) + "/" + std::to_string(idx));
				std::string dbName = "shard" + std::to_string(idx);
				const bool asProcess = kTestServersInSeparateProcesses && (c.nodeIdInThread < 0 || (size_t(c.nodeIdInThread) != idx));
				ServerControlConfig cfg(idx, GetDefaults().defaultRpcPort + idx, GetDefaults().defaultHttpPort + idx, std::move(pathToDb),
										std::move(dbName), true, 0, asProcess);
				cfg.disableNetworkTimeout = c.disableNetworkTimeout;
				svc_[shard][node].InitServerWithConfig(std::move(cfg), replConf, (kNodesInCluster > 1 ? clusterConf : std::string()),
													   config_.GetYml(), "");
			}
		}

		std::shared_ptr<client::SyncCoroReindexer> rx = getNode(0)->api.reindexer;

		for (auto& ns : namespaces) {
			if (ns.withData) {
				NamespaceDef nsDef(ns.name);
				if (c.createAdditionalIndexes) {
					nsDef.AddIndex(kFieldId, "hash", "int", IndexOpts());
					nsDef.AddIndex(kFieldLocation, "hash", "string", IndexOpts());
					nsDef.AddIndex(kFieldShard, "hash", "string", IndexOpts());
					nsDef.AddIndex(kFieldData, "hash", "string", IndexOpts());
					nsDef.AddIndex(kFieldFTData, "text", "string", IndexOpts());
					nsDef.AddIndex(kFieldIdLocation, {kFieldId, kFieldLocation}, "hash", "composite", IndexOpts().PK());
					nsDef.AddIndex(kFieldLocationId, {kFieldLocation, kFieldId}, "hash", "composite", IndexOpts());
				} else {
					nsDef.AddIndex(kFieldId, "hash", "int", IndexOpts().PK());
				}
				Error err = rx->AddNamespace(nsDef);
				ASSERT_TRUE(err.ok()) << err.what() << "; ns: " << ns.name;
			}
		}

		for (auto& ns : namespaces) {
			if (ns.withData) {
				size_t pos = 0;
				for (size_t i = 0; i < kShards; ++i) {
					Fill(ns.name, i, pos, c.rowsInTableOnShard, c.insertedItemsById);
					pos += c.rowsInTableOnShard;
				}
			}
		}

		if (kNodesInCluster > 1) {
			for (auto& ns : namespaces) {
				if (ns.withData) {
					waitSync(ns.name);
				}
			}
		}
	}

	void SetUp() override {
		reindexer::fs::RmDirAll(GetDefaults().baseTestsetDbPath);
		ReindexerApi::SetUp();
	}
	void TearDown() override {
		ReindexerApi::TearDown();
		Stop();
		svc_.clear();
		reindexer::fs::RmDirAll(GetDefaults().baseTestsetDbPath);
	}

	bool StopByIndex(size_t idx) {
		auto [i, j] = getSCIdxs(idx);
		assert(i < svc_.size());
		assert(j < svc_[i].size());
		return StopSC(svc_[i][j]);
	}
	bool StopByIndex(size_t shard, size_t node) {
		assert(shard < svc_.size());
		assert(node < svc_[shard].size());
		return StopSC(svc_[shard][node]);
	}

	bool Stop(size_t shardId) {
		assert(shardId < svc_.size());
		auto& cluster = svc_[shardId];
		for (auto& sc : cluster) {
			if (!sc.Get()) return false;
			sc.Stop();
		}
		for (auto& sc : cluster) {
			sc.Drop();
			auto now = std::chrono::milliseconds(0);
			const auto pause = std::chrono::milliseconds(10);
			const auto serverStartTime = std::chrono::seconds(15);
			while (sc.IsRunning()) {
				now += pause;
				EXPECT_TRUE(now < serverStartTime);
				assert(now < serverStartTime);
				std::this_thread::sleep_for(pause);
			}
		}
		return true;
	}
	void Stop() {
		for (size_t i = 0; i < svc_.size(); ++i) {
			for (size_t j = 0; j < svc_[i].size(); ++j) {
				auto& server = svc_[i][j];
				if (server.Get(false)) server.Get()->Stop();
			}
		}
		for (size_t i = 0; i < svc_.size(); ++i) {
			for (size_t j = 0; j < svc_[i].size(); ++j) {
				auto& server = svc_[i][j];
				if (!server.Get(false)) continue;
				server.Drop();
				auto now = std::chrono::milliseconds(0);
				const auto pause = std::chrono::milliseconds(10);
				const auto serverStartTime = std::chrono::seconds(15);
				while (server.IsRunning()) {
					now += pause;
					EXPECT_TRUE(now < serverStartTime);
					assert(now < serverStartTime);
					std::this_thread::sleep_for(pause);
				}
			}
		}
	}

	void Start(size_t shardId) {
		assert(shardId < svc_.size());
		auto& cluster = svc_[shardId];
		for (size_t i = 0; i < cluster.size(); ++i) {
			StartByIndex(shardId * kNodesInCluster + i);
		}
	}
	void StartByIndex(size_t idx) {
		auto [i, j] = getSCIdxs(idx);
		assert(i < svc_.size());
		assert(j < svc_[i].size());
		svc_[i][j].InitServer(
			ServerControlConfig(idx, GetDefaults().defaultRpcPort + idx, GetDefaults().defaultHttpPort + idx,
								fs::JoinPath(GetDefaults().baseTestsetDbPath, "shard" + std::to_string(i) + "/" + std::to_string(idx)),
								"shard" + std::to_string(idx)));
	}

	bool StopSC(ServerControl& sc) {
		if (!sc.Get()) return false;
		sc.Stop();
		sc.Drop();
		auto now = std::chrono::milliseconds(0);
		const auto pause = std::chrono::milliseconds(10);
		const auto serverStartTime = std::chrono::seconds(15);
		while (sc.IsRunning()) {
			now += pause;
			EXPECT_TRUE(now < serverStartTime);
			assert(now < serverStartTime);
			std::this_thread::sleep_for(pause);
		}
		return true;
	}
	bool StopSC(size_t i, size_t j) {
		assert(i < svc_.size());
		assert(j < svc_[i].size());
		return StopSC(svc_[i][j]);
	}

	client::Item CreateItem(std::string_view nsName, std::shared_ptr<client::SyncCoroReindexer> rx, std::string_view key, int index,
							WrSerializer& wrser, uint8_t strlen = 0) {
		client::Item item = rx->NewItem(nsName);
		if (!item.Status().ok()) return item;
		wrser.Reset();
		reindexer::JsonBuilder jsonBuilder(wrser, ObjType::TypeObject);
		jsonBuilder.Put(kFieldId, int(index));
		jsonBuilder.Put(kFieldLocation, key);
		jsonBuilder.Put(kFieldShard, key);
		jsonBuilder.Put(kFieldData, RandString(strlen));
		jsonBuilder.Put(kFieldFTData, RandString(strlen));
		{
			auto nested = jsonBuilder.Object(kFieldStruct);
			nested.Put(kFieldRand, rand() % 10);
		}
		jsonBuilder.End();
		Error err = item.FromJSON(wrser.Slice());
		assertf(err.ok(), "%s", err.what());
		return item;
	}

	Error CreateAndUpsertItem(std::string_view nsName, std::shared_ptr<client::SyncCoroReindexer> rx, std::string_view key, int index,
							  fast_hash_map<int, std::string>* insertedItemsById = nullptr, uint8_t strlen = 0) {
		WrSerializer wrser;
		client::Item item = CreateItem(nsName, rx, key, index, wrser, strlen);
		if (!item.Status().ok()) return item.Status();

		if (insertedItemsById) {
			const auto found = insertedItemsById->find(index);
			EXPECT_TRUE(found == insertedItemsById->end()) << "Item with id " << index << " already exists in indexes map and has value: '"
														   << found->second << "'. New item value is '" << wrser.Slice() << "'";
			(*insertedItemsById)[index] = std::string(wrser.Slice());
		}

		Error err = item.FromJSON(wrser.Slice());
		if (!err.ok()) {
			return err;
		}
		return rx->Upsert(nsName, item);
	}

	void Fill(std::string_view nsName, size_t shard, const size_t from, const size_t count,
			  fast_hash_map<int, std::string>* insertedItemsById = nullptr, uint8_t strlen = 0) {
		assert(shard < svc_.size());
		std::shared_ptr<client::SyncCoroReindexer> rx = svc_[shard][0].Get()->api.reindexer;
		Error err = rx->OpenNamespace(nsName);
		ASSERT_TRUE(err.ok()) << err.what();
		for (size_t index = from; index < from + count; ++index) {
			err = CreateAndUpsertItem(nsName, rx, string("key" + std::to_string((index % kShards) + 1)), index, insertedItemsById, strlen);
			ASSERT_TRUE(err.ok()) << err.what();
		}
	}

	void Fill(std::string_view nsName, std::shared_ptr<client::SyncCoroReindexer> rx, std::string_view key, const size_t from,
			  const size_t count) {
		for (size_t index = from; index < from + count; ++index) {
			Error err = CreateAndUpsertItem(nsName, rx, key, index);
			ASSERT_TRUE(err.ok()) << err.what();
		}
	}

	Error AddRow(std::string_view nsName, size_t shard, size_t nodeId, size_t index) {
		auto srv = svc_[shard][nodeId].Get(false);
		if (!srv) {
			return Error(errNotValid, "Server is not running");
		}
		auto rx = srv->api.reindexer;
		return CreateAndUpsertItem(nsName, rx, string("key" + std::to_string((index % kShards) + 1)), index);
	}

	void AwaitOnlineReplicationStatus(size_t idx) {
		const auto kMaxAwaitTime = std::chrono::seconds(10);
		const auto kPause = std::chrono::milliseconds(20);
		std::chrono::milliseconds timeLeft = kMaxAwaitTime;
		// Await, while node is becoming part of the cluster
		bool isReady = false;
		cluster::ReplicationStats stats;
		do {
			auto node = getNode(idx);
			Query qr = Query("#replicationstats").Where("type", CondEq, Variant("cluster"));
			BaseApi::QueryResultsType res;
			auto err = node->api.reindexer->Select(qr, res);
			if (err.ok()) {
				stats = node->GetReplicationStats("cluster");
				for (auto& nodeStat : stats.nodeStats) {
					if (nodeStat.dsn == node->kRPCDsn && nodeStat.syncState == cluster::NodeStats::SyncState::OnlineReplication) {
						isReady = true;
						break;
					}
				}
			}
			std::this_thread::sleep_for(kPause);
			timeLeft -= kPause;
		} while (!isReady && timeLeft.count() > 0);
		if (!isReady) {
			WrSerializer ser;
			stats.GetJSON(ser);
			ASSERT_TRUE(isReady) << "Node on '" << getNode(idx)->kRPCDsn << "' does not have online replication status: " << ser.Slice();
		}
	}
	size_t NodesCount() const { return kShards * kNodesInCluster; }

protected:
	class CompareShardId;
	struct Defaults {
		size_t defaultRpcPort;
		size_t defaultHttpPort;
		std::string baseTestsetDbPath;
	};
	virtual const Defaults& GetDefaults() const {
		static Defaults def{19000, 20000, fs::JoinPath(fs::GetTempDir(), "rx_test/ShardingBaseApi")};
		return def;
	}

	void waitSync(std::string_view ns) {
		for (auto& cluster : svc_) {
			ClusterizationApi::Cluster::doWaitSync(ns, cluster);
		}
	}
	void waitSync(size_t shardId, std::string_view ns) {
		assert(shardId < svc_.size());
		ClusterizationApi::Cluster::doWaitSync(ns, svc_[shardId]);
	}
	std::pair<size_t, size_t> getSCIdxs(size_t id) { return std::make_pair(id / kNodesInCluster, id % kNodesInCluster); }
	ServerControl::Interface::Ptr getNode(size_t idx) {
		auto [i, j] = getSCIdxs(idx);
		assert(i < svc_.size());
		assert(j < svc_[i].size());
		return svc_[i][j].Get();
	}

	void runSelectTest(std::string_view nsName);
	void runUpdateTest(std::string_view nsName);
	void runDeleteTest(std::string_view nsName);
	void runMetaTest(std::string_view nsName);
	void runSerialTest(std::string_view nsName);
	void runUpdateItemsTest(std::string_view nsName);
	void runDeleteItemsTest(std::string_view nsName);
	void runUpdateIndexTest(std::string_view nsName);
	void runDropNamespaceTest(std::string_view nsName);
	void runTransactionsTest(std::string_view nsName);

	size_t kShards = 3;
	size_t kNodesInCluster = 3;

	const std::string kFieldId = "id";
	const std::string kFieldLocation = "location";
	const std::string kFieldData = "data";
	const std::string kFieldFTData = "ft_data";
	const std::string kFieldStruct = "struct";
	const std::string kFieldRand = "rand";
	const std::string kFieldShard = "shard";
	const std::string kFieldNestedRand = kFieldStruct + '.' + kFieldRand;
	const std::string kFieldIdLocation = kFieldId + '+' + kFieldLocation;
	const std::string kFieldLocationId = kFieldLocation + '+' + kFieldId;

	reindexer::cluster::ShardingConfig config_;
	std::vector<std::vector<ServerControl>> svc_;  //[shard][nodeId]
};
