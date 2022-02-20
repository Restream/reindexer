#pragma once

#include <fstream>
#include "clusterization_api.h"
#include "core/cjson/jsonbuilder.h"
#include "tools/fsops.h"

struct InitShardingConfig {
	int shards = 3;
	int nodesInCluster = 3;
	int rowsInTableOnShard = 40;
	bool disableNetworkTimeout = false;
	bool createAdditionalIndexes = true;
	std::vector<std::string> additionalNss;
	std::chrono::seconds awaitTimeout = std::chrono::seconds(30);
	fast_hash_map<int, std::string>* insertedItemsById = nullptr;
};

class ShardingApi : public ReindexerApi {
public:
	void Init(InitShardingConfig c = InitShardingConfig()) {
		std::vector<std::string> namespaces = std::move(c.additionalNss);
		namespaces.emplace_back(default_namespace);
		kShards = c.shards;
		kNodesInCluster = c.nodesInCluster;
		svc_.resize(kShards);
		for (auto& cluster : svc_) {
			cluster.resize(kNodesInCluster);
		}
		keys_.resize(kShards);
		auto getHostDsn = [&](size_t id) {
			return "cproto://127.0.0.1:" + std::to_string(kDefaultRpcPort + id) + "/shard" + std::to_string(id);
		};
		config_.shardsAwaitingTimeout = c.awaitTimeout;
		size_t id = 0;
		std::string nsList("\n");
		config_.namespaces.clear();
		config_.namespaces.resize(namespaces.size());
		for (size_t i = 0; i < namespaces.size(); ++i) {
			config_.namespaces[i].ns = namespaces[i];
			config_.namespaces[i].index = kFieldLocation;
			config_.namespaces[i].defaultShard = 0;

			nsList += "  - ";
			nsList += namespaces[i];
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
			config_.namespaces[0].keys.emplace_back(std::move(key));
			keys_.emplace_back(string("key") + std::to_string(shard));
		}

		int syncThreadsCount = 2;
		int maxSyncCount = rand() % 3;
		std::chrono::milliseconds resyncTimeout = std::chrono::milliseconds(3000);
		std::cout << "Cluster's max_sync_count was chosen randomly: " << maxSyncCount << std::endl;

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
					"    dsn: " + fmt::format("cproto://127.0.0.1:{}/shard{}", kDefaultRpcPort + i,i) + "\n"
					"    server_id: " + std::to_string(i) + "\n");
				// clang-format on
			}

			config_.thisShardId = shard;
			for (size_t idx = startId, node = 0; idx < startId + kNodesInCluster; ++idx, ++node) {
				const std::string replConf = "cluster_id: " + std::to_string(shard) +
											 "\n"
											 "server_id: " +
											 std::to_string(idx);

				std::string pathToDb = fs::JoinPath(kStoragePath, "shard" + std::to_string(shard) + "/" + std::to_string(idx));
				std::string dbName = "shard" + std::to_string(idx);
				ServerControlConfig cfg(idx, kDefaultRpcPort + idx, kDefaultHttpPort + idx, std::move(pathToDb), std::move(dbName));
				cfg.disableNetworkTimeout = c.disableNetworkTimeout;
				svc_[shard][node].InitServerWithConfig(std::move(cfg), replConf, (kNodesInCluster > 1 ? clusterConf : std::string()),
													   config_.GetYml(), "");
			}
		}

		std::shared_ptr<client::SyncCoroReindexer> rx = getNode(0)->api.reindexer;

		NamespaceDef nsDef(default_namespace);
		if (c.createAdditionalIndexes) {
			nsDef.AddIndex(kFieldId, "hash", "int", IndexOpts());
			nsDef.AddIndex(kFieldLocation, "hash", "string", IndexOpts());
			nsDef.AddIndex(kFieldData, "hash", "string", IndexOpts());
			nsDef.AddIndex(kFieldFTData, "text", "string", IndexOpts());
			nsDef.AddIndex(kFieldId + "+" + kFieldLocation, {kFieldId, kFieldLocation}, "hash", "composite", IndexOpts().PK());
		} else {
			nsDef.AddIndex(kFieldId, "hash", "int", IndexOpts().PK());
		}
		Error err = rx->AddNamespace(nsDef);
		ASSERT_TRUE(err.ok()) << err.what();

		size_t pos = 0;
		for (size_t i = 0; i < kShards; ++i) {
			Fill(i, pos, c.rowsInTableOnShard, c.insertedItemsById);
			pos += c.rowsInTableOnShard;
		}

		if (kNodesInCluster > 1) {
			waitSync(default_namespace);
		}
	}

	void SetUp() override {
		reindexer::fs::RmDirAll(kStoragePath);
		ReindexerApi::SetUp();
	}
	void TearDown() override {
		ReindexerApi::TearDown();
		Stop();
		svc_.clear();
		reindexer::fs::RmDirAll(kStoragePath);
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
		svc_[i][j].InitServer(ServerControlConfig(idx, kDefaultRpcPort + idx, kDefaultHttpPort + idx,
												  fs::JoinPath(kStoragePath, "shard" + std::to_string(i) + "/" + std::to_string(idx)),
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

	client::Item CreateItem(std::shared_ptr<client::SyncCoroReindexer> rx, std::string_view key, int index, WrSerializer& wrser) {
		client::Item item = rx->NewItem(default_namespace);
		if (!item.Status().ok()) return item;
		wrser.Reset();
		reindexer::JsonBuilder jsonBuilder(wrser, ObjType::TypeObject);
		jsonBuilder.Put(kFieldId, int(index));
		jsonBuilder.Put(kFieldLocation, key);
		jsonBuilder.Put(kFieldData, RandString());
		jsonBuilder.Put(kFieldFTData, RandString());
		jsonBuilder.End();
		Error err = item.FromJSON(wrser.Slice());
		assertf(err.ok(), "%s", err.what());
		return item;
	}

	Error CreateAndUpsertItem(std::shared_ptr<client::SyncCoroReindexer> rx, std::string_view key, int index,
							  fast_hash_map<int, std::string>* insertedItemsById = nullptr) {
		WrSerializer wrser;
		client::Item item = CreateItem(rx, key, index, wrser);
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
		return rx->Upsert(default_namespace, item);
	}

	void Fill(size_t shard, const size_t from, const size_t count, fast_hash_map<int, std::string>* insertedItemsById = nullptr) {
		assert(shard < svc_.size());
		std::shared_ptr<client::SyncCoroReindexer> rx = svc_[shard][0].Get()->api.reindexer;
		Error err = rx->OpenNamespace(default_namespace);
		ASSERT_TRUE(err.ok()) << err.what();
		for (size_t index = from; index < from + count; ++index) {
			err = CreateAndUpsertItem(rx, string("key" + std::to_string((index % kShards) + 1)), index, insertedItemsById);
			ASSERT_TRUE(err.ok()) << err.what();
		}
	}

	void Fill(std::shared_ptr<client::SyncCoroReindexer> rx, std::string_view key, const size_t from, const size_t count) {
		for (size_t index = from; index < from + count; ++index) {
			Error err = CreateAndUpsertItem(rx, key, index);
			ASSERT_TRUE(err.ok()) << err.what();
		}
	}

	Error AddRow(size_t shard, size_t nodeId, size_t index) {
		auto srv = svc_[shard][nodeId].Get(false);
		if (!srv) {
			return Error(errNotValid, "Server is not running");
		}
		auto rx = srv->api.reindexer;
		return CreateAndUpsertItem(rx, string("key" + std::to_string((index % kShards) + 1)), index);
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
			stats = node->GetReplicationStats("cluster");
			for (auto& nodeStat : stats.nodeStats) {
				if (nodeStat.dsn == node->kRPCDsn && nodeStat.syncState == cluster::NodeStats::SyncState::OnlineReplication) {
					isReady = true;
					break;
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
	void waitSync(std::string_view ns) {
		for (auto& cluster : svc_) {
			ClusterizationApi::Cluster::doWaitSync(ns, cluster);
		}
	}
	void waitSync(size_t shardId, std::string_view ns) {
		assert(shardId < svc_.size());
		ClusterizationApi::Cluster::doWaitSync(ns, svc_[shardId]);
	}
	std::pair<size_t, size_t> getSCIdxs(size_t id) { return std::make_pair(id / kShards, id % kShards); }
	ServerControl::Interface::Ptr getNode(size_t idx) {
		auto [i, j] = getSCIdxs(idx);
		assert(i < svc_.size());
		assert(j < svc_[i].size());
		return svc_[i][j].Get();
	}

	size_t kShards = 3;
	size_t kNodesInCluster = 3;

	const size_t kDefaultRpcPort = 19000;
	const size_t kDefaultHttpPort = 19700;
	const std::string kStoragePath = fs::JoinPath(fs::GetTempDir(), "rx_test/sharding_api");

	const std::string kFieldId = "id";
	const std::string kFieldLocation = "location";
	const std::string kFieldData = "data";
	const std::string kFieldFTData = "ft_data";

	reindexer::cluster::ShardingConfig config_;
	std::vector<std::vector<ServerControl>> svc_;  //[shard][nodeId]
	vector<string> keys_;
};
