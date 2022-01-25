#pragma once

#ifdef WITH_SHARDING

#include <fstream>
#include "clusterization_api.h"
#include "core/cjson/jsonbuilder.h"
#include "tools/fsops.h"

class ShardingApi : public ReindexerApi {
public:
	ShardingApi() {}

	void Init(int shards = 3, int nodesInCluster = 3, int rowsInTableOnShard = 40) {
		kShards = shards;
		kNodesInCluster = nodesInCluster;
		svc_.resize(kShards);
		for (auto& cluster : svc_) {
			cluster.resize(kNodesInCluster);
		}
		keys_.resize(kShards);
		auto getHostDsn = [&](size_t id) {
			return "cproto://127.0.0.1:" + std::to_string(kDefaultRpcPort + id) + "/shard" + std::to_string(id);
		};
		size_t id = 0;
		config_.namespaces.clear();
		config_.namespaces.resize(1);
		config_.namespaces[0].ns = default_namespace;
		config_.namespaces[0].index = kFieldLocation;
		config_.shards.clear();
		for (size_t shard = 0; shard < kShards; ++shard) {
			if (shard) {
				config_.shards[shard].reserve(nodesInCluster);
				for (int node = 0; node < nodesInCluster; ++node) {
					config_.shards[shard].emplace_back(getHostDsn(id++));
				}
				reindexer::cluster::ShardingConfig::Key key;
				key.values.emplace_back(string("key") + std::to_string(shard));
				key.values.emplace_back(string("key") + std::to_string(shard) + "_" + std::to_string(shard));
				key.shardId = shard;
				config_.namespaces[0].keys.emplace_back(std::move(key));
				keys_.emplace_back(string("key") + std::to_string(shard));
			} else {
				for (int node = 0; node < nodesInCluster; ++node) {
					config_.proxyDsns.emplace_back(getHostDsn(id++));
				}
			}
		}

		std::string nsList;
		nsList += '\n';
		nsList += "  - ";
		nsList += default_namespace;
		nsList += '\n';

		int syncThreadsCount = 2;
		int maxSyncCount = rand() % 3;
		std::chrono::milliseconds resyncTimeout = std::chrono::milliseconds(3000);
		std::cout << "Cluster's max_sync_count was chosen randomly: " << maxSyncCount << std::endl;

		// clang-format off
		const std::string baseClusterConf =
			"app_name: rx_node\n"
			"namespaces: " + nsList +
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
			const size_t startId = shard * nodesInCluster;
			for (size_t i = startId; i < startId + nodesInCluster; ++i) {
				// clang-format off
				clusterConf.append(
				"  -\n"
					"    dsn: " + fmt::format("cproto://127.0.0.1:{}/shard{}", kDefaultRpcPort + i,i) + "\n"
					"    server_id: " + std::to_string(i) + "\n");
				// clang-format on
			}

			config_.thisShardId = shard;
			for (size_t idx = startId, node = 0; idx < startId + nodesInCluster; ++idx, ++node) {
				const std::string replConf = "cluster_id: " + std::to_string(shard) +
											 "\n"
											 "server_id: " +
											 std::to_string(idx);

				std::string pathToDb = fs::JoinPath(kStoragePath, "shard" + std::to_string(shard) + "/" + std::to_string(idx));
				std::string dbName = "shard" + std::to_string(idx);
				svc_[shard][node].InitServerWithConfig(
					ServerControlConfig(idx, kDefaultRpcPort + idx, kDefaultHttpPort + idx, pathToDb, dbName), replConf, clusterConf,
					config_.GetYml(), "");
			}
		}

		std::shared_ptr<client::SyncCoroReindexer> rx = getNode(0)->api.reindexer;

		NamespaceDef nsDef(default_namespace);
		nsDef.AddIndex(kFieldId, "hash", "int", IndexOpts());
		nsDef.AddIndex(kFieldLocation, "hash", "string", IndexOpts());
		nsDef.AddIndex(kFieldData, "hash", "string", IndexOpts());
		nsDef.AddIndex(kFieldFTData, "text", "string", IndexOpts());
		nsDef.AddIndex(kFieldId + "+" + kFieldLocation, {kFieldId, kFieldLocation}, "hash", "composite", IndexOpts().PK());
		Error err = rx->AddNamespace(nsDef);
		ASSERT_TRUE(err.ok()) << err.what();

		size_t pos = 0;
		const size_t count = rowsInTableOnShard;
		for (size_t i = 0; i < kShards; ++i) {
			Fill(i, pos, count);
			pos += count;
		}

		waitSync(default_namespace);
	}

	void SetUp() override {
		reindexer::fs::RmDirAll(kStoragePath);
		ReindexerApi::SetUp();
	}
	void TearDown() override {
		ReindexerApi::TearDown();
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
		svc_.clear();
		reindexer::fs::RmDirAll(kStoragePath);
	}

	bool Start(size_t shardId) {
		assert(shardId < svc_.size());
		auto& cluster = svc_[shardId];
		for (size_t i = 0; i < cluster.size(); ++i) {
			auto& server = cluster[i];
			const auto id = shardId * cluster.size() + i;
			server.InitServer(ServerControlConfig(id, kDefaultRpcPort + id, kDefaultHttpPort + id,
												  fs::JoinPath(kStoragePath, "shard" + std::to_string(shardId) + "/" + std::to_string(id)),
												  "shard" + std::to_string(id)));
		}
		return true;
	}

	bool Stop(size_t shardId) {
		assert(shardId < svc_.size());
		auto& cluster = svc_[shardId];
		for (auto& sc : cluster) {
			StopSC(sc);
		}
		return true;
	}

	bool StopByIndex(size_t idx) {
		auto [i, j] = getSCIdxs(idx);
		assert(i < svc_.size());
		assert(j < svc_[i].size());
		return StopSC(svc_[i][j]);
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

	Error CreateAndUpsertItem(std::shared_ptr<client::SyncCoroReindexer> rx, std::string_view key, int index) {
		client::Item item = rx->NewItem(default_namespace);
		if (!item.Status().ok()) return item.Status();
		WrSerializer wrser;
		reindexer::JsonBuilder jsonBuilder(wrser, ObjType::TypeObject);
		jsonBuilder.Put(kFieldId, int(index));
		jsonBuilder.Put(kFieldLocation, key);
		jsonBuilder.Put(kFieldData, RandString());
		jsonBuilder.Put(kFieldFTData, RandString());
		jsonBuilder.End();

		Error err = item.FromJSON(wrser.Slice());
		if (!err.ok()) {
			return err;
		}
		return rx->Upsert(default_namespace, item);
	}

	void Fill(size_t shard, const size_t from, const size_t count) {
		assert(shard < svc_.size());
		std::shared_ptr<client::SyncCoroReindexer> rx = svc_[shard][0].Get()->api.reindexer;
		Error err = rx->OpenNamespace(default_namespace);
		ASSERT_TRUE(err.ok()) << err.what();
		for (size_t index = from; index < from + count; ++index) {
			err = CreateAndUpsertItem(rx, string("key" + std::to_string((index % kShards) + 1)), index);
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

protected:
	void waitSync(std::string_view ns) {
		for (auto& cluster : svc_) {
			ClusterizationApi::Cluster::doWaitSync(ns, cluster);
		}
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
	const std::string kStoragePath = "rx_test/sharding_api";

	const std::string kFieldId = "id";
	const std::string kFieldLocation = "location";
	const std::string kFieldData = "data";
	const std::string kFieldFTData = "ft_data";

	reindexer::cluster::ShardingConfig config_;
	std::vector<std::vector<ServerControl>> svc_;
	vector<string> keys_;
};

#endif	// WITH_SHARDING
