#pragma once

#ifdef WITH_SHARDING

#include <fstream>
#include "clusterization_api.h"
#include "core/cjson/jsonbuilder.h"

class ShardingApi : public ReindexerApi {
public:
	ShardingApi() {}

	void Init(int shards = 3, int nodesInCluster = 3) {
		kShards = shards;
		svc_.resize(kShards * nodesInCluster);
		keys_.resize(kShards);
		auto getHostDsn = [&](size_t id) {
			return "cproto://127.0.0.1:" + std::to_string(kDefaultRpcPort + id) + "/shard" + std::to_string(id);
		};
		size_t id = 0;
		for (size_t shard = 0; shard < kShards; ++shard) {
			if (shard) {
				reindexer::cluster::ShardingKey key;
				key.ns = default_namespace;
				key.index = kFieldLocation;
				key.values.emplace_back(string("key") + std::to_string(shard));
				for (int node = 0; node < nodesInCluster; ++node) {
					key.hostsDsns.emplace_back(getHostDsn(id++));
				}
				key.shardId = shard;
				config_.keys.emplace_back(std::move(key));
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
		for (size_t shard = 0; shard < kShards; ++shard) {
			Start(shard, nodesInCluster);

			// clang-format on
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
			for (size_t idx = startId; idx < startId + nodesInCluster; ++idx) {
				const std::string replConf = "cluster_id: " + std::to_string(shard) +
											 "\n"
											 "server_id: " +
											 std::to_string(idx);
				svc_[idx].Get()->WriteReplicationConfig(replConf);
				svc_[idx].Get()->WriteClusterConfig(clusterConf);
				svc_[idx].Get()->WriteShardingConfig(config_.ToYml());
			}

			Stop(shard, nodesInCluster);
			Start(shard, nodesInCluster);
		}

		std::shared_ptr<client::SyncCoroReindexer> rx = svc_.front().Get()->api.reindexer;

		NamespaceDef nsDef(default_namespace);
		nsDef.AddIndex(kFieldId, "hash", "int", IndexOpts());
		nsDef.AddIndex(kFieldLocation, "hash", "string", IndexOpts());
		nsDef.AddIndex(kFieldData, "hash", "string", IndexOpts());
		nsDef.AddIndex(kFieldId + "+" + kFieldLocation, {kFieldId, kFieldLocation}, "hash", "composite", IndexOpts().PK());
		Error err = rx->AddNamespace(nsDef);
		ASSERT_TRUE(err.ok()) << err.what();

		ClusterizationApi::Cluster::doWaitSync(default_namespace, svc_);

		size_t pos = 0;
		const size_t count = 40;
		for (size_t i = 0; i < kShards; ++i) {
			Fill(i, pos, count, nodesInCluster);
			pos += count;
		}

		ClusterizationApi::Cluster::doWaitSync(default_namespace, svc_);
	}

	void SetUp() override {
		reindexer::fs::RmDirAll(kStoragePath);
		ReindexerApi::SetUp();
	}
	void TearDown() override {
		ReindexerApi::TearDown();
		for (size_t i = 0; i < svc_.size(); ++i) {
			auto& server = svc_[i];
			if (server.Get()) server.Get()->Stop();
		}
		for (size_t i = 0; i < svc_.size(); ++i) {
			auto& server = svc_[i];
			if (!server.Get()) continue;
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
		svc_.clear();
		reindexer::fs::RmDirAll(kStoragePath);
	}

	bool Start(size_t shardId, const int nodesInCluster = 1) {
		const size_t startId = shardId * nodesInCluster;
		for (size_t id = startId; id < startId + nodesInCluster; ++id) {
			assert(id < svc_.size());
			auto& server = svc_[id];
			server.InitServer(id, kDefaultRpcPort + id, kDefaultHttpPort + id,
							  fs::JoinPath(kStoragePath, "shard/" + std::to_string(shardId) + "/" + std::to_string(id)),
							  "shard" + std::to_string(id), true);
		}
		return true;
	}

	bool Stop(size_t shardId, const int nodesInCluster = 1) {
		const size_t startId = shardId * nodesInCluster;
		for (size_t id = startId; id < startId + nodesInCluster; ++id) {
			StopByIndex(id);
		}
		return true;
	}

	bool StopByIndex(size_t index) {
		assert(index < svc_.size());
		auto& server = svc_[index];
		if (!server.Get()) return false;
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
		return true;
	}

	void Fill(size_t shard, const size_t from, const size_t count, const int nodesInCluster = 1) {
		const size_t index = shard * nodesInCluster;
		assert(index < svc_.size());
		std::shared_ptr<client::SyncCoroReindexer> rx = svc_[index].Get()->api.reindexer;
		Error err = rx->OpenNamespace(default_namespace);
		ASSERT_TRUE(err.ok()) << err.what();
		for (size_t i = from; i < from + count; ++i) {
			client::Item item = rx->NewItem(default_namespace);
			ASSERT_TRUE(item.Status().ok());

			const string key = string("key" + std::to_string((i % kShards) + 1));

			WrSerializer wrser;
			reindexer::JsonBuilder jsonBuilder(wrser, ObjType::TypeObject);
			jsonBuilder.Put(kFieldId, int(i));
			jsonBuilder.Put(kFieldLocation, key);
			jsonBuilder.Put(kFieldData, RandString());
			jsonBuilder.End();

			err = item.FromJSON(wrser.Slice());
			ASSERT_TRUE(err.ok()) << err.what();

			err = rx->Upsert(default_namespace, item);
			ASSERT_TRUE(err.ok()) << err.what() << "; index = " << index << "; location = " << key;
		}
	}

protected:
	size_t kShards = 3;

	const size_t kDefaultRpcPort = 19000;
	const size_t kDefaultHttpPort = 19700;
	const std::string kStoragePath = "rx_test/sharding_api";

	const std::string kFieldId = "id";
	const std::string kFieldLocation = "location";
	const std::string kFieldData = "data";

	reindexer::cluster::ShardingConfig config_;
	std::vector<ServerControl> svc_;
	vector<string> keys_;
};

#endif	// WITH_SHARDING
