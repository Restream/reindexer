#pragma once

#include <fstream>
#include "clusterization_api.h"
#include "core/cjson/jsonbuilder.h"
#include "tools/fsops.h"
#include "yaml-cpp/yaml.h"

struct InitShardingConfig {
	using ShardingConfig = reindexer::cluster::ShardingConfig;
	class Namespace {
	public:
		Namespace(std::string n, bool wd = false) : name(std::move(n)), withData(wd) {}

		std::string name;
		bool withData;
		std::function<ShardingConfig::Key(int shard)> keyValuesNodeCreation = [](int shard) {
			ShardingConfig::Key key;
			key.values.emplace_back(Variant(std::string("key") + std::to_string(shard)));
			key.values.emplace_back(Variant(std::string("key") + std::to_string(shard) + "_" + std::to_string(shard)));
			key.shardId = shard;
			return key;
		};
		std::string indexName = "location";
	};

	int shards = 3;
	int nodesInCluster = 3;
	int rowsInTableOnShard = 40;
	bool disableNetworkTimeout = false;
	bool createAdditionalIndexes = true;
	bool needFillDefaultNs = true;
	std::vector<Namespace> additionalNss;
	std::chrono::seconds awaitTimeout = std::chrono::seconds(30);
	fast_hash_map<int, std::string>* insertedItemsById = nullptr;
	int nodeIdInThread = -1;  // Allows to run one of the nodes in thread, instead fo process
	uint8_t strlen = 0;		  // Strings len in items, which will be created during Fill()
	// if it is necessary to explicitly set specific cluster nodes in shards
	std::shared_ptr<std::map<int, std::vector<std::string>>> shardsMap;
};

class ShardingApi : public ReindexerApi {
public:
	using ShardingConfig = reindexer::cluster::ShardingConfig;
	static const std::string_view kConfigTemplate;
	enum class ApplyType : bool { Shared, Local };

	void Init(InitShardingConfig c = InitShardingConfig()) {
		std::vector<InitShardingConfig::Namespace> namespaces = std::move(c.additionalNss);
		namespaces.emplace_back(InitShardingConfig::Namespace{default_namespace, c.needFillDefaultNs});
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
		config_.namespaces.clear();
		config_.namespaces.resize(namespaces.size());
		for (size_t i = 0; i < namespaces.size(); ++i) {
			config_.namespaces[i].ns = namespaces[i].name;
			config_.namespaces[i].index = namespaces[i].indexName;
			config_.namespaces[i].defaultShard = 0;
		}
		config_.shards.clear();

		for (size_t shard = 0, id = 0; shard < kShards; ++shard) {
			if (c.shardsMap) {
				config_.shards[shard] = (*c.shardsMap).at(shard);
			} else {
				config_.shards[shard].reserve(kNodesInCluster);
				for (size_t node = 0; node < kNodesInCluster; ++node) {
					config_.shards[shard].emplace_back(getHostDsn(id++));
				}
			}
			for (size_t nsId = 0; nsId < namespaces.size(); ++nsId) {
				config_.namespaces[nsId].keys.emplace_back(namespaces[nsId].keyValuesNodeCreation(shard));
			}
		}

		int syncThreadsCount = 2;
		int maxSyncCount = rand() % 3;
		std::chrono::milliseconds resyncTimeout = std::chrono::milliseconds(3000);
		TestCout() << "Cluster's max_sync_count was chosen randomly: " << maxSyncCount << std::endl;

		for (size_t shard = 0; shard < kShards; ++shard) {
			YAML::Node clusterConf;
			clusterConf["namespaces"] = YAML::Node(YAML::NodeType::Sequence);
			clusterConf["sync_threads"] = syncThreadsCount;
			clusterConf["enable_compression"] = true;
			clusterConf["online_updates_timeout_sec"] = 20;
			clusterConf["sync_timeout_sec"] = 60;
			clusterConf["syncs_per_thread"] = maxSyncCount;
			clusterConf["leader_sync_threads"] = 2;
#ifdef REINDEX_WITH_TSAN
			clusterConf["batching_routines_count"] = 20;
#else
			clusterConf["batching_routines_count"] = 50;
#endif
			clusterConf["retry_sync_interval_msec"] = resyncTimeout.count();
			clusterConf["nodes"] = YAML::Node(YAML::NodeType::Sequence);
			const size_t startId = shard * kNodesInCluster;
			for (size_t i = startId; i < startId + kNodesInCluster; ++i) {
				YAML::Node node;
				node["dsn"] = fmt::format("cproto://127.0.0.1:{}/shard{}", GetDefaults().defaultRpcPort + i, i);
				node["server_id"] = i;
				clusterConf["nodes"].push_back(node);
			}

			config_.thisShardId = shard;
			config_.proxyConnCount = 4;
			config_.proxyConnThreads = 3;
			config_.proxyConnConcurrency = 8;
			config_.reconnectTimeout = std::chrono::milliseconds(6000);
			config_.sourceId = 999;	 // Some test value
			for (size_t idx = startId, node = 0; idx < startId + kNodesInCluster; ++idx, ++node) {
				YAML::Node replConf;
				replConf["cluster_id"] = shard;
				replConf["server_id"] = idx;
				clusterConf["app_name"] = fmt::sprintf("rx_node_%d", idx);

				std::string pathToDb =
					fs::JoinPath(GetDefaults().baseTestsetDbPath, "shard" + std::to_string(shard) + "/" + std::to_string(idx));
				std::string dbName = "shard" + std::to_string(idx);
				const bool asProcess = kTestServersInSeparateProcesses && (c.nodeIdInThread < 0 || (size_t(c.nodeIdInThread) != idx));
				ServerControlConfig cfg(idx, GetDefaults().defaultRpcPort + idx, GetDefaults().defaultHttpPort + idx, std::move(pathToDb),
										std::move(dbName), true, 0, asProcess);
				cfg.disableNetworkTimeout = c.disableNetworkTimeout;
				svc_[shard][node].InitServerWithConfig(std::move(cfg), replConf, (kNodesInCluster > 1 ? clusterConf : YAML::Node()),
													   config_.GetYAMLObj(), YAML::Node());
			}
		}

		auto& rx = *getNode(0)->api.reindexer;

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
					nsDef.AddIndex(kIndexDataInt, {kFieldDataInt}, "hash", "int", IndexOpts());
					nsDef.AddIndex(kIndexDataIntLocation, {kIndexDataInt, kFieldLocation}, "hash", "composite", IndexOpts());
					nsDef.AddIndex(kFieldLocationId, {kFieldLocation, kFieldId}, "hash", "composite", IndexOpts());
					nsDef.AddIndex(kIndexLocationDataInt, {kFieldLocation, kIndexDataInt}, "hash", "composite", IndexOpts());
					nsDef.AddIndex(kIndexDataString, {kFieldDataString}, "hash", "string", IndexOpts());
					nsDef.AddIndex(kSparseIndexDataInt, {kSparseFieldDataInt}, "hash", "int", IndexOpts().Sparse());
					nsDef.AddIndex(kSparseIndexDataString, {kSparseFieldDataString}, "hash", "string", IndexOpts().Sparse());
				} else {
					nsDef.AddIndex(kFieldId, "hash", "int", IndexOpts().PK());
				}
				Error err = rx.AddNamespace(nsDef);
				ASSERT_TRUE(err.ok()) << err.what() << "; ns: " << ns.name;
			}
		}

		for (auto& ns : namespaces) {
			if (ns.withData) {
				size_t pos = 0;
				for (size_t i = 0; i < kShards; ++i) {
					if (kNodesInCluster > 1) {
						auto err = svc_[i][0].Get()->api.reindexer->OpenNamespace(ns.name);
						ASSERT_TRUE(err.ok()) << err.what();
					}
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

	client::Item CreateItem(std::string_view nsName, client::Reindexer& rx, std::string_view key, int index, WrSerializer& wrser,
							uint8_t strlen = 0) {
		client::Item item = rx.NewItem(nsName);
		if (!item.Status().ok()) return item;
		wrser.Reset();
		reindexer::JsonBuilder jsonBuilder(wrser, ObjType::TypeObject);
		jsonBuilder.Put(kFieldId, int(index));
		jsonBuilder.Put(kFieldLocation, key);
		jsonBuilder.Put(kFieldShard, key);
		jsonBuilder.Put(kFieldData, RandString(strlen));
		jsonBuilder.Put(kFieldFTData, RandString(strlen));
		jsonBuilder.Put(kFieldDataInt, rand() % 100);
		jsonBuilder.Put(kSparseFieldDataInt, rand() % 100);
		jsonBuilder.Put(kFieldDataString, RandString(strlen));
		jsonBuilder.Put(kSparseFieldDataString, RandString(strlen));
		{
			auto nested = jsonBuilder.Object(kFieldStruct);
			nested.Put(kFieldRand, rand() % 10);
		}
		jsonBuilder.End();
		Error err = item.FromJSON(wrser.Slice());
		assertf(err.ok(), "%s", err.what());
		return item;
	}

	Error CreateAndUpsertItem(std::string_view nsName, client::Reindexer& rx, std::string_view key, int index,
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
		return rx.Upsert(nsName, item);
	}

	void Fill(std::string_view nsName, size_t shard, const size_t from, const size_t count,
			  fast_hash_map<int, std::string>* insertedItemsById = nullptr, uint8_t strlen = 0) {
		assert(shard < svc_.size());
		auto& rx = *svc_[shard][0].Get()->api.reindexer;
		for (size_t index = from; index < from + count; ++index) {
			auto err = CreateAndUpsertItem(nsName, rx, std::string("key" + std::to_string((index % kShards) + 1)), index, insertedItemsById,
										   strlen);
			ASSERT_TRUE(err.ok()) << err.what();
		}
	}

	void Fill(std::string_view nsName, client::Reindexer& rx, std::string_view key, const size_t from, const size_t count) {
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
		auto& rx = *srv->api.reindexer;
		return CreateAndUpsertItem(nsName, rx, std::string("key" + std::to_string((index % kShards) + 1)), index);
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

	void MultyThreadApplyConfigTest(ApplyType type);

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
	void checkTransactionErrors(client::Reindexer& rx, std::string_view nsName);

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

	template <typename T>
	void fillShards(const std::map<int, std::set<T>>& shardDataDistrib, const std::string& kNsName, const std::string& kFieldId);

	template <typename T>
	void fillShard(int shard, const std::set<T>& shardData, const std::string& kNsName, const std::string& kFieldId);

	template <typename T>
	void runLocalSelectTest(std::string_view nsName, const std::map<int, std::set<T>>& shardDataDistrib);
	template <typename T>
	void runSelectTest(std::string_view nsName, const std::map<int, std::set<T>>& shardDataDistrib);
	void runTransactionsTest(std::string_view nsName, const std::map<int, std::set<int>>& shardDataDistrib);

	template <typename T>
	ShardingConfig makeShardingConfigByDistrib(std::string_view nsName, const std::map<int, std::set<T>>& shardDataDistrib, int shards = 3,
											   int nodes = 3) const;

	Error applyNewShardingConfig(client::Reindexer& rx, const ShardingConfig& config, ApplyType type,
								 std::optional<int64_t> sourceId = std::optional<int64_t>()) const;

	void checkConfig(const ServerControl::Interface::Ptr& server, const cluster::ShardingConfig& config);
	void checkConfigThrow(const ServerControl::Interface::Ptr& server, const cluster::ShardingConfig& config);
	int64_t getSourceIdFrom(const ServerControl::Interface::Ptr& server);
	std::optional<ShardingConfig> getShardingConfigFrom(reindexer::client::Reindexer& rx);

	void changeClusterLeader(int shardId);

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
	const std::string kFieldDataInt = "data_int";
	const std::string kSparseFieldDataInt = "sparse_data_int";
	const std::string kIndexDataInt = "data_int_index";
	const std::string kIndexDataIntLocation = kIndexDataInt + '+' + kFieldLocation;
	const std::string kIndexLocationDataInt = kFieldLocation + '+' + kIndexDataInt;
	const std::string kSparseIndexDataInt = "sparse_data_int_index";
	const std::string kFieldDataString = "data_string";
	const std::string kIndexDataString = "data_string_index";
	const std::string kSparseFieldDataString = "sparse_data_string";
	const std::string kSparseIndexDataString = "sparse_data_string_index";

	ShardingConfig config_;
	std::vector<std::vector<ServerControl>> svc_;  //[shard][nodeId]
};
