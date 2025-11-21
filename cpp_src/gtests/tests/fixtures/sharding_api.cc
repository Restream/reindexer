#include "sharding_api.h"
#include "cluster_operation_api.h"
#include "core/cjson/jsonbuilder.h"
#include "core/system_ns_names.h"
#include "gtests/tests/gtest_cout.h"
#include "yaml-cpp/yaml.h"

using namespace reindexer;

void ShardingApi::Init(InitShardingConfig c) {
	std::vector<InitShardingConfig::Namespace> namespaces = std::move(c.additionalNss);
	namespaces.emplace_back(InitShardingConfig::Namespace{default_namespace, c.needFillDefaultNs});
	kShards = c.shards;
	kNodesInCluster = c.nodesInCluster;
	svc_.resize(kShards);
	for (auto& cluster : svc_) {
		cluster.resize(kNodesInCluster);
	}

	ShardingConfig shardingConfig;
	shardingConfig.shardsAwaitingTimeout = c.awaitTimeout;
	shardingConfig.namespaces.clear();
	shardingConfig.namespaces.resize(namespaces.size());
	for (size_t i = 0; i < namespaces.size(); ++i) {
		shardingConfig.namespaces[i].ns = namespaces[i].name;
		shardingConfig.namespaces[i].index = namespaces[i].indexName;
		shardingConfig.namespaces[i].defaultShard = 0;
	}
	shardingConfig.shards.clear();

	for (size_t shard = 0, id = 0; shard < kShards; ++shard) {
		if (c.shardsMap) {
			shardingConfig.shards[shard] = (*c.shardsMap).at(shard);
		} else {
			shardingConfig.shards[shard].reserve(kNodesInCluster);
			for (size_t node = 0; node < kNodesInCluster; ++node) {
				shardingConfig.shards[shard].emplace_back(MakeDsn(reindexer_server::UserRole::kRoleSharding, id,
																  GetDefaults().defaultRpcPort + id, "shard" + std::to_string(id)));
				id++;
			}
		}
		for (size_t nsId = 0; nsId < namespaces.size(); ++nsId) {
			shardingConfig.namespaces[nsId].keys.emplace_back(namespaces[nsId].keyValuesNodeCreation(shard));
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
			node["dsn"] =
				MakeDsn(reindexer_server::UserRole::kRoleReplication, i, GetDefaults().defaultRpcPort + i, "shard" + std::to_string(i));
			node["server_id"] = i;
			clusterConf["nodes"].push_back(node);
		}

		shardingConfig.thisShardId = shard;
		shardingConfig.proxyConnCount = 4;
		shardingConfig.proxyConnThreads = 3;
		shardingConfig.proxyConnConcurrency = 8;
		shardingConfig.reconnectTimeout = std::chrono::milliseconds(6000);
		shardingConfig.sourceId = 999;	// some initial test value
		for (size_t idx = startId, node = 0; idx < startId + kNodesInCluster; ++idx, ++node) {
			YAML::Node replConf;
			replConf["cluster_id"] = shard;
			replConf["server_id"] = idx;
			clusterConf["app_name"] = fmt::format("rx_node_{}", idx);

			std::string pathToDb =
				fs::JoinPath(GetDefaults().baseTestsetDbPath, "shard" + std::to_string(shard) + "/" + std::to_string(idx));
			std::string dbName = "shard" + std::to_string(idx);
			const bool asProcess = kTestServersInSeparateProcesses && (c.nodeIdInThread < 0 || (size_t(c.nodeIdInThread) != idx));
			ServerControlConfig cfg(idx, GetDefaults().defaultRpcPort + idx, GetDefaults().defaultHttpPort + idx, std::move(pathToDb),
									std::move(dbName), true, 0, asProcess);
			cfg.disableNetworkTimeout = c.disableNetworkTimeout;
			svc_[shard][node].InitServerWithConfig(std::move(cfg), replConf, (kNodesInCluster > 1 ? clusterConf : YAML::Node()),
												   shardingConfig.GetYAMLObj(), YAML::Node());
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

	while (!std::all_of(svc_.begin(), svc_.end(), [](auto& shard) {
		auto replState = shard[0].Get()->GetReplicationStats("cluster");
		return std::all_of(replState.nodeStats.begin(), replState.nodeStats.end(),
						   [](const auto& nodeStat) { return nodeStat.isSynchronized; });
	})) {
	}

	if (kNodesInCluster > 1) {
		for (auto& ns : namespaces) {
			if (ns.withData) {
				ASSERT_TRUE(checkSync(ns.name));
			}
		}
	}
}

void ShardingApi::SetUp() {
	std::ignore = reindexer::fs::RmDirAll(GetDefaults().baseTestsetDbPath);
	ReindexerApi::SetUp();
}

void ShardingApi::TearDown() {
	checkMasking();
	ReindexerApi::TearDown();
	Stop();
	svc_.clear();
	std::ignore = reindexer::fs::RmDirAll(GetDefaults().baseTestsetDbPath);
}

bool ShardingApi::StopByIndex(size_t idx) {
	auto [i, j] = getSCIdxs(idx);
	assert(i < svc_.size());
	assert(j < svc_[i].size());
	return StopSC(svc_[i][j]);
}

bool ShardingApi::StopByIndex(size_t shard, size_t node) {
	assert(shard < svc_.size());
	assert(node < svc_[shard].size());
	return StopSC(svc_[shard][node]);
}

bool ShardingApi::Stop(size_t shardId) {
	assert(shardId < svc_.size());
	auto& cluster = svc_[shardId];
	for (auto& sc : cluster) {
		if (!sc.Get()) {
			return false;
		}
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

void ShardingApi::Stop() {
	for (size_t i = 0; i < svc_.size(); ++i) {
		for (size_t j = 0; j < svc_[i].size(); ++j) {
			auto& server = svc_[i][j];
			if (server.Get(false)) {
				server.Get()->Stop();
			}
		}
	}
	for (size_t i = 0; i < svc_.size(); ++i) {
		for (size_t j = 0; j < svc_[i].size(); ++j) {
			auto& server = svc_[i][j];
			if (!server.Get(false)) {
				continue;
			}
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

void ShardingApi::Start(size_t shardId) {
	assert(shardId < svc_.size());
	auto& cluster = svc_[shardId];
	for (size_t i = 0; i < cluster.size(); ++i) {
		StartByIndex(shardId * kNodesInCluster + i);
	}
}

void ShardingApi::StartByIndex(size_t idx) {
	auto [i, j] = getSCIdxs(idx);
	assert(i < svc_.size());
	assert(j < svc_[i].size());
	svc_[i][j].InitServer(
		ServerControlConfig(idx, GetDefaults().defaultRpcPort + idx, GetDefaults().defaultHttpPort + idx,
							fs::JoinPath(GetDefaults().baseTestsetDbPath, "shard" + std::to_string(i) + "/" + std::to_string(idx)),
							"shard" + std::to_string(idx)));
}

bool ShardingApi::StopSC(ServerControl& sc) {
	if (!sc.Get()) {
		return false;
	}
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

bool ShardingApi::StopSC(size_t i, size_t j) {
	assert(i < svc_.size());
	assert(j < svc_[i].size());
	return StopSC(svc_[i][j]);
}

client::Item ShardingApi::CreateItem(std::string_view nsName, client::Reindexer& rx, std::string_view key, int index, WrSerializer& wrser,
									 uint8_t strlen) {
	client::Item item = rx.NewItem(nsName);
	if (!item.Status().ok()) {
		return item;
	}
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
	assertf(err.ok(), "{}", err.what());
	return item;
}

Error ShardingApi::CreateAndUpsertItem(std::string_view nsName, client::Reindexer& rx, std::string_view key, int index,
									   fast_hash_map<int, std::string>* insertedItemsById, uint8_t strlen) {
	WrSerializer wrser;
	client::Item item = CreateItem(nsName, rx, key, index, wrser, strlen);
	if (!item.Status().ok()) {
		return item.Status();
	}

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

void ShardingApi::Fill(std::string_view nsName, size_t shard, const size_t from, const size_t count,
					   fast_hash_map<int, std::string>* insertedItemsById, uint8_t strlen) {
	assert(shard < svc_.size());
	auto& rx = *svc_[shard][0].Get()->api.reindexer;
	for (size_t index = from; index < from + count; ++index) {
		auto err =
			CreateAndUpsertItem(nsName, rx, std::string("key" + std::to_string((index % kShards) + 1)), index, insertedItemsById, strlen);
		ASSERT_TRUE(err.ok()) << err.what();
	}
}

void ShardingApi::Fill(std::string_view nsName, client::Reindexer& rx, std::string_view key, const size_t from, const size_t count) {
	for (size_t index = from; index < from + count; ++index) {
		Error err = CreateAndUpsertItem(nsName, rx, key, index);
		ASSERT_TRUE(err.ok()) << err.what();
	}
}

Error ShardingApi::AddRow(std::string_view nsName, size_t shard, size_t nodeId, size_t index) {
	auto srv = svc_[shard][nodeId].Get(false);
	if (!srv) {
		return Error(errNotValid, "Server is not running");
	}
	auto& rx = *srv->api.reindexer;
	return CreateAndUpsertItem(nsName, rx, std::string("key" + std::to_string((index % kShards) + 1)), index);
}

void ShardingApi::AwaitOnlineReplicationStatus(size_t idx) {
	const auto kMaxAwaitTime = std::chrono::seconds(10);
	const auto kPause = std::chrono::milliseconds(20);
	std::chrono::milliseconds timeLeft = kMaxAwaitTime;
	// Await, while node is becoming part of the cluster
	bool isReady = false;
	cluster::ReplicationStats stats;
	do {
		auto node = getNode(idx);
		Query qr = Query(kReplicationStatsNamespace).Where("type", CondEq, Variant("cluster"));
		BaseApi::QueryResultsType res;
		auto err = node->api.reindexer->Select(qr, res);
		if (err.ok()) {
			stats = node->GetReplicationStats("cluster");
			for (auto& nodeStat : stats.nodeStats) {
				if (nodeStat.dsn == MakeDsn(reindexer_server::UserRole::kRoleReplication, node) &&
					nodeStat.syncState == cluster::NodeStats::SyncState::OnlineReplication) {
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
		ASSERT_TRUE(isReady) << "Node on '" << fmt::format("{}", getNode(idx)->kRPCDsn)
							 << "' does not have online replication status: " << ser.Slice();
	}
}

void ShardingApi::waitSync(std::string_view ns) {
	for (auto& cluster : svc_) {
		ClusterOperationApi::Cluster::doWaitSync(ns, cluster);
	}
}

bool ShardingApi::checkSync(std::string_view ns) {
	for (auto& cluster : svc_) {
		if (cluster.size() != ClusterOperationApi::Cluster::getSyncCnt(ns, cluster)) {
			ClusterOperationApi::Cluster::PrintClusterInfo(ns, cluster);
			return false;
		}
	}
	return true;
}

void ShardingApi::waitSync(size_t shardId, std::string_view ns) {
	assert(shardId < svc_.size());
	ClusterOperationApi::Cluster::doWaitSync(ns, svc_[shardId]);
}

ServerControl::Interface::Ptr ShardingApi::getNode(size_t idx) {
	auto [i, j] = getSCIdxs(idx);
	assert(i < svc_.size());
	assert(j < svc_[i].size());
	return svc_[i][j].Get();
}

void ShardingApi::checkMasking() {
	if (WithSecurity() && !svc_.empty()) {
		for (size_t shardId = 0; shardId < kShards; ++shardId) {
			if (svc_[shardId][0].IsRunning()) {
				checkMaskedDSNsInConfig(shardId);
			}
		}
	}
}
