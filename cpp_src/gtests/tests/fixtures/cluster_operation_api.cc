#include "cluster_operation_api.h"

#include "cluster/consts.h"
#include "core/formatters/lsn_fmt.h"
#include "core/system_ns_names.h"
#include "gtests/tests/gtest_cout.h"
#include "gtests/tools.h"
#include "tools/fsops.h"
#include "vendor/fmt/ranges.h"
#include "yaml-cpp/yaml.h"

using namespace reindexer;

void ClusterOperationApi::SetUp() { std::ignore = fs::RmDirAll(GetDefaults().baseTestsetDbPath); }

void ClusterOperationApi::TearDown() { std::ignore = fs::RmDirAll(GetDefaults().baseTestsetDbPath); }

const ClusterOperationApi::Defaults& ClusterOperationApi::GetDefaults() const {
	static Defaults defs{14000, 16000, fs::JoinPath(fs::GetTempDir(), "rx_test/ClusterOperationApi")};
	return defs;
}

void ClusterOperationApi::Cluster::initCluster(size_t count, size_t initialServerId, const YAML::Node& clusterConf) {
	for (size_t i = 0; i < count; ++i) {
		YAML::Node replConf;
		replConf["cluster_id"] = 2;
		replConf["server_id"] = i + initialServerId;

		svc_.emplace_back();
		InitServer(i, clusterConf, replConf, initialServerId);

		[[maybe_unused]] auto err = clients_.emplace_back().Connect(svc_.back().Get(false)->kRPCDsn, loop_);
		assertf(err.ok(), "{}", err.what());
	}
}

ClusterOperationApi::Cluster::Cluster(dynamic_loop& loop, size_t initialServerId, size_t count, Defaults ports, size_t maxUpdatesSize,
									  const YAML::Node& clusterConf)
	: loop_(loop), defaults_(std::move(ports)), maxUpdatesSize_(maxUpdatesSize) {
	initCluster(count, initialServerId, clusterConf);
}

ClusterOperationApi::Cluster::Cluster(dynamic_loop& loop, size_t initialServerId, size_t count, Defaults ports,
									  const std::vector<std::string>& nsList, std::chrono::milliseconds resyncTimeout, int maxSyncCount,
									  int syncThreadsCount, size_t maxUpdatesSize)
	: loop_(loop), defaults_(std::move(ports)), maxUpdatesSize_(maxUpdatesSize) {
	// Initializing cluster config as YAML node
	if (maxSyncCount < 0) {
		maxSyncCount = rand() % 3;
		TestCout() << "Cluster's max_sync_count was chosen randomly: " << maxSyncCount << std::endl;
	}
	YAML::Node clusterConf = CreateClusterConfig(initialServerId, count, nsList, resyncTimeout, maxSyncCount, syncThreadsCount);

	initCluster(count, initialServerId, clusterConf);
}

ClusterOperationApi::Cluster::~Cluster() {
	StopClients();
	for (auto& sv : svc_) {
		if (sv.IsRunning()) {
			sv.Get()->Stop();
		}
	}
}

void ClusterOperationApi::Cluster::InitNs(size_t id, std::string_view nsName) {
	// until we use shared ptr it will be not destroyed
	assert(id < svc_.size());
	auto srv = svc_[id].Get();
	auto& api = srv->api;

	Error err = api.reindexer->OpenNamespace(nsName, StorageOpts().Enabled(true));
	ASSERT_TRUE(err.ok()) << err.what();

	api.DefineNamespaceDataset(
		std::string(nsName),
		{IndexDeclaration{kIdField, "hash", "int", IndexOpts().PK(), 0}, IndexDeclaration{kIntField, "tree", "int", IndexOpts(), 0},
		 IndexDeclaration{kStringField, "hash", "string", IndexOpts(), 0}, IndexDeclaration{kFTField, "text", "string", IndexOpts(), 0},
		 IndexDeclaration{kVectorField, "hnsw", "float_vector",
						  IndexOpts{}.SetFloatVector(IndexHnsw, FloatVectorIndexOpts{}
																	.SetDimension(kVectorDims)
																	.SetStartSize(100)
																	.SetM(16)
																	.SetEfConstruction(200)
																	.SetMetric(VectorMetric::Cosine)),
						  0}});
}

void ClusterOperationApi::Cluster::DropNs(size_t id, std::string_view nsName) {
	// until we use shared ptr it will be not destroyed
	assert(id < svc_.size());
	Error err = svc_[id].Get()->api.reindexer->DropNamespace(nsName);
	ASSERT_TRUE(err.ok()) << err.what();
}

void ClusterOperationApi::Cluster::FillData(size_t id, std::string_view nsName, size_t from, size_t count) {
	// until we use shared ptr it will be not destroyed
	assert(id < svc_.size());
	auto srv = svc_[id].Get();
	auto& api = srv->api;

	for (size_t i = from; i < from + count; ++i) {
		auto item = api.NewItem(nsName);
		FillItem(api, item, i);
		api.Upsert(nsName, item);
	}
}

void ClusterOperationApi::Cluster::FillDataTx(size_t id, std::string_view nsName, size_t from, size_t count) {
	// until we use shared ptr it will be not destroyed
	assert(id < svc_.size());
	auto srv = svc_[id].Get();
	auto& api = srv->api;

	auto tx = api.reindexer->NewTransaction(nsName);
	ASSERT_TRUE(tx.Status().ok()) << tx.Status().what();
	for (size_t i = from; i < from + count; ++i) {
		auto item = tx.NewItem();
		FillItem(api, item, i);
		auto err = tx.Upsert(std::move(item));
		ASSERT_TRUE(err.ok()) << err.what();
	}
	BaseApi::QueryResultsType results;
	auto err = api.reindexer->CommitTransaction(tx, results);
	ASSERT_TRUE(err.ok()) << err.what();
}

void ClusterOperationApi::Cluster::AddRow(size_t id, std::string_view nsName, int pk) {
	assert(id < svc_.size());
	auto srv = svc_[id].Get();
	auto& api = srv->api;
	auto item = api.NewItem(nsName);
	FillItem(api, item, pk);
	api.Upsert(nsName, item);
}

Error ClusterOperationApi::Cluster::AddRowWithErr(size_t id, std::string_view nsName, int pk, DataParam param, std::string* resultJson) {
	assert(id < svc_.size());
	if (!svc_[id].IsRunning()) {
		return Error(errNotValid, "Server is not running");
	}
	auto srv = svc_[id].Get(false);
	if (!srv) {
		return Error(errNotValid, "Server is not running");
	}
	auto& api = srv->api;
	auto item = api.NewItem(nsName);
	FillItem(api, item, pk, std::move(param));
	if (resultJson) {
		*resultJson = item.GetJSON();
	}
	return api.reindexer->Upsert(nsName, item);
}

size_t ClusterOperationApi::Cluster::InitServer(size_t id, const YAML::Node& clusterYml, const YAML::Node& replYml, size_t offset) {
	assert(id < svc_.size());
	auto& server = svc_[id];
	id += offset;
	ServerControlConfig scConfig(id, defaults_.defaultRpcPort + id, defaults_.defaultHttpPort + id,
								 fs::JoinPath(defaults_.baseTestsetDbPath, "node/" + std::to_string(id)), "node" + std::to_string(id), true,
								 maxUpdatesSize_);
	server.InitServerWithConfig(std::move(scConfig), replYml, clusterYml, YAML::Node(), YAML::Node());
	return id;
}

bool ClusterOperationApi::Cluster::StartServer(size_t id) {
	assert(id < svc_.size());
	auto& server = svc_[id];
	if (server.IsRunning()) {
		return false;
	}
	ServerControlConfig scConfig(id, defaults_.defaultRpcPort + id, defaults_.defaultHttpPort + id,
								 fs::JoinPath(defaults_.baseTestsetDbPath, "node/" + std::to_string(id)), "node" + std::to_string(id), true,
								 maxUpdatesSize_);
	server.InitServer(std::move(scConfig));
	return true;
}

bool ClusterOperationApi::Cluster::StopServer(size_t id) {
	assert(id < svc_.size());
	auto& server = svc_[id];
	if (!server.Get()) {
		return false;
	}
	server.Stop();
	server.Drop();
	auto now = std::chrono::milliseconds(0);
	const auto pause = std::chrono::milliseconds(10);
	while (server.IsRunning()) {
		now += pause;
		EXPECT_TRUE(now < kMaxServerStartTime);
		assert(now < kMaxServerStartTime);

		std::this_thread::sleep_for(pause);
	}
	return true;
}

void ClusterOperationApi::Cluster::StopServers(size_t from, size_t to) {
	assert(from <= to);
	for (size_t id = from; id < to; ++id) {
		auto srv = GetNode(id);
		if (srv) {
			srv->Stop();
		}
	}
	for (size_t id = from; id < to; ++id) {
		if (GetNode(id)) {
			TestCout() << "Stopping " << id << std::endl;
			ASSERT_TRUE(StopServer(id));
		}
	}
}

void ClusterOperationApi::Cluster::StopServers(const std::vector<size_t>& ids) {
	for (auto id : ids) {
		GetNode(id)->Stop();
	}
	for (auto id : ids) {
		TestCout() << "Stopping " << id << std::endl;
		ASSERT_TRUE(StopServer(id));
	}
}

int ClusterOperationApi::Cluster::AwaitLeader(std::chrono::seconds timeout, bool fulltime) {
	using cluster::RaftInfo;
	auto beg = steady_clock_w::now();
	auto end = beg;
	int leaderId;
	do {
		leaderId = -1;
		for (size_t i = 0; i < clients_.size(); ++i) {
			RaftInfo info;
			auto err = clients_[i].GetRaftInfo(info);
			if (err.ok()) {
				if (info.role == RaftInfo::Role::Leader) {
					EXPECT_EQ(leaderId, -1);
					leaderId = i;
				}
			}
		}
		if (!fulltime && leaderId >= 0) {
			return leaderId;
		}
		loop_.sleep(std::chrono::milliseconds(100));
		end = steady_clock_w::now();
	} while (end - beg < timeout);
	return leaderId;
}

void ClusterOperationApi::Cluster::doWaitSync(std::string_view ns, std::vector<ServerControl>& svc, lsn_t expectedLsn,
											  lsn_t expectedNsVersion, std::chrono::seconds maxSyncTime) {
	auto now = std::chrono::milliseconds(0);
	const auto pause = std::chrono::milliseconds(100);
	size_t syncedCnt = 0;
	const auto syncTime = maxSyncTime.count() ? maxSyncTime : kMaxSyncTime;
	while (syncedCnt != svc.size()) {
		now += pause;
		if (now >= syncTime) {
			PrintClusterInfo(ns, svc);
		}
		ASSERT_TRUE(now < syncTime);
		syncedCnt = getSyncCnt(ns, svc, expectedLsn, expectedNsVersion);
		std::this_thread::sleep_for(pause);
	}
}

size_t ClusterOperationApi::Cluster::getSyncCnt(std::string_view ns, std::vector<ServerControl>& svc, lsn_t expectedLsn,
												lsn_t expectedNsVersion) {
	size_t syncedCnt = 0;
	bool empty = true;
	ReplicationTestState state;
	for (auto& node : svc) {
		if (node.IsRunning()) {
			auto xstate = node.Get(false)->GetState(std::string(ns));
			if (empty) {
				state = xstate;
				empty = false;
			}
			if (!state.tmStatetoken.has_value() || !xstate.tmStatetoken.has_value() || !xstate.tmVersion.has_value() ||
				!state.tmVersion.has_value()) {
				continue;
			}
			const bool hasSameTms =
				xstate.tmStatetoken.value() == state.tmStatetoken.value() && xstate.tmVersion.value() == state.tmVersion.value();
			const bool hasSameLSN = xstate.lsn == state.lsn && xstate.nsVersion == state.nsVersion;
			if (hasSameLSN && hasSameTms && (expectedLsn.isEmpty() || state.lsn == expectedLsn) &&
				(expectedNsVersion.isEmpty() || state.nsVersion == expectedNsVersion)) {
				EXPECT_EQ(xstate.dataHash, state.dataHash);
				++syncedCnt;
			}
		} else {
			++syncedCnt;
		}
	}
	return syncedCnt;
}

void ClusterOperationApi::Cluster::WaitSync(std::string_view ns, lsn_t expectedLsn, lsn_t expectedNsVersion,
											std::chrono::seconds maxSyncTime) {
	doWaitSync(ns, svc_, expectedLsn, expectedNsVersion, maxSyncTime);
}

void ClusterOperationApi::Cluster::PrintClusterInfo(std::string_view ns, std::vector<ServerControl>& svc) {
	fmt::println(stderr, "Cluster info for {}:", ns);
	for (size_t id = 0; id < svc.size(); ++id) {
		fmt::print(stderr, "Node {}: ", id);
		if (svc[id].IsRunning()) {
			auto xstate = svc[id].Get()->GetState(std::string(ns));
			const std::string tmStateToken = xstate.tmStatetoken.has_value() ? std::to_string(xstate.tmStatetoken.value()) : "<none>";
			const std::string tmVersion = xstate.tmVersion.has_value() ? std::to_string(xstate.tmVersion.value()) : "<none>";
			fmt::println(stderr, "{{ ns_version: {}, lsn: {}, data_hash: {}, tm_token: {}, tm_version: {} }}", xstate.nsVersion, xstate.lsn,
						 xstate.dataHash, tmStateToken, tmVersion);
		} else {
			fmt::println(stderr, "down");
		}
	}
}

void ClusterOperationApi::Cluster::PrintClusterNsList(const std::vector<ClusterOperationApi::NamespaceData>& expected) {
	for (size_t id = 0; id < svc_.size(); ++id) {
		std::cerr << "Node " << id << ": ";
		auto& node = svc_[id];
		if (node.IsRunning()) {
			std::cerr << " up\n";
			std::vector<NamespaceDef> nsDefs;
			auto err = node.Get(false)->api.reindexer->EnumNamespaces(nsDefs, EnumNamespacesOpts().HideSystem().WithClosed().OnlyNames());
			if (!err.ok()) {
				std::cerr << "EnumNamespaces error: " << err.what() << "\n";
			}
			std::cerr << "Expected namespaces:\n";
			for (auto& ns : expected) {
				std::cerr << ns.name << "\n";
			}
			std::cerr << "Actual namespaces:\n";
			for (auto& ns : nsDefs) {
				std::cerr << ns.name << "\n";
			}
		} else {
			std::cerr << " down\n";
		}
	}
}

void ClusterOperationApi::Cluster::StopClients() {
	for (auto& client : clients_) {
		client.Stop();
	}
}

ServerControl::Interface::Ptr ClusterOperationApi::Cluster::GetNode(size_t id) {
	assert(id < svc_.size());
	return svc_[id].Get(false);
}

void ClusterOperationApi::Cluster::FillItem(BaseApi& api, BaseApi::ItemType& item, size_t id, DataParam param) {
	std::string json;
	if (!param.emptyVector) {
		std::array<float, kVectorDims> buf;
		for (float& v : buf) {
			v = randBin<float>(-10, 10);
		}
		json = fmt::format(R"json({{"id":{},"int":{},"string":"{}","ft_str":"{}","vec":[{:f}]}})json", id, rand(), api.RandString(),
						   api.RandString(), fmt::join(buf, ","));
	} else {
		json = fmt::format(R"json({{"id":{},"int":{},"string":"{}","ft_str":"{}","vec":[]}})json", id, rand(), api.RandString(),
						   api.RandString());
	}
	Error err = item.FromJSON(json);
	ASSERT_TRUE(err.ok()) << err.what();
}

void ClusterOperationApi::Cluster::ValidateNamespaceList(const std::vector<ClusterOperationApi::NamespaceData>& namespaces) {
	for (auto& nsData : namespaces) {
		WaitSync(nsData.name, nsData.expectedLsn);
	}

	auto now = std::chrono::milliseconds(0);
	const auto pause = std::chrono::milliseconds(100);
	size_t syncedCnt = 0;
	std::vector<NamespaceDef> nsDefs;
	while (syncedCnt != svc_.size()) {
		now += pause;
		if (now >= kMaxSyncTime) {
			PrintClusterNsList(namespaces);
		}
		ASSERT_TRUE(now < kMaxSyncTime);
		syncedCnt = 0;
		for (auto& node : svc_) {
			if (node.IsRunning()) {
				nsDefs.clear();
				auto err =
					node.Get(false)->api.reindexer->EnumNamespaces(nsDefs, EnumNamespacesOpts().HideSystem().WithClosed().OnlyNames());
				ASSERT_TRUE(err.ok()) << err.what();
				if (namespaces.size() != nsDefs.size()) {
					continue;
				}
				bool isSynced = true;
				for (auto& nsData : namespaces) {
					auto found =
						std::find_if(nsDefs.begin(), nsDefs.end(), [&nsData](const NamespaceDef& def) { return nsData.name == def.name; });
					if (found == nsDefs.end()) {
						isSynced = false;
					}
				}
				if (isSynced) {
					++syncedCnt;
				}
			} else {
				++syncedCnt;
			}
		}
		std::this_thread::sleep_for(pause);
	}
}

size_t ClusterOperationApi::Cluster::GetSynchronizedNodesCount(size_t nodeId) {
	auto stats = GetNode(nodeId)->GetReplicationStats(cluster::kClusterReplStatsType);
	size_t cnt = 0;
	for (auto& node : stats.nodeStats) {
		if (node.isSynchronized) {
			++cnt;
		}
	}
	return cnt;
}

void ClusterOperationApi::Cluster::EnablePerfStats(size_t nodeId) {
	GetNode(nodeId)->api.UpsertJSON(
		kConfigNamespace,
		R"json({"type":"profiling","profiling":{"queriesperfstats":true,"queries_threshold_us":100,"perfstats":true,"memstats":true,"activitystats":true}})json");
}

void ClusterOperationApi::Cluster::ChangeLeader(int& curLeaderId, int newLeaderId) {
	GetNode(curLeaderId)->SetClusterLeader(newLeaderId);
	curLeaderId = AwaitLeader(kMaxElectionsTime);
	ASSERT_EQ(curLeaderId, newLeaderId);
}

void ClusterOperationApi::Cluster::AddAsyncNode(size_t nodeId, const ServerControl::Interface::Ptr& node, AsyncReplicationMode replMode,
												std::optional<std::vector<std::string>>&& nsList) {
	assert(nodeId < svc_.size());
	auto asyncLeader = svc_[nodeId].Get();
	asyncLeader->AddFollower(node, std::move(nsList), replMode);
}

void ClusterOperationApi::Cluster::AwaitLeaderBecomeAvailable(size_t nodeId, std::chrono::milliseconds awaitTime) {
	auto now = std::chrono::milliseconds(0);
	const auto pause = std::chrono::milliseconds(100);
	const Query q = Query(kReplicationStatsNamespace).Where("type", CondEq, "cluster");
	while (now < awaitTime) {
		BaseApi::QueryResultsType qr;
		auto err = GetNode(nodeId)->api.reindexer->WithTimeout(pause).Select(q, qr);
		if (err.ok()) {
			break;
		}
		awaitTime += pause;
		std::this_thread::sleep_for(pause);
	}
	ASSERT_TRUE(now < awaitTime) << "Leader is not available from node " << nodeId;
}

YAML::Node ClusterOperationApi::Cluster::CreateClusterConfigStatic(size_t initialServerId, size_t count, const Defaults& ports,
																   const std::vector<std::string>& nsList,
																   std::chrono::milliseconds resyncTimeout, int maxSyncCount,
																   int syncThreadsCount) {
	std::vector<size_t> nodeIds;
	nodeIds.reserve(count);
	for (size_t id = initialServerId; id < initialServerId + count; ++id) {
		nodeIds.emplace_back(id);
	}

	return CreateClusterConfigStatic(nodeIds, ports, nsList, resyncTimeout, maxSyncCount, syncThreadsCount);
}

YAML::Node ClusterOperationApi::Cluster::CreateClusterConfigStatic(const std::vector<size_t>& nodeIds, const Defaults& ports,
																   const std::vector<std::string>& nsList,
																   std::chrono::milliseconds resyncTimeout, int maxSyncCount,
																   int syncThreadsCount) {
	if (maxSyncCount < 0) {
		maxSyncCount = rand() % 3;
		TestCout() << "Cluster's max_sync_count was chosen randomly: " << maxSyncCount << std::endl;
	}
	YAML::Node clusterConf;
	clusterConf["app_name"] = "rx_node";
	clusterConf["namespaces"] = nsList;
	clusterConf["sync_threads"] = syncThreadsCount;
	clusterConf["enable_compression"] = true;
	clusterConf["online_updates_timeout_sec"] = 20;
	clusterConf["sync_timeout_sec"] = 60;
	clusterConf["syncs_per_thread"] = maxSyncCount;
	clusterConf["retry_sync_interval_msec"] = resyncTimeout.count();
	clusterConf["log_level"] = logLevelToString(LogTrace);
	clusterConf["nodes"] = YAML::Node(YAML::NodeType::Sequence);
	for (size_t id : nodeIds) {
		YAML::Node node;
		node["dsn"] = MakeDsn(reindexer_server::UserRole::kRoleReplication, id, ports.defaultRpcPort + id, "node" + std::to_string(id));
		node["server_id"] = id;
		clusterConf["nodes"].push_back(node);
	}

	return clusterConf;
}

YAML::Node ClusterOperationApi::Cluster::CreateClusterConfig(size_t initialServerId, size_t count, const std::vector<std::string>& nsList,
															 std::chrono::milliseconds resyncTimeout, int maxSyncCount,
															 int syncThreadsCount) {
	return Cluster::CreateClusterConfigStatic(initialServerId, count, defaults_, nsList, resyncTimeout, maxSyncCount, syncThreadsCount);
}
