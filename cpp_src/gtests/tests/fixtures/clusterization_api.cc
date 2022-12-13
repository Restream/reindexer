#include "clusterization_api.h"
#include <fstream>
#include <thread>
#include "core/cjson/jsonbuilder.h"
#include "core/dbconfig.h"
#include "tools/fsops.h"
#include "vendor/gason/gason.h"
#include "yaml-cpp/yaml.h"

const std::string ClusterizationApi::kConfigNs = "#config";
const std::chrono::seconds ClusterizationApi::kMaxServerStartTime = std::chrono::seconds(15);
const std::chrono::seconds ClusterizationApi::kMaxSyncTime = std::chrono::seconds(15);
const std::chrono::seconds ClusterizationApi::kMaxElectionsTime = std::chrono::seconds(12);

const std::string ClusterizationApi::kIdField = "id";
const std::string ClusterizationApi::kStringField = "string";
const std::string ClusterizationApi::kIntField = "int";
const std::string ClusterizationApi::kFTField = "ft_str";

void ClusterizationApi::SetUp() {
	const auto def = GetDefaults();
	reindexer::fs::RmDirAll(def.baseTestsetDbPath);
}

void ClusterizationApi::TearDown()	// -V524
{
	const auto def = GetDefaults();
	reindexer::fs::RmDirAll(def.baseTestsetDbPath);
}

ClusterizationApi::Cluster::Cluster(net::ev::dynamic_loop& loop, size_t initialServerId, size_t count, Defaults ports,
									const std::vector<std::string>& nsList, std::chrono::milliseconds resyncTimeout, int maxSyncCount,
									int syncThreadsCount, size_t maxUpdatesSize)
	: loop_(loop), defaults_(std::move(ports)), maxUpdatesSize_(maxUpdatesSize) {
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
	clusterConf["nodes"] = YAML::Node(YAML::NodeType::Sequence);
	clusterConf["log_level"] = logLevelToString(LogTrace);
	for (size_t i = initialServerId; i < initialServerId + count; ++i) {
		YAML::Node node;
		node["dsn"] = fmt::format("cproto://127.0.0.1:{}/node{}", defaults_.defaultRpcPort + i, i);
		node["server_id"] = i;
		clusterConf["nodes"].push_back(node);
	}

	size_t id = initialServerId;
	for (size_t i = 0; i < count; ++i) {
		YAML::Node replConf;
		replConf["cluster_id"] = 2;
		replConf["server_id"] = id;
		svc_.emplace_back();
		InitServer(i, clusterConf, replConf, initialServerId);
		clients_.emplace_back();
		clients_.back().Connect(svc_.back().Get(false)->kRPCDsn, loop_);
		++id;
	}
}

ClusterizationApi::Cluster::~Cluster() {
	StopClients();
	for (size_t i = 0; i < svc_.size(); ++i) {
		if (svc_[i].IsRunning()) {
			svc_[i].Get()->Stop();
		}
	}
}

void ClusterizationApi::Cluster::InitNs(size_t id, std::string_view nsName) {
	auto opt = StorageOpts().Enabled(true);

	// until we use shared ptr it will be not destroyed
	assert(id < svc_.size());
	auto srv = svc_[id].Get();
	auto& api = srv->api;

	Error err = api.reindexer->OpenNamespace(nsName, opt);
	ASSERT_TRUE(err.ok()) << err.what();

	api.DefineNamespaceDataset(std::string(nsName), {
														IndexDeclaration{kIdField.c_str(), "hash", "int", IndexOpts().PK(), 0},
														IndexDeclaration{kIntField.c_str(), "tree", "int", IndexOpts(), 0},
														IndexDeclaration{kStringField.c_str(), "hash", "string", IndexOpts(), 0},
														IndexDeclaration{kFTField.c_str(), "text", "string", IndexOpts(), 0},
													});
}

void ClusterizationApi::Cluster::DropNs(size_t id, std::string_view nsName) {
	// until we use shared ptr it will be not destroyed
	assert(id < svc_.size());
	auto srv = svc_[id].Get();
	auto& api = srv->api;
	Error err = api.reindexer->DropNamespace(nsName);
	ASSERT_TRUE(err.ok()) << err.what();
}

void ClusterizationApi::Cluster::FillData(size_t id, std::string_view nsName, size_t from, size_t count) {
	// until we use shared ptr it will be not destroyed
	assert(id < svc_.size());
	auto srv = svc_[id].Get();
	auto& api = srv->api;

	for (size_t i = from; i < from + count; ++i) {
		auto item = api.NewItem(nsName);
		FillItem(api, item, i);
		api.Upsert(nsName, item);
	}
	api.Commit(nsName);
}

void ClusterizationApi::Cluster::FillDataTx(size_t id, std::string_view nsName, size_t from, size_t count) {
	// until we use shared ptr it will be not destroyed
	assert(id < svc_.size());
	auto srv = svc_[id].Get();
	auto& api = srv->api;

	auto tx = api.reindexer->NewTransaction(nsName);
	ASSERT_TRUE(tx.Status().ok()) << tx.Status().what();
	for (size_t i = from; i < from + count; ++i) {
		auto item = tx.NewItem();
		FillItem(api, item, i);
		tx.Upsert(std::move(item));
	}
	BaseApi::QueryResultsType results;
	auto err = api.reindexer->CommitTransaction(tx, results);
	ASSERT_TRUE(err.ok()) << err.what();
}

void ClusterizationApi::Cluster::AddRow(size_t id, std::string_view nsName, int pk) {
	assert(id < svc_.size());
	auto srv = svc_[id].Get();
	auto& api = srv->api;
	auto item = api.NewItem(nsName);
	FillItem(api, item, pk);
	api.Upsert(nsName, item);
}

Error ClusterizationApi::Cluster::AddRowWithErr(size_t id, std::string_view nsName, int pk, std::string* resultJson) {
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
	FillItem(api, item, pk);
	if (resultJson) {
		*resultJson = item.GetJSON();
	}
	return api.reindexer->Upsert(nsName, item);
}

size_t ClusterizationApi::Cluster::InitServer(size_t id, const YAML::Node& clusterYml, const YAML::Node& replYml, size_t offset) {
	assert(id < svc_.size());
	auto& server = svc_[id];
	id += offset;
	auto storagePath = fs::JoinPath(defaults_.baseTestsetDbPath, "node/" + std::to_string(id));

	ServerControlConfig scConfig(id, defaults_.defaultRpcPort + id, defaults_.defaultHttpPort + id,
								 fs::JoinPath(defaults_.baseTestsetDbPath, "node/" + std::to_string(id)), "node" + std::to_string(id), true,
								 maxUpdatesSize_);
	server.InitServerWithConfig(std::move(scConfig), replYml, clusterYml, YAML::Node(), YAML::Node());
	return id;
}

bool ClusterizationApi::Cluster::StartServer(size_t id) {
	assert(id < svc_.size());
	auto& server = svc_[id];
	if (server.IsRunning()) return false;
	ServerControlConfig scConfig(id, defaults_.defaultRpcPort + id, defaults_.defaultHttpPort + id,
								 fs::JoinPath(defaults_.baseTestsetDbPath, "node/" + std::to_string(id)), "node" + std::to_string(id), true,
								 maxUpdatesSize_);
	server.InitServer(std::move(scConfig));
	return true;
}

bool ClusterizationApi::Cluster::StopServer(size_t id) {
	assert(id < svc_.size());
	auto& server = svc_[id];
	if (!server.Get()) return false;
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

void ClusterizationApi::Cluster::StopServers(size_t from, size_t to) {
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

void ClusterizationApi::Cluster::StopServers(const std::vector<size_t>& ids) {
	for (auto id : ids) {
		GetNode(id)->Stop();
	}
	for (auto id : ids) {
		TestCout() << "Stopping " << id << std::endl;
		ASSERT_TRUE(StopServer(id));
	}
}

int ClusterizationApi::Cluster::AwaitLeader(std::chrono::seconds timeout, bool fulltime) {
	auto beg = std::chrono::high_resolution_clock::now();
	auto end = beg;
	int leaderId = -1;
	do {
		leaderId = -1;
		for (size_t i = 0; i < clients_.size(); ++i) {
			cluster::RaftInfo info;
			auto err = clients_[i].GetRaftInfo(info);
			if (err.ok()) {
				if (info.role == cluster::RaftInfo::Role::Leader) {
					EXPECT_EQ(leaderId, -1);
					leaderId = i;
				}
			}
		}
		if (!fulltime && leaderId >= 0) {
			return leaderId;
		}
		loop_.sleep(std::chrono::milliseconds(100));
		end = std::chrono::high_resolution_clock::now();
	} while (end - beg < timeout);
	return leaderId;
}

void ClusterizationApi::Cluster::doWaitSync(std::string_view ns, std::vector<ServerControl>& svc, lsn_t expectedLsn,
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
		bool empty = true;
		ReplicationStateApi state{lsn_t(), lsn_t(), 0, 0, {}, {}};
		syncedCnt = 0;
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
					ASSERT_EQ(xstate.dataHash, state.dataHash);
					++syncedCnt;
				}
			} else {
				++syncedCnt;
			}
		}
		std::this_thread::sleep_for(pause);
	}
}

void ClusterizationApi::Cluster::WaitSync(std::string_view ns, lsn_t expectedLsn, lsn_t expectedNsVersion,
										  std::chrono::seconds maxSyncTime) {
	doWaitSync(ns, svc_, expectedLsn, expectedNsVersion, maxSyncTime);
}

void ClusterizationApi::Cluster::PrintClusterInfo(std::string_view ns, std::vector<ServerControl>& svc) {
	std::cerr << "Cluster info for " << ns << ":\n";
	for (size_t id = 0; id < svc.size(); ++id) {
		std::cerr << "Node " << id << ": ";
		if (svc[id].IsRunning()) {
			auto xstate = svc[id].Get()->GetState(std::string(ns));
			const std::string tmStateToken = xstate.tmStatetoken.has_value() ? std::to_string(xstate.tmStatetoken.value()) : "<none>";
			const std::string tmVersion = xstate.tmVersion.has_value() ? std::to_string(xstate.tmVersion.value()) : "<none>";
			std::cerr << "{ ns_version: " << xstate.nsVersion << ", lsn: " << xstate.lsn << ", data_hash: " << xstate.dataHash
					  << ", tm_token: " << tmStateToken << ", tm_version: " << tmVersion << " }";
		} else {
			std::cerr << "down";
		}
		std::cerr << std::endl;
	}
}

void ClusterizationApi::Cluster::PrintClusterNsList(const std::vector<ClusterizationApi::NamespaceData>& expected) {
	for (size_t id = 0; id < svc_.size(); ++id) {
		std::cerr << "Node " << id << ": ";
		auto& node = svc_[id];
		if (node.IsRunning()) {
			std::cerr << " up\n";
			std::vector<NamespaceDef> nsDefs;
			node.Get(false)->api.reindexer->EnumNamespaces(nsDefs, EnumNamespacesOpts().HideSystem().WithClosed().OnlyNames());
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

void ClusterizationApi::Cluster::StopClients() {
	for (auto& client : clients_) {
		auto err = client.Stop();
		ASSERT_TRUE(err.ok()) << err.what();
	}
}

ServerControl::Interface::Ptr ClusterizationApi::Cluster::GetNode(size_t id) {
	assert(id < svc_.size());
	return svc_[id].Get(false);
}

void ClusterizationApi::Cluster::FillItem(BaseApi& api, BaseApi::ItemType& item, size_t id) {
	// clang-format off
	std::string json = "{\n"
					   "\"id\":" + std::to_string(id)+",\n"
					   "\"int\":" + std::to_string(rand())+",\n"
					   "\"string\":\"" + api.RandString()+"\"\n"
					   "\"ft_str\":\"" + api.RandString()+"\"\n"
					   "}";
	// clang-format on
	Error err = item.FromJSON(json);
	ASSERT_TRUE(err.ok()) << err.what();
}

void ClusterizationApi::Cluster::ValidateNamespaceList(const std::vector<ClusterizationApi::NamespaceData>& namespaces) {
	for (auto& nsData : namespaces) {
		WaitSync(nsData.name, nsData.expectedLsn);
	}

	auto now = std::chrono::milliseconds(0);
	const auto pause = std::chrono::milliseconds(100);
	size_t syncedCnt = 0;
	while (syncedCnt != svc_.size()) {
		now += pause;
		if (now >= kMaxSyncTime) {
			PrintClusterNsList(namespaces);
		}
		ASSERT_TRUE(now < kMaxSyncTime);
		syncedCnt = 0;
		for (auto& node : svc_) {
			if (node.IsRunning()) {
				std::vector<NamespaceDef> nsDefs;
				node.Get(false)->api.reindexer->EnumNamespaces(nsDefs, EnumNamespacesOpts().HideSystem().WithClosed().OnlyNames());
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

size_t ClusterizationApi::Cluster::GetSynchronizedNodesCount(size_t nodeId) {
	auto stats = GetNode(nodeId)->GetReplicationStats(cluster::kClusterReplStatsType);
	size_t cnt = 0;
	for (auto& node : stats.nodeStats) {
		if (node.isSynchronized) {
			++cnt;
		}
	}
	return cnt;
}

void ClusterizationApi::Cluster::EnablePerfStats(size_t nodeId) {
	auto node = GetNode(nodeId);
	auto rx = node->api.reindexer;
	auto item = rx->NewItem(kConfigNs);
	ASSERT_TRUE(item.Status().ok()) << item.Status().what();
	auto err = item.FromJSON(
		R"json({"type":"profiling","profiling":{"queriesperfstats":true,"queries_threshold_us":100,"perfstats":true,"memstats":true,"activitystats":true}})json");
	ASSERT_TRUE(err.ok()) << err.what();
	err = rx->Upsert(kConfigNs, item);
	ASSERT_TRUE(err.ok()) << err.what();
}

void ClusterizationApi::Cluster::ChangeLeader(int& curLeaderId, int newLeaderId) {
	GetNode(curLeaderId)->SetClusterLeader(newLeaderId);
	curLeaderId = AwaitLeader(kMaxElectionsTime);
	ASSERT_EQ(curLeaderId, newLeaderId);
}

void ClusterizationApi::Cluster::AddAsyncNode(size_t nodeId, const std::string& dsn, cluster::AsyncReplicationMode replMode,
											  std::optional<std::vector<std::string>>&& nsList) {
	assert(nodeId < svc_.size());
	auto asyncLeader = svc_[nodeId].Get();
	asyncLeader->AddFollower(dsn, std::move(nsList), replMode);
}

void ClusterizationApi::Cluster::AwaitLeaderBecomeAvailable(size_t nodeId, std::chrono::milliseconds awaitTime) {
	auto now = std::chrono::milliseconds(0);
	const auto pause = std::chrono::milliseconds(100);
	while (now < awaitTime) {
		Query q = Query("#replicationstats").Where("type", CondEq, Variant("cluster"));
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
