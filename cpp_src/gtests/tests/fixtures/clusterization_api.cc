#include "clusterization_api.h"
#include <fstream>
#include <thread>
#include "core/cjson/jsonbuilder.h"
#include "core/dbconfig.h"
#include "tools/fsops.h"
#include "vendor/gason/gason.h"

const std::string ClusterizationApi::kConfigNs = "#config";
const std::chrono::seconds ClusterizationApi::kMaxServerStartTime = std::chrono::seconds(15);
const std::chrono::seconds ClusterizationApi::kMaxSyncTime = std::chrono::seconds(15);
const std::chrono::seconds ClusterizationApi::kMaxElectionsTime = std::chrono::seconds(8);

const std::string ClusterizationApi::kIdField = "id";
const std::string ClusterizationApi::kStringField = "string";
const std::string ClusterizationApi::kIntField = "int";

void ClusterizationApi::SetUp() {
	const auto def = GetDefaultPorts();
	reindexer::fs::RmDirAll(def.baseTestsetDbPath);
}

void ClusterizationApi::TearDown()	// -V524
{
	const auto def = GetDefaultPorts();
	reindexer::fs::RmDirAll(def.baseTestsetDbPath);
}

ClusterizationApi::Cluster::Cluster(net::ev::dynamic_loop& loop, size_t initialServerId, size_t count, Defaults ports,
									std::vector<std::string> nsList, std::chrono::milliseconds resyncTimeout, int maxSyncCount,
									int syncThreadsCount, size_t maxUpdatesSize)
	: loop_(loop), defaults_(ports), maxUpdatesSize_(maxUpdatesSize) {
	std::string nsListStr;
	if (nsList.empty()) {
		nsListStr = "[]\n";
	} else {
		nsListStr += '\n';
		for (auto& ns : nsList) {
			nsListStr += "  - ";
			nsListStr += ns;
			nsListStr += '\n';
		}
	}
	if (maxSyncCount < 0) {
		maxSyncCount = rand() % 3;
		std::cout << "Cluster's max_sync_count was chosen randomly: " << maxSyncCount << std::endl;
	}
	// clang-format off
	const std::string kBaseClusterConf =
		"app_name: rx_node\n"
		"namespaces: " + nsListStr +
		"sync_threads: " + std::to_string(syncThreadsCount) + "\n"
		"enable_compression: true\n"
		"online_updates_timeout_sec: 20\n"
		"sync_timeout_sec: 60\n"
		"syncs_per_thread: " + std::to_string(maxSyncCount) + "\n"
		"retry_sync_interval_msec: " + std::to_string(resyncTimeout.count()) + "\n"
		"nodes:\n";
	// clang-format on
	std::string fullClusterConf = kBaseClusterConf;
	for (size_t i = initialServerId; i < initialServerId + count; ++i) {
		// clang-format off
		fullClusterConf.append(
			"  - dsn: " + fmt::format("cproto://127.0.0.1:{}/node{}", defaults_.defaultRpcPort + i,i) + "\n"
			"    server_id: " + std::to_string(i) + "\n"
		);
		// clang-format on
	}

	size_t id = initialServerId;
	for (size_t i = 0; i < count; ++i) {
		const std::string kReplConf =
			"cluster_id: 2\n"
			"server_id: " +
			std::to_string(id);
		svc_.emplace_back();
		InitServer(i, fullClusterConf, kReplConf);
		clients_.emplace_back();
		clients_.back().Connect(svc_.back().Get(false)->kClusterManagementDsn, loop_);
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

	api.DefineNamespaceDataset(string(nsName), {
												   IndexDeclaration{kIdField.c_str(), "hash", "int", IndexOpts().PK(), 0},
												   IndexDeclaration{kIntField.c_str(), "tree", "int", IndexOpts(), 0},
												   IndexDeclaration{kStringField.c_str(), "hash", "string", IndexOpts(), 0},
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

size_t ClusterizationApi::Cluster::InitServer(size_t id, const std::string& clusterYml, const std::string& replYml) {
	assert(id < svc_.size());
	auto& server = svc_[id];
	auto storagePath = fs::JoinPath(defaults_.baseTestsetDbPath, "node/" + std::to_string(id));
	server.InitServer(id, defaults_.defaultRpcPort + id, defaults_.defaultHttpPort + id,
					  fs::JoinPath(defaults_.baseTestsetDbPath, "node/" + std::to_string(id)), "node" + std::to_string(id), true,
					  maxUpdatesSize_);
	server.Get()->WriteReplicationConfig(replYml);
	server.Get()->WriteClusterConfig(clusterYml);
	EXPECT_TRUE(StopServer(id));
	EXPECT_TRUE(StartServer(id));
	return id;
}

bool ClusterizationApi::Cluster::StartServer(size_t id) {
	assert(id < svc_.size());
	auto& server = svc_[id];
	if (server.IsRunning()) return false;
	server.InitServer(id, defaults_.defaultRpcPort + id, defaults_.defaultHttpPort + id,
					  fs::JoinPath(defaults_.baseTestsetDbPath, "node/" + std::to_string(id)), "node" + std::to_string(id), true,
					  maxUpdatesSize_);
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
			std::cout << "Stopping " << id << std::endl;
			ASSERT_TRUE(StopServer(id));
		}
	}
}

void ClusterizationApi::Cluster::StopServers(const std::vector<size_t>& ids) {
	for (auto id : ids) {
		GetNode(id)->Stop();
	}
	for (auto id : ids) {
		std::cout << "Stopping " << id << std::endl;
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
											lsn_t expectedNsVersion) {
	auto now = std::chrono::milliseconds(0);
	const auto pause = std::chrono::milliseconds(100);
	size_t syncedCnt = 0;
	while (syncedCnt != svc.size()) {
		now += pause;
		if (now >= kMaxSyncTime) {
			PrintClusterInfo(ns, svc);
		}
		ASSERT_TRUE(now < kMaxSyncTime);
		bool empty = true;
		ReplicationStateApi state{lsn_t(), lsn_t(), 0, 0};
		syncedCnt = 0;
		for (auto& node : svc) {
			if (node.IsRunning()) {
				auto xstate = node.Get(false)->GetState(string(ns));
				if (empty) {
					state = xstate;
					empty = false;
				}
				if (xstate.lsn == state.lsn && xstate.nsVersion == state.nsVersion && (expectedLsn.isEmpty() || state.lsn == expectedLsn) &&
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

void ClusterizationApi::Cluster::WaitSync(std::string_view ns, lsn_t expectedLsn, lsn_t expectedNsVersion) {
	doWaitSync(ns, svc_, expectedLsn, expectedNsVersion);
}

void ClusterizationApi::Cluster::PrintClusterInfo(std::string_view ns, std::vector<ServerControl>& svc) {
	std::cerr << "Cluster info for " << ns << ":\n";
	for (size_t id = 0; id < svc.size(); ++id) {
		std::cerr << "Node " << id << ": ";
		if (svc[id].IsRunning()) {
			auto xstate = svc[id].Get()->GetState(string(ns));
			std::cerr << "{ ns_version: " << xstate.nsVersion << ", lsn: " << xstate.lsn << ", data_hash: " << xstate.dataHash << " }";
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
