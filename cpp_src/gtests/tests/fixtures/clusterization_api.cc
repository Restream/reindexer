#include "clusterization_api.h"
#include <fstream>
#include <thread>
#include "core/cjson/jsonbuilder.h"
#include "core/dbconfig.h"
#include "tools/fsops.h"
#include "vendor/gason/gason.h"

const std::string ClusterizationApi::kClusterConfigFilename = "cluster.conf";
const std::string ClusterizationApi::kReplicationConfigFilename = "replication.conf";
const std::string ClusterizationApi::kConfigNs = "#config";
const size_t ClusterizationApi::kDefaultRpcPort = 14000;
const size_t ClusterizationApi::kDefaultHttpPort = 15000;
const size_t ClusterizationApi::kDefaultClusterPort = 16000;
const size_t ClusterizationApi::kDefaultRpcClusterPort = 18000;
const std::chrono::seconds ClusterizationApi::kMaxServerStartTime = std::chrono::seconds(15);
const std::chrono::seconds ClusterizationApi::kMaxSyncTime = std::chrono::seconds(15);
const std::chrono::seconds ClusterizationApi::kMaxElectionsTime = std::chrono::seconds(8);
const std::string ClusterizationApi::kBaseTestsetDbPath = fs::JoinPath(fs::GetTempDir(), "rx_test/ClusterizationApi");
const std::string ClusterizationApi::kIdField = "id";
const std::string ClusterizationApi::kStringField = "string";
const std::string ClusterizationApi::kIntField = "int";

void ClusterizationApi::SetUp() { reindexer::fs::RmDirAll(kBaseTestsetDbPath); }

void ClusterizationApi::TearDown() {}

ClusterizationApi::Cluster::Cluster(net::ev::dynamic_loop& loop, size_t initialServerId, size_t count,
									std::chrono::milliseconds resyncTimeout)
	: loop_(loop) {
	// clang-format off
	const std::string kBaseClusterConf =
		"app_name: rx_node\n"
		"namespaces: []\n"
		"sync_threads: 4\n"
		"enable_compression: true\n"
		"updates_timeout_sec: 10\n"
		"retry_sync_interval_msec: " + std::to_string(resyncTimeout.count()) + "\n"
		"nodes:\n";
	// clang-format off
	std::string fullClusterConf = kBaseClusterConf;
	for (size_t i = initialServerId; i < initialServerId + count; ++i) {
		// clang-format off
		fullClusterConf.append(
			"  -\n"
			"    ip_addr: 127.0.0.1\n"
			"    server_id: " + std::to_string(i) + "\n"
			"    database: node" + std::to_string(i) + "\n"
			"    rpc_port: " + std::to_string(kDefaultRpcClusterPort + i) + "\n"
			"    management_port: " + std::to_string(kDefaultClusterPort + i) + "\n"
		);
		// clang-format off
	}

	size_t id = initialServerId;
	for (size_t i = 0; i < count; ++i) {
		const std::string kReplConf =
			"role: master\n"
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

ClusterizationApi::Cluster::~Cluster()
{
	StopClients();
	for (size_t i = 0; i < svc_.size(); ++i) {
		if (svc_[i].IsRunning()) {
			StopServer(i);
		}
	}
}

void ClusterizationApi::Cluster::InitNs(size_t id, string_view nsName) {
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

void ClusterizationApi::Cluster::FillData(size_t id, string_view nsName, size_t from, size_t count) {
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

void ClusterizationApi::Cluster::FillDataTx(size_t id, string_view nsName, size_t from, size_t count) {
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
	auto err = api.reindexer->CommitTransaction(tx);
	ASSERT_TRUE(err.ok()) << err.what();
}

void ClusterizationApi::Cluster::AddRow(size_t id, string_view nsName, int pk) {
	assert(id < svc_.size());
	auto srv = svc_[id].Get();
	auto& api = srv->api;
	auto item = api.NewItem(nsName);
	Error err = item.FromJSON(
		"{\n"
		"\"id\":" +
		std::to_string(pk) +
		",\n"
		"\"int\":" +
		std::to_string(rand()) +
		",\n"
		"\"string\":\"" +
		api.RandString() +
		"\"\n"
		"}");

	api.Upsert(nsName, item);
}

size_t ClusterizationApi::Cluster::InitServer(size_t id, const std::string& clusterYml, const std::string& replYml) {
	assert(id < svc_.size());
	auto& server = svc_[id];
	auto storagePath = fs::JoinPath(kBaseTestsetDbPath, "node/" + std::to_string(id));
	server.InitServer(id, kDefaultRpcPort + id, kDefaultHttpPort + id, kDefaultClusterPort + id,
					  fs::JoinPath(kBaseTestsetDbPath, "node/" + std::to_string(id)), "node" + std::to_string(id), true, 0,
					  kDefaultRpcClusterPort + id);
	server.Get()->WriteClusterConfig(clusterYml);
	server.Get()->WriteReplicationConfig(replYml);
	EXPECT_TRUE(StopServer(id));
	EXPECT_TRUE(StartServer(id));
	return id;
}

bool ClusterizationApi::Cluster::StartServer(size_t id) {
	assert(id < svc_.size());
	auto& server = svc_[id];
	if (server.IsRunning()) return false;
	server.InitServer(id, kDefaultRpcPort + id, kDefaultHttpPort + id, kDefaultClusterPort + id,
					  fs::JoinPath(kBaseTestsetDbPath, "node/" + std::to_string(id)), "node" + std::to_string(id), true, 0,
					  kDefaultRpcClusterPort + id);
	return true;
}

bool ClusterizationApi::Cluster::StopServer(size_t id) {
	assert(id < svc_.size());
	auto& server = svc_[id];
	if (!server.Get()) return false;
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

void ClusterizationApi::Cluster::WaitSync(string_view ns) {
	auto now = std::chrono::milliseconds(0);
	const auto pause = std::chrono::milliseconds(100);
	size_t syncedCnt = 0;
	while (syncedCnt != svc_.size()) {
		now += pause;
		if (now >= kMaxSyncTime) {
			PrintClusterInfo(ns);
		}
		ASSERT_TRUE(now < kMaxSyncTime);
		bool empty = true;
		ReplicationStateApi state{lsn_t(), lsn_t(), 0, 0, false};
		syncedCnt = 0;
		for (auto& node : svc_) {
			if (node.IsRunning()) {
				auto xstate = node.Get(false)->GetState(string(ns));
				if (empty) {
					state = xstate;
					empty = false;
				}
				if (xstate.lsn == state.lsn) {
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

void ClusterizationApi::Cluster::PrintClusterInfo(string_view ns) {
	std::cerr << "Cluster info for " << ns << ":\n";
	for (size_t id = 0; id < svc_.size(); ++id) {
		std::cerr << "Node " << id << ": ";
		if (svc_[id].IsRunning()) {
			auto xstate = svc_[id].Get()->GetState(string(ns));
			std::cerr << "{ lsn: " << xstate.lsn << ", data_hash: " << xstate.dataHash << " }";
		} else {
			std::cerr << "down";
		}
		std::cerr << std::endl;
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
	Error err = item.FromJSON(
					"{\n"
					"\"id\":" + std::to_string(id)+",\n"
					"\"int\":" + std::to_string(rand())+",\n"
					"\"string\":\"" + api.RandString()+"\"\n"
					"}");
	// clang-format on
	ASSERT_TRUE(err.ok()) << err.what();
}
