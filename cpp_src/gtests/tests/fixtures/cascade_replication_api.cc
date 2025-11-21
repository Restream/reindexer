#include "cascade_replication_api.h"
#include "core/system_ns_names.h"
#include "vendor/gason/gason.h"

using namespace reindexer;

void CascadeReplicationApi::SetUp() { std::ignore = fs::RmDirAll(kBaseTestsetDbPath); }

void CascadeReplicationApi::TearDown()	// -V524
{
	std::ignore = fs::RmDirAll(kBaseTestsetDbPath);
}

void CascadeReplicationApi::ValidateNsList(const CascadeReplicationApi::ServerPtr& s, const std::vector<std::string>& expected) {
	std::vector<NamespaceDef> nsDefs;
	auto err = s->api.reindexer->EnumNamespaces(nsDefs, EnumNamespacesOpts().OnlyNames().HideSystem().WithClosed());
	ASSERT_TRUE(err.ok()) << err.what();
	EXPECT_EQ(nsDefs.size(), expected.size());
	bool valid = nsDefs.size() == expected.size();
	for (auto& ns : nsDefs) {
		auto found = std::find(expected.begin(), expected.end(), ns.name);
		EXPECT_NE(found, expected.end());
		if (found == expected.end()) {
			valid = false;
			break;
		}
	}
	if (!valid) {
		std::cerr << "ServerId: " << s->Id() << "\n";
		std::cerr << "Expected: \n";
		for (auto& ns : expected) {
			std::cerr << ns << "\n";
		}
		std::cerr << "Actual: \n";
		for (auto& ns : nsDefs) {
			std::cerr << ns.name << "\n";
		}
		std::cerr << std::endl;
	}
}

CascadeReplicationApi::Cluster CascadeReplicationApi::CreateConfiguration(const std::vector<int>& clusterConfig, int basePort,
																		  int baseServerId, const std::string& dbPathMaster) {
	std::vector<CascadeReplicationApi::FollowerConfig> config;
	config.reserve(clusterConfig.size());
	for (auto& c : clusterConfig) {
		config.emplace_back(c);
	}
	return CreateConfiguration(std::move(config), basePort, baseServerId, dbPathMaster, {});
}

CascadeReplicationApi::Cluster CascadeReplicationApi::CreateConfiguration(std::vector<CascadeReplicationApi::FollowerConfig> clusterConfig,
																		  int basePort, int baseServerId, const std::string& dbPathMaster,
																		  const AsyncReplicationConfigTest::NsSet& nsList) {
	if (clusterConfig.empty()) {
		return CascadeReplicationApi::Cluster(basePort);
	}
	std::vector<ServerControl> nodes;
	nodes.reserve(clusterConfig.size());
	using ReplNode = AsyncReplicationConfigTest::Node;
	for (size_t i = 0; i < clusterConfig.size(); ++i) {
		const int serverId = baseServerId + i;
		nodes.emplace_back().InitServer(
			ServerControlConfig(serverId, basePort + i, basePort + 1000 + i, dbPathMaster + std::to_string(i), "db"));
		const bool isFollower = clusterConfig[i].leaderId >= 0;
		AsyncReplicationConfigTest config(isFollower ? "follower" : "leader", std::vector<ReplNode>(), false, true, serverId,
										  "node_" + std::to_string(serverId), nsList);
		auto srv = nodes.back().Get();
		srv->SetReplicationConfig(config);

		if (isFollower) {
			assert(int(nodes.size()) > clusterConfig[i].leaderId + 1);
			nodes[clusterConfig[i].leaderId].Get()->AddFollower(srv, std::move(clusterConfig[i].nsList));
		}
	}
	return CascadeReplicationApi::Cluster(baseServerId, std::move(nodes));
}

void CascadeReplicationApi::UpdateReplTokensByConfiguration(CascadeReplicationApi::Cluster& cluster,
															const std::vector<int>& clusterConfig) {
	std::vector<std::string> tokens(clusterConfig.size());

	for (size_t nodeId = 0; nodeId < clusterConfig.size(); ++nodeId) {
		int leaderIndex = clusterConfig[nodeId];

		// skip setting admissible tokens for leader
		if (leaderIndex < 0) {
			continue;
		}

		// set self token on leader only once
		if (tokens[leaderIndex].empty()) {
			tokens[leaderIndex] = randStringAlph(20);
			cluster.Get(leaderIndex)->UpdateConfigReplTokens(tokens[leaderIndex]);
		}
		// set admissible tokens on follower
		cluster.Get(nodeId)->UpdateConfigReplTokens(NsNamesHashMapT<std::string>{{NamespaceName("*"), tokens[leaderIndex]}});
	}
}

void CascadeReplicationApi::UpdateReplicationConfigs(const ServerPtr& sc, const std::string& selfToken,
													 const std::string& admissibleLeaderToken) {
	if (!selfToken.empty()) {
		sc->UpdateConfigReplTokens(selfToken);
	}
	if (!admissibleLeaderToken.empty()) {
		sc->UpdateConfigReplTokens(NsNamesHashMapT<std::string>{{NamespaceName("*"), admissibleLeaderToken}});
	}
}

void CascadeReplicationApi::ApplyConfig(const ServerPtr& sc, std::string_view json) {
	auto& rx = *sc->api.reindexer;
	auto item = rx.NewItem(kConfigNamespace);
	ASSERT_TRUE(item.Status().ok()) << item.Status().what();
	auto err = item.FromJSON(json);
	ASSERT_TRUE(err.ok()) << err.what();
	err = rx.Upsert(kConfigNamespace, item);
	ASSERT_TRUE(err.ok()) << err.what();
}

void CascadeReplicationApi::CheckTxCopyEventsCount(const ServerPtr& sc, int expectedCount) {
	auto& rx = *sc->api.reindexer;
	client::QueryResults qr;
	auto err = rx.Select(Query(kPerfStatsNamespace), qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qr.Count(), 1);
	WrSerializer ser;
	err = qr.begin().GetJSON(ser, false);
	ASSERT_TRUE(err.ok()) << err.what();
	gason::JsonParser parser;
	auto resJS = parser.Parse(ser.Slice());
	ASSERT_EQ(resJS["transactions"]["total_copy_count"].As<int>(-1), expectedCount) << ser.Slice();
}

CascadeReplicationApi::TestNamespace1::TestNamespace1(const ServerPtr& srv, std::string_view nsName) : nsName_(nsName) {
	auto opt = StorageOpts().Enabled(true);
	auto err = srv->api.reindexer->OpenNamespace(nsName_, opt);
	EXPECT_TRUE(err.ok()) << err.what();
	srv->api.DefineNamespaceDataset(nsName_, {IndexDeclaration{"id", "hash", "int", IndexOpts().PK(), 0}});
}

void CascadeReplicationApi::TestNamespace1::AddRows(const ServerPtr& srv, int from, unsigned int count, size_t dataLen) {
	for (unsigned int i = 0; i < count; i++) {
		auto item = srv->api.NewItem(nsName_);
		auto err = item.FromJSON(dataLen ? fmt::format(R"json({{"id":{}, "data":"{}"}})json", from + i, randStringAlph(dataLen))
										 : fmt::format(R"json({{"id":{}}})json", from + i));
		ASSERT_TRUE(err.ok()) << err.what();
		srv->api.Upsert(nsName_, item);
		ASSERT_TRUE(err.ok()) << err.what();
	}
}

void CascadeReplicationApi::TestNamespace1::AddRowsTx(const ServerPtr& srv, int from, unsigned int count, size_t dataLen) {
	auto& rx = *srv->api.reindexer;
	auto tr = rx.NewTransaction(nsName_);
	ASSERT_TRUE(tr.Status().ok()) << tr.Status().what();
	for (unsigned int i = 0; i < count; i++) {
		client::Item item = tr.NewItem();
		auto err = item.FromJSON(dataLen ? fmt::format(R"json({{"id":{}, "data":"{}"}})json", from + i, randStringAlph(dataLen))
										 : fmt::format(R"json({{"id":{}}})json", from + i));
		ASSERT_TRUE(err.ok()) << err.what();
		err = tr.Upsert(std::move(item));
		ASSERT_TRUE(err.ok()) << err.what();
	}
	client::QueryResults qr;
	auto err = rx.CommitTransaction(tr, qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qr.Count(), count);
}

void CascadeReplicationApi::TestNamespace1::GetData(const ServerPtr& srv, std::vector<int>& ids) {
	auto qr = Query(nsName_).Sort("id", false);
	BaseApi::QueryResultsType res;
	auto err = srv->api.reindexer->Select(qr, res);
	EXPECT_TRUE(err.ok()) << err.what();
	for (auto it : res) {
		WrSerializer ser;
		err = it.GetJSON(ser, false);
		EXPECT_TRUE(err.ok()) << err.what();
		gason::JsonParser parser;
		auto root = parser.Parse(ser.Slice());
		ids.push_back(root["id"].As<int>());
	}
}

void CascadeReplicationApi::Cluster::RestartServer(size_t id, int port, const std::string& dbPathMaster) {
	assert(id < nodes_.size());
	ShutdownServer(id);
	nodes_[id].InitServer(ServerControlConfig(baseServerId_ + id, port + id, port + 1000 + id, dbPathMaster + std::to_string(id), "db"));
}

void CascadeReplicationApi::Cluster::ShutdownServer(size_t id) {
	assert(id < nodes_.size());
	if (nodes_[id].Get()) {
		nodes_[id].Stop();
		nodes_[id].Drop();
		size_t counter = 0;
		while (nodes_[id].IsRunning()) {
			counter++;
			// we have only 10sec timeout to restart server!!!!
			EXPECT_TRUE(counter < 1000);
			assert(counter < 1000);
			std::this_thread::sleep_for(std::chrono::milliseconds(10));
		}
	}
}

void CascadeReplicationApi::Cluster::InitServer(size_t id, unsigned short rpcPort, unsigned short httpPort, const std::string& storagePath,
												const std::string& dbName, bool enableStats) {
	assert(id < nodes_.size());
	nodes_[id].InitServer(ServerControlConfig(baseServerId_ + id, rpcPort, httpPort, storagePath, dbName, enableStats, 0));
}

CascadeReplicationApi::Cluster::~Cluster() {
	std::vector<std::thread> shutdownThreads(nodes_.size());
	for (size_t i = 0; i < shutdownThreads.size(); ++i) {
		shutdownThreads[i] = std::thread(
			[this](size_t id) {
				const auto srv = nodes_[id].Get(false);
				if (srv) {
					srv->Stop();
				}
			},
			i);
	}
	for (auto& th : shutdownThreads) {
		th.join();
	}
}
