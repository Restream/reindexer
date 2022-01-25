#include "cascade_replication_api.h"

void CascadeReplicationApi::SetUp() { reindexer::fs::RmDirAll(kBaseTestsetDbPath); }

void CascadeReplicationApi::TearDown()	// -V524
{
	reindexer::fs::RmDirAll(kBaseTestsetDbPath);
}

void CascadeReplicationApi::WaitSync(ServerPtr s1, ServerPtr s2, const std::string& nsName) {
	auto now = std::chrono::milliseconds(0);
	const auto pause = std::chrono::milliseconds(50);
	ReplicationStateApi state1, state2;
	while (true) {
		now += pause;
		const std::string tmStateToken1 = state1.tmStatetoken.has_value() ? std::to_string(state1.tmStatetoken.value()) : "<none>";
		const std::string tmStateToken2 = state2.tmStatetoken.has_value() ? std::to_string(state2.tmStatetoken.value()) : "<none>";
		const std::string tmVersion1 = state1.tmVersion.has_value() ? std::to_string(state1.tmVersion.value()) : "<none>";
		const std::string tmVersion2 = state2.tmVersion.has_value() ? std::to_string(state2.tmVersion.value()) : "<none>";
		ASSERT_TRUE(now < kMaxSyncTime) << "Wait sync is too long. s1 lsn: " << state1.lsn << "; s2 lsn: " << state2.lsn
										<< "; s1 count: " << state1.dataCount << "; s2 count: " << state2.dataCount
										<< " s1 hash: " << state1.dataHash << "; s2 hash: " << state2.dataHash
										<< " s1 tm_token: " << tmStateToken1 << "; s2 tm_token: " << tmStateToken2
										<< " s1 tm_version: " << tmVersion1 << "; s2 tm_version: " << tmVersion2;
		state1 = s1->GetState(nsName);
		state2 = s2->GetState(nsName);

		if (state1.tmStatetoken.has_value() && state2.tmStatetoken.has_value() && state1.tmVersion.has_value() &&
			state2.tmVersion.has_value()) {
			const bool hasSameTms =
				state1.tmStatetoken.value() == state2.tmStatetoken.value() && state1.tmVersion.value() == state2.tmVersion.value();
			const bool hasSameLSN = state1.lsn == state2.lsn && state1.nsVersion == state2.nsVersion;

			if (hasSameTms && hasSameLSN) {
				ASSERT_EQ(state1.dataHash, state2.dataHash);
				return;
			}
		}
		std::this_thread::sleep_for(pause);
	}
}

void CascadeReplicationApi::ValidateNsList(CascadeReplicationApi::ServerPtr s, const std::vector<std::string>& expected) {
	std::vector<reindexer::NamespaceDef> nsDefs;
	auto err = s->api.reindexer->EnumNamespaces(nsDefs, reindexer::EnumNamespacesOpts().OnlyNames().HideSystem().WithClosed());
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
																		  AsyncReplicationConfigTest::NsSet&& nsList) {
	if (clusterConfig.empty()) {
		return CascadeReplicationApi::Cluster();
	}
	std::vector<ServerControl> nodes;
	using ReplNode = AsyncReplicationConfigTest::Node;
	for (size_t i = 0; i < clusterConfig.size(); ++i) {
		nodes.push_back(ServerControl());
		const int serverId = baseServerId + i;
		nodes.back().InitServer(ServerControlConfig(serverId, basePort + i, basePort + 1000 + i, dbPathMaster + std::to_string(i), "db"));
		const bool isFollower = clusterConfig[i].leaderId >= 0;
		AsyncReplicationConfigTest config(isFollower ? "follower" : "leader", std::vector<ReplNode>(), false, true, serverId,
										  "node_" + std::to_string(serverId), std::move(nsList));
		auto srv = nodes.back().Get();
		srv->SetReplicationConfig(config);

		if (isFollower) {
			assert(int(nodes.size()) > clusterConfig[i].leaderId + 1);
			nodes[clusterConfig[i].leaderId].Get()->AddFollower(fmt::format("cproto://127.0.0.1:{}/db", srv->RpcPort()),
																std::move(clusterConfig[i].nsList));
		}
	}
	return CascadeReplicationApi::Cluster(std::move(nodes));
}

CascadeReplicationApi::TestNamespace1::TestNamespace1(ServerPtr srv, const std::string nsName) : nsName_(nsName) {
	auto opt = StorageOpts().Enabled(true);
	auto err = srv->api.reindexer->OpenNamespace(nsName_, opt);
	srv->api.DefineNamespaceDataset(nsName_, {IndexDeclaration{"id", "hash", "int", IndexOpts().PK(), 0}});
}

void CascadeReplicationApi::TestNamespace1::AddRows(ServerPtr srv, int from, unsigned int count, size_t dataLen) {
	for (unsigned int i = 0; i < count; i++) {
		auto item = srv->api.NewItem(nsName_);
		auto err = item.FromJSON("{\"id\":" + std::to_string(from + i) +
								 (dataLen ? (",\"data\":\"" + reindexer::randStringAlph(dataLen) + "\"") : "") + "}");
		ASSERT_TRUE(err.ok()) << err.what();
		srv->api.Upsert(nsName_, item);
		ASSERT_TRUE(err.ok()) << err.what();
	}
}

void CascadeReplicationApi::TestNamespace1::GetData(ServerPtr srv, std::vector<int>& ids) {
	auto qr = reindexer::Query(nsName_).Sort("id", false);
	BaseApi::QueryResultsType res;
	auto err = srv->api.reindexer->Select(qr, res);
	EXPECT_TRUE(err.ok()) << err.what();
	for (auto it : res) {
		reindexer::WrSerializer ser;
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
	nodes_[id].InitServer(ServerControlConfig(id, port + id, port + 1000 + id, dbPathMaster + std::to_string(id), "db"));
}

void CascadeReplicationApi::Cluster::ShutdownServer(size_t id) {
	assert(id < nodes_.size());
	if (nodes_[id].Get()) {
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
	nodes_[id].InitServer(ServerControlConfig(id, rpcPort, httpPort, storagePath, dbName, enableStats));
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
