#include "cascade_replication_api.h"
#include <algorithm>
#include <thread>
#include <type_traits>
#include "core/system_ns_names.h"
#include "vendor/gason/gason.h"

namespace reindexer_tests {

using namespace reindexer;

template <typename T>
const typename CascadeReplicationApiP<T>::Defaults& CascadeReplicationApiP<T>::GetDefaults() const {
	if constexpr (std::is_same_v<T, sq8_test::TestSyncType>) {
		static Defaults defs{7970, 8080, fs::JoinPath(fs::GetTempDir(), "rx_test/Sq8CascadeReplicationApi")};
		return defs;
	} else {
		static Defaults defs{7770, 7880, fs::JoinPath(fs::GetTempDir(), "rx_test/CascadeReplicationApi")};
		return defs;
	}
}

template <typename T>
void CascadeReplicationApiP<T>::SetUp() {
	std::ignore = fs::RmDirAll(GetDefaults().baseTestsetDbPath);
}

template <typename T>
void CascadeReplicationApiP<T>::TearDown()	// -V524
{
	std::ignore = fs::RmDirAll(GetDefaults().baseTestsetDbPath);
}

template <typename T>
void CascadeReplicationApiP<T>::ValidateNsList(const CascadeReplicationApiP::ServerPtr& s, const std::vector<std::string>& expected) {
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

template <typename T>
void CascadeReplicationApiP<T>::AwaitNsAbsence(const CascadeReplicationApiP::ServerPtr& s, std::string_view nsName,
											   std::chrono::milliseconds timeout) {
	const auto step = std::chrono::milliseconds(50);
	std::vector<NamespaceDef> nsDefs;
	for (auto remain = timeout; remain.count() > 0; remain -= step) {
		nsDefs.clear();
		auto err = s->api.reindexer->EnumNamespaces(nsDefs, EnumNamespacesOpts().OnlyNames().HideSystem().HideTemporary().WithClosed());
		ASSERT_TRUE(err.ok()) << err.what();
		const bool found =
			std::find_if(nsDefs.begin(), nsDefs.end(), [&](const NamespaceDef& def) { return def.name == nsName; }) != nsDefs.end();
		if (!found) {
			return;
		}
		std::this_thread::sleep_for(step);
	}
	std::string actual;
	for (const auto& def : nsDefs) {
		if (!actual.empty()) {
			actual.append(", ");
		}
		actual.append(def.name);
	}
	ASSERT_TRUE(false) << "Namespace '" << nsName << "' is still present on server " << s->Id() << ". Actual: [" << actual << "]";
}
template <typename T>
CascadeReplicationApiP<T>::Cluster CascadeReplicationApiP<T>::CreateConfiguration(const std::vector<int>& clusterConfig, int baseServerId,
																				  const std::string& dbPathMaster) {
	std::vector<CascadeReplicationApiP::FollowerConfig> config;
	config.reserve(clusterConfig.size());
	for (auto& c : clusterConfig) {
		config.emplace_back(c);
	}
	return CreateConfiguration(std::move(config), baseServerId, dbPathMaster, {});
}

template <typename T>
CascadeReplicationApiP<T>::Cluster CascadeReplicationApiP<T>::CreateConfiguration(
	std::vector<CascadeReplicationApiP::FollowerConfig> clusterConfig, int baseServerId, const std::string& dbPathMaster,
	const AsyncReplicationConfigTest::NsSet& nsList, bool asServerProcess, size_t maxUpdatesSize) {
	const auto& ports = GetDefaults();
	if (clusterConfig.empty()) {
		return CascadeReplicationApiP::Cluster(baseServerId, ports);
	}
	std::vector<ServerControl> nodes;
	nodes.reserve(clusterConfig.size());
	using ReplNode = AsyncReplicationConfigTest::Node;
	for (size_t i = 0; i < clusterConfig.size(); ++i) {
		const int serverId = baseServerId + i;
		nodes.emplace_back().InitServer(ServerControlConfig(serverId, ports.defaultRpcPort + i, ports.defaultHttpPort + i,
															dbPathMaster + std::to_string(i), "db", true, maxUpdatesSize, asServerProcess));
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
	return CascadeReplicationApiP<T>::Cluster(baseServerId, ports, std::move(nodes));
}

template <typename T>
void CascadeReplicationApiP<T>::UpdateReplTokensByConfiguration(CascadeReplicationApiP::Cluster& cluster,
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

template <typename T>
void CascadeReplicationApiP<T>::UpdateReplicationConfigs(const ServerPtr& sc, const std::string& selfToken,
														 const std::string& admissibleLeaderToken) {
	if (!selfToken.empty()) {
		sc->UpdateConfigReplTokens(selfToken);
	}
	if (!admissibleLeaderToken.empty()) {
		sc->UpdateConfigReplTokens(NsNamesHashMapT<std::string>{{NamespaceName("*"), admissibleLeaderToken}});
	}
}

template <typename T>
void CascadeReplicationApiP<T>::ApplyConfig(const ServerPtr& sc, std::string_view json) {
	auto& rx = *sc->api.reindexer;
	auto item = rx.NewItem(kConfigNamespace);
	ASSERT_TRUE(item.Status().ok()) << item.Status().what();
	auto err = item.FromJSON(json);
	ASSERT_TRUE(err.ok()) << err.what();
	err = rx.Upsert(kConfigNamespace, item);
	ASSERT_TRUE(err.ok()) << err.what();
}

template <typename T>
void CascadeReplicationApiP<T>::CheckTxCopyEventsCount(const ServerPtr& sc, int expectedCount) {
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

template <typename T>
CascadeReplicationApiP<T>::TestNamespace1::TestNamespace1(const ServerPtr& srv, std::string_view nsName, EnableStorage enableStorage)
	: nsName_(nsName) {
	auto opt = StorageOpts().Enabled(enableStorage == EnableStorage::Yes);
	auto err = srv->api.reindexer->OpenNamespace(nsName_, opt);
	EXPECT_TRUE(err.ok()) << err.what();
	srv->api.DefineNamespaceDataset(nsName_, {IndexDeclaration{"id", "hash", "int", IndexOpts().PK(), 0}});
}

template <typename T>
void CascadeReplicationApiP<T>::TestNamespace1::AddRows(const ServerPtr& srv, int from, unsigned int count, size_t dataLen) {
	for (unsigned int i = 0; i < count; i++) {
		auto item = srv->api.NewItem(nsName_);
		auto err = item.FromJSON(dataLen ? fmt::format(R"json({{"id":{}, "data":"{}"}})json", from + i, randStringAlph(dataLen))
										 : fmt::format(R"json({{"id":{}}})json", from + i));
		ASSERT_TRUE(err.ok()) << err.what();
		srv->api.Upsert(nsName_, item);
		ASSERT_TRUE(err.ok()) << err.what();
	}
}

template <typename T>
void CascadeReplicationApiP<T>::TestNamespace1::AddRowsTx(const ServerPtr& srv, int from, unsigned int count, size_t dataLen) {
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

template <typename T>
void CascadeReplicationApiP<T>::TestNamespace1::GetData(const ServerPtr& srv, std::vector<int>& ids) {
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

template <typename T>
void CascadeReplicationApiP<T>::Cluster::RestartServer(size_t id, const std::string& dbPathMaster) {
	assert(id < nodes_.size());
	ShutdownServer(id);
	InitServer(id, dbPathMaster + std::to_string(id), "db", true);
}

template <typename T>
void CascadeReplicationApiP<T>::Cluster::ShutdownServer(size_t id) {
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

template <typename T>
void CascadeReplicationApiP<T>::Cluster::InitServer(size_t id, const std::string& storagePath, const std::string& dbName, bool enableStats,
													bool asServerProcess) {
	assert(id < nodes_.size());
	nodes_[id].InitServer(ServerControlConfig(baseServerId_ + id, ports_.defaultRpcPort + id, ports_.defaultHttpPort + id, storagePath,
											  dbName, enableStats, 0, asServerProcess));
}

template <typename T>
CascadeReplicationApiP<T>::Cluster::~Cluster() {
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

template class CascadeReplicationApiP<void*>;
template class CascadeReplicationApiP<sq8_test::TestSyncType>;

}  // namespace reindexer_tests
