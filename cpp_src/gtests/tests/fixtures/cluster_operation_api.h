#pragma once

#include "client/raftclient.h"
#include "cluster/config.h"
#include "reindexer_api.h"
#include "servercontrol.h"

using reindexer::lsn_t;
using reindexer::client::RaftClient;
using reindexer::cluster::AsyncReplicationMode;
using reindexer::DSN;

namespace reindexer::net::ev {
class dynamic_loop;
}
using reindexer::net::ev::dynamic_loop;

class [[nodiscard]] ClusterOperationApi : public ::testing::Test {
public:
	static constexpr auto kMaxServerStartTime = std::chrono::seconds(15);
	static constexpr auto kMaxSyncTime = std::chrono::seconds(25);
	static constexpr auto kMaxElectionsTime = std::chrono::seconds(12);

	static constexpr std::string_view kIdField = "id";
	static constexpr std::string_view kStringField = "string";
	static constexpr std::string_view kIntField = "int";
	static constexpr std::string_view kFTField = "ft_str";
	static constexpr std::string_view kVectorField = "vec";
	static constexpr unsigned kVectorDims = 8;

	void SetUp() override;
	void TearDown() override;

	struct [[nodiscard]] Defaults {
		size_t defaultRpcPort;
		size_t defaultHttpPort;
		std::string baseTestsetDbPath;
	};

	virtual const Defaults& GetDefaults() const;
	struct [[nodiscard]] NamespaceData {
		std::string name;
		lsn_t expectedLsn;
		lsn_t expectedNsVersion;
	};

	class [[nodiscard]] Cluster {
	public:
		struct [[nodiscard]] DataParam {
			bool emptyVector = false;
		};

		Cluster(dynamic_loop& loop, size_t initialServerId, size_t count, Defaults ports, size_t maxUpdatesSize,
				const YAML::Node& clusterConf);
		Cluster(dynamic_loop& loop, size_t initialServerId, size_t count, Defaults defaults,
				const std::vector<std::string>& nsList = std::vector<std::string>(),
				std::chrono::milliseconds resyncTimeout = std::chrono::milliseconds(3000), int maxSyncCount = -1, int syncThreadsCount = 2,
				size_t maxUpdatesSize = 0);
		~Cluster();

		void InitNs(size_t id, std::string_view nsName);
		void DropNs(size_t id, std::string_view nsName);
		void FillData(size_t id, std::string_view nsName, size_t from, size_t count);
		void FillDataTx(size_t id, std::string_view nsName, size_t from, size_t count);
		size_t InitServer(size_t id, const YAML::Node& clusterYml, const YAML::Node& replYml, size_t offset);
		void AddRow(size_t id, std::string_view nsName, int pk);
		Error AddRowWithErr(size_t id, std::string_view nsName, int pk, DataParam param, std::string* resultJson = nullptr);
		bool StartServer(size_t id);
		bool StopServer(size_t id);
		void StopServers(size_t from, size_t to);
		void StopServers(const std::vector<size_t>& ids);
		int AwaitLeader(std::chrono::seconds timeout, bool fulltime = false);
		void WaitSync(std::string_view ns, lsn_t expectedLsn = lsn_t(), lsn_t expectedNsVersion = lsn_t(),
					  std::chrono::seconds maxSyncTime = std::chrono::seconds());
		static void PrintClusterInfo(std::string_view ns, std::vector<ServerControl>& svc);
		void PrintClusterNsList(const std::vector<NamespaceData>& expected);
		RaftClient& GetClient(size_t id) {
			assert(id < clients_.size());
			return clients_[id];
		}
		void StopClients();
		ServerControl::Interface::Ptr GetNode(size_t id);
		void FillItem(BaseApi& api, BaseApi::ItemType& item, size_t id, DataParam param = DataParam{.emptyVector = false});
		void ValidateNamespaceList(const std::vector<NamespaceData>& namespaces);
		static void doWaitSync(std::string_view ns, std::vector<ServerControl>& svc, lsn_t expectedLsn = lsn_t(),
							   lsn_t expectedNsVersion = lsn_t(), std::chrono::seconds maxSyncTime = std::chrono::seconds());
		static size_t getSyncCnt(std::string_view ns, std::vector<ServerControl>& svc, lsn_t expectedLsn = lsn_t(),
								 lsn_t expectedNsVersion = lsn_t());
		size_t GetSynchronizedNodesCount(size_t nodeId);
		void EnablePerfStats(size_t nodeId);
		void ChangeLeader(int& curLeaderId, int newLeaderId);
		void AddAsyncNode(size_t nodeId, const ServerControl::Interface::Ptr& node, AsyncReplicationMode replMode,
						  std::optional<std::vector<std::string> >&& nsList = {});
		void AwaitLeaderBecomeAvailable(size_t nodeId, std::chrono::milliseconds awaitTime = std::chrono::milliseconds(5000));

		/// @brief Creates cluster config YAML, uses previously defined Defaults.defaultRpcPort on Cluster class creation.
		YAML::Node CreateClusterConfig(size_t initialServerId, size_t count,
									   const std::vector<std::string>& nsList = std::vector<std::string>(),
									   std::chrono::milliseconds resyncTimeout = std::chrono::milliseconds(3000), int maxSyncCount = -1,
									   int syncThreadsCount = 2);

		static YAML::Node CreateClusterConfigStatic(size_t initialServerId, size_t count, const Defaults& ports,
													const std::vector<std::string>& nsList = std::vector<std::string>(),
													std::chrono::milliseconds resyncTimeout = std::chrono::milliseconds(3000),
													int maxSyncCount = -1, int syncThreadsCount = 2);

		static YAML::Node CreateClusterConfigStatic(const std::vector<size_t>& nodeIds, const Defaults& ports,
													const std::vector<std::string>& nsList = std::vector<std::string>(),
													std::chrono::milliseconds resyncTimeout = std::chrono::milliseconds(3000),
													int maxSyncCount = -1, int syncThreadsCount = 2);

	protected:
		void initCluster(size_t count, size_t initialServerId, const YAML::Node& clusterConf);

	private:
		std::vector<ServerControl> svc_;
		std::vector<RaftClient> clients_;
		dynamic_loop& loop_;
		Defaults defaults_;
		size_t maxUpdatesSize_ = 0;
	};
};
