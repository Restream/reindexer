#include <gtest/gtest.h>
#include "servercontrol.h"
#include "tools/fsops.h"

class [[nodiscard]] CascadeReplicationApi : public ::testing::Test {
protected:
	void SetUp() override;
	void TearDown() override;

public:
	using ServerPtr = ServerControl::Interface::Ptr;

	class [[nodiscard]] TestNamespace1 {
	public:
		TestNamespace1(const ServerPtr& srv, std::string_view nsName = "ns1");

		void AddRows(const ServerPtr& srv, int from, unsigned int count, size_t dataLen = 0);
		void AddRowsTx(const ServerPtr& srv, int from, unsigned int count, size_t dataLen = 8);

		void GetData(const ServerPtr& node, std::vector<int>& ids);

		const std::string nsName_;
	};

	class [[nodiscard]] Cluster {
	public:
		Cluster(int baseServerId, std::vector<ServerControl> nodes = std::vector<ServerControl>())
			: nodes_(std::move(nodes)), baseServerId_(baseServerId) {}
		void RestartServer(size_t id, int port, const std::string& dbPathMaster);
		void ShutdownServer(size_t id);
		void InitServer(size_t id, unsigned short rpcPort, unsigned short httpPort, const std::string& storagePath,
						const std::string& dbName, bool enableStats);
		ServerPtr Get(size_t id, bool wait = true) {
			assert(id < nodes_.size());
			return nodes_[id].Get(wait);
		}
		size_t Size() const noexcept { return nodes_.size(); }
		~Cluster();

	private:
		std::vector<ServerControl> nodes_;
		const int baseServerId_ = 0;
	};

	void WaitSync(const ServerPtr& s1, const ServerPtr& s2, const std::string& nsName) { ServerControl::WaitSync(s1, s2, nsName); }
	void ValidateNsList(const ServerPtr& s, const std::vector<std::string>& expected);

	class [[nodiscard]] FollowerConfig {
	public:
		FollowerConfig(int lid, std::optional<std::vector<std::string>> nss = std::optional<std::vector<std::string>>())
			: leaderId(lid), nsList(std::move(nss)) {}

		int leaderId;
		std::optional<std::vector<std::string>> nsList;
	};

	Cluster CreateConfiguration(const std::vector<int>& clusterConfig, int basePort, int baseServerId, const std::string& dbPathMaster);
	Cluster CreateConfiguration(std::vector<FollowerConfig> clusterConfig, int basePort, int baseServerId, const std::string& dbPathMaster,
								const AsyncReplicationConfigTest::NsSet& nsList);

	void UpdateReplTokensByConfiguration(CascadeReplicationApi::Cluster& cluster, const std::vector<int>& clusterConfig);
	static void UpdateReplicationConfigs(const ServerPtr& sc, const std::string& selfToken, const std::string& admissibleLeaderToken);

	void ApplyConfig(const ServerPtr& sc, std::string_view json);
	void CheckTxCopyEventsCount(const ServerPtr& sc, int expectedCount);

protected:
	const std::string kBaseTestsetDbPath = reindexer::fs::JoinPath(reindexer::fs::GetTempDir(), "rx_test/CascadeReplicationApi");
};
