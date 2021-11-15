#include <gtest/gtest.h>
#include "core/namespace/namespacestat.h"
#include "servercontrol.h"
#include "tools/fsops.h"

class CascadeReplicationApi : public ::testing::Test {
protected:
	void SetUp() override;
	void TearDown() override;

public:
	using ServerPtr = ServerControl::Interface::Ptr;

	class TestNamespace1 {
	public:
		TestNamespace1(ServerPtr srv, const std::string nsName = std::string("ns1"));

		void AddRows(ServerPtr srv, int from, unsigned int count, size_t dataLen = 0);
		void GetData(ServerPtr node, std::vector<int>& ids);

		const std::string nsName_;
	};

	class Cluster {
	public:
		Cluster(std::vector<ServerControl> nodes = std::vector<ServerControl>()) : nodes_(std::move(nodes)) {}
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
	};

	void WaitSync(ServerPtr s1, ServerPtr s2, const std::string& nsName);
	void ValidateNsList(ServerPtr s, const std::vector<std::string>& expected);

	class FollowerConfig {
	public:
		FollowerConfig(int lid, std::optional<std::vector<std::string>> nss = std::optional<std::vector<std::string>>())
			: leaderId(lid), nsList(std::move(nss)) {}

		int leaderId;
		std::optional<std::vector<std::string>> nsList;
	};

	Cluster CreateConfiguration(const std::vector<int>& clusterConfig, int basePort, int baseServerId, const std::string& dbPathMaster);
	Cluster CreateConfiguration(std::vector<FollowerConfig> clusterConfig, int basePort, int baseServerId, const std::string& dbPathMaster,
								AsyncReplicationConfigTest::NsSet&& nsList);

protected:
	const std::string kBaseTestsetDbPath = reindexer::fs::JoinPath(reindexer::fs::GetTempDir(), "rx_test/CascadeReplicationApi");
	const std::chrono::seconds kMaxSyncTime = std::chrono::seconds(15);
};
