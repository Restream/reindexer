#include <gtest/gtest.h>
#include <chrono>
#include "quantization_helpers.h"
#include "servercontrol.h"
#include "tools/fsops.h"

namespace reindexer_tests {

template <typename T>
class [[nodiscard]] CascadeReplicationApiP : public ::testing::TestWithParam<T> {
protected:
	void SetUp() override;
	void TearDown() override;

public:
	using ServerPtr = ServerControl::Interface::Ptr;

	struct [[nodiscard]] Defaults {
		size_t defaultRpcPort;
		size_t defaultHttpPort;
		std::string baseTestsetDbPath;
	};

	virtual const Defaults& GetDefaults() const;

	class [[nodiscard]] TestNamespace1 {
	public:
		enum class [[nodiscard]] EnableStorage : bool { No = false, Yes = true };

		TestNamespace1(const ServerPtr& srv, std::string_view nsName = "ns1", EnableStorage enableStorage = EnableStorage::Yes);

		void AddRows(const ServerPtr& srv, int from, unsigned int count, size_t dataLen = 0);
		void AddRowsTx(const ServerPtr& srv, int from, unsigned int count, size_t dataLen = 8);

		void GetData(const ServerPtr& node, std::vector<int>& ids);

		const std::string nsName_;
	};

	class [[nodiscard]] Cluster {
	public:
		Cluster(int baseServerId, Defaults ports, std::vector<ServerControl> nodes = std::vector<ServerControl>())
			: nodes_(std::move(nodes)), baseServerId_(baseServerId), ports_(std::move(ports)) {}
		void RestartServer(size_t id, const std::string& dbPathMaster);
		void ShutdownServer(size_t id);
		void InitServer(size_t id, const std::string& storagePath, const std::string& dbName, bool enableStats,
						bool asServerProcess = kTestServersInSeparateProcesses);
		ServerPtr Get(size_t id, bool wait = true) {
			assert(id < nodes_.size());
			return nodes_[id].Get(wait);
		}
		size_t Size() const noexcept { return nodes_.size(); }
		~Cluster();

	private:
		std::vector<ServerControl> nodes_;
		const int baseServerId_ = 0;
		const Defaults ports_;
	};

	void WaitSync(const ServerPtr& s1, const ServerPtr& s2, const std::string& nsName) { ServerControl::WaitSync(s1, s2, nsName); }
	void ValidateNsList(const ServerPtr& s, const std::vector<std::string>& expected);
	void AwaitNsAbsence(const ServerPtr& s, std::string_view nsName, std::chrono::milliseconds timeout = std::chrono::milliseconds(15000));

	class [[nodiscard]] FollowerConfig {
	public:
		FollowerConfig(int lid, std::optional<std::vector<std::string>> nss = std::optional<std::vector<std::string>>())
			: leaderId(lid), nsList(std::move(nss)) {}

		int leaderId;
		std::optional<std::vector<std::string>> nsList;
	};

	Cluster CreateConfiguration(const std::vector<int>& clusterConfig, int baseServerId, const std::string& dbPathMaster);
	Cluster CreateConfiguration(std::vector<FollowerConfig> clusterConfig, int baseServerId, const std::string& dbPathMaster,
								const AsyncReplicationConfigTest::NsSet& nsList, bool asServerProcess = kTestServersInSeparateProcesses,
								size_t maxUpdatesSize = 0);

	void UpdateReplTokensByConfiguration(CascadeReplicationApiP::Cluster& cluster, const std::vector<int>& clusterConfig);
	static void UpdateReplicationConfigs(const ServerPtr& sc, const std::string& selfToken, const std::string& admissibleLeaderToken);

	void ApplyConfig(const ServerPtr& sc, std::string_view json);
	void CheckTxCopyEventsCount(const ServerPtr& sc, int expectedCount);
};

using CascadeReplicationApi = CascadeReplicationApiP<void*>;
using Sq8CascadeReplicationApi = CascadeReplicationApiP<sq8_test::TestSyncType>;

}  // namespace reindexer_tests
