#pragma once

#include <mutex>
#include "client/cororeindexer.h"
#include "client/raftclient.h"
#include "debug/backtrace.h"
#include "estl/fast_hash_set.h"
#include "reindexer_api.h"
#include "reindexertestapi.h"
#include "server/dbmanager.h"
#include "server/server.h"
#include "thread"
#include "tools/fsops.h"
#include "tools/logger.h"
#include "tools/serializer.h"

using namespace reindexer_server;
using std::shared_ptr;

class ClusterizationApi : public ::testing::Test {
public:
	static const std::string kConfigNs;
	static const std::chrono::seconds kMaxServerStartTime;
	static const std::chrono::seconds kMaxSyncTime;
	static const std::chrono::seconds kMaxElectionsTime;

	static const std::string kIdField;
	static const std::string kStringField;
	static const std::string kIntField;
	static const std::string kFTField;

	void SetUp();
	void TearDown();

	struct Defaults {
		size_t defaultRpcPort;
		size_t defaultHttpPort;
		std::string baseTestsetDbPath;
	};

	virtual const Defaults& GetDefaults() const {
		static Defaults defs{14000, 16000, fs::JoinPath(fs::GetTempDir(), "rx_test/ClusterizationApi")};
		return defs;
	}
	struct NamespaceData {
		std::string name;
		lsn_t expectedLsn;
		lsn_t expectedNsVersion;
	};

	class Cluster {
	public:
		Cluster(net::ev::dynamic_loop& loop, size_t initialServerId, size_t count, Defaults defaults,
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
		Error AddRowWithErr(size_t id, std::string_view nsName, int pk, std::string* resultJson = nullptr);
		bool StartServer(size_t id);
		bool StopServer(size_t id);
		void StopServers(size_t from, size_t to);
		void StopServers(const std::vector<size_t>& ids);
		int AwaitLeader(std::chrono::seconds timeout, bool fulltime = false);
		void WaitSync(std::string_view ns, lsn_t expectedLsn = lsn_t(), lsn_t expectedNsVersion = lsn_t(),
					  std::chrono::seconds maxSyncTime = std::chrono::seconds());
		static void PrintClusterInfo(std::string_view ns, std::vector<ServerControl>& svc);
		void PrintClusterNsList(const std::vector<NamespaceData>& expected);
		client::RaftClient& GetClient(size_t id) {
			assert(id < clients_.size());
			return clients_[id];
		}
		void StopClients();
		ServerControl::Interface::Ptr GetNode(size_t id);
		void FillItem(BaseApi& api, BaseApi::ItemType& item, size_t id);
		void ValidateNamespaceList(const std::vector<NamespaceData>& namespaces);
		static void doWaitSync(std::string_view ns, std::vector<ServerControl>& svc, lsn_t expectedLsn = lsn_t(),
							   lsn_t expectedNsVersion = lsn_t(), std::chrono::seconds maxSyncTime = std::chrono::seconds());
		size_t GetSynchronizedNodesCount(size_t nodeId);
		void EnablePerfStats(size_t nodeId);
		void ChangeLeader(int& curLeaderId, int newLeaderId);
		void AddAsyncNode(size_t nodeId, const std::string& dsn, cluster::AsyncReplicationMode replMode,
						  std::optional<std::vector<std::string> >&& nsList = {});
		void AwaitLeaderBecomeAvailable(size_t nodeId, std::chrono::milliseconds awaitTime = std::chrono::milliseconds(5000));

	private:
		std::vector<ServerControl> svc_;
		std::vector<client::RaftClient> clients_;
		net::ev::dynamic_loop& loop_;
		Defaults defaults_;
		size_t maxUpdatesSize_ = 0;
	};

	std::function<void()> ExceptionWrapper(std::function<void()>&& func) {
		return [f = std::move(func)] {	// NOLINT(*.NewDeleteLeaks) False positive
			try {
				f();
			} catch (Error& e) {
				ASSERT_TRUE(false) << e.what();
			} catch (std::exception& e) {
				ASSERT_TRUE(false) << e.what();
			} catch (...) {
				ASSERT_TRUE(false) << "Unknown exception";
			}
		};
	}
};
