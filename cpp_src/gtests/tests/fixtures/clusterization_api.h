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
	static const std::string kClusterConfigFilename;
	static const std::string kReplicationConfigFilename;
	static const std::string kConfigNs;
	static const size_t kDefaultRpcPort;
	static const size_t kDefaultRpcClusterPort;
	static const size_t kDefaultHttpPort;
	static const size_t kDefaultClusterPort;
	static const std::chrono::seconds kMaxServerStartTime;
	static const std::chrono::seconds kMaxSyncTime;
	static const std::chrono::seconds kMaxElectionsTime;
	static const std::string kBaseTestsetDbPath;

	static const std::string kIdField;
	static const std::string kStringField;
	static const std::string kIntField;

	void SetUp();
	void TearDown();

	class Cluster {
	public:
		Cluster(net::ev::dynamic_loop& loop, size_t initialServerId, size_t count,
				std::chrono::milliseconds resyncTimeout = std::chrono::milliseconds(3000));
		~Cluster();

		void InitNs(size_t id, string_view nsName);
		void FillData(size_t id, string_view nsName, size_t from, size_t count);
		void FillDataTx(size_t id, string_view nsName, size_t from, size_t count);
		void AddRow(size_t id, string_view nsName, int pk);
		size_t InitServer(size_t id, const std::string& clusterYml, const std::string& replYml);
		bool StartServer(size_t id);
		bool StopServer(size_t id);
		int AwaitLeader(std::chrono::seconds timeout, bool fulltime = false);
		void WaitSync(string_view ns);
		void PrintClusterInfo(string_view ns);
		client::RaftClient& GetClient(size_t id) {
			assert(id < clients_.size());
			return clients_[id];
		}
		void StopClients();
		ServerControl::Interface::Ptr GetServerControl(size_t id) {
			assert(id < svc_.size());
			return svc_[id].Get();
		}

		ServerControl::Interface::Ptr GetNode(size_t id);
		void FillItem(BaseApi& api, BaseApi::ItemType& item, size_t id);

	private:
		std::vector<ServerControl> svc_;
		std::vector<client::RaftClient> clients_;
		net::ev::dynamic_loop& loop_;
	};
};
