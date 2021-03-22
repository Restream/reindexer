#pragma once

#include "client/cororeindexer.h"
#include "cluster/config.h"
#include "core/dbconfig.h"
#include "datareplicator.h"
#include "insdatareplicator.h"
#include "net/ev/ev.h"
#include "raftmanager.h"

namespace reindexer {

class ReindexerImpl;

namespace cluster {

class Clusterizator : public INsDataReplicator {
public:
	struct ControlCmd {};

	Clusterizator(ReindexerImpl &thisNode);

	Error Start();
	bool Configure(ClusterConfigData clusterConfig, ReplicationConfigData replConfig);
	void Stop() noexcept;
	void Enable() noexcept { enabled_.store(true, std::memory_order_release); }
	Error SuggestLeader(const cluster::NodeData &suggestion, cluster::NodeData &response);
	Error LeadersPing(const cluster::NodeData &);
	RaftInfo GetRaftInfo(bool allowTransitState, const RdxContext &ctx) const { return replicator_.GetRaftInfo(allowTransitState, ctx); }
	void RequestElectionsRestart() noexcept { restartElections_ = true; }

	Error Replicate(UpdateRecord &&rec, std::function<void()> beforeWaitF, const RdxContext &ctx) override final;
	Error Replicate(UpdatesContainer &&recs, std::function<void()> beforeWaitF, const RdxContext &ctx) override final;
	void AwaitSynchronization(string_view nsName, const RdxContext &ctx) const override final {
		if (enabled_.load(std::memory_order_acquire)) {
			replicator_.AwaitSynchronization(nsName, ctx);
		}
	}
	bool IsSynchronized(string_view name) const override final {
		return !enabled_.load(std::memory_order_acquire) || replicator_.IsSynchronized(name);
	}

private:
	void run();
	void clusterControlRoutine();
	std::string getManagementDsn(int id) const;
	void stop() noexcept;
	Error validateConfig() const noexcept;

	net::ev::dynamic_loop loop_;
	std::thread controlThread_;
	std::thread replicatorThread_;
	std::mutex mtx_;
	std::atomic<bool> terminate_ = {false};
	std::atomic<bool> enabled_ = {false};
	std::atomic<bool> restartElections_ = {false};

	ClusterConfigData clusterConfig_;
	ReplicationConfigData replConfig_;
	RaftManager raftManager_;
	DataReplicator replicator_;
};

}  // namespace cluster
}  // namespace reindexer
