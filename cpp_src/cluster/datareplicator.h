#pragma once

#include <deque>
#include "client/cororeindexer.h"
#include "cluster/config.h"
#include "coroutine/channel.h"
#include "coroutine/waitgroup.h"
#include "insdatareplicator.h"
#include "net/ev/ev.h"
#include "shardedqueue.h"
#include "sharedsyncstate.h"
#include "updaterecord.h"

#include <unordered_map>

namespace reindexer {

class ReindexerImpl;

namespace coroutine {
class wait_group;
}

namespace cluster {

constexpr size_t k8kCoroStack = 8 * 1024;

class DataReplicator {
public:
	DataReplicator(ReindexerImpl &, std::function<void()> requestElectionsRestartCb);

	void AwaitSynchronization(string_view nsName, const RdxContext &ctx) const { sharedSyncState_.AwaitSynchronization(nsName, ctx); }
	bool IsSynchronized(string_view name) const { return sharedSyncState_.IsSynchronized(name); }
	Error Replicate(UpdateRecord &&rec, std::function<void()> beforeWaitF, const RdxContext &ctx);
	Error Replicate(UpdatesContainer &&recs, std::function<void()> beforeWaitF, const RdxContext &ctx);
	void Run(int serverId, ClusterConfigData config);
	void SetTerminateFlag(bool val) noexcept {
		terminate_ = val;
		if (val) {
			terminateAsync_.send();
		}
	}
	void OnRoleChanged(RaftInfo::Role newRole, int leaderId);
	RaftInfo GetRaftInfo(bool allowTransitState, const RdxContext &ctx) const;

private:
	using UpdatesQueueT = ShardedQueue<UpdateRecord>;
	using UpdatesQueueShardT = UpdatesQueueT::Shard;
	using UpdatesIteratorT = UpdatesQueueShardT::ListT::iterator;
	using NsNamesHashSetT = fast_hash_set<string, nocase_hash_str, nocase_equal_str>;

	struct Update {
	public:
		Update(UpdatesIteratorT it) : data(std::move(it->data)), queueIt_(std::move(it)) {}
		void OnResult(Error &&err) noexcept {
			assert(!erased_);
			erased_ = true;
			queueIt_->OnResult(std::move(err));
		}
		bool RequireResult() const noexcept { return !erased_ && queueIt_->RequireResult(); }

		UpdateRecord data;
		size_t replicas = 0;
		size_t aproves = 1;	 // 1 aprove from local node
		size_t errors = 0;

	private:
		bool erased_ = false;
		UpdatesIteratorT queueIt_;
	};

	using UpdatesChT = coroutine::channel<std::list<Update>::iterator>;
	struct UpdatesListener {
		UpdatesChT *ch;
	};

	class UpdatesListeners {
	public:
		void Notify(std::list<Update>::iterator it) {
			for (auto &l : listeners) {
				if (l.ch->empty()) {
					l.ch->push(it);
				}
			}
		}
		void Add(UpdatesChT *ch) { listeners.emplace_back(UpdatesListener{ch}); }
		bool Empty() const noexcept { return listeners.empty(); }

	private:
		std::vector<UpdatesListener> listeners;
	};

	struct NamedChannel {
		NamedChannel(string_view _name) : name(_name) {}

		string_view name;
		UpdatesChT ch;
	};

	struct UpdatesData {
		UpdatesData() = default;
		UpdatesData(UpdatesData &&d) noexcept
			: listeners(std::move(d.listeners)), updates(std::move(d.updates)) {}  // Expliciti declaration for win build
		UpdatesData(const UpdatesData &) = delete;
		UpdatesData &operator=(UpdatesData &&) noexcept = default;
		UpdatesData &operator=(const UpdatesData &) = delete;

		UpdatesListeners listeners;
		std::list<Update> updates;	// TODO: maybe we need more efficient container?
	};

	struct NamespaceData {
		lsn_t latestLsn;
		client::CoroTransaction tx;
	};

	struct ReplicatorNode {
		enum class Type { Async, Cluster };

		ReplicatorNode(Type _type, int _serverId, const client::CoroReindexerConfig &config)
			: type(_type), serverId(_serverId), client(config) {}
		Type type;
		std::string dsn;
		int serverId;
		client::CoroReindexer client;
		std::deque<NamedChannel> updateNotifiers;
		std::unordered_map<string_view, NamespaceData, nocase_hash_str, nocase_equal_str>
			namespaceData;	// This map should not invalidate references
		coroutine::wait_group wg;
	};
	struct SyncThread {
		void ConnectClusterNodes() {
			for (auto &node : nodes) {
				if (node.type == ReplicatorNode::Type::Cluster) {
					node.client.Connect(node.dsn, loop);
				}
			}
		}
		void DisconnectClusterNodes() {
			coroutine::wait_group swg;
			for (auto &node : nodes) {
				if (node.type == ReplicatorNode::Type::Cluster) {
					swg.add(1);
					loop.spawn(
						[&node, &swg] {
							coroutine::wait_group_guard wgg(swg);
							node.client.Stop();
						},
						k8kCoroStack);
				}
			}
			swg.wait();
		}

		std::thread th;
		std::deque<ReplicatorNode> nodes;  // TODO: Async node has to be in a separated container with own connections
		net::ev::dynamic_loop loop;
		std::unordered_map<std::string, UpdatesData, nocase_hash_str, nocase_equal_str>
			nsUpdatesData;	// This map should not invalidate references
		coroutine::channel<bool> leadershipAwaitCh;
		coroutine::channel<RaftInfo::Role> nextRoleCh;
		net::ev::async roleSwitchAsync;
		net::ev::async updatesAsync;
		coroutine::wait_group wg;
	};

	void dataReplicationThread(size_t shardId, const ClusterConfigData &config);
	void nodeReplicationRoutine(size_t nodeId, SyncThread &threadLocal);
	void nsReplicationRoutine(size_t nodeId, string_view nsName, DataReplicator::SyncThread &threadLocal, bool newNs);
	void updatesReadingRoutine(size_t shardId);
	void workerRoleMonitoringRoutine(size_t shardId);
	void initialLeadersSync(size_t shardId);
	Error syncLatestNamespaceToLeader(string_view nsName, DataReplicator::SyncThread &threadLocal);
	NsNamesHashSetT collectNsNames(SyncThread &threadLocal);
	bool startUpdateRoutineForNewNs(const std::string &nsName, const std::shared_ptr<UpdatesQueueShardT> &shard,
									DataReplicator::SyncThread &threadLocal);

	Error syncNamespace(size_t nodeId, string_view nsName, const ReplicationStateV2 &followerState, SyncThread &threadLocal);
	Error nsUpdatesHandlingLoop(size_t nodeId, string_view nsName, SyncThread &threadLocal);
	void handleUpdatesWithError(size_t nodeId, string_view nsName, SyncThread &threadLocal, const Error &err);
	void onTerminateAsync(net::ev::async &watcher) noexcept;
	void onRoleSwitchAsync(net::ev::async &);
	bool isLeader(const DataReplicator::SyncThread &threadLocal) const noexcept { return !threadLocal.leadershipAwaitCh.opened(); }
	void dropSyncUpdates(SyncThread &threadLocal);

	Error applyUpdate(const UpdateRecord &rec, ReplicatorNode &node, NamespaceData &nsData) noexcept;
	static bool isNetworkError(const Error &err) noexcept;
	static bool isLeaderChangedError(const Error &err) noexcept;

	UpdatesQueueT updatesQueue_;
	ReindexerImpl &thisNode_;
	std::atomic<bool> terminate_ = {false};
	std::deque<SyncThread> syncThreads_;
	net::ev::dynamic_loop loop_;
	net::ev::async terminateAsync_;
	net::ev::async roleSwitchAsync_;
	size_t consensusCnt_ = 0;
	int serverId_ = -1;
	SharedSyncState<> sharedSyncState_;
	ClusterConfigData config_;
	RdxContext dummyCtx_;
	std::function<void()> requestElectionsRestartCb_;
};

}  // namespace cluster
}  // namespace reindexer
