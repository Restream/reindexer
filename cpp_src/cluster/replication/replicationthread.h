#pragma once

#include <numeric>
#include "client/cororeindexer.h"
#include "cluster/config.h"
#include "cluster/logger.h"
#include "cluster/stats/relicationstatscollector.h"
#include "core/dbconfig.h"
#include "coroutine/tokens_pool.h"
#include "net/ev/ev.h"
#include "sharedsyncstate.h"
#include "updates/updaterecord.h"
#include "updates/updatesqueue.h"

namespace reindexer {

class ReindexerImpl;

namespace cluster {

constexpr size_t kUpdatesContainerOverhead = 48;

struct ReplThreadConfig {
	ReplThreadConfig() = default;
	ReplThreadConfig(const ReplicationConfigData& baseConfig, const AsyncReplConfigData& config) {
		AppName = config.appName;
		EnableCompression = config.enableCompression;
		UpdatesTimeoutSec = config.onlineUpdatesTimeoutSec;
		RetrySyncIntervalMSec = config.retrySyncIntervalMSec;
		ParallelSyncsPerThreadCount = config.parallelSyncsPerThreadCount;
		BatchingRoutinesCount = config.batchingRoutinesCount > 0 ? size_t(config.batchingRoutinesCount) : 100;
		MaxWALDepthOnForceSync = config.maxWALDepthOnForceSync;
		SyncTimeoutSec = std::max(config.syncTimeoutSec, config.onlineUpdatesTimeoutSec);
		ClusterID = baseConfig.clusterID;
		if (config.onlineUpdatesDelayMSec > 0) {
			OnlineUpdatesDelaySec = double(config.onlineUpdatesDelayMSec) / 1000.;
		} else if (config.onlineUpdatesDelayMSec == 0) {
			OnlineUpdatesDelaySec = 0;
		} else {
			OnlineUpdatesDelaySec = 0.1;
		}
	}
	ReplThreadConfig(const ReplicationConfigData& baseConfig, const ClusterConfigData& config) {
		AppName = config.appName;
		EnableCompression = config.enableCompression;
		UpdatesTimeoutSec = config.onlineUpdatesTimeoutSec;
		RetrySyncIntervalMSec = config.retrySyncIntervalMSec;
		ParallelSyncsPerThreadCount = config.parallelSyncsPerThreadCount;
		ClusterID = baseConfig.clusterID;
		MaxWALDepthOnForceSync = config.maxWALDepthOnForceSync;
		SyncTimeoutSec = std::max(config.syncTimeoutSec, config.onlineUpdatesTimeoutSec);
		BatchingRoutinesCount = config.batchingRoutinesCount > 0 ? size_t(config.batchingRoutinesCount) : 100;
		OnlineUpdatesDelaySec = 0;
	}

	std::string AppName = "rx_node";
	int UpdatesTimeoutSec = 20;
	int SyncTimeoutSec = 60;
	int RetrySyncIntervalMSec = 3000;
	int ParallelSyncsPerThreadCount = 2;
	int ClusterID = 1;
	size_t BatchingRoutinesCount = 100;
	int64_t MaxWALDepthOnForceSync = 1000;
	bool EnableCompression = true;
	double OnlineUpdatesDelaySec = 0;
};

struct UpdateApplyStatus {
	UpdateApplyStatus(Error&& _err = Error(), updates::URType _type = updates::URType::None) noexcept : err(std::move(_err)), type(_type) {}
	template <typename BehaviourParamT>
	bool IsHaveToResync() const noexcept;

	Error err;
	updates::URType type;
};

template <typename BehaviourParamT>
class ReplThread {
public:
	using UpdatesQueueT = updates::UpdatesQueue<updates::UpdateRecord, ReplicationStatsCollector, Logger>;
	using UpdatesChT = coroutine::channel<bool>;

	class NamespaceData {
	public:
		void UpdateLsnOnRecord(const updates::UpdateRecord& rec) {
			if (!rec.IsDbRecord()) {
				// Updates with *Namespace types have fake lsn. Those updates should not be count in latestLsn
				latestLsn = rec.ExtLSN();
			} else if (rec.Type() == updates::URType::AddNamespace) {
				if (latestLsn.NsVersion().isEmpty() || latestLsn.NsVersion().Counter() < rec.ExtLSN().NsVersion().Counter()) {
					latestLsn = ExtendedLsn(rec.ExtLSN().NsVersion(), lsn_t());
				}
			} else if (rec.Type() == updates::URType::DropNamespace) {
				latestLsn = ExtendedLsn();
			}
		}

		ExtendedLsn latestLsn;
		client::CoroTransaction tx;
		bool requiresTmUpdate = true;
		bool isClosed = false;
	};

	struct Node {
		Node(int _serverId, uint32_t _uid, const client::ReindexerConfig& config) noexcept
			: serverId(_serverId), uid(_uid), client(config) {}
		void Reconnect(net::ev::dynamic_loop& loop, const ReplThreadConfig& config) {
			if (connObserverId.has_value()) {
				auto err = client.RemoveConnectionStateObserver(*connObserverId);
				(void)err;	// ignored
				connObserverId.reset();
			}
			client.Stop();
			client::ConnectOpts opts;
			opts.CreateDBIfMissing().WithExpectedClusterID(config.ClusterID);
			auto err = client.Connect(dsn, loop, opts);
			(void)err;	// ignored; Error will be checked during the further requests
		}

		int serverId;
		uint32_t uid;
		DSN dsn;
		client::CoroReindexer client;
		std::unique_ptr<UpdatesChT> updateNotifier = std::make_unique<UpdatesChT>();
		std::unordered_map<NamespaceName, NamespaceData, NamespaceNameHash, NamespaceNameEqual>
			namespaceData;	// This map should not invalidate references
		uint64_t nextUpdateId = 0;
		bool requireResync = false;
		std::optional<int64_t> connObserverId;
	};

	ReplThread(int serverId_, ReindexerImpl& thisNode, std::shared_ptr<UpdatesQueueT>, BehaviourParamT&&, ReplicationStatsCollector,
			   const Logger&);

	template <typename NodeConfigT>
	void Run(ReplThreadConfig, const std::vector<std::pair<uint32_t, NodeConfigT>>& nodesList, size_t consensusCnt,
			 size_t requiredReplicas);
	void SetTerminate(bool val) noexcept;
	bool Terminated() const noexcept { return terminate_; }
	void DisconnectNodes() {
		coroutine::wait_group swg;
		for (auto& node : nodes) {
			loop.spawn(
				swg,
				[&node]() noexcept {
					if (node.connObserverId.has_value()) {
						auto err = node.client.RemoveConnectionStateObserver(*node.connObserverId);
						(void)err;	// ignore
						node.connObserverId.reset();
					}
					node.client.Stop();
				},
				k16kCoroStack);
		}
		swg.wait();
	}
	void SetNodesRequireResync() {
		for (auto& node : nodes) {
			node.requireResync = true;
		}
	}
	void SendUpdatesAsyncNotification() { updatesAsync_.send(); }

	std::deque<Node> nodes;
	net::ev::dynamic_loop loop;
	coroutine::wait_group wg;
	ReindexerImpl& thisNode;

private:
	constexpr static bool isClusterReplThread() noexcept;
	void updateNodeStatus(size_t uid, NodeStats::Status st);
	void nodeReplicationRoutine(Node& node);
	Error nodeReplicationImpl(Node& node);
	void updatesNotifier() noexcept;
	void terminateNotifier() noexcept;
	std::tuple<bool, UpdateApplyStatus> handleNetworkCheckRecord(Node& node, UpdatesQueueT::UpdatePtr& updPtr, uint16_t offset,
																 bool currentlyOnline, const updates::UpdateRecord& rec) noexcept;

	Error syncNamespace(Node&, const NamespaceName&, const ReplicationStateV2& followerState);
	[[nodiscard]] Error syncShardingConfig(Node& node) noexcept;
	UpdateApplyStatus nodeUpdatesHandlingLoop(Node& node) noexcept;
	bool handleUpdatesWithError(Node& node, const Error& err);
	Error checkIfReplicationAllowed(Node& node, LogLevel& logLevel);

	UpdateApplyStatus applyUpdate(const updates::UpdateRecord& rec, Node& node, NamespaceData& nsData) noexcept;
	static bool isNetworkError(const Error& err) noexcept { return err.code() == errNetwork; }
	static bool isTimeoutError(const Error& err) noexcept { return err.code() == errTimeout || err.code() == errCanceled; }
	static bool isLeaderChangedError(const Error& err) noexcept { return err.code() == errWrongReplicationData; }
	static bool isTxCopyError(const Error& err) noexcept { return err.code() == errTxDoesNotExist; }
	constexpr static std::string_view logModuleName() noexcept {
		using namespace std::string_view_literals;
		if constexpr (isClusterReplThread()) {
			return "replicator:sync_t"sv;
		} else {
			return "replicator:async_t"sv;
		}
	}

	const int serverId_ = -1;
	uint32_t consensusCnt_ = 0;
	uint32_t requiredReplicas_ = 0;
	std::unique_ptr<coroutine::tokens_pool<bool>> nsSyncTokens_;
	net::ev::async updatesAsync_;
	net::ev::timer updatesTimer_;
	bool notificationInProgress_ = false;
	bool hasPendingNotificaions_ = false;
	std::atomic<bool> terminate_ = {false};
	BehaviourParamT bhvParam_;
	ReplThreadConfig config_;
	std::shared_ptr<UpdatesQueueT> updates_;
	coroutine::channel<bool> terminateCh_;
	ReplicationStatsCollector statsCollector_;
	const Logger& log_;
};

}  // namespace cluster
}  // namespace reindexer
