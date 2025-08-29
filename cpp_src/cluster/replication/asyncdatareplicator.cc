#include "asyncdatareplicator.h"
#include "cluster/clusterizator.h"
#include "core/reindexer_impl/reindexerimpl.h"

namespace reindexer {
namespace cluster {

AsyncDataReplicator::AsyncDataReplicator(AsyncDataReplicator::UpdatesQueueT& q, SharedSyncState& syncState, ReindexerImpl& thisNode,
										 ClusterManager& clusterManager)
	: statsCollector_(std::string(kAsyncReplStatsType)),
	  updatesQueue_(q),
	  syncState_(syncState),
	  thisNode_(thisNode),
	  clusterManager_(clusterManager) {}

void AsyncDataReplicator::Configure(AsyncReplConfigData config) {
	lock_guard lck(mtx_);
	if ((config_.has_value() && config_.value() != config) || !config_.has_value()) {
		stop();
		config_ = std::move(config);
	}
}

void AsyncDataReplicator::Configure(ReplicationConfigData config) {
	lock_guard lck(mtx_);
	if ((baseConfig_.has_value() && baseConfig_.value() != config) || !baseConfig_.has_value()) {
		stop();
		baseConfig_ = std::move(config);
	}
}

bool AsyncDataReplicator::IsExpectingStartup() const noexcept {
	lock_guard lck(mtx_);
	return isExpectingStartup();
}

void AsyncDataReplicator::Run() {
	auto localNamespaces = getLocalNamespaces();
	{
		lock_guard lck(mtx_);
		if (!isExpectingStartup()) {
			log_.Warn([] { return "AsyncDataReplicator: startup is not expected"; });
			return;
		}

		// NOLINTBEGIN (bugprone-unchecked-optional-access) Optionals were checked in isExpectingStartup()
		if (config_->nodes.size() > UpdatesQueueT::kMaxReplicas) {
			throw Error(errParams, "Async replication nodes limit was reached: {}", UpdatesQueueT::kMaxReplicas);
		}
		statsCollector_.Init(config_->nodes);
		log_.SetLevel(config_->logLevel);
		updatesQueue_.ReinitAsyncQueue(statsCollector_, std::optional<NsNamesHashSetT>(getMergedNsConfig(config_.value())), log_);
		assert(replThreads_.empty());

		ReplThreadConfig threadsConfig(baseConfig_.value(), config_.value());
		for (size_t i = 0; i < threadsCount(); ++i) {
			std::vector<std::pair<uint32_t, AsyncReplNodeConfig>> nodesShard;
			nodesShard.reserve(config_->nodes.size() / threadsCount() + 1);
			for (size_t j = i; j < config_->nodes.size(); j += threadsCount()) {
				nodesShard.emplace_back(std::make_pair(j, config_->nodes[j]));
			}
			if (nodesShard.size()) {
				replThreads_.emplace_back(baseConfig_->serverID, thisNode_, updatesQueue_.GetAsyncQueue(), config_->nodes, config_->mode,
										  syncState_, statsCollector_, log_);
				replThreads_.back().Run(threadsConfig, std::move(nodesShard), config_->nodes.size());
			}
		}
	}
	if (config_->role == AsyncReplConfigData::Role::Leader) {
		for (auto& ns : localNamespaces) {
			if (!clusterManager_.NamespaceIsInClusterConfig(ns)) {
				auto err = thisNode_.SetClusterOperationStatus(
					ns, ClusterOperationStatus{baseConfig_->serverID, ClusterOperationStatus::Role::None}, RdxContext());
				if (!err.ok()) {
					logWarn("SetClusterOperationStatus for the local '{}' namespace error: {}", ns, err.what());
				}
			}
		}
	}
	// NOLINTEND (bugprone-unchecked-optional-access) Optionals were checked in isExpectingStartup()
}

void AsyncDataReplicator::Stop(bool resetConfig) {
	lock_guard lck(mtx_);
	stop();
	if (resetConfig) {
		config_.reset();
	}
}

ReplicationStats AsyncDataReplicator::GetReplicationStats() const {
	auto stats = statsCollector_.Get();
	for (auto& node : stats.nodeStats) {
		node.role = RaftInfo::Role::Follower;
	}
	stats.logLevel = log_.GetLevel();
	return stats;
}

bool AsyncDataReplicator::NamespaceIsInAsyncConfig(std::string_view nsName) const {
	if (isSystemNamespaceNameFastReplication(nsName)) {
		return false;
	}

	const UpdatesQueueT::HashT h;
	const size_t hash = h(nsName);

	lock_guard lck(mtx_);
	return updatesQueue_.GetAsyncQueue()->TokenIsInWhiteList(nsName, hash);
}

bool AsyncDataReplicator::isExpectingStartup() const noexcept {
	return config_.has_value() && baseConfig_.has_value() && config_->nodes.size() && baseConfig_->serverID >= 0 && !isRunning() &&
		   config_->role != AsyncReplConfigData::Role::None;
}

size_t AsyncDataReplicator::threadsCount() const noexcept {
	return config_.has_value() && config_->replThreadsCount > 0 ? config_->replThreadsCount : kDefaultReplThreadCount;
}

void AsyncDataReplicator::stop() {
	if (isRunning()) {
		for (auto& th : replThreads_) {
			th.SendTerminate();
		}
		for (auto& th : replThreads_) {
			th.AwaitTermination();
		}
		replThreads_.clear();
		statsCollector_.Clear();
		updatesQueue_.GetAsyncQueue()->SetWritable(false, Error());
		updatesQueue_.ReinitAsyncQueue(statsCollector_, std::optional<NsNamesHashSetT>(), log_);
	}
}

NsNamesHashSetT AsyncDataReplicator::getLocalNamespaces() {
	std::vector<NamespaceDef> nsDefs;
	NsNamesHashSetT namespaces;
	auto err = thisNode_.EnumNamespaces(nsDefs, EnumNamespacesOpts().OnlyNames().HideSystem().HideTemporary().WithClosed(), RdxContext());
	if (!err.ok()) {
		throw Error(errNotValid, "Unable to enum local namespaces on async repl configuration: {}", err.what());
	}
	for (auto& ns : nsDefs) {
		namespaces.emplace(std::move(ns.name));
	}
	return namespaces;
}

NsNamesHashSetT AsyncDataReplicator::getMergedNsConfig(const AsyncReplConfigData& config) {
	assert(config.nodes.size());
	bool hasNodesWithDefaultConfig = false;
	for (auto& node : config.nodes) {
		if (!node.HasOwnNsList()) {
			hasNodesWithDefaultConfig = true;
			break;
		}
	}
	NsNamesHashSetT nss = hasNodesWithDefaultConfig ? config.namespaces->data : NsNamesHashSetT();
	if (hasNodesWithDefaultConfig && nss.empty()) {
		return nss;
	}
	for (auto& node : config.nodes) {
		if (node.HasOwnNsList()) {
			if (node.Namespaces()->Empty()) {
				return NsNamesHashSetT();
			}
			for (auto& ns : node.Namespaces()->data) {
				nss.emplace(ns);
			}
		}
	}
	return nss;
}

}  // namespace cluster
}  // namespace reindexer
