#include "asyncdatareplicator.h"
#include "cluster/clusterizator.h"
#include "core/reindexerimpl.h"

namespace reindexer {
namespace cluster {

AsyncDataReplicator::AsyncDataReplicator(AsyncDataReplicator::UpdatesQueueT& q, SharedSyncState<>& syncState, ReindexerImpl& thisNode,
										 Clusterizator& clusterizator)
	: statsCollector_(std::string(kAsyncReplStatsType)),
	  updatesQueue_(q),
	  syncState_(syncState),
	  thisNode_(thisNode),
	  clusterizator_(clusterizator) {}

void AsyncDataReplicator::Configure(AsyncReplConfigData config) {
	std::lock_guard lck(mtx_);
	if ((config_.has_value() && config_.value() != config) || !config_.has_value()) {
		stop();
		config_ = std::move(config);
	}
}

void AsyncDataReplicator::Configure(ReplicationConfigData config) {
	std::lock_guard lck(mtx_);
	if ((baseConfig_.has_value() && baseConfig_.value() != config) || !baseConfig_.has_value()) {
		stop();
		baseConfig_ = config;
	}
}

bool AsyncDataReplicator::IsExpectingStartup() const noexcept {
	std::lock_guard lck(mtx_);
	return isExpectingStartup();
}

void AsyncDataReplicator::Run() {
	auto localNamespaces = getLocalNamespaces();
	fast_hash_set<std::string, nocase_hash_str, nocase_equal_str> namespaces;
	{
		std::lock_guard lck(mtx_);
		if (!isExpectingStartup()) {
			logPrintf(LogWarning, "AsyncDataReplicator: startup is not expected");
			return;
		}

		// NOLINTBEGIN (bugprone-unchecked-optional-access) Optionals were checked in isExpectingStartup()
		statsCollector_.Init(config_->nodes);
		updatesQueue_.ReinitAsyncQueue(statsCollector_, std::optional<NsNamesHashSetT>(getMergedNsConfig(config_.value())));
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
										  syncState_, statsCollector_);
				replThreads_.back().Run(threadsConfig, std::move(nodesShard), config_->nodes.size());
			}
		}
	}
	if (config_->role == AsyncReplConfigData::Role::Leader) {
		for (auto& ns : localNamespaces) {
			if (!clusterizator_.NamespaceIsInClusterConfig(ns)) {
				thisNode_.SetClusterizationStatus(ns, ClusterizationStatus{baseConfig_->serverID, ClusterizationStatus::Role::None},
												  RdxContext());
			}
		}
	}
	// NOLINTEND (bugprone-unchecked-optional-access) Optionals were checked in isExpectingStartup()
}

void AsyncDataReplicator::Stop(bool resetConfig) {
	std::lock_guard lck(mtx_);
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
	return stats;
}

bool AsyncDataReplicator::NamespaceIsInAsyncConfig(std::string_view nsName) const {
	if (nsName.size() && (nsName[0] == '#' || nsName[0] == '@')) return false;

	const UpdatesQueueT::HashT h;
	const size_t hash = h(nsName);

	std::lock_guard lck(mtx_);
	return updatesQueue_.GetAsyncQueue()->TokenIsInWhiteList(nsName, hash);
}

bool AsyncDataReplicator::isExpectingStartup() const noexcept {
	return config_.has_value() && baseConfig_.has_value() && config_->nodes.size() && baseConfig_->serverID >= 0 && !isRunning() &&
		   config_->role != AsyncReplConfigData::Role::None;
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
		statsCollector_.Reset();
		updatesQueue_.GetAsyncQueue()->SetWritable(false, Error());
		updatesQueue_.ReinitAsyncQueue(statsCollector_, std::optional<NsNamesHashSetT>());
	}
}

AsyncDataReplicator::NsNamesHashSetT AsyncDataReplicator::getLocalNamespaces() {
	std::vector<NamespaceDef> nsDefs;
	NsNamesHashSetT namespaces;
	auto err = thisNode_.EnumNamespaces(nsDefs, EnumNamespacesOpts().OnlyNames().HideSystem().HideTemporary().WithClosed(), RdxContext());
	if (!err.ok()) {
		throw Error(errNotValid, "Unable to enum local namespaces on async repl configuration: %s", err.what());
	}
	for (auto& ns : nsDefs) {
		namespaces.emplace(std::move(ns.name));
	}
	return namespaces;
}

AsyncDataReplicator::NsNamesHashSetT AsyncDataReplicator::getMergedNsConfig(const AsyncReplConfigData& config) {
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
