#pragma once

#include <deque>
#include <list>
#include "client/cororeindexer.h"
#include "cluster/stats/relicationstatscollector.h"
#include "cluster/stats/synchronizationlist.h"
#include "core/namespace/namespacestat.h"
#include "net/ev/ev.h"
#include "sharedsyncstate.h"
#include "tools/lsn.h"

namespace reindexer {

class ReindexerImpl;

namespace cluster {

class Logger;

class LeaderSyncQueue {
public:
	struct Entry {
		struct NodeData {
			bool HasDataCount() const noexcept { return count != ReplicationStateV2::kNoDataCount; }

			uint64_t hash = 0;
			int64_t count = ReplicationStateV2::kNoDataCount;
		};

		bool IsLocal() const noexcept {
			try {
				return localLsn == latestLsn;
			} catch (Error&) {
				return false;
			}
		}

		std::vector<uint32_t> nodes;
		std::vector<NodeData> data;
		std::string_view nsName;
		ExtendedLsn latestLsn;
		ExtendedLsn localLsn;
		NodeData localData;
	};

	LeaderSyncQueue(size_t maxSyncsPerNode) : maxSyncsPerNode_(maxSyncsPerNode) {}

	void Refill(std::list<Entry>&& entries) {
		std::lock_guard lck(mtx_);
		entries_ = std::move(entries);
		currentSyncsPerNode_.clear();
	}
	size_t Size() {
		std::lock_guard lck(mtx_);
		return entries_.size();
	}
	void SyncDone(uint32_t nodeId) {
		std::lock_guard lck(mtx_);
		auto found = currentSyncsPerNode_.find(nodeId);
		assert(found != currentSyncsPerNode_.end());
		assert(found->second != 0);
		if (found->second > 0) {
			--found->second;
		}
	}
	bool TryToGetEntry(int32_t preferredNodeId, Entry& out, uint32_t& outIdx) {
		std::lock_guard lck(mtx_);
		if (preferredNodeId >= 0) {
			const auto found = currentSyncsPerNode_.find(uint32_t(preferredNodeId));
			if (found == currentSyncsPerNode_.end() || found->second >= int(maxSyncsPerNode_)) {
				preferredNodeId = -1;
			}
		}
		for (uint32_t retry = 0; retry < 2; ++retry) {
			for (auto it = entries_.begin(); it != entries_.end(); ++it) {
				for (uint32_t idx = 0; idx < it->nodes.size(); ++idx) {
					if (preferredNodeId >= 0 && uint32_t(preferredNodeId) != it->nodes[idx]) {
						continue;
					}
					bool isSyncAllowed = false;
					const auto n = it->nodes[idx];
					if (maxSyncsPerNode_ == 0) {
						currentSyncsPerNode_[n] = -1;
						isSyncAllowed = true;
					} else if (size_t(currentSyncsPerNode_[n]) < maxSyncsPerNode_) {
						++currentSyncsPerNode_[n];
						isSyncAllowed = true;
					}
					if (isSyncAllowed) {
						outIdx = idx;
						out = std::move(*it);
						entries_.erase(it);
						return true;
					}
				}
			}
			if (preferredNodeId >= 0) {
				preferredNodeId = -1;
			} else {
				break;
			}
		}
		return false;
	}

private:
	const size_t maxSyncsPerNode_;
	std::mutex mtx_;
	std::list<Entry> entries_;
	std::map<uint32_t, int> currentSyncsPerNode_;
};

class LeaderSyncThread {
public:
	struct Config {
		const std::vector<std::string>& dsns;
		int64_t maxWALDepthOnForceSync;
		int clusterId;
		int serverId;
		bool enableCompression;
		std::chrono::milliseconds netTimeout;
	};

	LeaderSyncThread(const Config& cfg, LeaderSyncQueue& syncQueue, SharedSyncState<>& sharedSyncState, ReindexerImpl& thisNode,
					 ReplicationStatsCollector statsCollector, const Logger& l, std::once_flag& actShardingCfg)
		: cfg_(cfg),
		  syncQueue_(syncQueue),
		  sharedSyncState_(sharedSyncState),
		  thisNode_(thisNode),
		  statsCollector_(statsCollector),
		  client_(client::ReindexerConfig{10000, 0, cfg_.netTimeout, cfg_.enableCompression, true, "cluster_leader_syncer"}),
		  log_(l),
		  actShardingCfg_(actShardingCfg) {
		terminateAsync_.set(loop_);
		terminateAsync_.set([this](net::ev::async&) { client_.Stop(); });
		thread_ = std::thread([this]() noexcept { sync(); });
	}
	void Terminate() {
		if (!terminate_) {
			terminate_ = true;
			terminateAsync_.send();
		}
	}
	bool IsTerminated() const noexcept { return terminate_; }
	void Join() { thread_.join(); }
	const Error& LastError() const noexcept { return lastError_; }

private:
	void sync();
	void syncNamespaceImpl(bool forced, const LeaderSyncQueue::Entry& syncEntry, std::string& tmpNsName);
	void actualizeShardingConfig();
	static constexpr std::string_view logModuleName() noexcept { return std::string_view("leadersyncer_t"); }

	const Config& cfg_;
	LeaderSyncQueue& syncQueue_;
	Error lastError_;
	std::atomic<bool> terminate_ = false;
	SharedSyncState<>& sharedSyncState_;
	ReindexerImpl& thisNode_;
	ReplicationStatsCollector statsCollector_;
	client::CoroReindexer client_;
	std::thread thread_;
	net::ev::async terminateAsync_;
	net::ev::dynamic_loop loop_;
	const Logger& log_;
	std::once_flag& actShardingCfg_;
};

class LeaderSyncer {
public:
	struct Config {
		const std::vector<std::string>& dsns;
		int64_t maxWALDepthOnForceSync;
		int clusterId;
		int serverId;
		size_t threadsCount;
		size_t maxSyncsPerNode;
		bool enableCompression;
		std::chrono::milliseconds netTimeout;
	};

	LeaderSyncer(const Config& cfg, const Logger& l) noexcept : syncQueue_(cfg.maxSyncsPerNode), cfg_(cfg), log_(l) {}

	void Terminate() {
		std::lock_guard lck(mtx_);
		for (auto& th : threads_) {
			th.Terminate();
		}
	}
	Error Sync(std::list<LeaderSyncQueue::Entry>&& entries, SharedSyncState<>& sharedSyncState, ReindexerImpl& thisNode,
			   ReplicationStatsCollector statsCollector);

private:
	static constexpr std::string_view logModuleName() noexcept { return std::string_view("leadersyncer"); }

	LeaderSyncQueue syncQueue_;
	const Config cfg_;
	std::mutex mtx_;
	std::deque<LeaderSyncThread> threads_;
	const Logger& log_;
};

}  // namespace cluster
}  // namespace reindexer
