#pragma once

#include "cluster/consts.h"
#include "cluster_operation_api.h"
#include "tools/fsops.h"

class [[nodiscard]] ClusterOperationExtrasApi : public ClusterOperationApi {
public:
	void SetUp() override {	 // -V524
		std::ignore = reindexer::fs::RmDirAll(GetDefaults().baseTestsetDbPath);
	}
	void TearDown() override {	// -V524
		std::ignore = reindexer::fs::RmDirAll(GetDefaults().baseTestsetDbPath);
	}
	const Defaults& GetDefaults() const override {
		static Defaults defs{14300, 16300, reindexer::fs::JoinPath(reindexer::fs::GetTempDir(), "rx_test/ClusterOperationExtrasApi")};
		return defs;
	}

protected:
	struct ExpectedStats {
		size_t nodesCount;
		size_t queuedNssCount;
		bool isSynchronized;
	};

	void AwaitSyncNodesStats(ClusterOperationApi::Cluster& cluster, size_t leaderId, const std::vector<ExpectedStats>& expected,
							 std::chrono::seconds maxTime) {
		std::unordered_map<size_t, size_t> expectedQueuedNssCounters;
		std::unordered_map<bool, size_t> expectedSynchronizedCounters;
		for (auto& exp : expected) {
			expectedQueuedNssCounters[exp.queuedNssCount] += exp.nodesCount;
			expectedSynchronizedCounters[exp.isSynchronized] += exp.nodesCount;
		}
		std::chrono::milliseconds step(100);
		std::chrono::milliseconds awaitTime = std::chrono::duration_cast<std::chrono::milliseconds>(maxTime);
		while (awaitTime.count() > 0) {
			std::unordered_map<size_t, size_t> queuedNssCounters;
			std::unordered_map<bool, size_t> synchronizedCounters;
			auto stats = cluster.GetNode(leaderId)->GetReplicationStats(reindexer::cluster::kClusterReplStatsType);
			for (auto& nodeStat : stats.nodeStats) {
				++queuedNssCounters[nodeStat.nssSyncQueue];
				++synchronizedCounters[nodeStat.isSynchronized];
			}
			if (expectedQueuedNssCounters == queuedNssCounters && expectedSynchronizedCounters == synchronizedCounters) {
				return;
			}
			awaitTime -= step;
			std::this_thread::sleep_for(step);
		}
		auto stats = cluster.GetNode(leaderId)->GetReplicationStats(reindexer::cluster::kClusterReplStatsType);
		reindexer::WrSerializer wser;
		stats.GetJSON(wser);
		ASSERT_TRUE(false) << "Stats: " << wser.Slice();
	}
};
