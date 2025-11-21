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
	void AwaitSyncNodesCount(ClusterOperationApi::Cluster& cluster, size_t leaderId, size_t expectedCount, std::chrono::seconds maxTime) {
		std::chrono::milliseconds step(100);
		std::chrono::milliseconds awaitTime = std::chrono::duration_cast<std::chrono::milliseconds>(maxTime);
		while (awaitTime.count() > 0) {
			if (cluster.GetSynchronizedNodesCount(leaderId) == expectedCount) {
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
