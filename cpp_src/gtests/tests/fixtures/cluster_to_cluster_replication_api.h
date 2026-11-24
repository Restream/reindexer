#pragma once

#include "cluster_operation_api.h"
#include "tools/fsops.h"

namespace reindexer_tests {

class [[nodiscard]] ClusterToClusterReplicationApi : public ClusterOperationApi {
public:
	void SetUp() override {	 // -V524
		std::ignore = reindexer::fs::RmDirAll(GetDefaults().baseTestsetDbPath);
	}
	void TearDown() override {	// -V524
		std::ignore = reindexer::fs::RmDirAll(GetDefaults().baseTestsetDbPath);
	}
	const Defaults& GetDefaults() const override {
		static Defaults defs{15000, 17000, reindexer::fs::JoinPath(reindexer::fs::GetTempDir(), "rx_test/ClusterToClusterReplicationApi")};
		return defs;
	}
};

}  // namespace reindexer_tests
