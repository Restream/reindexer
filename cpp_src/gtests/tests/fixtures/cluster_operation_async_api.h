#pragma once

#include "cluster_operation_api.h"
#include "tools/fsops.h"

class [[nodiscard]] ClusterOperationAsyncApi : public ClusterOperationApi {
public:
	void SetUp() override {	 // -V524
		std::ignore = reindexer::fs::RmDirAll(GetDefaults().baseTestsetDbPath);
	}
	void TearDown() override {	// -V524
		std::ignore = reindexer::fs::RmDirAll(GetDefaults().baseTestsetDbPath);
	}
	const Defaults& GetDefaults() const override {
		static Defaults defs{14200, 16200, reindexer::fs::JoinPath(reindexer::fs::GetTempDir(), "rx_test/ClusterOperationAsyncApi")};
		return defs;
	}
};
