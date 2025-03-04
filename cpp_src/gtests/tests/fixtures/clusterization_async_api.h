#pragma once

#include "clusterization_api.h"
#include "tools/fsops.h"

class ClusterizationAsyncApi : public ClusterizationApi {
public:
	void SetUp() override {	 // -V524
		reindexer::fs::RmDirAll(GetDefaults().baseTestsetDbPath);
	}
	void TearDown() override {	// -V524
		reindexer::fs::RmDirAll(GetDefaults().baseTestsetDbPath);
	}
	const Defaults& GetDefaults() const override {
		static Defaults defs{14200, 16200, reindexer::fs::JoinPath(reindexer::fs::GetTempDir(), "rx_test/ClusterizationAsyncApi")};
		return defs;
	}
};
