#pragma once

#include "clusterization_api.h"

class ClusterizationAsyncApi : public ClusterizationApi {
public:
	void SetUp() override {	 // -V524
		const auto def = GetDefaults();
		reindexer::fs::RmDirAll(def.baseTestsetDbPath);
	}
	void TearDown() override {	// -V524
		const auto def = GetDefaults();
		reindexer::fs::RmDirAll(def.baseTestsetDbPath);
	}
	const Defaults& GetDefaults() const override {
		static Defaults defs{14200, 16200, fs::JoinPath(fs::GetTempDir(), "rx_test/ClusterizationAsyncApi")};
		return defs;
	}
};
