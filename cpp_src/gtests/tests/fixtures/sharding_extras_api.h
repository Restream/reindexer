#pragma once

#include "sharding_api.h"

class [[nodiscard]] ShardingExtrasApi : public ShardingApi {
public:
	const Defaults& GetDefaults() const override {
		static Defaults def{19100, 20100, reindexer::fs::JoinPath(reindexer::fs::GetTempDir(), "rx_test/ShardingExtrasApi")};
		return def;
	}

protected:
};
