#pragma once

#include "sharding_api.h"

class ShardingExtrasApi : public ShardingApi {
public:
	const Defaults& GetDefaults() const override {
		static Defaults def{19100, 20100, fs::JoinPath(fs::GetTempDir(), "rx_test/ShardingExtrasApi")};
		return def;
	}

protected:
};
