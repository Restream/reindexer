#pragma once

#include "sharding_api.h"

class [[nodiscard]] ShardingSystemApi : public ShardingApi {
public:
	const Defaults& GetDefaults() const override {
		static Defaults def{19200, 20200, reindexer::fs::JoinPath(reindexer::fs::GetTempDir(), "rx_test/ShardingSystemApi")};
		return def;
	}

protected:
	void ValidateNamespaces(size_t shard, const std::vector<std::string>& expected, const std::vector<reindexer::NamespaceDef>& actual) {
		if (actual.size() == expected.size()) {
			bool hasUnexpectedNamespaces = false;
			for (auto& ns : actual) {
				auto found = std::find(expected.begin(), expected.end(), ns.name);
				if (found == expected.end()) {
					hasUnexpectedNamespaces = true;
					break;
				}
			}
			if (!hasUnexpectedNamespaces) {
				return;
			}
		}
		std::cerr << "Expected:\n";
		for (auto& ns : expected) {
			std::cerr << ns << std::endl;
		}
		std::cerr << "Actual:\n";
		for (auto& ns : actual) {
			std::cerr << ns.name << std::endl;
		}
		ASSERT_TRUE(false) << "shard: " << shard;
	}
};
