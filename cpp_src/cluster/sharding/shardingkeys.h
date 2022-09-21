#pragma once

#include <vector>
#include "core/keyvalue/variant.h"
#include "estl/fast_hash_map.h"
#include "tools/stringstools.h"

namespace reindexer {

namespace cluster {
struct ShardingConfig;
}

namespace sharding {

constexpr size_t kHvectorConnStack = 9;
using ShardIDsContainer = h_vector<int, kHvectorConnStack>;

class ShardingKeys {
public:
	using ValuesData = fast_hash_map<Variant, int>;
	struct ShardIndexWithValues {
		std::string_view name;
		const ValuesData* values;
	};

	explicit ShardingKeys(const reindexer::cluster::ShardingConfig& config);
	ShardIndexWithValues GetIndex(std::string_view nsName) const;
	int GetDefaultHost(std::string_view nsName) const;
	bool IsShardIndex(std::string_view ns, std::string_view index) const;
	int GetShardId(std::string_view ns, std::string_view index, const VariantArray& v, bool& isShardKey) const;
	ShardIDsContainer GetShardsIds(std::string_view ns) const;
	ShardIDsContainer GetShardsIds() const;
	bool IsSharded(std::string_view ns) const noexcept { return keys_.find(ns) != keys_.end(); }

private:
	using NsName = std::string_view;
	struct NsData {
		std::string_view indexName;
		ValuesData keysToShard;
		int defaultShard;
		uint8_t padding[4];
	};

	fast_hash_map<NsName, NsData, nocase_hash_str, nocase_equal_str> keys_;
	// ns
	//   value_i1 - shardNodeId1
	//   value_i2 - shardNodeId2
};

}  // namespace sharding
}  // namespace reindexer
