#pragma once

#include <unordered_map>
#include <unordered_set>
#include <vector>
#include "core/keyvalue/variant.h"
#include "tools/stringstools.h"

namespace reindexer {

namespace cluster {
struct ShardingConfig;
}

namespace sharding {

class ShardingKeys {
public:
	explicit ShardingKeys(const reindexer::cluster::ShardingConfig& config);
	std::vector<std::string_view> GetIndexes(std::string_view nsName) const;
	int GetShardId(std::string_view ns, std::string_view index) const;
	int GetShardId(std::string_view ns, std::string_view index, const VariantArray& v, bool& isShardKey) const;
	std::vector<int> GetShardsIds(std::string_view ns) const;
	std::vector<int> GetShardsIds() const;

private:
	using NsName = std::string_view;
	using IndexName = std::string_view;
	using ValuesData = std::unordered_map<size_t, int>;
	using IndexesData = std::unordered_map<IndexName, ValuesData, nocase_hash_str, nocase_equal_str>;
	std::unordered_map<NsName, IndexesData, nocase_hash_str, nocase_equal_str> keys_;
};

}  // namespace sharding
}  // namespace reindexer
