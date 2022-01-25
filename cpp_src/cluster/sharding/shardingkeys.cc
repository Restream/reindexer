#include "shardingkeys.h"
#include <set>
#include "cluster/config.h"
#include "sharding.h"

namespace reindexer::sharding {

ShardingKeys::ShardingKeys(const reindexer::cluster::ShardingConfig& config) {
	for (const auto& ns : config.namespaces) {
		IndexesData& indexesData = keys_[ns.ns];
		ValuesData& valuesData = indexesData[ns.index];
		for (const auto& key : ns.keys) {
			for (const Variant& v : key.values) {
				valuesData[v] = key.shardId;
			}
		}
	}
}

std::vector<std::string_view> ShardingKeys::GetIndexes(std::string_view nsName) const {
	std::vector<std::string_view> indexes;
	auto itNsData = keys_.find(nsName);
	if (itNsData != keys_.end()) {
		indexes.reserve(itNsData->second.size());
		for (auto it = itNsData->second.begin(); it != itNsData->second.end(); ++it) {
			indexes.emplace_back(std::string_view(it->first));
		}
	}
	return indexes;
}

int ShardingKeys::GetShardId(std::string_view ns, std::string_view index) const {
	auto itNsData = keys_.find(ns);
	if (itNsData == keys_.end()) return int(ShardIdType::NotSet);
	auto itIndexData = itNsData->second.find(index);
	if (itIndexData == itNsData->second.end()) return int(ShardIdType::NotSet);
	if (itIndexData->second.size() > 0) {
		return itIndexData->second.begin()->second;
	}
	return int(ShardIdType::NotSet);
}

int ShardingKeys::GetShardId(std::string_view ns, std::string_view index, const VariantArray& vals, bool& isShardKey) const {
	isShardKey = false;
	auto itNsData = keys_.find(ns);
	if (itNsData == keys_.end()) return int(ShardIdType::NotSet);
	auto itIndexData = itNsData->second.find(index);
	if (itIndexData == itNsData->second.end()) return int(ShardIdType::NotSet);
	isShardKey = true;
	int shardId = int(ShardIdType::NotSet);
	const ValuesData& valuesData = itIndexData->second;
	if (vals.empty() || vals.size() > 1) {
		throw Error(errLogic, "Sharding key value cannot be empty or an array");
	}
	auto it = valuesData.find(vals[0]);
	if ((it == valuesData.end()) || (shardId != int(ShardIdType::NotSet) && shardId != it->second)) {
		return int(ShardIdType::NotSet);
	}
	shardId = it->second;

	return shardId;
}

std::vector<int> ShardingKeys::GetShardsIds(std::string_view ns) const {
	auto itNsData = keys_.find(ns);
	if (itNsData == keys_.end()) return {};
	std::vector<int> ids;
	std::set<int> uniqueIds = {0};	// with Proxy host
	for (auto itIndexData = itNsData->second.begin(); itIndexData != itNsData->second.end(); ++itIndexData) {
		for (auto itValuesData = itIndexData->second.begin(); itValuesData != itIndexData->second.end(); ++itValuesData) {
			uniqueIds.insert(itValuesData->second);
		}
	}
	ids.reserve(uniqueIds.size());
	std::copy(uniqueIds.begin(), uniqueIds.end(), std::back_inserter(ids));
	return ids;
}

std::vector<int> ShardingKeys::GetShardsIds() const {
	std::vector<int> ids;
	std::set<int> uniqueIds = {0};
	for (auto itNsData = keys_.begin(); itNsData != keys_.end(); ++itNsData) {
		for (auto itIndexData = itNsData->second.begin(); itIndexData != itNsData->second.end(); ++itIndexData) {
			for (auto itValuesData = itIndexData->second.begin(); itValuesData != itIndexData->second.end(); ++itValuesData) {
				uniqueIds.insert(itValuesData->second);
			}
		}
	}
	ids.reserve(uniqueIds.size());
	std::copy(uniqueIds.begin(), uniqueIds.end(), std::back_inserter(ids));
	return ids;
}

}  // namespace reindexer::sharding
