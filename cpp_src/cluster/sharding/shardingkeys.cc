#include "shardingkeys.h"
#include "cluster/config.h"

namespace reindexer::sharding {

ShardingKeys::ShardingKeys(const reindexer::cluster::ShardingConfig& config) {
	// id == 0 is for proxy hosts
	int shardId = 0;
	for (const reindexer::cluster::ShardingKey& key : config.keys) {
		IndexesData& indexesData = keys_[key.ns];
		ValuesData& valuesData = indexesData[key.index];
		++shardId;
		for (const Variant& v : key.values) {
			valuesData[v.Hash()] = shardId;
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
	if (itNsData == keys_.end()) return IndexValueType::NotSet;
	auto itIndexData = itNsData->second.find(index);
	if (itIndexData == itNsData->second.end()) return IndexValueType::NotSet;
	if (itIndexData->second.size() > 0) {
		return itIndexData->second.begin()->second;
	}
	return IndexValueType::NotSet;
}

int ShardingKeys::GetShardId(std::string_view ns, std::string_view index, const VariantArray& vals, bool& isShardKey) const {
	auto itNsData = keys_.find(ns);
	if (itNsData == keys_.end()) return IndexValueType::NotSet;
	auto itIndexData = itNsData->second.find(index);
	if (itIndexData == itNsData->second.end()) return IndexValueType::NotSet;
	isShardKey = true;
	int shardId = IndexValueType::NotSet;
	const ValuesData& valuesData = itIndexData->second;
	for (const Variant& v : vals) {
		auto it = valuesData.find(v.Hash());
		if ((it == valuesData.end()) || (shardId != IndexValueType::NotSet && shardId != it->second)) {
			return IndexValueType::NotSet;
		}
		shardId = it->second;
	}
	return shardId;
}

std::vector<int> ShardingKeys::GetShardsIds(std::string_view ns) const {
	auto itNsData = keys_.find(ns);
	if (itNsData == keys_.end()) return {};
	std::vector<int> ids;
	ids.reserve(itNsData->second.size());
	ids.emplace_back(0);  // with Proxy host
	for (auto itIndexData = itNsData->second.begin(); itIndexData != itNsData->second.end(); ++itIndexData) {
		for (auto itValuesData = itIndexData->second.begin(); itValuesData != itIndexData->second.end(); ++itValuesData) {
			ids.emplace_back(itValuesData->second);
		}
	}
	return ids;
}

std::vector<int> ShardingKeys::GetShardsIds() const {
	std::vector<int> ids = {0};
	for (auto itNsData = keys_.begin(); itNsData != keys_.end(); ++itNsData) {
		for (auto itIndexData = itNsData->second.begin(); itIndexData != itNsData->second.end(); ++itIndexData) {
			for (auto itValuesData = itIndexData->second.begin(); itValuesData != itIndexData->second.end(); ++itValuesData) {
				ids.emplace_back(itValuesData->second);
			}
		}
	}
	return ids;
}

}  // namespace reindexer::sharding
