#include "shardingkeys.h"
#include <set>
#include "cluster/config.h"
#include "sharding.h"

namespace reindexer::sharding {

ShardingKeys::ShardingKeys(const reindexer::cluster::ShardingConfig& config) {
	for (const auto& ns : config.namespaces) {
		NsData& nsData = keys_[ns.ns];
		nsData.indexName = ns.index;
		nsData.defaultShard = ns.defaultShard;
		for (const auto& key : ns.keys) {
			for (const Variant& v : key.values) {
				nsData.keysToShard[v] = key.shardId;
			}
		}
	}
}

ShardingKeys::ShardIndexWithValues ShardingKeys::GetIndex(std::string_view nsName) const {
	auto itNsData = keys_.find(nsName);
	if (itNsData != keys_.end()) {
		return ShardIndexWithValues{itNsData->second.indexName, &itNsData->second.keysToShard};
	}
	throw Error(errLogic, "Can not find index for sharded ns [%s]", nsName);
}

int ShardingKeys::GetDefaultHost(std::string_view nsName) const {
	auto itNsData = keys_.find(nsName);
	if (itNsData != keys_.end()) {
		return itNsData->second.defaultShard;
	}
	throw Error(errLogic, "Can not find defaultShard for sharded ns [%s]", nsName);
}

bool ShardingKeys::IsShardIndex(std::string_view ns, std::string_view index) const {
	const auto itNsData = keys_.find(ns);
	if (itNsData == keys_.end()) return false;
	return itNsData->second.indexName == index;
}

int ShardingKeys::GetShardId(std::string_view ns, std::string_view index, const VariantArray& vals, bool& isShardKey) const {
	isShardKey = false;
	auto itNsData = keys_.find(ns);
	if (itNsData == keys_.end()) {
		throw Error(errLogic, "Namespace [%s] not found", ns);
	}
	if (itNsData->second.indexName != index) {
		return ShardingKeyType::ProxyOff;
	}
	isShardKey = true;
	const auto& valuesData = itNsData->second.keysToShard;
	if (vals.empty() || vals.size() > 1) {
		throw Error(errLogic, "Sharding key value cannot be empty or an array");
	}
	auto it = valuesData.find(vals[0]);
	if (it == valuesData.end()) {
		return itNsData->second.defaultShard;
	}
	return it->second;
}

ShardIDsContainer ShardingKeys::GetShardsIds(std::string_view ns) const {
	auto itNsData = keys_.find(ns);
	if (itNsData == keys_.end()) {
		throw Error(errLogic, "Namespace [%s] not found", ns);
	}
	ShardIDsContainer ids;
	std::set<int> uniqueIds = {itNsData->second.defaultShard};	 // with Default host
	for (auto itValuesData = itNsData->second.keysToShard.begin(); itValuesData != itNsData->second.keysToShard.end(); ++itValuesData) {
		uniqueIds.insert(itValuesData->second);
	}
	ids.reserve(uniqueIds.size());
	std::copy(uniqueIds.begin(), uniqueIds.end(), std::back_inserter(ids));
	return ids;
}

ShardIDsContainer ShardingKeys::GetShardsIds() const {
	ShardIDsContainer ids;
	std::set<int> uniqueIds;
	for (auto itNsData = keys_.begin(); itNsData != keys_.end(); ++itNsData) {
		uniqueIds.insert(itNsData->second.defaultShard);	 // with Default host
		for (auto itValuesData = itNsData->second.keysToShard.begin(); itValuesData != itNsData->second.keysToShard.end(); ++itValuesData) {
			uniqueIds.insert(itValuesData->second);
		}
	}
	ids.reserve(uniqueIds.size());
	std::copy(uniqueIds.begin(), uniqueIds.end(), std::back_inserter(ids));
	return ids;
}

}  // namespace reindexer::sharding
