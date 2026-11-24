#include "shardingkeys.h"
#include "cluster/config.h"
#include "sharding.h"

namespace reindexer::sharding {

ShardingKeys::ShardingKeys(const reindexer::cluster::ShardingConfig& config) {
	for (const auto& ns : config.namespaces) {
		NsData& nsData = keys_[ns.ns];
		nsData.indexName = ns.index;
		nsData.defaultShard = ns.defaultShard;
		if (auto it = std::find_if(ns.keys.begin(), ns.keys.end(),
								   [](const auto& x) { return x.algorithmType == ShardingAlgorithmType::ByRange; });
			it != ns.keys.end()) {
			nsData.keysToShard = Variant4SegmentMap{};
		}

		for (const auto& key : ns.keys) {
			for (const auto& [l, r, _] : key.values) {
				(void)_;
				std::visit(overloaded{[&key, left = l](VariantHashMap& values) { values[left] = key.shardId; },
									  [&key, left = l, right = r](Variant4SegmentMap& values) {
										  values[{left}] = key.shardId;
										  if (left != right) {
											  values[{right, true}] = key.shardId;
										  }
									  }},
						   nsData.keysToShard);
			}
		}
	}
}

ShardingKeys::ShardIndexWithValues ShardingKeys::GetIndex(std::string_view nsName) const {
	auto itNsData = keys_.find(nsName);
	if (itNsData != keys_.end()) {
		return ShardIndexWithValues{itNsData->second.indexName, &itNsData->second.keysToShard};
	}
	throw Error(errLogic, "Can not find index for sharded ns [{}]", nsName);
}

int ShardingKeys::GetDefaultHost(std::string_view nsName) const {
	auto itNsData = keys_.find(nsName);
	if (itNsData != keys_.end()) {
		return itNsData->second.defaultShard;
	}
	throw Error(errLogic, "Can not find defaultShard for sharded ns [{}]", nsName);
}

bool ShardingKeys::IsShardIndex(std::string_view ns, std::string_view index) const {
	const auto itNsData = keys_.find(ns);
	if (itNsData == keys_.end()) {
		return false;
	}
	return itNsData->second.indexName == index;
}

int ShardingKeys::GetShardId(std::string_view ns, std::string_view index, const VariantArray& vals, bool& isShardKey) const {
	isShardKey = false;
	auto itNsData = keys_.find(ns);
	if (itNsData == keys_.end()) {
		throw Error(errLogic, "Namespace [{}] not found in sharding config", ns);
	}
	if (itNsData->second.indexName != index) {
		return ShardingKeyType::ProxyOff;
	}
	isShardKey = true;
	if (vals.empty() || vals.size() > 1) {
		throw Error(errLogic, "Sharding key value cannot be empty or an array");
	}

	return itNsData->second.GetShardId(vals[0]);
}

int ShardingKeys::GetShardId(std::string_view ns, const Variant& v) const {
	auto itNsData = keys_.find(ns);
	if (itNsData == keys_.end()) {
		throw Error(errLogic, "Namespace [{}] not found in sharding config", ns);
	}
	return itNsData->second.GetShardId(v);
}

fast_hash_set<int> ShardingKeys::getShardsIds(const NsData& nsData) const {
	fast_hash_set<int> uniqueIds = {nsData.defaultShard};  // with Default host
	std::visit(overloaded{[&uniqueIds, this](const VariantHashMap& values) { FillUniqueIds(uniqueIds, values); },
						  [&uniqueIds, this](const Variant4SegmentMap& values) { FillUniqueIds(uniqueIds, values); }},
			   nsData.keysToShard);
	return uniqueIds;
}

ShardIDsContainer ShardingKeys::GetShardsIds(std::string_view ns) const {
	auto itNsData = keys_.find(ns);
	if (itNsData == keys_.end()) {
		throw Error(errLogic, "Namespace [{}] not found in sharding config", ns);
	}
	ShardIDsContainer ids;

	auto uniqueIds = getShardsIds(itNsData->second);
	ids.reserve(uniqueIds.size());
	std::copy(uniqueIds.begin(), uniqueIds.end(), std::back_inserter(ids));
	return ids;
}

ShardIDsContainer ShardingKeys::GetShardsIds() const {
	fast_hash_set<int> uniqueIds;
	for (auto itNsData = keys_.begin(); itNsData != keys_.end(); ++itNsData) {
		auto ids = getShardsIds(itNsData->second);
		std::copy(ids.begin(), ids.end(), std::inserter(uniqueIds, uniqueIds.end()));
	}

	ShardIDsContainer ids;
	ids.reserve(uniqueIds.size());
	std::copy(uniqueIds.begin(), uniqueIds.end(), std::back_inserter(ids));
	return ids;
}

}  // namespace reindexer::sharding
