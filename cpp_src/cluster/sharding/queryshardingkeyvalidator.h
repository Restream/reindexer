#pragma once

#include "core/query/query.h"
#include "shardingkeys.h"
#include "tools/errors.h"

#include <string_view>
#include <utility>

namespace reindexer {

class Variant;

namespace sharding {

template <typename ContainsShardingKey>
class [[nodiscard]] QueryShardingKeyValidator {
public:
	QueryShardingKeyValidator(const ShardingKeys& keys, int& hostId, Variant& shardKey, bool& hasShardingKeys,
							  ContainsShardingKey&& containsShardingKey)
		: keys_{keys},
		  hostId_{hostId},
		  shardKey_{shardKey},
		  hasShardingKeys_{hasShardingKeys},
		  containsShardingKey_{std::move(containsShardingKey)} {}

	void Validate(const Query& query) {
		validateJoinQueries(query);
		validateMergeQueries(query);
		validateSubQueries(query);
	}

private:
	void validateShardedQuery(const Query& query, std::string_view errorMsg) {
		if (keys_.IsSharded(query.NsName())) {
			if (containsShardingKey_(query, hostId_, shardKey_)) {
				hasShardingKeys_ = true;
			} else if (hasShardingKeys_) {
				throw Error(errLogic, errorMsg);
			}
		}
	}

	void validateJoinQueries(const Query& query) {
		for (const auto& jq : query.GetJoinQueries()) {
			validateShardedQuery(jq, "Join query must contain shard key");
			validateJoinQueries(jq);
			validateSubQueries(jq);
			validateMergeQueries(jq);
		}
	}

	void validateSubQueries(const Query& query) {
		for (const auto& sq : query.GetSubQueries()) {
			validateShardedQuery(sq, "Subquery must contain shard key");
			validateJoinQueries(sq);
			validateSubQueries(sq);
			validateMergeQueries(sq);
		}
	}

	void validateMergeQueries(const Query& query) {
		for (const auto& mq : query.GetMergeQueries()) {
			try {
				validateMergeQuery(mq);
			} catch (const Error& err) {
				throw Error(errParams, std::string_view{err.whatStr()});
			}
		}
	}

	void validateMergeQuery(const Query& query) {
		validateShardedQuery(query, "Merge query must contain shard key");
		validateJoinQueries(query);
		validateSubQueries(query);
		validateMergeQueries(query);
	}

	const ShardingKeys& keys_;
	int& hostId_;
	Variant& shardKey_;
	bool& hasShardingKeys_;
	ContainsShardingKey containsShardingKey_;
};

}  // namespace sharding
}  // namespace reindexer
