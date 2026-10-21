#pragma once

#include "core/query/query_impl.h"
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

	void Validate(ConstQueryImpl query) {
		validateJoinQueries(query);
		validateMergeQueries(query);
		validateSubQueries(query);
	}

private:
	void validateShardedQuery(ConstQueryImpl query, std::string_view errorMsg) {
		if (keys_.IsSharded(query.NsName())) {
			if (containsShardingKey_(*query, hostId_, shardKey_)) {
				hasShardingKeys_ = true;
			} else if (hasShardingKeys_) {
				throw Error(errLogic, errorMsg);
			}
		}
	}

	void validateJoinQueries(ConstQueryImpl query) {
		for (const auto& jq : query.JoinQueries()) {
			const auto jqImpl = Impl(jq);
			validateShardedQuery(jqImpl, "Join query must contain shard key");
			validateJoinQueries(jqImpl);
			validateSubQueries(jqImpl);
			validateMergeQueries(jqImpl);
		}
	}

	void validateSubQueries(ConstQueryImpl query) {
		for (const auto& sq : query.SubQueries()) {
			const auto sqImpl = Impl(sq);
			validateShardedQuery(sqImpl, "Subquery must contain shard key");
			validateJoinQueries(sqImpl);
			validateSubQueries(sqImpl);
			validateMergeQueries(sqImpl);
		}
	}

	void validateMergeQueries(ConstQueryImpl query) {
		for (const auto& mq : query.MergeQueries()) {
			try {
				validateMergeQuery(Impl(mq));
			} catch (const Error& err) {
				throw Error(errParams, std::string_view{err.whatStr()});
			}
		}
	}

	void validateMergeQuery(ConstQueryImpl query) {
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
