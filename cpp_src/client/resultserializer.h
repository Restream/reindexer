#pragma once
#include <functional>
#include <optional>
#include "core/namespace/incarnationtags.h"
#include "core/queryresults/aggregationresult.h"
#include "core/rank_t.h"
#include "tools/lsn.h"
#include "tools/serializer.h"

struct msgpack_object;

namespace reindexer {
namespace client {

class [[nodiscard]] ResultSerializer : public Serializer {
public:
	using Serializer::Serializer;
	enum [[nodiscard]] Option { LazyMode = 0x1, ClearAggregations = 0x1 << 1 };
	class [[nodiscard]] Options {
	public:
		explicit Options(unsigned v = 0) noexcept : v_(v) {}

		bool IsWithLazyMode() const noexcept { return v_ & LazyMode; }
		bool IsWithClearAggs() const noexcept { return v_ & ClearAggregations; }

	private:
		unsigned v_;
	};

	struct [[nodiscard]] ItemParams {
		IdType id = -1;
		uint16_t nsid = 0;
		RankT rank{};
		lsn_t lsn;
		std::string_view data;
		bool raw = false;
		int shardId = ShardingKeyType::ProxyOff;
	};

	struct [[nodiscard]] QueryParams {
		int totalcount = 0;
		int qcount = 0;
		int count = 0;
		int flags = 0;
		std::optional<std::vector<AggregationResult>> aggResults;
		std::optional<std::string> explainResults;
		NsShardsIncarnationTags nsIncarnationTags;
		int64_t shardingConfigVersion = ShardingSourceId::NotSet;
		int shardId = ShardingKeyType::ProxyOff;
	};

	struct [[nodiscard]] ParsingData {
		struct [[nodiscard]] Range {
			unsigned begin = 0;
			unsigned end = 0;
		};

		unsigned itemsPos = 0;
		Range pts;
		Range extraData;
	};

	bool ContainsPayloads() const {
		Serializer ser(Buf(), Len());
		return ser.GetVarUInt() & kResultsWithPayloadTypes;
	}
	void GetRawQueryParams(QueryParams& ret, const std::function<void(int nsId)>& updatePayloadFunc, Options options,
						   ParsingData& parsingData);
	void GetExtraParams(QueryParams& ret, Options opts);
	ItemParams GetItemData(int flags, int shardId);
};

}  // namespace client
}  // namespace reindexer
