#pragma once
#include <functional>
#include <optional>
#include "core/queryresults/aggregationresult.h"
#include "tools/lsn.h"
#include "tools/serializer.h"

struct msgpack_object;

namespace reindexer {
namespace client {

class ResultSerializer : public Serializer {
public:
	using Serializer::Serializer;

	struct ItemParams {
		int id = -1;
		int16_t nsid = 0;
		int16_t proc = 0;
		lsn_t lsn;
		std::string_view data;
		bool raw = false;
		int shardId = ShardingKeyType::ProxyOff;
	};

	struct QueryParams {
		int totalcount = 0;
		int qcount = 0;
		int count = 0;
		int flags = 0;
		std::optional<std::vector<AggregationResult>> aggResults;
		std::optional<std::string> explainResults;
		int64_t shardingConfigVersion = -1;
		int shardId = ShardingKeyType::ProxyOff;
	};

	struct ParsingData {
		struct Range {
			unsigned begin = 0;
			unsigned end = 0;
		};

		unsigned itemsPos = 0;
		Range pts;
		Range extraData;
	};

	bool ContainsPayloads() const {
		Serializer ser(Buf(), Len());
		return ser.GetVarUint() & kResultsWithPayloadTypes;
	}
	void GetRawQueryParams(QueryParams &ret, const std::function<void(int nsId)> &updatePayloadFunc, bool lazyMode,
						   ParsingData &parsingData);
	void GetExtraParams(QueryParams &ret, bool lazyMode);
	ItemParams GetItemData(int flags, int shardId);
};

}  // namespace client
}  // namespace reindexer
