#include "resultserializer.h"
#include "core/payload/payloadtypeimpl.h"
#include "vendor/msgpack/msgpack.h"

namespace reindexer {
namespace client {

void ResultSerializer::GetRawQueryParams(ResultSerializer::QueryParams& ret, std::function<void(int nsId)> updatePayloadFunc, bool lazyMode,
										 ParsingData& parsingData) {
	ret.flags = GetVarUint();
	ret.totalcount = GetVarUint();
	ret.qcount = GetVarUint();
	ret.count = GetVarUint();
	if (lazyMode) {
		ret.aggResults = std::nullopt;
		ret.explainResults = std::nullopt;
	} else {
		ret.aggResults.emplace();	   // std::vector<AggregationResult>();
		ret.explainResults.emplace();  // = std::string();
	}
	parsingData = ParsingData();

	parsingData.pts.begin = Pos();
	if (ret.flags & kResultsWithPayloadTypes) {
		int ptCount = GetVarUint();

		for (int i = 0; i < ptCount; ++i) {
			int nsid = GetVarUint();
			GetVString();

			assertrx(updatePayloadFunc != nullptr);
			updatePayloadFunc(nsid);
		}
	}

	parsingData.pts.end = parsingData.extraData.begin = Pos();
	GetExtraParams(ret, lazyMode);
	parsingData.extraData.end = parsingData.itemsPos = Pos();
}

void ResultSerializer::GetExtraParams(ResultSerializer::QueryParams& ret, bool lazyMode) {
	for (;;) {
		const int tag = GetVarUint();
		if (tag == QueryResultEnd) break;
		switch (tag) {
			case QueryResultAggregation: {
				std::string_view data = GetSlice();
				if (!lazyMode) {
					ret.aggResults->emplace_back();
					if ((ret.flags & kResultsFormatMask) == kResultsMsgPack) {
						ret.aggResults->back().FromMsgPack(data);
					} else {
						ret.aggResults->back().FromJSON(giftStr(data));
					}
				}
				break;
			}
			case QueryResultExplain: {
				if (!lazyMode) {
					ret.explainResults = GetSlice();
				}
				break;
			}
			case QueryResultShardingVersion: {
				ret.shardingConfigVersion = GetVarUint();
				break;
			}
			case QueryResultShardId: {
				ret.shardId = GetVarUint();
				break;
			}
		}
	}
}

ResultSerializer::ItemParams ResultSerializer::GetItemData(int flags, int shardId) {
	ItemParams ret;

	if (flags & kResultsWithItemID) {
		ret.id = int(GetVarUint());
		ret.lsn = lsn_t(GetVarUint());
	}

	if (flags & kResultsWithNsID) {
		ret.nsid = int(GetVarUint());
	}
	if (flags & kResultsWithRank) {
		ret.proc = int(GetVarUint());
	}

	if (flags & kResultsWithRaw) {
		ret.raw = int(GetBool());
	}

	if (flags & kResultsWithShardId) {
		if (shardId != ShardingKeyType::ProxyOff) {
			ret.shardId = shardId;
		} else {
			ret.shardId = int(GetVarUint());
		}
	}
	switch (flags & kResultsFormatMask) {
		case kResultsJson:
		case kResultsCJson:
		case kResultsMsgPack:
			ret.data = GetSlice();
			break;
		case kResultsPure:
			break;
		default:
			throw Error(errParseBin, "Server returned data in unknown format %d", flags & kResultsFormatMask);
	}

	return ret;
}

}  // namespace client
}  // namespace reindexer
