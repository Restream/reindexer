#include "resultserializer.h"
#include "core/payload/payloadtypeimpl.h"
#include "vendor/msgpack/msgpack.h"

namespace reindexer {
namespace client {

void ResultSerializer::GetRawQueryParams(ResultSerializer::QueryParams& ret, const std::function<void(int nsId)>& updatePayloadFunc,
										 Options opts, ParsingData& parsingData) {
	ret.flags = GetVarUint();
	ret.totalcount = GetVarUint();
	ret.qcount = GetVarUint();
	ret.count = GetVarUint();
	ret.nsIncarnationTags.clear();
	ret.shardingConfigVersion = ShardingSourceId::NotSet;
	if (opts.IsWithClearAggs()) {
		if (opts.IsWithLazyMode()) {
			ret.aggResults = std::nullopt;
			ret.explainResults = std::nullopt;
		} else {
			ret.aggResults.emplace();
			ret.explainResults.emplace();
		}
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
	GetExtraParams(ret, opts);
	parsingData.extraData.end = parsingData.itemsPos = Pos();
}

void ResultSerializer::GetExtraParams(ResultSerializer::QueryParams& ret, Options opts) {
	if (!opts.IsWithLazyMode() && opts.IsWithClearAggs()) {
		if (!ret.aggResults.has_value()) {
			throw Error(errLogic, "Empty lazy aggregations value");
		}
		if (!ret.explainResults.has_value()) {
			throw Error(errLogic, "Empty lazy explain value");
		}
	}
	bool firstLazyData = true;
	for (;;) {
		const int tag = GetVarUint();
		switch (tag) {
			case QueryResultEnd:
				return;
			case QueryResultAggregation: {
				std::string_view data = GetSlice();
				if (!opts.IsWithLazyMode()) {
					if (firstLazyData) {
						firstLazyData = false;
						ret.aggResults.emplace();
						ret.explainResults.emplace();
					}
					// firstLazyData guaranties, that aggResults will be non-'nullopt'
					ret.aggResults->emplace_back();	 // NOLINT(bugprone-unchecked-optional-access)
					if ((ret.flags & kResultsFormatMask) == kResultsMsgPack) {
						ret.aggResults->back().FromMsgPack(data);  // NOLINT(bugprone-unchecked-optional-access)
					} else {
						ret.aggResults->back().FromJSON(giftStr(data));	 // NOLINT(bugprone-unchecked-optional-access)
					}
				}
				break;
			}
			case QueryResultExplain: {
				if (opts.IsWithLazyMode()) {
					GetSlice();
				} else {
					if (firstLazyData) {
						firstLazyData = false;
						ret.aggResults.emplace();
					}
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
			case QueryResultIncarnationTags: {
				const auto size = GetVarUint();
				if (size) {
					ret.nsIncarnationTags.reserve(size);
					for (size_t i = 0; i < size; ++i) {
						auto& shardTags = ret.nsIncarnationTags.emplace_back();
						shardTags.shardId = GetVarint();
						const auto tagsSize = GetVarUint();
						shardTags.tags.reserve(tagsSize);
						for (size_t j = 0; j < tagsSize; ++j) {
							shardTags.tags.emplace_back(GetVarint());
						}
					}
				}
				break;
			}
			default:
				throw Error(errLogic, "Unexpected Query tag: %d", tag);
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
