#include "resultserializer.h"
#include "core/payload/payloadtypeimpl.h"
#include "estl/gift_str.h"

namespace reindexer {
namespace client {

void ResultSerializer::GetRawQueryParams(ResultSerializer::QueryParams& ret, const std::function<void(int nsId)>& updatePayloadFunc,
										 Options opts, ParsingData& parsingData) {
	ret.flags = GetVarUInt();
	ret.totalcount = GetVarUInt();
	ret.qcount = GetVarUInt();
	ret.count = GetVarUInt();
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
		int ptCount = GetVarUInt();

		for (int i = 0; i < ptCount; ++i) {
			int nsid = GetVarUInt();
			std::ignore = GetVString();

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
		const int tag = GetVarUInt();
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
					if (auto aggRes = ((ret.flags & kResultsFormatMask) == kResultsMsgPack) ? AggregationResult::FromMsgPack(data)
																							: AggregationResult::FromJSON(giftStr(data))) {
						// firstLazyData guaranties, that aggResults will be non-'nullopt'
						ret.aggResults->emplace_back(std::move(*aggRes));  // NOLINT(bugprone-unchecked-optional-access)
					} else {
						throw aggRes.error();
					}
				}
				break;
			}
			case QueryResultExplain: {
				if (opts.IsWithLazyMode()) {
					std::ignore = GetSlice();
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
				ret.shardingConfigVersion = GetVarUInt();
				break;
			}
			case QueryResultShardId: {
				ret.shardId = GetVarUInt();
				break;
			}
			case QueryResultIncarnationTags: {
				const auto size = GetVarUInt();
				if (size) {
					ret.nsIncarnationTags.reserve(size);
					for (size_t i = 0; i < size; ++i) {
						auto& shardTags = ret.nsIncarnationTags.emplace_back();
						shardTags.shardId = GetVarint();
						const auto tagsSize = GetVarUInt();
						shardTags.tags.reserve(tagsSize);
						for (size_t j = 0; j < tagsSize; ++j) {
							shardTags.tags.emplace_back(GetVarint());
						}
					}
				}
				break;
			}
			case QueryResultRankFormat: {
				const auto format = GetVarUInt();
				if (format != RankFormat::SingleFloatValue) {
					throw Error(errLogic, "Unexpected rank format value tag: {} - only supported format is 0 (single float rank)",
								int(format));
				}
				break;
			}
			default:
				throw Error(errLogic, "Unexpected Query tag: {}", tag);
		}
	}
}

ResultSerializer::ItemParams ResultSerializer::GetItemData(int flags, int shardId) {
	ItemParams ret;

	if (flags & kResultsWithItemID) {
		ret.id = int(GetVarUInt());
		ret.lsn = lsn_t(GetVarUInt());
	}

	if (flags & kResultsWithNsID) {
		ret.nsid = int(GetVarUInt());
	}
	if (flags & kResultsWithRank) {
		ret.rank = GetRank();
	}

	if (flags & kResultsWithRaw) {
		ret.raw = int(GetBool());
	}

	if (flags & kResultsWithShardId) {
		if (shardId != ShardingKeyType::ProxyOff) {
			ret.shardId = shardId;
		} else {
			ret.shardId = int(GetVarUInt());
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
			throw Error(errParseBin, "Server returned data in unknown format {}", flags & kResultsFormatMask);
	}

	return ret;
}

}  // namespace client
}  // namespace reindexer
