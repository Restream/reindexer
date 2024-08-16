#include "resultserializer.h"
#include "core/payload/payloadtypeimpl.h"

namespace reindexer {
namespace client {

void ResultSerializer::GetRawQueryParams(ResultSerializer::QueryParams& ret, const std::function<void(int nsId)>& updatePayloadFunc,
										 AggsFlag clearAggs) {
	ret.flags = GetVarUint();
	ret.totalcount = GetVarUint();
	ret.qcount = GetVarUint();
	ret.count = GetVarUint();
	if (clearAggs == AggsFlag::ClearAggregations) {
		ret.aggResults.clear();
		ret.explainResults.clear();
	}

	if (ret.flags & kResultsWithPayloadTypes) {
		int ptCount = GetVarUint();

		for (int i = 0; i < ptCount; i++) {
			int nsid = GetVarUint();
			GetVString();

			assertrx(updatePayloadFunc != nullptr);
			updatePayloadFunc(nsid);
		}
	}

	bool firstAgg = true;
	for (;;) {
		int tag = GetVarUint();
		if (tag == QueryResultEnd) {
			break;
		}
		if ((clearAggs == AggsFlag::DontClearAggregations) && firstAgg) {
			firstAgg = false;
			ret.aggResults.clear();
			ret.explainResults.clear();
		}
		std::string_view data = GetSlice();
		switch (tag) {
			case QueryResultAggregation: {
				auto& aggRes = ret.aggResults.emplace_back();
				Error err;
				if ((ret.flags & kResultsFormatMask) == kResultsMsgPack) {
					err = aggRes.FromMsgPack(giftStr(data));
				} else {
					err = aggRes.FromJSON(giftStr(data));
				}
				if (!err.ok()) {
					throw err;
				}
			} break;
			case QueryResultExplain:
				ret.explainResults = std::string(data);
				break;
			default:
				throw Error(errLogic, "Unexpected Query tag: %d", tag);
		}
	}
}

ResultSerializer::ItemParams ResultSerializer::GetItemParams(int flags) {
	ItemParams ret;

	if (flags & kResultsWithItemID) {
		ret.id = int(GetVarUint());
		ret.lsn = int64_t(GetVarUint());
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

	switch (flags & kResultsFormatMask) {
		case kResultsJson:
		case kResultsCJson:
		case kResultsMsgPack:
			ret.data = GetSlice();
			break;
		default:
			throw Error(errParseBin, "Server returned data in unknown format %d", flags & kResultsFormatMask);
	}

	return ret;
}

}  // namespace client
}  // namespace reindexer
