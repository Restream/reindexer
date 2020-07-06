#include "resultserializer.h"
#include "vendor/msgpack/msgpack.h"

namespace reindexer {
namespace client {

void ResultSerializer::GetRawQueryParams(ResultSerializer::QueryParams& ret, std::function<void(int nsId)> updatePayloadFunc) {
	ret.flags = GetVarUint();
	ret.totalcount = GetVarUint();
	ret.qcount = GetVarUint();
	ret.count = GetVarUint();
	ret.aggResults.clear();
	ret.explainResults.clear();

	if (ret.flags & kResultsWithPayloadTypes) {
		int ptCount = GetVarUint();

		for (int i = 0; i < ptCount; i++) {
			int nsid = GetVarUint();
			GetVString();

			assert(updatePayloadFunc != nullptr);
			updatePayloadFunc(nsid);
		}
	}

	for (;;) {
		int tag = GetVarUint();
		if (tag == QueryResultEnd) break;
		string_view data = GetSlice();
		switch (tag) {
			case QueryResultAggregation:
				ret.aggResults.push_back({});
				if ((ret.flags & kResultsFormatMask) == kResultsMsgPack) {
					ret.aggResults.back().FromMsgPack(giftStr(data));
				} else {
					ret.aggResults.back().FromJSON(giftStr(data));
				}
				break;
			case QueryResultExplain:
				ret.explainResults = string(data);
				break;
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
