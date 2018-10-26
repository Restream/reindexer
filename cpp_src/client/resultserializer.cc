#include "resultserializer.h"

namespace reindexer {
namespace client {

ResultSerializer::QueryParams ResultSerializer::GetRawQueryParams(std::function<void(int nsId)> updatePayloadFunc) {
	QueryParams ret;

	ret.flags = GetVarUint();
	ret.totalcount = GetVarUint();
	ret.qcount = GetVarUint();
	ret.count = GetVarUint();

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
				ret.aggResults.back().FromJSON(const_cast<char *>(data.ToString().c_str()));
				break;
			case QueryResultExplain:
				ret.explainResults = data.ToString();
				break;
		}
	}

	return ret;
}

ResultSerializer::ItemParams ResultSerializer::GetItemParams(int flags) {
	ItemParams ret;

	if (flags & kResultsWithItemID) {
		ret.id = int(GetVarUint());
		ret.lsn = int(GetVarUint());
	}

	if (flags & kResultsWithNsID) {
		ret.nsid = int(GetVarUint());
	}
	if (flags & kResultsWithPercents) {
		ret.proc = int(GetVarUint());
	}

	switch (flags & kResultsFormatMask) {
		case kResultsJson:
		case kResultsCJson:
			ret.data = GetSlice();
			break;
		default:
			throw Error(errParseBin, "Server returned data in unknown format %d", int(flags & kResultsFormatMask));
	}

	return ret;
}

}  // namespace client
}  // namespace reindexer
