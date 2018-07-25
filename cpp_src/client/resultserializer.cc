#include "resultserializer.h"

namespace reindexer {
namespace client {

ResultSerializer::QueryParams ResultSerializer::GetRawQueryParams(std::function<void(int nsId)> updatePayloadFunc) {
	(void)updatePayloadFunc;
	QueryParams ret;

	// cptr flag - skip
	GetUInt64();

	ret.totalcount = GetVarUint();
	ret.qcount = GetVarUint();
	ret.count = GetVarUint();
	ret.haveProcent = (GetVarUint() != 0);
	ret.nonCacheableData = (GetVarUint() != 0);
	ret.nsCount = GetVarUint();

	int ptCount = GetVarUint();

	for (int i = 0; i < ptCount; i++) {
		int nsid = GetVarUint();

		assert(updatePayloadFunc != nullptr);
		updatePayloadFunc(nsid);
	}

	int aggResCount = GetVarUint();

	for (int i = 0; i < aggResCount; i++) {
		ret.aggResults.push_back(GetDouble());
	}

	return ret;
}

ResultSerializer::ItemParams ResultSerializer::GetItemParams() {
	ItemParams ret;
	ret.id = int(GetVarUint());
	ret.version = int(GetVarUint());
	ret.nsid = int(GetVarUint());
	ret.proc = int(GetVarUint());
	int format = int(GetVarUint());

	switch (format) {
		case kResultsWithJson:
		case kResultsWithCJson:
			ret.data = GetSlice();
			break;
		default:
			break;
	}

	return ret;
}

}  // namespace client
}  // namespace reindexer
