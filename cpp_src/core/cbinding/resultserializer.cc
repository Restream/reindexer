#include "resultserializer.h"
#include "core/cjson/tagsmatcher.h"
#include "core/query/queryresults.h"
#include "tools/logger.h"

namespace reindexer {

WrResultSerializer::WrResultSerializer(bool allowInBuf, const ResultFetchOpts& opts) : WrSerializer(allowInBuf), opts_(opts) {}

void WrResultSerializer::putQueryParams(const QueryResults* results) {
	// Pointer to query results
	if (opts_.flags & kResultsWithPtrs) {
		PutUInt64(uintptr_t(results));
	} else {
		PutUInt64(0);
	}

	// Total
	PutVarUint(results->totalCount);
	// Count of returned items by query
	PutVarUint(results->Count());
	// Count of serialized items
	PutVarUint(opts_.fetchLimit);

	PutVarUint(results->haveProcent);
	PutVarUint(results->nonCacheableData);

	// Count of namespaces
	PutVarUint(results->ctxs.size());

	if (opts_.flags & kResultsWithPayloadTypes) {
		assert(opts_.ptVersions);
		if (opts_.ptVersionsCount != results->getMergedNSCount()) {
			logPrintf(LogWarning, "ptVersionsCount != results->getMergedNSCount. Client's meta data can become incosistent.");
		}
		int cnt = 0, totalCnt = std::min(results->getMergedNSCount(), opts_.ptVersionsCount);

		for (int i = 0; i < totalCnt; i++) {
			const TagsMatcher& tm = results->getTagsMatcher(i);
			if (int32_t(tm.version() ^ tm.cacheToken()) != opts_.ptVersions[i]) cnt++;
		}
		PutVarUint(cnt);
		for (int i = 0; i < totalCnt; i++) {
			const TagsMatcher& tm = results->getTagsMatcher(i);
			if (int32_t(tm.version() ^ tm.cacheToken()) != opts_.ptVersions[i]) {
				PutVarUint(i);
				putPayloadType(results, i);
			}
		}
	} else {
		PutVarUint(0);
	}

	putAggregationParams(results);
}

void WrResultSerializer::putAggregationParams(const QueryResults* results) {
	PutVarUint(results->aggregationResults.size());
	for (auto ar : results->aggregationResults) PutDouble(ar);
}

void WrResultSerializer::putItemParams(const QueryResults* result, int idx, bool useOffset) {
	int ridx = idx + (useOffset ? opts_.fetchOffset : 0);

	auto it = result->begin() + ridx;
	auto itemRef = it.GetItemRef();

	PutVarUint(itemRef.id);
	PutVarUint(itemRef.version);
	PutVarUint(itemRef.nsid);
	PutVarUint(itemRef.proc);
	int format = (opts_.flags & 0x3);

	if (idx < 63 && !(opts_.fetchDataMask & (1ULL << idx))) {
		format = kResultsPure;
	}

	PutVarUint(format);

	switch (format) {
		case kResultsWithJson:
			it.GetJSON(*this);
			break;
		case kResultsWithCJson:
			it.GetCJSON(*this);
			break;
		case kResultsWithPtrs:
			PutUInt64(uintptr_t(itemRef.value.Ptr()));
			break;
		case kResultsPure:
			break;
		default:
			abort();
	}
}

void WrResultSerializer::putPayloadType(const QueryResults* results, int nsid) {
	const PayloadType& t = results->getPayloadType(nsid);
	const TagsMatcher& m = results->getTagsMatcher(nsid);

	// Serialize tags matcher
	PutVarUint(m.cacheToken());
	PutVarUint(m.version());
	m.serialize(*this);

	// Serialize payload type
	t->serialize(*this);
}

bool WrResultSerializer::PutResults(const QueryResults* result) {
	if (opts_.fetchOffset > result->Count()) {
		opts_.fetchOffset = result->Count();
	}

	if (opts_.fetchOffset + opts_.fetchLimit > result->Count()) {
		opts_.fetchLimit = result->Count() - opts_.fetchOffset;
	}

	putQueryParams(result);

	for (unsigned i = 0; i < opts_.fetchLimit; i++) {
		// Put Item ID and version
		putItemParams(result, i, true);

		if ((opts_.flags & 0x3) == kResultsWithJson) {
			PutVarUint(0);
			continue;
		}

		auto rowIt = result->begin() + (i + opts_.fetchOffset);

		const QRVector& jres = rowIt.GetJoined();
		// Put count of joined subqueires for item ID
		PutVarUint(jres.size());
		for (auto& jfres : jres) {
			// Put count of returned items from joined namespace
			PutVarUint(jfres.Count());
			for (unsigned j = 0; j < jfres.Count(); j++) {
				putItemParams(&jfres, j, false);
			}
		}
	}
	return opts_.fetchOffset + opts_.fetchLimit >= result->Count();
}

}  // namespace reindexer
