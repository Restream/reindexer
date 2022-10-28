#include "resultserializer.h"
#include "core/cjson/jsonbuilder.h"
#include "core/cjson/tagsmatcher.h"
#include "core/queryresults/joinresults.h"
#include "core/queryresults/queryresults.h"
#include "replicator/walrecord.h"
#include "tools/logger.h"

namespace reindexer {

constexpr uint64_t GetKnownFlagsBitMask(int maxFlagValue) {
	unsigned n = 1;
	while (maxFlagValue >>= 1) {
		++n;
	}
	return ~(~uint64_t(0) << n);
}

static_assert(GetKnownFlagsBitMask(kResultsFlagMaxValue) < uint64_t(std::numeric_limits<int>::max()), "Too large value for int mask");
constexpr int kKnownResultsFlagsMask = int(GetKnownFlagsBitMask(kResultsFlagMaxValue));

void WrResultSerializer::resetUnknownFlags() noexcept { opts_.flags &= kKnownResultsFlagsMask; }

void WrResultSerializer::putQueryParams(const QueryResults* results) {
	// Flags of present objects
	PutVarUint(opts_.flags);
	// Total
	PutVarUint(results->totalCount);
	// Count of returned items by query
	PutVarUint(results->Count());
	// Count of serialized items
	PutVarUint(opts_.fetchLimit);

	if (opts_.flags & kResultsWithPayloadTypes) {
		assertrx(opts_.ptVersions.data());
		if (int(opts_.ptVersions.size()) != results->getMergedNSCount()) {
			logPrintf(LogWarning, "ptVersionsCount != results->getMergedNSCount: %d != %d. Client's meta data can become incosistent.",
					  opts_.ptVersions.size(), results->getMergedNSCount());
		}
		int cnt = 0, totalCnt = std::min(results->getMergedNSCount(), int(opts_.ptVersions.size()));

		for (int i = 0; i < totalCnt; i++) {
			const TagsMatcher& tm = results->getTagsMatcher(i);
			if (int32_t(tm.version() ^ tm.stateToken()) != opts_.ptVersions[i]) cnt++;
		}
		PutVarUint(cnt);
		for (int i = 0; i < totalCnt; i++) {
			const TagsMatcher& tm = results->getTagsMatcher(i);
			if (int32_t(tm.version() ^ tm.stateToken()) != opts_.ptVersions[i]) {
				PutVarUint(i);
				PutVString(results->getPayloadType(i)->Name());
				putPayloadType(results, i);
			}
		}
	}

	putExtraParams(results);
}

void WrResultSerializer::putExtraParams(const QueryResults* results) {
	for (const AggregationResult& aggregationRes : results->aggregationResults) {
		PutVarUint(QueryResultAggregation);
		auto slicePosSaver = StartSlice();
		if ((opts_.flags & kResultsFormatMask) == kResultsMsgPack) {
			aggregationRes.GetMsgPack(*this);
		} else {
			aggregationRes.GetJSON(*this);
		}
	}

	if (!results->explainResults.empty()) {
		PutVarUint(QueryResultExplain);
		PutSlice(results->explainResults);
	}
	PutVarUint(QueryResultEnd);
}

void WrResultSerializer::putItemParams(const QueryResults* result, int idx, bool useOffset) {
	int ridx = idx + (useOffset ? opts_.fetchOffset : 0);

	auto it = result->begin() + ridx;
	auto& itemRef = it.GetItemRef();

	if (opts_.flags & kResultsWithItemID) {
		PutVarUint(itemRef.Id());
		PutVarUint(itemRef.Value().GetLSN());
	}

	if (opts_.flags & kResultsWithNsID) {
		PutVarUint(itemRef.Nsid());
	}

	if (opts_.flags & kResultsWithRank) {
		PutVarUint(itemRef.Proc());
	}

	if (opts_.flags & kResultsWithRaw) {
		PutBool(itemRef.Raw());
		if (itemRef.Raw()) {
			PutSlice(it.GetRaw());
			return;
		}
	}
	if (result->IsWALQuery() && (opts_.flags & kResultsFormatMask) == kResultsJson) {
		auto slicePosSaver = StartSlice();
		JsonBuilder builder(*this, ObjType::TypePlain);
		auto obj = builder.Object(nullptr);
		{
			const lsn_t lsn(itemRef.Value().GetLSN());
			auto lsnObj = obj.Object(kWALParamLsn);
			lsn.GetJSON(lsnObj);
		}
		if (!itemRef.Raw()) {
			obj.Raw(kWALParamItem, "");
			auto err = it.GetJSON(*this, false);
			if (!err.ok()) {
				throw Error(err.code(), "Unable to get JSON for WAL item: %s", err.what());
			}
		} else {
			reindexer::WALRecord rec(it.GetRaw());
			rec.GetJSON(obj, [&itemRef, &result](std::string_view cjson) {
				ItemImpl item(result->getPayloadType(itemRef.Nsid()), result->getTagsMatcher(itemRef.Nsid()));
				auto err = item.FromCJSON(cjson);
				if (!err.ok()) {
					throw Error(err.code(), "Unable to parse CJSON for WAL item: %s", err.what());
				}
				return std::string(item.GetJSON());
			});
		}
		return;
	}

	Error err;

	switch ((opts_.flags & kResultsFormatMask)) {
		case kResultsJson:
			err = it.GetJSON(*this);
			break;
		case kResultsCJson:
			err = it.GetCJSON(*this);
			break;
		case kResultsPtrs:
			PutUInt64(uintptr_t(itemRef.Value().Ptr()));
			break;
		case kResultsPure:
			break;
		case kResultsMsgPack:
			err = it.GetMsgPack(*this);
			break;
		default:
			throw Error(errParams, "Can't serialize query results: unknown format %d", int((opts_.flags & kResultsFormatMask)));
	}
	if (!err.ok()) throw Error(errParseBin, "Internal error serializing query results: %s", err.what());
}

void WrResultSerializer::putPayloadType(const QueryResults* results, int nsid) {
	const PayloadType& t = results->getPayloadType(nsid);
	const TagsMatcher& m = results->getTagsMatcher(nsid);

	// Serialize tags matcher
	PutVarUint(m.stateToken());
	PutVarUint(m.version());
	m.serialize(*this);

	// Serialize payload type
	t->serialize(*this);
}

bool WrResultSerializer::PutResults(const QueryResults* result) {
	if (result->IsWALQuery() && !(opts_.flags & kResultsWithRaw) && (opts_.flags & kResultsFormatMask) != kResultsJson) {
		throw Error(errParams,
					"Query results contain WAL items. Query results from WAL must either be requested in JSON format or with client, "
					"supporting RAW items");
	}
	if (opts_.fetchOffset > result->Count()) {
		opts_.fetchOffset = result->Count();
	}

	if (opts_.fetchOffset + opts_.fetchLimit > result->Count()) {
		opts_.fetchLimit = result->Count() - opts_.fetchOffset;
	}

	// Result has items from multiple namespaces, so pass nsid to each item
	if (result->getMergedNSCount() > 1) opts_.flags |= kResultsWithNsID;
	// Result has joined items, so pass them to client within items from main NS
	if (result->joined_.size() > 0) opts_.flags |= kResultsWithJoined;

	if (result->haveRank) opts_.flags |= kResultsWithRank;
	if (result->needOutputRank) opts_.flags |= kResultsNeedOutputRank;
	// If data is not cacheable, just do not pass item's ID and LSN. Clients should not cache this data
	if (result->nonCacheableData) opts_.flags &= ~kResultsWithItemID;
	// MsgPack items contain fields names so there is no need to transfer payload types
	// and joined data, as well as for JSON (they both contain it already)
	if ((opts_.flags & kResultsFormatMask) == kResultsJson || (opts_.flags & kResultsFormatMask) == kResultsMsgPack) {
		opts_.flags &= ~(kResultsWithJoined | kResultsWithPayloadTypes);
	}

	putQueryParams(result);
	size_t saveLen = len_;

	for (unsigned i = 0; i < opts_.fetchLimit; ++i) {
		// Put Item ID and version
		putItemParams(result, i, true);

		if (opts_.flags & kResultsWithJoined) {
			auto rowIt = result->begin() + (i + opts_.fetchOffset);
			auto jIt = rowIt.GetJoined();
			PutVarUint(jIt.getJoinedItemsCount() > 0 ? jIt.getJoinedFieldsCount() : 0);
			if (jIt.getJoinedItemsCount() > 0) {
				size_t joinedField = rowIt.qr_->joined_.size();
				for (size_t ns = 0; ns < rowIt.GetItemRef().Nsid(); ++ns) {
					joinedField += rowIt.qr_->joined_[ns].GetJoinedSelectorsCount();
				}
				for (auto it = jIt.begin(); it != jIt.end(); ++it, ++joinedField) {
					PutVarUint(it.ItemsCount());
					if (it.ItemsCount() == 0) continue;
					QueryResults qr = it.ToQueryResults();
					qr.addNSContext(result->getPayloadType(joinedField), result->getTagsMatcher(joinedField),
									result->getFieldsFilter(joinedField), result->getSchema(joinedField));
					for (size_t idx = 0; idx < qr.Count(); idx++) putItemParams(&qr, idx, false);
				}
			}
		}
		if (i == 0) grow((opts_.fetchLimit - 1) * (len_ - saveLen));
	}
	return opts_.fetchOffset + opts_.fetchLimit >= result->Count();
}

}  // namespace reindexer
