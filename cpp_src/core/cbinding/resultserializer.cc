#include "resultserializer.h"
#include "core/cjson/jsonbuilder.h"
#include "core/cjson/tagsmatcher.h"
#include "core/queryresults/joinresults.h"
#include "core/queryresults/queryresults.h"
#include "core/type_consts.h"
#include "tools/logger.h"
#include "wal/walrecord.h"

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

void WrResultSerializer::putQueryParams(const BindingCapabilities& caps, QueryResults& results) {
	// Flags of present objects
	PutVarUint(opts_.flags);
	// Total
	PutVarUint(results.TotalCount());
	// Count of returned items by query
	PutVarUint(results.Count());
	// Count of serialized items
	PutVarUint(opts_.fetchLimit);

	if (opts_.flags & kResultsWithPayloadTypes) {
		assertrx(opts_.ptVersions.data());
		const auto mergedNsCount = results.GetMergedNSCount();
		if (int(opts_.ptVersions.size()) != mergedNsCount) {
			logFmt(LogWarning, "ptVersionsCount != results->GetMergedNSCount: {} != {}. Client's metadata can become inconsistent.",
				   opts_.ptVersions.size(), mergedNsCount);
		}
		auto cntP = getPtUpdatesCount(results);
		putPayloadTypes(*this, results, opts_, cntP.first, cntP.second);
	}

	putExtraParams(caps, results);
}

void WrResultSerializer::putExtraParams(const BindingCapabilities& caps, QueryResults& results) {
	if (opts_.withAggregations) {
		for (const AggregationResult& aggregationRes : results.GetAggregationResults()) {
			PutVarUint(QueryResultAggregation);
			auto slicePosSaver = StartSlice();
			if ((opts_.flags & kResultsFormatMask) == kResultsMsgPack) {
				aggregationRes.GetMsgPack(*this);
			} else {
				aggregationRes.GetJSON(*this);
			}
		}

		if (!results.GetExplainResults().empty()) {
			PutVarUint(QueryResultExplain);
			PutSlice(results.GetExplainResults());
		}
	}

	if (opts_.flags & kResultsWithShardId) {
		if (!results.IsDistributed() && results.Count() > 0) {
			PutVarUint(QueryResultShardId);
			PutVarUint(results.GetCommonShardID());
			opts_.flags &= ~kResultsWithShardId;  // not set shardId for item
		}
	}

	if (caps.HasIncarnationTags()) {
		int64_t shardingConfVer = results.GetShardingConfigVersion();
		if (shardingConfVer != -1) {
			PutVarUint(QueryResultShardingVersion);
			PutVarUint(shardingConfVer);
		}
		PutVarUint(QueryResultIncarnationTags);
		auto tags = results.GetIncarnationTags();
		PutVarUint(tags.size());
		for (auto& shardTags : tags) {
			PutVarint(shardTags.shardId);
			PutVarUint(shardTags.tags.size());
			for (auto& t : shardTags.tags) {
				PutVarint(int64_t(t));
			}
		}
	}

	if (caps.HasComplexRank() && results.HaveRank() && (opts_.flags & kResultsWithRank)) {
		PutVarUint(QueryResultRankFormat);
		PutVarUint(RankFormat::SingleFloatValue);
	}

	PutVarUint(QueryResultEnd);
}

static ItemRef GetItemRefWithStore(const LocalQueryResults::Iterator& it, QueryResults::ProxiedRefsStorage*) { return it.GetItemRef(); }

static ItemRef GetItemRefWithStore(QueryResults::Iterator& it, QueryResults::ProxiedRefsStorage* storage) { return it.GetItemRef(storage); }

template <typename ItT>
void WrResultSerializer::putItemParams(ItT& it, int shardId, QueryResults::ProxiedRefsStorage* storage, const QueryResults* result,
									   const BindingCapabilities& caps) {
	const auto itemRef = GetItemRefWithStore(it, storage);

	if (opts_.flags & kResultsWithItemID) {
		PutVarUint(itemRef.Id());
		PutVarUint(uint64_t(itemRef.Value().GetLSN()));
	}

	if (opts_.flags & kResultsWithNsID) {
		PutVarUint(itemRef.Nsid());
	}

	if (opts_.flags & kResultsWithRank) {
		const RankT rank = it.IsRanked() ? it.GetItemRefRanked().Rank() : RankT{};
		if (caps.HasComplexRank()) {
			PutRank(rank);
		} else {
			PutVarUint(uint16_t(rank.Value()));
		}
	}

	if (opts_.flags & kResultsWithRaw) {
		PutBool(itemRef.Raw());
		if (itemRef.Raw()) {
			PutSlice(it.GetRaw());
			return;
		}
	}

	if (opts_.flags & kResultsWithShardId) {
		PutVarUint(shardId);
	}

	if (result && result->IsWALQuery() && (opts_.flags & kResultsFormatMask) == kResultsJson) {
		auto slicePosSaver = StartSlice();
		JsonBuilder builder(*this, ObjType::TypePlain);
		auto obj = builder.Object();
		{
			auto lsnObj = obj.Object(kWALParamLsn);
			itemRef.Value().GetLSN().GetJSON(lsnObj);
		}
		if (!itemRef.Raw()) {
			obj.Raw(kWALParamItem, "");
			auto err = it.GetJSON(*this, false);
			if (!err.ok()) {
				throw Error(err.code(), "Unable to get JSON for WAL item: {}", err.what());
			}
		} else {
			reindexer::WALRecord rec(it.GetRaw());
			rec.GetJSON(obj, [&itemRef, result](std::string_view cjson) {
				ItemImpl item(result->GetPayloadType(itemRef.Nsid()), result->GetTagsMatcher(itemRef.Nsid()));
				item.FromCJSON(cjson);
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
			throw Error(errParams, "Can't serialize query results: unknown format {}", int((opts_.flags & kResultsFormatMask)));
	}
	if (!err.ok()) {
		throw Error(errParseBin, "Internal error serializing query results: {}", err.what());
	}
}

void WrResultSerializer::putPayloadTypes(WrSerializer& ser, const QueryResults& results, const ResultFetchOpts& opts, int cnt,
										 int totalCnt) {
	ser.PutVarUint(cnt);
	for (int nsid = 0; nsid < totalCnt; ++nsid) {
		const TagsMatcher& tm = results.GetTagsMatcher(nsid);
		if (int32_t(tm.version() ^ tm.stateToken()) != opts.ptVersions[nsid]) {
			ser.PutVarUint(nsid);
			ser.PutVString(results.GetPayloadType(nsid)->Name());
			const PayloadType& t = results.GetPayloadType(nsid);
			// Serialize tags matcher
			ser.PutVarUint(tm.stateToken());
			ser.PutVarUint(tm.version());
			tm.serialize(ser);
			// Serialize payload type
			t->serialize(ser);
		}
	}
}

std::pair<int, int> WrResultSerializer::getPtUpdatesCount(const QueryResults& results) {
	if (opts_.flags & kResultsWithPayloadTypes) {
		assertrx(opts_.ptVersions.data());
		const auto mergedNsCount = results.GetMergedNSCount();
		if (int(opts_.ptVersions.size()) != mergedNsCount) {
			logFmt(LogWarning, "ptVersionsCount != results->GetMergedNSCount: {} != {}. Client's meta data can become inconsistent.",
				   opts_.ptVersions.size(), mergedNsCount);
		}
		int cnt = 0, totalCnt = std::min(mergedNsCount, int(opts_.ptVersions.size()));

		for (int i = 0; i < totalCnt; i++) {
			const TagsMatcher& tm = results.GetTagsMatcher(i);
			if (int32_t(tm.version() ^ tm.stateToken()) != opts_.ptVersions[i]) {
				++cnt;
			}
		}
		return std::make_pair(cnt, totalCnt);
	}
	return std::make_pair(0, 0);
}

bool WrResultSerializer::PutResults(QueryResults& result, const BindingCapabilities& caps, QueryResults::ProxiedRefsStorage* storage) {
	if (result.IsWALQuery() && !(opts_.flags & kResultsWithRaw) && (opts_.flags & kResultsFormatMask) != kResultsJson) {
		throw Error(errParams,
					"Query results contain WAL items. Query results from WAL must either be requested in JSON format or with client, "
					"supporting RAW items");
	}
	if (opts_.fetchOffset > result.Count()) {
		opts_.fetchOffset = result.Count();
	}

	if (opts_.fetchOffset + opts_.fetchLimit > result.Count()) {
		opts_.fetchLimit = result.Count() - opts_.fetchOffset;
	}

	// Result has items from multiple namespaces, so pass nsid to each item
	if (result.GetMergedNSCount() > 1) {
		opts_.flags |= kResultsWithNsID;
	}
	// Result has joined items, so pass them to client within items from main NS
	if (result.HaveJoined()) {
		opts_.flags |= kResultsWithJoined;
	}

	if (result.HaveRank()) {
		opts_.flags |= kResultsWithRank;
	}
	if (result.NeedOutputRank()) {
		opts_.flags |= kResultsNeedOutputRank;
	}
	// If data is not cacheable, just do not pass item's ID and LSN. Clients should not cache this data
	if (!result.IsCacheEnabled()) {
		opts_.flags &= ~kResultsWithItemID;
	}
	// MsgPack items contain fields names so there is no need to transfer payload types
	// and joined data, as well as for JSON (they both contain it already)
	if ((opts_.flags & kResultsFormatMask) == kResultsJson || (opts_.flags & kResultsFormatMask) == kResultsMsgPack) {
		opts_.flags &= ~(kResultsWithJoined | kResultsWithPayloadTypes);
	}

	// client with version 'compareVersionShardId' not support shardId
	const bool resultsHaveShardIDs = result.HaveShardIDs();
	if (resultsHaveShardIDs && (opts_.flags & kResultsWithItemID) && !(opts_.flags & kResultsWithShardId)) {
		if (caps.HasResultsWithShardIDs()) {
			opts_.flags |= kResultsWithShardId;
		} else {
			opts_.flags &= ~kResultsWithItemID;
		}
	}
	if (!resultsHaveShardIDs) {
		opts_.flags &= ~kResultsWithShardId;
	}

	putQueryParams(caps, result);
	size_t saveLen = len_;
	const bool storeAsPointers = (opts_.flags & kResultsFormatMask) == kResultsPtrs;
	auto ptrStorage = storeAsPointers ? storage : nullptr;
	if (ptrStorage && result.HasProxiedResults()) {
		storage->reserve(result.HaveJoined() ? 2 * opts_.fetchLimit : opts_.fetchLimit);
	}

	auto rowIt = result.begin() + opts_.fetchOffset;
	for (unsigned i = 0, limit = opts_.fetchLimit; i < limit; ++i, ++rowIt) {
		// Put Item ID and version
		putItemParams(rowIt, rowIt.GetShardId(), storage, &result, caps);
		if (opts_.flags & kResultsWithJoined) {
			auto jIt = rowIt.GetJoined(storage);
			PutVarUint(jIt.getJoinedItemsCount() > 0 ? jIt.getJoinedFieldsCount() : 0);
			if (jIt.getJoinedItemsCount() > 0) {
				size_t joinedField = rowIt.GetJoinedField();
				for (auto it = jIt.begin(), end = jIt.end(); it != end; ++it, ++joinedField) {
					PutVarUint(it.ItemsCount());
					if (it.ItemsCount() == 0) {
						continue;
					}
					LocalQueryResults qr = it.ToQueryResults();
					qr.addNSContext(result, joinedField, lsn_t());
					for (auto& jit : qr) {
						putItemParams(jit, rowIt.GetShardId(), storage, nullptr, caps);
					}
				}
			}
		}
		if (i == 0) {
			grow((opts_.fetchLimit - 1) * (len_ - saveLen));
		}
	}
	return opts_.fetchOffset + opts_.fetchLimit >= result.Count();
}

bool WrResultSerializer::PutResultsRaw(QueryResults& result, std::string_view* rawBufOut) {
	if (opts_.fetchOffset > result.Count()) {
		opts_.fetchOffset = result.Count();
	}

	if (opts_.fetchOffset + opts_.fetchLimit > result.Count()) {
		opts_.fetchLimit = result.Count() - opts_.fetchOffset;
	}

	result.FetchRawBuffer(opts_.flags, opts_.fetchOffset, opts_.fetchLimit);

	client::ParsedQrRawBuffer raw;
	const bool holdsRemoteData = result.GetRawProxiedBuffer(raw);
	auto cntP = getPtUpdatesCount(result);
	auto& buf = *raw.buf;
	if (cntP.first) {
		Serializer ser(buf.data(), buf.size());
		if (!raw.parsingData.pts.begin || !raw.parsingData.pts.end) {
			throw Error(errLogic, "Unexpected payload types offset in proxied RAW query results. [{}, {}]", raw.parsingData.pts.begin,
						raw.parsingData.pts.end);
		}

		// Inject new payload types
		Write(std::string_view(buf.data(), raw.parsingData.pts.begin));
		putPayloadTypes(*this, result, opts_, cntP.first, cntP.second);
		Write(std::string_view(buf.data() + raw.parsingData.pts.end, buf.size() - raw.parsingData.pts.end));
	} else if (rawBufOut) {
		*rawBufOut = std::string_view(buf.data(), buf.size());
	} else {
		Write(std::string_view(buf.data(), buf.size()));
	}
	return !holdsRemoteData;
}

}  // namespace reindexer
