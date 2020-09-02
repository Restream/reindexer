
#include "walselecter.h"
#include "core/cjson/jsonbuilder.h"
#include "core/namespace/namespaceimpl.h"
#include "core/nsselecter/nsselecter.h"
#include "tools/semversion.h"

namespace reindexer {

const SemVersion kMinUnknownReplSupportRxVersion("2.6.0");

WALSelecter::WALSelecter(const NamespaceImpl *ns) : ns_(ns) {}

void WALSelecter::operator()(QueryResults &result, SelectCtx &params) {
	const Query &q = params.query;
	int count = q.count;
	int start = q.start;
	result.totalCount = 0;

	if (!q.IsWALQuery()) {
		throw Error(errLogic, "Query to WAL should contain only 1 condition '#lsn > number'");
	}

	result.addNSContext(ns_->payloadType_, ns_->tagsMatcher_, FieldsSet(ns_->tagsMatcher_, q.selectFilter_));

	int lsnIdx = -1;
	int versionIdx = -1;
	for (size_t i = 0; i < q.entries.Size(); ++i) {
		if ("#lsn"_sv == q.entries[i].index) {
			lsnIdx = i;
		} else if ("#slave_version"_sv == q.entries[i].index) {
			versionIdx = i;
		} else {
			throw Error(errLogic, "Unexpected index in WAL select query: %s", q.entries[i].index);
		}
	}
	auto &lsnEntry = q.entries[lsnIdx];
	if (lsnEntry.values.size() == 1 && lsnEntry.condition == CondGt) {
		lsn_t fromLSN = lsn_t(std::min(lsnEntry.values[0].As<int64_t>(), std::numeric_limits<int64_t>::max() - 1));
		if (fromLSN.Server() != ns_->serverId_)
			throw Error(errOutdatedWAL, "Query to WAL with incorrect LSN %ld, LSN counter %ld", int64_t(fromLSN), ns_->wal_.LSNCounter());
		if (ns_->wal_.LSNCounter() != (fromLSN.Counter() + 1) && ns_->wal_.is_outdated(fromLSN.Counter() + 1) && count)
			throw Error(errOutdatedWAL, "Query to WAL with outdated LSN %ld, LSN counter %ld", int64_t(fromLSN), ns_->wal_.LSNCounter());

		auto slaveVersion = versionIdx < 0 ? SemVersion() : SemVersion(q.entries[versionIdx].values[0].As<string>());
		const auto walEnd = ns_->wal_.end();
		for (auto it = ns_->wal_.upper_bound(fromLSN.Counter()); count && it != walEnd; ++it) {
			WALRecord rec = *it;
			switch (rec.type) {
				case WalItemUpdate:
					if (ns_->items_[rec.id].IsFree()) break;
					if (start) {
						start--;
					} else if (count) {
						// Put as usual ItemRef
						assertf(lsn_t(ns_->items_[rec.id].GetLSN()).Counter() == (lsn_t(it.GetLSN()).Counter()), "lsn %ld != %ld, ns=%s",
								ns_->items_[rec.id].GetLSN(), it.GetLSN(), ns_->name_);
						result.Add(ItemRef(rec.id, ns_->items_[rec.id]));
						count--;
					}
					result.totalCount++;
					break;
				case WalInitTransaction:
				case WalCommitTransaction:
					if (versionIdx < 0) {
						break;
					}
					if (q.entries[versionIdx].condition != CondEq || slaveVersion < kMinUnknownReplSupportRxVersion) {
						break;
					}
					// fall-through
				case WalIndexAdd:
				case WalIndexDrop:
				case WalIndexUpdate:
				case WalPutMeta:
				case WalUpdateQuery:
				case WalItemModify:
				case WalSetSchema:
					if (rec.type == WalSetSchema && slaveVersion < kMinUnknownReplSupportRxVersion) {
						break;
					}
					if (start) {
						start--;
					} else if (count) {
						auto data = it.GetRaw();
						// Put as ItemRef with raw container
						PayloadValue pv(data.size(), data.data());
						pv.SetLSN(it.GetLSN());
						result.Add(ItemRef(rec.id, pv, 0, 0, true));
						count--;
					}
					result.totalCount++;
					break;
				case WalEmpty:
					break;
				default:
					std::abort();
			}
		}
	} else if (lsnEntry.condition == CondAny) {
		for (size_t id = 0; count && id < ns_->items_.size(); ++id) {
			if (ns_->items_[id].IsFree()) continue;
			if (start) {
				start--;
			} else if (count) {
				result.Add(ItemRef(id, ns_->items_[id]));
				count--;
			}
			result.totalCount++;
		}
	} else {
		throw Error(errLogic, "Query to WAL should contain condition '#lsn > number' or '#lsn is not null'");
	}
	putReplState(result);
}

void WALSelecter::putReplState(QueryResults &result) {
	WrSerializer ser;
	JsonBuilder jb(ser);
	// prepare json with replication state
	ns_->getReplState().GetJSON(jb);
	jb.End();

	// wrap JSON into PackedWALRecord
	PackedWALRecord wr;
	wr.Pack(WALRecord(WalReplState, ser.Slice()));

	// Put as ItemRef with raw container
	PayloadValue pv(wr.size(), wr.data());
	pv.SetLSN(-1);
	result.Add(ItemRef(-1, pv, 0, 0, true));
}
}  // namespace reindexer
