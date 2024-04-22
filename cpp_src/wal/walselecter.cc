#include "walselecter.h"
#include "core/cjson/jsonbuilder.h"
#include "core/namespace/namespaceimpl.h"
#include "core/nsselecter/nsselecter.h"
#include "core/rdxcontext.h"
#include "tools/semversion.h"

namespace reindexer {

const SemVersion kMinUnknownReplSupportRxVersion("2.6.0");

WALSelecter::WALSelecter(const NamespaceImpl *ns, bool allowTxWithoutBegining) : ns_(ns), allowTxWithoutBegining_(allowTxWithoutBegining) {}

void WALSelecter::operator()(LocalQueryResults &result, SelectCtx &params, bool snapshot) {
	using namespace std::string_view_literals;
	const Query &q = params.query;
	int count = q.Limit();
	int start = q.Offset();
	result.totalCount = 0;

	if (!q.IsWALQuery()) {
		throw Error(errLogic, "Query to WAL should contain only 1 condition '#lsn > number'");
	}

	result.addNSContext(ns_->payloadType_, ns_->tagsMatcher_, FieldsSet(ns_->tagsMatcher_, q.SelectFilters()), ns_->schema_,
						ns_->incarnationTag_);

	int lsnIdx = -1;
	int versionIdx = -1;
	for (size_t i = 0; i < q.Entries().Size(); ++i) {
		q.Entries().Visit(
			i,
			[&lsnIdx, &versionIdx, i] RX_PRE_LMBD_ALWAYS_INLINE(const QueryEntry &qe) RX_POST_LMBD_ALWAYS_INLINE {
				if ("#lsn"sv == qe.FieldName()) {
					lsnIdx = i;
				} else if ("#slave_version"sv == qe.FieldName()) {
					versionIdx = i;
				} else {
					throw Error(errLogic, "Unexpected index in WAL select query: %s", qe.FieldName());
				}
			},
			[&q] RX_PRE_LMBD_ALWAYS_INLINE(const auto &)
				RX_POST_LMBD_ALWAYS_INLINE { throw Error(errLogic, "Unexpected WAL select query: %s", q.GetSQL()); });
	}
	auto slaveVersion = versionIdx < 0 ? SemVersion() : SemVersion(q.Entries().Get<QueryEntry>(versionIdx).Values()[0].As<std::string>());
	auto &lsnEntry = q.Entries().Get<QueryEntry>(lsnIdx);
	if (lsnEntry.Values().size() == 1 && (lsnEntry.Condition() == CondGt || lsnEntry.Condition() == CondGe)) {
		lsn_t fromLSN = lsn_t(std::min(lsnEntry.Values()[0].As<int64_t>(), std::numeric_limits<int64_t>::max() - 1));
		if (fromLSN.isEmpty()) {
			throw Error(errOutdatedWAL, "Query to WAL with empty LSN, LSN counter %ld", ns_->wal_.LSNCounter());
		}
		if (lsnEntry.Condition() == CondGt && ns_->wal_.LSNCounter() != (fromLSN.Counter() + 1) && ns_->wal_.is_outdated(fromLSN) &&
			count) {
			throw Error(errOutdatedWAL, "Query (gt) to WAL with outdated LSN %ld, LSN counter %ld, walSize = %d, count = %d",
						int64_t(fromLSN), ns_->wal_.LSNCounter(), ns_->wal_.size(), count);
		}
		if (lsnEntry.Condition() == CondGe && ns_->wal_.is_outdated(fromLSN) && count) {
			throw Error(errOutdatedWAL, "Query (ge) to WAL with outdated LSN %ld, LSN counter %ld, walSize = %d, count = %d",
						int64_t(fromLSN), ns_->wal_.LSNCounter(), ns_->wal_.size(), count);
		}

		const auto walEnd = ns_->wal_.end();
		auto putWalRecord = [&result](WALTracker::iterator it, const WALRecord &rec) {
			auto data = it.GetRaw();
			// Put as ItemRef with raw container
			PayloadValue pv(data.size(), data.data());
			pv.SetLSN(it.GetLSN());
			result.Add(ItemRef(rec.id, pv, 0, 0, true));
		};
		const auto firstIt = lsnEntry.Condition() == CondGt ? ns_->wal_.upper_bound(fromLSN) : ns_->wal_.inclusive_upper_bound(fromLSN);
		if (firstIt != walEnd) {
			WALRecord firstRec = *firstIt;
			if (!allowTxWithoutBegining_ && firstRec.inTransaction && firstRec.type != WalInitTransaction) {
				throw Error(errOutdatedWAL, "WAL starts from init tx record. LSN: %d, type: %d", firstIt.GetLSN(), firstRec.type);
			}
		}
		for (auto it = firstIt; count && it != walEnd; ++it) {
			WALRecord rec = *it;
			switch (rec.type) {
				case WalItemUpdate:
					if (ns_->items_[rec.id].IsFree()) {
						if (snapshot) {
							assertrx(!start);
							assertrx(count < 0);
							putWalRecord(it, rec);
						}
						break;
					}
					if (start) {
						start--;
					} else if (count) {
						// Put as usual ItemRef
						assertf(ns_->items_[rec.id].GetLSN() == it.GetLSN(), "lsn %ld != %ld, ns=%s", ns_->items_[rec.id].GetLSN(),
								it.GetLSN(), ns_->name_);
						result.Add(ItemRef(rec.id, ns_->items_[rec.id]));
						count--;
					}
					result.totalCount++;
					break;
				case WalInitTransaction:
				case WalCommitTransaction:
					if (!snapshot) {
						if (versionIdx < 0) {
							break;
						}
						if (q.Entries().Get<QueryEntry>(versionIdx).Condition() != CondEq ||
							slaveVersion < kMinUnknownReplSupportRxVersion) {
							break;
						}
					}
					// fall-through
				case WalIndexAdd:
				case WalIndexDrop:
				case WalIndexUpdate:
				case WalPutMeta:
				case WalDeleteMeta:
				case WalUpdateQuery:
				case WalItemModify:
				case WalSetSchema:
					if (!snapshot && rec.type == WalSetSchema && slaveVersion < kMinUnknownReplSupportRxVersion) {
						break;
					}
					if (start) {
						start--;
					} else if (count) {
						putWalRecord(it, rec);
						count--;
					}
					result.totalCount++;
					break;
				case WalEmpty:
					if (snapshot) {
						assertrx(!start);
						assertrx(count < 0);
						putWalRecord(it, rec);	// TODO: Check if it's possible to remove empty records from, snapshot
					}
					break;
				case WalReplState:
				case WalNamespaceAdd:
				case WalNamespaceDrop:
				case WalNamespaceRename:
				case WalForceSync:
				case WalWALSync:
				case WalRawItem:
				case WalShallowItem:
				case WalTagsMatcher:
				case WalResetLocalWal:
					std::abort();
			}
		}
	} else if (lsnEntry.Condition() == CondAny) {
		bool enableSpecialRecords = snapshot || !(slaveVersion < kMinUnknownReplSupportRxVersion);
		if (start == 0 && enableSpecialRecords) {
			auto addSpRecord = [&result](const WALRecord &wrec) {
				PackedWALRecord wr;
				wr.Pack(wrec);
				PayloadValue val(wr.size(), wr.data());
				val.SetLSN(lsn_t());
				result.Add(ItemRef(-1, val, 0, 0, true));
			};
			for (unsigned int i = 1; i < ns_->indexes_.size(); i++) {
				auto indexDef = ns_->getIndexDefinition(i);
				WrSerializer ser;
				indexDef.GetJSON(ser);
				WALRecord wrec(WalIndexAdd, ser.Slice());
				addSpRecord(wrec);
			}
			std::vector<std::string> metaKeys = ns_->enumMeta();
			for (const auto &key : metaKeys) {
				auto metaVal = ns_->getMeta(key);
				WALRecord wrec(WalPutMeta, key, metaVal, false);
				addSpRecord(wrec);
			}
			if (ns_->schema_) {
				WrSerializer ser;
				ns_->schema_->GetJSON(ser);
				WALRecord wrec(WalSetSchema, ser.Slice());
				addSpRecord(wrec);
			}
		}
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

void WALSelecter::putReplState(LocalQueryResults &result) {
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
	pv.SetLSN(lsn_t());
	result.Add(ItemRef(-1, pv, 0, 0, true));
}
}  // namespace reindexer
