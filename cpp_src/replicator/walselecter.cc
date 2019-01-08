
#include "walselecter.h"
#include "core/cjson/jsonbuilder.h"
#include "core/namespace.h"

namespace reindexer {

WALSelecter::WALSelecter(Namespace *ns) : ns_(ns) {}

void WALSelecter::operator()(QueryResults &result, SelectCtx &params) {
	const Query &q = params.query;
	int count = q.count;
	int start = q.start;
	result.totalCount = 0;

	if (q.entries.size() != 1 || q.entries[0].values.size() != 1 || q.entries[0].condition != CondGt)
		throw Error(errLogic, "Query to WAL should contain only 1 condition '#lsn > number'");

	int64_t fromLSN = q.entries[0].values[0].As<int64_t>();

	if (ns_->wal_.is_outdated(fromLSN))
		throw Error(errOutdatedWAL, "Query to WAL with outdated LSN %ld, LSN counter %ld", long(fromLSN), long(ns_->wal_.LSNCounter()));

	if (ns_->repl_.slaveMode) throw Error(errNoWAL, "Query to WAL, but WAL is disabled. Set replication role to master to continue");

	result.addNSContext(ns_->payloadType_, ns_->tagsMatcher_, FieldsSet(ns_->tagsMatcher_, q.selectFilter_));
	putReplState(result);

	for (auto it = ns_->wal_.upper_bound(fromLSN); it != ns_->wal_.end(); ++it) {
		WALRecord rec = *it;
		switch (rec.type) {
			case WalItemUpdate:
				if (ns_->items_[rec.id].IsFree()) break;
				if (start) {
					start--;
				} else if (count) {
					// Put as usual ItemRef
					assertf(ns_->items_[rec.id].GetLSN() == it.GetLSN(), "lsn %ld != %ld, ns=%s", long(ns_->items_[rec.id].GetLSN()),
							long(it.GetLSN()), ns_->name_.c_str());
					result.Add(ItemRef(rec.id, ns_->items_[rec.id]));
					count--;
				}
				result.totalCount++;
				break;
			case WalIndexAdd:
			case WalIndexDrop:
			case WalIndexUpdate:
			case WalPutMeta:
			case WalUpdateQuery:
			case WalItemModify:
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
}

void WALSelecter::putReplState(QueryResults &result) {
	// prepare json with replication state
	ReplicationState replState = ns_->repl_;
	WrSerializer ser;
	JsonBuilder jb(ser);
	replState.GetJSON(jb);
	jb.End();

	// wrap JSON into PackedWALRecord
	PackedWALRecord wr;
	wr.Pack(WALRecord(WalReplState, ser.Slice()));

	// Put as ItemRef with raw container
	PayloadValue pv(wr.size(), wr.data());
	pv.SetLSN(-1);
	result.Add(ItemRef(-1, pv, 0, 0, true));
}

};  // namespace reindexer
