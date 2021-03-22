#include "snapshothandler.h"
#include "core/namespace/namespace.h"
#include "core/namespace/namespaceimpl.h"
#include "core/nsselecter/nsselecter.h"
#include "replicator/walselecter.h"

namespace reindexer {

Snapshot SnapshotHandler::CreateSnapshot(lsn_t from) const {
	QueryResults walQr;
	try {
		Query q = Query(ns_.name_).Where("#lsn", CondGt, int64_t(from));
		SelectCtx selCtx(q);
		SelectFunctionsHolder func;
		selCtx.functions = &func;
		selCtx.contextCollectingMode = true;
		WALSelecter selecter(&ns_);
		selecter(walQr, selCtx, true);
		return Snapshot(ns_.payloadType_, ns_.tagsMatcher_, ns_.wal_.LastLSN(), ns_.repl_.dataHash, std::move(walQr));
	} catch (Error& err) {
		if (err.code() != errOutdatedWAL) {
			throw err;
		}
		auto minLsn = ns_.wal_.FirstLSN();
		if (minLsn.isEmpty()) {
			return Snapshot();
		}
		{
			Query q = Query(ns_.name_).Where("#lsn", CondGt, int64_t(minLsn));
			SelectCtx selCtx(q);
			SelectFunctionsHolder func;
			selCtx.functions = &func;
			selCtx.contextCollectingMode = true;
			WALSelecter selecter(&ns_);
			selecter(walQr, selCtx, true);
		}

		QueryResults fullQr;
		{
			Query q = Query(ns_.name_).Where("#lsn", CondAny, {});
			SelectCtx selCtx(q);
			SelectFunctionsHolder func;
			selCtx.functions = &func;
			selCtx.contextCollectingMode = true;
			WALSelecter selecter(&ns_);
			selecter(fullQr, selCtx, true);
		}

		return Snapshot(ns_.payloadType_, ns_.tagsMatcher_, ns_.wal_.LastLSN(), ns_.repl_.dataHash, std::move(walQr), std::move(fullQr));
	}
}

void SnapshotHandler::ApplyChunk(const SnapshotChunk& ch, h_vector<cluster::UpdateRecord, 1>& repl) {
	ChunkContext ctx;
	ctx.wal = ch.IsWAL();
	ctx.shallow = ch.IsShallow();
	for (auto& rec : ch.Records()) {
		applyRecord(rec, ctx, repl);
	}
}

void SnapshotHandler::applyRecord(const SnapshotRecord& snRec, const ChunkContext& ctx, h_vector<cluster::UpdateRecord, 1>& pendedRepl) {
	Error err;
	if (ctx.shallow) {
		auto unpacked = snRec.Unpack();
		err = applyShallowRecord(snRec.LSN(), unpacked.type, snRec.Record(), ctx);
	} else {
		err = applyRealRecord(snRec.LSN(), snRec, ctx, pendedRepl);
	}
	if (!err.ok()) {
		throw err;
	}
}

Error SnapshotHandler::applyShallowRecord(lsn_t lsn, WALRecType type, const PackedWALRecord& prec, const ChunkContext& chCtx) {
	switch (type) {
		case WalEmpty:
		case WalIndexAdd:
		case WalIndexDrop:
		case WalPutMeta:
		case WalIndexUpdate:
		case WalItemModify:
		case WalInitTransaction:
		case WalCommitTransaction:
		case WalSetSchema:
		case WalUpdateQuery:
			ns_.wal_.Add(type, prec, lsn);
			return errOK;
		case WalRawItem:
			ns_.wal_.Add(rebuildUpdateWalRec(prec, chCtx), lsn);
			return errOK;
		case WalReplState:
			return errOK;
		case WalNamespaceAdd:
		case WalNamespaceDrop:
		case WalItemUpdate:
		case WalNamespaceRename:
		case WalForceSync:
		case WalWALSync:
		case WalTagsMatcher:
		case WalResetLocalWal:
		default:
			break;
	}

	return Error(errParams, "Unexpected record type for shallow record: %d", type);
}

Error SnapshotHandler::applyRealRecord(lsn_t lsn, const SnapshotRecord& snRec, const ChunkContext& chCtx,
									   h_vector<cluster::UpdateRecord, 1>& pendedRepl) {
	Error err;
	IndexDef iDef;
	Item item;
	NsContext ctx(dummyCtx_);
	ctx.InSnapshot(lsn, chCtx.wal);
	auto rec = snRec.Unpack();

	switch (rec.type) {
		// Modify item
		case WalItemModify: {
			item = ns_.newItem();
			err = item.FromCJSON(rec.itemModify.itemCJson, false);
			auto mode = static_cast<ItemModifyMode>(rec.itemModify.modifyMode);
			if (err.ok()) {
				if (mode == ModeDelete) {
					ns_.deleteItem(item, pendedRepl, ctx);
				} else {
					ns_.doModifyItem(item, rec.itemModify.modifyMode, pendedRepl, ctx);
				}
			}
			break;
		}
		// Index added
		case WalIndexAdd:
			err = iDef.FromJSON(giftStr(rec.data));
			if (err.ok()) {
				ns_.doAddIndex(iDef, pendedRepl, ctx);
				ns_.saveIndexesToStorage();
			}
			break;
		// Index dropped
		case WalIndexDrop:
			err = iDef.FromJSON(giftStr(rec.data));
			if (err.ok()) {
				ns_.doDropIndex(iDef, pendedRepl, ctx);
				ns_.saveIndexesToStorage();
			}
			break;
		// Index updated
		case WalIndexUpdate:
			err = iDef.FromJSON(giftStr(rec.data));
			if (err.ok()) {
				ns_.doUpdateIndex(iDef, pendedRepl, ctx);
				ns_.saveIndexesToStorage();
			}
			break;
		// Metadata updated
		case WalPutMeta:
			ns_.putMeta(std::string(rec.putMeta.key), rec.putMeta.value, pendedRepl, ctx);
			break;
		// Update query
		case WalUpdateQuery: {
			QueryResults result;
			Query q;
			q.FromSQL(rec.data);
			switch (q.type_) {
				case QueryDelete:
					ns_.doDelete(q, result, pendedRepl, ctx);
					break;
				case QueryUpdate:
					ns_.doUpdate(q, result, pendedRepl, ctx);
					break;
				case QueryTruncate:
					ns_.doTruncate(pendedRepl, ctx);
					break;
				default:
					break;
			}
			break;
		}
		case WalResetLocalWal: {
			ns_.wal_.Reset();
			break;
		}
		case WalSetSchema:
			ns_.setSchema(rec.data, pendedRepl, ctx);
			ns_.saveSchemaToStorage();
			break;
		case WalRawItem: {
			Serializer ser(rec.rawItem.itemCJson.data(), rec.rawItem.itemCJson.size());
			item = ns_.newItem();
			err = item.FromCJSON(rec.rawItem.itemCJson, false);
			if (err.ok()) ns_.doModifyItem(item, ModeUpsert, pendedRepl, ctx, (chCtx.wal) ? -1 : rec.rawItem.id);
			break;
		}
		case WalTagsMatcher: {
			TagsMatcher tm;
			Serializer ser(rec.data.data(), rec.data.size());
			tm.deserialize(ser);
			ns_.tagsMatcher_ = std::move(tm);
			ns_.tagsMatcher_.UpdatePayloadType(ns_.payloadType_);
			ns_.saveTagsMatcherToStorage();
			break;
		}
		case WalInitTransaction:
		case WalCommitTransaction:
			if (chCtx.wal) err = Error(errLogic, "Unexpected tx WAL record %d\n", int(rec.type));
			break;
		case WalEmpty:
			ns_.wal_.Add(WALRecord(WalEmpty), lsn);
			break;
		case WalReplState:
			break;
		case WalForceSync:
		case WalWALSync:
		case WalNamespaceRename:
		case WalNamespaceDrop:
		case WalNamespaceAdd:
		case WalItemUpdate:
		default:
			err = Error(errLogic, "Unexpected WAL rec type %d\n", int(rec.type));
			break;
	}
	return err;
}

WALRecord SnapshotHandler::rebuildUpdateWalRec(const PackedWALRecord& wrec, const ChunkContext& chCtx) {
	WALRecord unpacked(wrec);
	Serializer ser(unpacked.rawItem.itemCJson.data(), unpacked.rawItem.itemCJson.size());
	auto item = ns_.newItem();
	auto err = item.FromCJSON(unpacked.rawItem.itemCJson, false);
	if (!err.ok()) {
		throw err;
	}
	auto idp = ns_.findByPK(item.impl_, dummyCtx_);
	if (!idp.second) {
		throw Error(errParams, "Unable to find snapshot update by PK");
	}
	return WALRecord(WalItemUpdate, idp.first, chCtx.tx);
}

void SnapshotTxHandler::ApplyChunk(const SnapshotChunk& ch, const RdxContext& rdxCtx) {
	assert(ch.IsTx());
	assert(!ch.IsShallow());
	assert(ch.IsWAL());
	auto& records = ch.Records();
	if (records.size() < 2) {
		throw Error(errParams, "Unexpected tx chunk size: %d", ch.Records().size());
	}
	WALRecord initRec(records.front().Record());
	if (initRec.type != WalInitTransaction) {
		throw Error(errParams, "Unexpected tx chunk init record type: %d", initRec.type);
	}
	WALRecord commitRecord(records.back().Record());
	if (commitRecord.type != WalCommitTransaction) {
		throw Error(errParams, "Unexpected tx chunk commit record type: %d", commitRecord.type);
	}
	auto tx = ns_.NewTransaction(RdxContext(ch.Records().front().LSN()));
	for (size_t i = 1; i < records.size() - 1; ++i) {
		auto lsn = records[i].LSN();
		WALRecord wrec(records[i].Record());
		switch (wrec.type) {
			case WalItemModify: {
				Item item = tx.NewItem();
				auto err = item.FromCJSON(wrec.itemModify.itemCJson, false);
				if (!err.ok()) throw err;
				tx.Modify(std::move(item), static_cast<ItemModifyMode>(wrec.itemModify.modifyMode), lsn);
				break;
			}
			case WalUpdateQuery: {
				Query q;
				q.FromSQL(wrec.data);
				tx.Modify(std::move(q), lsn);
				break;
			}
			case WalRawItem: {
				Serializer ser(wrec.rawItem.itemCJson.data(), wrec.rawItem.itemCJson.size());
				Item item = tx.NewItem();
				auto err = item.FromCJSON(wrec.rawItem.itemCJson, false);
				if (!err.ok()) throw err;
				tx.Modify(std::move(item), ModeUpsert, lsn);
				break;
			}
			case WalEmpty: {
				tx.Nop(lsn);
				break;
			}
			default:
				throw Error(errLogic, "Unexpected tx WAL rec type %d\n", wrec.type);
		}
	}

	QueryResults qr;
	NsContext nsCtx = NsContext(rdxCtx);
	nsCtx.InSnapshot(ch.Records().back().LSN(), ch.IsWAL());
	ns_.CommitTransaction(tx, qr, nsCtx);
}

}  // namespace reindexer
