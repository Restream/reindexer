#include "snapshothandler.h"
#include "core/namespace/namespace.h"
#include "core/namespace/namespaceimpl.h"
#include "core/nsselecter/nsselecter.h"
#include "tools/logger.h"
#include "wal/walselecter.h"

namespace reindexer {

Snapshot SnapshotHandler::CreateSnapshot(const SnapshotOpts& opts) const {
	LocalQueryResults walQr;
	const auto from = opts.from;
	try {
		if (!from.IsCompatibleByNsVersion(ExtendedLsn(ns_.repl_.nsVersion, ns_.wal_.LastLSN()))) {
			throw Error(errOutdatedWAL);
		}
		Query q = Query(ns_.name_).Where("#lsn", CondGt, int64_t(from.LSN()));
		SelectCtx selCtx(q, nullptr);
		SelectFunctionsHolder func;
		selCtx.functions = &func;
		selCtx.contextCollectingMode = true;
		WALSelecter selecter(&ns_, false);
		selecter(walQr, selCtx, true);
		return Snapshot(ns_.payloadType_, ns_.tagsMatcher_, ns_.repl_.nsVersion, ns_.wal_.LastLSN(), ns_.repl_.dataHash, ns_.ItemsCount(),
						ns_.repl_.clusterStatus, std::move(walQr));
	} catch (Error& err) {
		if (err.code() != errOutdatedWAL) {
			throw err;
		}
		const auto minLsn = ns_.wal_.LSNByOffset(opts.maxWalDepthOnForceSync);
		if (minLsn.isEmpty()) {
			return Snapshot(ns_.tagsMatcher_, ns_.repl_.nsVersion, ns_.repl_.dataHash, ns_.ItemsCount(), ns_.repl_.clusterStatus);
		}
		{
			Query q = Query(ns_.name_).Where("#lsn", CondGe, int64_t(minLsn));
			SelectCtx selCtx(q, nullptr);
			SelectFunctionsHolder func;
			selCtx.functions = &func;
			selCtx.contextCollectingMode = true;
			WALSelecter selecter(&ns_, true);
			selecter(walQr, selCtx, true);
		}

		LocalQueryResults fullQr;
		{
			Query q = Query(ns_.name_).Where("#lsn", CondAny, VariantArray{});
			SelectCtx selCtx(q, nullptr);
			SelectFunctionsHolder func;
			selCtx.functions = &func;
			selCtx.contextCollectingMode = true;
			WALSelecter selecter(&ns_, true);
			selecter(fullQr, selCtx, true);
		}

		return Snapshot(ns_.payloadType_, ns_.tagsMatcher_, ns_.repl_.nsVersion, ns_.wal_.LastLSN(), ns_.repl_.dataHash, ns_.ItemsCount(),
						ns_.repl_.clusterStatus, std::move(walQr), std::move(fullQr));
	}
}

void SnapshotHandler::ApplyChunk(const SnapshotChunk& ch, bool isInitialLeaderSync, h_vector<cluster::UpdateRecord, 2>& repl) {
	ChunkContext ctx;
	ctx.wal = ch.IsWAL();
	ctx.shallow = ch.IsShallow();
	ctx.initialLeaderSync = isInitialLeaderSync;
	for (auto& rec : ch.Records()) {
		applyRecord(rec, ctx, repl);
	}
}

void SnapshotHandler::applyRecord(const SnapshotRecord& snRec, const ChunkContext& ctx, h_vector<cluster::UpdateRecord, 2>& pendedRepl) {
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
		case WalShallowItem:
			ns_.wal_.Add(WALRecord(WalItemUpdate, WALRecord(prec).id, chCtx.tx), lsn);
			return errOK;
		case WalItemUpdate:
			ns_.wal_.Add(WALRecord(WalEmpty, WALRecord(prec).id, chCtx.tx), lsn);
			return errOK;
		case WalReplState:
			return errOK;
		case WalNamespaceAdd:
		case WalNamespaceDrop:
		case WalNamespaceRename:
		case WalForceSync:
		case WalWALSync:
		case WalTagsMatcher:
		case WalResetLocalWal:
		case WalRawItem:
		default:
			break;
	}

	return Error(errParams, "Unexpected record type for shallow record: %d", type);
}

Error SnapshotHandler::applyRealRecord(lsn_t lsn, const SnapshotRecord& snRec, const ChunkContext& chCtx,
									   h_vector<cluster::UpdateRecord, 2>& pendedRepl) {
	Error err;
	IndexDef iDef;
	Item item;
	NsContext ctx(dummyCtx_);
	ctx.InSnapshot(lsn, chCtx.wal, false, chCtx.initialLeaderSync);
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
				ns_.doAddIndex(iDef, false, pendedRepl, ctx);
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
			LocalQueryResults result;
			const Query q = Query::FromSQL(rec.data);
			switch (q.type_) {
				case QueryDelete:
					result.AddNamespace(&ns_, true);
					ns_.doDelete(q, result, pendedRepl, ctx);
					break;
				case QueryUpdate:
					result.AddNamespace(&ns_, true);
					ns_.doUpdate(q, result, pendedRepl, ctx);
					break;
				case QueryTruncate:
					ns_.doTruncate(pendedRepl, ctx);
					break;
				case QuerySelect:
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
			const auto version = ser.GetVarint();
			const auto stateToken = ser.GetVarint();
			tm.deserialize(ser, version, stateToken);
			logPrintf(LogInfo, "Changing tm's statetoken on %d: %08X->%08X", ns_.wal_.GetServer(), ns_.tagsMatcher_.stateToken(),
					  stateToken);
			ns_.tagsMatcher_ = std::move(tm);
			ns_.tagsMatcher_.UpdatePayloadType(ns_.payloadType_, NeedChangeTmVersion::No);
			ns_.tagsMatcher_.setUpdated();
			ns_.saveTagsMatcherToStorage(false);
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
		case WalShallowItem:
		default:
			err = Error(errLogic, "Unexpected WAL rec type %d\n", int(rec.type));
			break;
	}
	return err;
}

void SnapshotTxHandler::ApplyChunk(const SnapshotChunk& ch, bool isInitialLeaderSync, const RdxContext& rdxCtx) {
	assertrx(ch.IsTx());
	assertrx(!ch.IsShallow());
	assertrx(ch.IsWAL());
	auto& records = ch.Records();
	if (records.empty()) {
		throw Error(errParams, "Unexpected tx chunk size: empty", ch.Records().size());
	}
	WALRecord initRec(records.front().Record());
	if (initRec.type != WalInitTransaction) {
		throw Error(errParams, "Unexpected tx chunk init record type: %d. LSN: %d", initRec.type, records.front().LSN());
	}
	if (records.size() == 1) {
		throw Error(errParams, "Unexpected tx chunk size: 1 (at least 2 required)");
	}
	WALRecord commitRecord(records.back().Record());
	if (commitRecord.type != WalCommitTransaction) {
		throw Error(errParams, "Unexpected tx chunk commit record type: %d. LSN: %d", commitRecord.type, records.back().LSN());
	}
	auto tx = Transaction(ns_.NewTransaction(RdxContext(ch.Records().front().LSN())));
	for (size_t i = 1; i < records.size() - 1; ++i) {
		auto lsn = records[i].LSN();
		WALRecord wrec(records[i].Record());
		switch (wrec.type) {
			case WalItemModify: {
				Item item = tx.NewItem();
				auto err = item.Unsafe().FromCJSON(wrec.itemModify.itemCJson, false);
				if (!err.ok()) throw err;
				err = tx.Modify(std::move(item), static_cast<ItemModifyMode>(wrec.itemModify.modifyMode), lsn);
				if (!err.ok()) throw err;
				break;
			}
			case WalUpdateQuery: {
				auto err = tx.Modify(Query::FromSQL(wrec.data), lsn);
				if (!err.ok()) throw err;
				break;
			}
			case WalRawItem: {
				Serializer ser(wrec.rawItem.itemCJson.data(), wrec.rawItem.itemCJson.size());
				Item item = tx.NewItem();
				auto err = item.Unsafe().FromCJSON(wrec.rawItem.itemCJson, false);
				if (!err.ok()) throw err;
				err = tx.Modify(std::move(item), ModeUpsert, lsn);
				if (!err.ok()) throw err;
				break;
			}
			case WalEmpty: {
				auto err = tx.Nop(lsn);
				if (!err.ok()) throw err;
				break;
			}
			case WalReplState:
			case WalItemUpdate:
			case WalIndexAdd:
			case WalIndexDrop:
			case WalIndexUpdate:
			case WalPutMeta:
			case WalNamespaceAdd:
			case WalNamespaceDrop:
			case WalNamespaceRename:
			case WalInitTransaction:
			case WalCommitTransaction:
			case WalForceSync:
			case WalSetSchema:
			case WalWALSync:
			case WalTagsMatcher:
			case WalResetLocalWal:
			case WalShallowItem:
				throw Error(errLogic, "Unexpected tx WAL rec type %d\n", wrec.type);
		}
	}

	LocalQueryResults qr;
	NsContext nsCtx = NsContext(rdxCtx);
	nsCtx.InSnapshot(ch.Records().back().LSN(), ch.IsWAL(), ch.IsLastChunk(), isInitialLeaderSync);
	auto ltx = Transaction::Transform(std::move(tx));
	ns_.CommitTransaction(ltx, qr, nsCtx);
}

}  // namespace reindexer
