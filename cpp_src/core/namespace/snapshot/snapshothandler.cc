#include "snapshothandler.h"
#include "core/ft/functions/ft_function.h"
#include "core/namespace/namespace.h"
#include "core/namespace/namespaceimpl.h"
#include "core/nsselecter/selectctx.h"
#include "estl/gift_str.h"
#include "tools/logger.h"
#include "wal/walselecter.h"

namespace reindexer {

Snapshot SnapshotHandler::CreateSnapshot(const SnapshotOpts& opts) const {
	LocalQueryResults walQr;
	const auto from = opts.from;
	try {
		if (!from.IsCompatibleByNsVersion(ExtendedLsn(ns_.repl_.nsVersion, ns_.wal_.LastLSN()))) {
			throw Error(errOutdatedWAL, "Requested LSN is not compatible by NS version ({}). Current namespace has {}", from.NsVersion(),
						ns_.repl_.nsVersion);
		}
		Query q = Query(ns_.name_).Where("#lsn", CondGt, int64_t(from.LSN())).SelectAllFields();
		SelectCtx selCtx(q, nullptr, &walQr.GetFloatVectorsHolder());
		FtFunctionsHolder func;
		selCtx.functions = &func;
		selCtx.contextCollectingMode = true;
		WALSelecter selecter(&ns_, false);
		selecter(walQr, selCtx, true);
		return Snapshot(ns_.payloadType_, ns_.tagsMatcher_, ns_.repl_.nsVersion, ns_.wal_.LastLSN(), ns_.repl_.dataHash, ns_.itemsCount(),
						ns_.repl_.clusterStatus, std::move(walQr));
	} catch (Error& err) {
		if (err.code() != errOutdatedWAL) {
			throw err;
		}
		logFmt(LogInfo, "[repl:{}]:{} Creating RAW (force sync) snapshot. Reason: {}", ns_.name_, ns_.wal_.GetServer(), err.what());
		const auto minLsn = ns_.wal_.LSNByOffset(opts.maxWalDepthOnForceSync);
		if (minLsn.isEmpty()) {
			return Snapshot(ns_.tagsMatcher_, ns_.repl_.nsVersion, ns_.repl_.dataHash, ns_.itemsCount(), ns_.repl_.clusterStatus);
		}
		{
			Query q = Query(ns_.name_).Where("#lsn", CondGe, int64_t(minLsn)).SelectAllFields();
			SelectCtx selCtx(q, nullptr, &walQr.GetFloatVectorsHolder());
			FtFunctionsHolder func;
			selCtx.functions = &func;
			selCtx.contextCollectingMode = true;
			WALSelecter selecter(&ns_, true);
			selecter(walQr, selCtx, true);
		}

		LocalQueryResults fullQr;
		{
			Query q = Query(ns_.name_).Where("#lsn", CondAny, VariantArray{}).SelectAllFields();
			// Reusing walQr's FloatVectorsHolder here
			SelectCtx selCtx(q, nullptr, &walQr.GetFloatVectorsHolder());
			FtFunctionsHolder func;
			selCtx.functions = &func;
			selCtx.contextCollectingMode = true;
			WALSelecter selecter(&ns_, true);
			selecter(fullQr, selCtx, true);
		}

		return Snapshot(ns_.payloadType_, ns_.tagsMatcher_, ns_.repl_.nsVersion, ns_.wal_.LastLSN(), ns_.repl_.dataHash, ns_.itemsCount(),
						ns_.repl_.clusterStatus, std::move(walQr), std::move(fullQr));
	}
}

void SnapshotHandler::ApplyChunk(const SnapshotChunk& ch, bool isInitialLeaderSync, UpdatesContainer& repl) {
	ChunkContext ctx;
	ctx.wal = ch.IsWAL();
	ctx.shallow = ch.IsShallow();
	ctx.initialLeaderSync = isInitialLeaderSync;

	for (auto& rec : ch.Records()) {
		applyRecord(rec, ctx, repl);
	}
	ns_.storage_.TryForceFlush();
}

void SnapshotHandler::applyRecord(const SnapshotRecord& snRec, const ChunkContext& ctx, UpdatesContainer& pendedRepl) {
	if (ctx.shallow) {
		auto unpacked = snRec.Unpack();
		applyShallowRecord(snRec.LSN(), unpacked.type, snRec.Record(), ctx);
	} else {
		applyRealRecord(snRec.LSN(), snRec, ctx, pendedRepl);
	}
}

void SnapshotHandler::applyShallowRecord(lsn_t lsn, WALRecType type, const PackedWALRecord& prec, const ChunkContext& chCtx) {
	switch (type) {
		case WalEmpty:
		case WalIndexAdd:
		case WalIndexDrop:
		case WalPutMeta:
		case WalDeleteMeta:
		case WalIndexUpdate:
		case WalItemModify:
		case WalInitTransaction:
		case WalCommitTransaction:
		case WalSetSchema:
		case WalUpdateQuery:
			// NOLINTNEXTLINE (bugprone-unused-return-value)
			ns_.wal_.Add(type, prec, lsn);
			return;
		case WalShallowItem:
			// NOLINTNEXTLINE (bugprone-unused-return-value)
			ns_.wal_.Add(WALRecord(WalItemUpdate, WALRecord(prec).id, chCtx.tx), lsn);
			return;
		case WalItemUpdate:
			// NOLINTNEXTLINE (bugprone-unused-return-value)
			ns_.wal_.Add(WALRecord(WalEmpty, WALRecord(prec).id, chCtx.tx), lsn);
			return;
		case WalReplState:
			return;
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

	throw Error(errParams, "Unexpected record type for shallow record: {}", type);
}

void SnapshotHandler::applyRealRecord(lsn_t lsn, const SnapshotRecord& snRec, const ChunkContext& chCtx, UpdatesContainer& pendedRepl) {
	if (chCtx.wal && !lsn.isEmpty()) {
		ns_.checkSnapshotLSN(lsn);
	}

	Item item;
	NsContext ctx(dummyCtx_);
	std::ignore = ctx.InSnapshot(lsn, chCtx.wal, false, chCtx.initialLeaderSync);
	auto rec = snRec.Unpack();
	switch (rec.type) {
		// Modify item
		case WalItemModify: {
			item = ns_.newItem();
			auto err = item.FromCJSON(rec.itemModify.itemCJson, false);
			if (!err.ok()) {
				throw err;
			}
			const auto mode = static_cast<ItemModifyMode>(rec.itemModify.modifyMode);
			if (mode == ModeDelete) {
				ns_.deleteItem(item, pendedRepl, ctx);
			} else {
				ns_.doModifyItem(item, rec.itemModify.modifyMode, pendedRepl, ctx);
			}
			break;
		}
		// Index added
		case WalIndexAdd: {
			auto iDef = IndexDef::FromJSON(giftStr(rec.data));
			if (iDef) {
				ns_.doAddIndex(*iDef, false, pendedRepl, ctx);
				ns_.saveIndexesToStorage();
			} else {
				throw iDef.error();
			}
			break;
		}
		// Index dropped
		case WalIndexDrop: {
			auto iDef = IndexDef::FromJSON(giftStr(rec.data));
			if (iDef) {
				ns_.doDropIndex(*iDef, pendedRepl, ctx);
				ns_.saveIndexesToStorage();
			} else {
				throw iDef.error();
			}
			break;
		}
		// Index updated
		case WalIndexUpdate: {
			auto iDef = IndexDef::FromJSON(giftStr(rec.data));
			if (iDef) {
				std::ignore = ns_.doUpdateIndex(*iDef, pendedRepl, ctx);
				ns_.saveIndexesToStorage();
			} else {
				throw iDef.error();
			}
			break;
		}
		// Metadata updated
		case WalPutMeta:
			ns_.putMeta(std::string(rec.itemMeta.key), rec.itemMeta.value, pendedRepl, ctx);
			break;
		case WalDeleteMeta:
			ns_.deleteMeta(std::string(rec.itemMeta.key), pendedRepl, ctx);
			break;
		// Update query
		case WalUpdateQuery: {
			LocalQueryResults result;
			const Query q = Query::FromSQL(rec.data);
			switch (q.type_) {
				case QueryDelete:
					// TODO disabled due to #1771
					// Query can contain join query
					throw Error(errLogic, "Unexpected WAL update Query {}\n", rec.data);

				case QueryUpdate:
					// TODO disabled due to #1771
					throw Error(errLogic, "Unexpected WAL update Query {}\n", rec.data);
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
			auto err = item.FromCJSON(rec.rawItem.itemCJson, false);
			if (!err.ok()) {
				throw err;
			}
			ns_.doModifyItem(item, ModeUpsert, pendedRepl, ctx, (chCtx.wal) ? -1 : rec.rawItem.id);
			break;
		}
		case WalTagsMatcher: {
			TagsMatcher tm;
			Serializer ser(rec.data.data(), rec.data.size());
			const auto version = ser.GetVarint();
			const auto stateToken = ser.GetVarint();
			tm.deserialize(ser, version, stateToken);
			logFmt(LogInfo, "[{}]: Changing tm's statetoken on {}: {:#08x}->{:#08x}", ns_.name_, ns_.wal_.GetServer(),
				   ns_.tagsMatcher_.stateToken(), stateToken);
			ns_.tagsMatcher_ = std::move(tm);
			ns_.tagsMatcher_.UpdatePayloadType(ns_.payloadType_, ns_.indexes_.SparseIndexes(), NeedChangeTmVersion::No);
			ns_.tagsMatcher_.setUpdated();
			ns_.saveTagsMatcherToStorage(false);
			break;
		}
		case WalInitTransaction:
		case WalCommitTransaction:
			if (chCtx.wal) {
				throw Error(errLogic, "Unexpected tx WAL record {}\n", int(rec.type));
			}
			break;
		case WalEmpty:
			// NOLINTNEXTLINE (bugprone-unused-return-value)
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
			throw Error(errLogic, "Unexpected WAL rec type {}\n", int(rec.type));
	}
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
		throw Error(errParams, "Unexpected tx chunk init record type: {}. LSN: {}", initRec.type, records.front().LSN());
	}
	if (records.size() == 1) {
		throw Error(errParams, "Unexpected tx chunk size: 1 (at least 2 required)");
	}
	WALRecord commitRecord(records.back().Record());
	if (commitRecord.type != WalCommitTransaction) {
		throw Error(errParams, "Unexpected tx chunk commit record type: {}. LSN: {}", commitRecord.type, records.back().LSN());
	}

	auto tx = Transaction(ns_.NewTransaction(RdxContext{ch.Records().front().LSN(), rdxCtx.GetCancelCtx()}));
	for (size_t i = 1; i < records.size() - 1; ++i) {
		auto lsn = records[i].LSN();
		WALRecord wrec(records[i].Record());
		switch (wrec.type) {
			case WalItemModify: {
				Item item = tx.NewItem();
				auto err = item.Unsafe().FromCJSON(wrec.itemModify.itemCJson, false);
				if (!err.ok()) {
					throw err;
				}
				err = tx.Modify(std::move(item), static_cast<ItemModifyMode>(wrec.itemModify.modifyMode), lsn);
				if (!err.ok()) {
					throw err;
				}
				break;
			}
			case WalUpdateQuery: {
				auto err = tx.Modify(Query::FromSQL(wrec.data), lsn);
				if (!err.ok()) {
					throw err;
				}
				break;
			}
			case WalRawItem: {
				Item item = tx.NewItem();
				auto err = item.Unsafe().FromCJSON(wrec.rawItem.itemCJson, false);
				if (!err.ok()) {
					throw err;
				}
				err = tx.Modify(std::move(item), ModeUpsert, lsn);
				if (!err.ok()) {
					throw err;
				}
				break;
			}
			case WalEmpty: {
				auto err = tx.Nop(lsn);
				if (!err.ok()) {
					throw err;
				}
				break;
			}
			case WalReplState:
			case WalItemUpdate:
			case WalIndexAdd:
			case WalIndexDrop:
			case WalIndexUpdate:
			case WalPutMeta:
			case WalDeleteMeta:
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
				throw Error(errLogic, "Unexpected tx WAL rec type {}\n", wrec.type);
		}
	}

	LocalQueryResults qr;
	NsContext nsCtx = NsContext(rdxCtx);
	std::ignore = nsCtx.InSnapshot(ch.Records().back().LSN(), ch.IsWAL(), ch.IsLastChunk(), isInitialLeaderSync);
	auto ltx = Transaction::Transform(std::move(tx));
	ns_.CommitTransaction(ltx, qr, nsCtx);
}

}  // namespace reindexer
