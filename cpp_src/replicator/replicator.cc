
#include "replicator.h"
#include "client/itemimpl.h"
#include "client/reindexer.h"
#include "core/itemimpl.h"
#include "core/namespace/namespaceimpl.h"
#include "core/namespacedef.h"
#include "core/reindexerimpl.h"
#include "tools/logger.h"
#include "tools/stringstools.h"
#include "walrecord.h"

namespace reindexer {

using namespace net;

static constexpr size_t kTmpNsPostfixLen = 20;

Replicator::Replicator(ReindexerImpl *slave) : slave_(slave), terminate_(false), state_(StateInit), enabled_(false) {
	stop_.set(loop_);
	resync_.set(loop_);
	resyncTimer_.set(loop_);
}

Replicator::~Replicator() { Stop(); }

Error Replicator::Start() {
	std::lock_guard<std::mutex> lck(masterMtx_);
	if (master_) {
		return Error(errLogic, "Replicator is already started");
	}

	if (config_.role != ReplicationSlave) return errOK;

	master_.reset(
		new client::Reindexer(client::ReindexerConfig(config_.connPoolSize, config_.workerThreads, 10000,
													  std::chrono::seconds(config_.timeoutSec), std::chrono::seconds(config_.timeoutSec))));

	auto err = master_->Connect(config_.masterDSN, client::ConnectOpts().WithExpectedClusterID(config_.clusterID));
	if (err.ok()) err = master_->Status();
	if (err.code() == errOK || err.code() == errNetwork) {
		err = errOK;
		terminate_ = false;
	}
	if (err.ok()) thread_ = std::thread([this]() { this->run(); });

	return err;
}

bool Replicator::Configure(const ReplicationConfigData &config) {
	if (!enabled_.load(std::memory_order_acquire)) {
		return false;
	}
	std::unique_lock<std::mutex> lck(masterMtx_);
	bool changed = (config_ != config);

	if (changed) {
		if (master_) stop();
		config_ = config;
	}

	return changed || !master_;
}

void Replicator::Stop() {
	std::unique_lock<std::mutex> lck(masterMtx_);
	stop();
}

void Replicator::stop() {
	terminate_ = true;
	stop_.send();

	if (thread_.joinable()) {
		thread_.join();
	}

	if (master_) {
		master_->Stop();
		master_.reset();
	}
	terminate_ = false;
}

void Replicator::run() {
	stop_.set([&](ev::async &sig) { sig.loop.break_loop(); });
	stop_.start();
	logPrintf(LogInfo, "[repl] Replicator with %s started", config_.masterDSN);

	syncMtx_.lock();
	state_.store(StateInit, std::memory_order_release);
	syncMtx_.unlock();

	master_->SubscribeUpdates(this, true);

	resync_.set([this](ev::async &) { syncDatabase(); });
	resync_.start();
	resyncTimer_.set([this](ev::timer &, int) { syncDatabase(); });

	syncDatabase();

	while (!terminate_) {
		loop_.run();
	}

	resync_.stop();
	stop_.stop();
	resyncTimer_.stop();
	logPrintf(LogInfo, "[repl] Replicator with %s stopped", config_.masterDSN);
}

static bool errorIsFatal(Error err) {
	switch (err.code()) {
		case errOK:
		case errNetwork:
			return false;
		default:
			return true;
	}
}

// Sync database
Error Replicator::syncDatabase() {
	vector<NamespaceDef> nses;
	logPrintf(LogInfo, "[repl] Starting sync from '%s'", config_.masterDSN);

	Error err = master_->EnumNamespaces(nses, EnumNamespacesOpts());
	if (!err.ok()) {
		logPrintf(LogError, "[repl] EnumNamespaces error: %s%s", err.what(), terminate_ ? ", terminatng replication"_sv : ""_sv);
		if (err.code() == errForbidden || err.code() == errReplParams) {
			terminate_ = true;
		} else if (!errorIsFatal(err)) {
			resyncTimer_.start(config_.retrySyncIntervalSec);
			logPrintf(LogInfo, "[repl] Sync done with errors, resync is scheduled");
		}
		return err;
	}

	syncMtx_.lock();
	// Protection for concurent updates stream of same namespace
	// if state is StateSyncing is set, then concurent updates will not modify data, but just set maxLsns_[nsname]
	for (auto &ns : nses) maxLsns_[ns.name] = -1;
	state_.store(StateSyncing, std::memory_order_release);
	syncMtx_.unlock();

	bool requireRetry = false;
	// Loop for all master namespaces
	for (auto &ns : nses) {
		// skip system & non enabled namespaces
		if (!isSyncEnabled(ns.name)) continue;

		if (terminate_) break;

		ReplicationState replState;
		err = slave_->OpenNamespace(ns.name, StorageOpts().Enabled().SlaveMode());
		auto slaveNs = slave_->getNamespaceNoThrow(ns.name, dummyCtx_);
		if (err.ok() && slaveNs) {
			replState = slaveNs->GetReplState(dummyCtx_);
			slaveNs->SetSlaveReplStatus(ReplicationState::Status::Syncing, errOK, dummyCtx_);
		} else if (err.code() != errNotFound) {
			logPrintf(LogWarning, "[repl:%s] Error: %s", ns.name, err.what());
		}

		// If there are open error or fatal error in state, then force full sync
		bool forceSync = !err.ok() || (config_.forceSyncOnLogicError && replState.status == ReplicationState::Status::Fatal);

		err = errOK;
		for (bool done = false; err.ok() && !done && !terminate_;) {
			if (!forceSync) {
				err = syncNamespaceByWAL(ns);
				if (!err.ok() && !terminate_) {
					if ((err.code() == errDataHashMismatch && config_.forceSyncOnWrongDataHash) ||
						(err.code() != errNetwork && config_.forceSyncOnLogicError)) {
						err = syncNamespaceForced(ns, err.what());
					}
				}
			} else {
				err = syncNamespaceForced(ns, !replState.replError.ok() ? replState.replError.what() : "Namespace doesn't exists");
				forceSync = false;
			}
			slaveNs = slave_->getNamespaceNoThrow(ns.name, dummyCtx_);
			if (err.ok() && slaveNs) {
				int64_t curLSN = -1;
				curLSN = slaveNs->GetReplState(dummyCtx_).lastLsn;
				{
					std::lock_guard<std::mutex> lck(syncMtx_);
					// Check, if concurrent update attempt happened with LSN bigger, than current LSN
					// In this case retry sync
					if (maxLsns_[ns.name] <= curLSN) {
						done = true;
						maxLsns_.erase(ns.name);
					} else {
						logPrintf(LogInfo, "[repl:%s] Retrying sync due to concurrent online WAL update (lsn=%d) > %d", ns.name,
								  maxLsns_[ns.name], curLSN);
					}
				}
				slaveNs->SetSlaveReplStatus(ReplicationState::Status::Idle, errOK, dummyCtx_);
			} else {
				if (!errorIsFatal(err)) {
					requireRetry = true;
				}
				if (slaveNs) {
					slaveNs->SetSlaveReplStatus(errorIsFatal(err) ? ReplicationState::Status::Fatal : ReplicationState::Status::Error, err,
												dummyCtx_);
				}
			}
		}
	}
	if (requireRetry) {
		resyncTimer_.start(config_.retrySyncIntervalSec);
		logPrintf(LogInfo, "[repl] Sync done with errors, resync is scheduled");
	} else {
		resyncTimer_.stop();
	}
	state_.store(StateIdle, std::memory_order_release);
	logPrintf(LogInfo, "[repl] Done sync with '%s'", config_.masterDSN);

	return err;
}

Error Replicator::syncNamespaceByWAL(const NamespaceDef &nsDef) {
	auto slaveNs = slave_->getNamespaceNoThrow(nsDef.name, dummyCtx_);
	if (!slaveNs) return Error(errNotFound, "Namespace %s not found", nsDef.name);

	auto lsn = slaveNs->GetReplState(dummyCtx_).lastLsn;

	logPrintf(LogTrace, "[repl:%s] Start sync items, lsn %ld", nsDef.name, lsn);
	//  Make query to master's WAL
	client::QueryResults qr(kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
	Error err = master_->Select(Query(nsDef.name).Where("#lsn", CondGt, lsn), qr);

	switch (err.code()) {
		case errOutdatedWAL:
			// Check if WAL has been outdated, if yes, then force resync
			return syncNamespaceForced(nsDef, err.what());
		case errOK:
			return applyWAL(slaveNs, qr);
		case errNoWAL:
			terminate_ = true;
			return err;
		default:
			return err;
	}
}

// Forced namespace sync
// This will completely drop slave namespace
// read all indexes and data from master, then apply to slave
Error Replicator::syncNamespaceForced(const NamespaceDef &ns, string_view reason) {
	logPrintf(LogWarning, "[repl:%s] Start FORCED sync: %s", ns.name, reason);

	// Create temporary namespace
	NamespaceDef tmpNsDef;
	tmpNsDef.storage = StorageOpts().Enabled().CreateIfMissing().SlaveMode().Temporary();
	Namespace::Ptr tmpNs;
	tmpNsDef.name = ns.name + "_tmp_" + randStringAlph(kTmpNsPostfixLen);
	auto err = slave_->AddNamespace(tmpNsDef);
	if (!err.ok()) {
		logPrintf(LogWarning, "Unable to create temporary namespace %s for the force sync: %s", tmpNsDef.name, err.what());
		return err;
	}

	tmpNs = slave_->getNamespaceNoThrow(tmpNsDef.name, dummyCtx_);
	if (!tmpNs) {
		logPrintf(LogWarning, "Unable to get temporary namespace %s for the force sync:", tmpNsDef.name);
		return Error(errNotFound, "Namespace %s not found", tmpNsDef.name);
	}

	tmpNs->SetSlaveReplStatus(ReplicationState::Status::Syncing, errOK, dummyCtx_);
	err = syncIndexesForced(tmpNs, ns);
	if (err.ok()) err = syncMetaForced(tmpNs, ns.name);

	//  Make query to complete master's namespace data
	client::QueryResults qr(kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
	if (err.ok()) err = master_->Select(Query(ns.name).Where("#lsn", CondAny, {}), qr);
	if (err.ok()) {
		tmpNs->ReplaceTagsMatcher(qr.getTagsMatcher(0), dummyCtx_);
		err = applyWAL(tmpNs, qr);
		if (err.code() == errDataHashMismatch) {
			logPrintf(LogError, "[repl:%s] Internal error. dataHash mismatch while fullSync!", ns.name, err.what());
			err = errOK;
		}
	}
	if (err.ok()) err = slave_->RenameNamespace(tmpNsDef.name, ns.name);
	if (!err.ok()) {
		logPrintf(LogError, "[repl:%s] FORCED sync error: %s", ns.name, err.what());
		auto dropErr = slave_->closeNamespace(tmpNsDef.name, dummyCtx_, true, true);
		if (!dropErr.ok()) logPrintf(LogWarning, "Unable to drop temporary namespace %s: %s", tmpNsDef.name, dropErr.what());
	}

	return err;
}

Error Replicator::applyWAL(Namespace::Ptr slaveNs, client::QueryResults &qr) {
	Error err;
	SyncStat stat;
	WrSerializer ser;
	const auto &nsName = slaveNs->GetName();
	// process WAL
	int64_t slaveLSN = slaveNs->GetReplState(dummyCtx_).lastLsn;
	for (auto it : qr) {
		if (terminate_) break;
		if (qr.Status().ok()) {
			try {
				int64_t lsn = it.GetLSN();
				slaveLSN = std::max(lsn, slaveLSN);
				if (it.IsRaw()) {
					err = applyWALRecord(lsn, nsName, slaveNs, WALRecord(it.GetRaw()), stat);
				} else {
					// Simple item updated
					ser.Reset();
					err = it.GetCJSON(ser, false);
					if (err.ok()) err = modifyItem(lsn, slaveNs, ser.Slice(), ModeUpsert, qr.getTagsMatcher(0), stat);
				}
			} catch (const Error &e) {
				err = e;
			}
			if (!err.ok()) {
				logPrintf(LogTrace, "[repl:%s] Error process WAL record with LSN #%ld : %s", nsName, it.GetLSN(), err.what());
				stat.lastError = err;
				stat.errors++;
			}
			stat.processed++;
		} else {
			stat.lastError = qr.Status();
			logPrintf(LogInfo, "[repl:%s] Error executing WAL query: %s", nsName, stat.lastError.what());
			break;
		}
	}

	if (stat.lastError.ok() && !terminate_) {
		// Set slave LSN if operation successfull
		slaveLSN = std::max(stat.masterState.lastLsn, slaveLSN);
		slaveNs->SetSlaveLSN(slaveLSN, dummyCtx_);
	}

	ReplicationState slaveState = slaveNs->GetReplState(dummyCtx_);

	// Check data hash, if operation successfull
	if (stat.masterState.lastLsn >= 0 && stat.lastError.ok() && !terminate_ && slaveState.dataHash != stat.masterState.dataHash) {
		stat.lastError = Error(errDataHashMismatch, "[repl:%s] dataHash mismatch with master %u != %u; itemsCount %d %d; lsn %d %d", nsName,
							   stat.masterState.dataHash, slaveState.dataHash, stat.masterState.dataCount, slaveState.dataCount,
							   stat.masterState.lastLsn, slaveLSN);
	}

	ser.Reset();
	stat.Dump(ser) << "lsn #" << slaveState.lastLsn;

	logPrintf(!stat.lastError.ok() ? LogError : LogInfo, "[repl:%s] Sync %s: %s", nsName, terminate_ ? "terminated" : "done", ser.Slice());

	return stat.lastError;
}

Error Replicator::applyTxWALRecord(int64_t lsn, string_view nsName, Namespace::Ptr slaveNs, const WALRecord &rec) {
	switch (rec.type) {
		// Modify item
		case WalItemModify: {
			std::lock_guard<std::mutex> lck(syncMtx_);
			Transaction &tx = transactions_[slaveNs.get()];
			if (tx.IsFree()) return Error(errLogic, "[repl:%s] Transaction was not initiated.", nsName);
			Item item = tx.NewItem();
			const Error err = unpackItem(item, lsn, rec.itemModify.itemCJson, master_->NewItem(nsName).impl_->tagsMatcher());
			if (!err.ok()) return err;
			tx.Modify(std::move(item), static_cast<ItemModifyMode>(rec.itemModify.modifyMode));
		} break;
		// Update query
		case WalUpdateQuery: {
			QueryResults result;
			Query q;
			q.FromSQL(rec.data);
			std::lock_guard<std::mutex> lck(syncMtx_);
			Transaction &tx = transactions_[slaveNs.get()];
			if (tx.IsFree()) return Error(errLogic, "[repl:%s] Transaction was not initiated.", nsName);
			tx.Modify(std::move(q));
		} break;
		case WalInitTransaction: {
			std::lock_guard<std::mutex> lck(syncMtx_);
			Transaction &tx = transactions_[slaveNs.get()];
			if (!tx.IsFree()) logPrintf(LogError, "[repl:%s] Init transaction befor commit of previous one.", nsName);
			tx = slaveNs->NewTransaction(dummyCtx_);
		} break;
		case WalCommitTransaction: {
			QueryResults res;
			std::lock_guard<std::mutex> lck(syncMtx_);
			Transaction &tx = transactions_[slaveNs.get()];
			if (tx.IsFree()) return Error(errLogic, "[repl:%s] Commit of transaction befor initiate it.", nsName);
			slaveNs->CommitTransaction(tx, res, dummyCtx_);
			tx = Transaction{};
		} break;
		default:
			return Error(errLogic, "Unexpected for transaction WAL rec type %d\n", int(rec.type));
	}
	return {};
}

void Replicator::checkNoOpenedTransaction(string_view nsName, Namespace::Ptr slaveNs) {
	std::lock_guard<std::mutex> lck(syncMtx_);
	Transaction &tx = transactions_[slaveNs.get()];
	if (!tx.IsFree()) {
		logPrintf(LogError, "[repl:%s] Transaction started but not commited", nsName);
		tx = Transaction{};
	}
}

Error Replicator::applyWALRecord(int64_t lsn, string_view nsName, Namespace::Ptr slaveNs, const WALRecord &rec, SyncStat &stat) {
	Error err;
	IndexDef iDef;

	if (!slaveNs && rec.type != WalNamespaceAdd) {
		return Error(errParams, "Namespace %s not found", nsName);
	}

	if (rec.inTransaction) return applyTxWALRecord(lsn, nsName, slaveNs, rec);

	switch (rec.type) {
		// Modify item
		case WalItemModify:
			checkNoOpenedTransaction(nsName, slaveNs);
			err = modifyItem(lsn, slaveNs, rec.itemModify.itemCJson, rec.itemModify.modifyMode,
							 master_->NewItem(nsName).impl_->tagsMatcher(), stat);
			break;
		// Index added
		case WalIndexAdd:
			err = iDef.FromJSON(giftStr(rec.data));
			if (err.ok()) slaveNs->AddIndex(iDef, dummyCtx_);
			stat.updatedIndexes++;
			break;
		// Index dropped
		case WalIndexDrop:
			err = iDef.FromJSON(giftStr(rec.data));
			if (err.ok()) slaveNs->DropIndex(iDef, dummyCtx_);
			stat.deletedIndexes++;
			break;
		// Index updated
		case WalIndexUpdate:
			err = iDef.FromJSON(giftStr(rec.data));
			if (err.ok()) slaveNs->UpdateIndex(iDef, dummyCtx_);
			stat.updatedIndexes++;
			break;
		// Metadata updated
		case WalPutMeta:
			slaveNs->PutMeta(string(rec.putMeta.key), rec.putMeta.value, NsContext(dummyCtx_).Lsn(lsn));
			stat.updatedMeta++;
			break;
		// Update query
		case WalUpdateQuery: {
			checkNoOpenedTransaction(nsName, slaveNs);
			QueryResults result;
			Query q;
			q.FromSQL(rec.data);
			const auto nsCtx = NsContext(dummyCtx_).Lsn(lsn);
			switch (q.type_) {
				case QueryDelete:
					slaveNs->Delete(q, result, nsCtx);
					break;
				case QueryUpdate:
					slaveNs->Update(q, result, nsCtx);
					break;
				case QueryTruncate:
					slaveNs->Truncate(nsCtx);
					break;
				default:
					break;
			}
			break;
		}
		// New namespace
		case WalNamespaceAdd:
			err = slave_->OpenNamespace(nsName, StorageOpts().Enabled().CreateIfMissing().SlaveMode());
			break;
		// Drop namespace
		case WalNamespaceDrop:
			err = slave_->closeNamespace(nsName, dummyCtx_, true, true);
			break;
		// Rename namespace
		case WalNamespaceRename:
			err = slave_->RenameNamespace(nsName, string(rec.data));
			break;
		// Replication state
		case WalReplState: {
			stat.processed--;
			stat.masterState.FromJSON(giftStr(rec.data));
			MasterState masterState;
			masterState.dataCount = stat.masterState.dataCount;
			masterState.dataHash = stat.masterState.dataHash;
			masterState.lastLsn = stat.masterState.lastLsn;
			masterState.updatedUnixNano = stat.masterState.updatedUnixNano;
			slaveNs->SetSlaveReplMasterState(masterState, dummyCtx_);
			break;
		}
		default:
			return Error(errLogic, "Unexpected WAL rec type %d\n", int(rec.type));
	}
	return err;
}

Error Replicator::unpackItem(Item &item, int64_t lsn, string_view cjson, const TagsMatcher &tm) {
	if (item.impl_->tagsMatcher().size() < tm.size()) {
		const bool res = item.impl_->tagsMatcher().try_merge(tm);
		if (!res) return Error(errNotValid, "Can't merge tagsmatcher of item with lsn %ld", lsn);
	}
	item.setLSN(lsn);
	return item.FromCJSON(cjson);
}

Error Replicator::modifyItem(int64_t lsn, Namespace::Ptr slaveNs, string_view cjson, int modifyMode, const TagsMatcher &tm,
							 SyncStat &stat) {
	Item item = slaveNs->NewItem(dummyCtx_);
	Error err = unpackItem(item, lsn, cjson, tm);
	if (err.ok()) {
		switch (modifyMode) {
			case ModeDelete:
				slaveNs->Delete(item, dummyCtx_);
				stat.deleted++;
				break;
			case ModeInsert:
				slaveNs->Insert(item, dummyCtx_);
				stat.updated++;
				break;
			case ModeUpsert:
				slaveNs->Upsert(item, dummyCtx_);
				stat.updated++;
				break;
			case ModeUpdate:
				slaveNs->Update(item, dummyCtx_);
				stat.updated++;
				break;
			default:
				return Error(errNotValid, "Unknown modify mode %d of item with lsn %ul", modifyMode, lsn);
		}
	}
	return err;
}

WrSerializer &Replicator::SyncStat::Dump(WrSerializer &ser) {
	if (updated) ser << updated << " items updated; ";
	if (deleted) ser << deleted << " items deleted; ";
	if (updatedIndexes) ser << updatedIndexes << " indexes updated; ";
	if (deletedIndexes) ser << deletedIndexes << " indexes deleted; ";
	if (updatedMeta) ser << updatedMeta << " meta updated; ";
	if (errors || !lastError.ok()) ser << errors << " errors (" << lastError.what() << ") ";
	if (!ser.Len()) ser << "Up to date; ";
	if (processed) ser << "processed " << processed << " WAL records ";
	return ser;
}

Error Replicator::syncIndexesForced(Namespace::Ptr slaveNs, const NamespaceDef &masterNsDef) {
	const string &nsName = masterNsDef.name;

	Error err = errOK;
	for (auto &idx : masterNsDef.indexes) {
		logPrintf(LogTrace, "[repl:%s] Updating index '%s'", nsName, idx.name_);
		try {
			slaveNs->AddIndex(idx, dummyCtx_);
		} catch (const Error &e) {
			logPrintf(LogError, "[repl:%s] Error add index '%s': %s", nsName, idx.name_, err.what());
			err = e;
		}
	}

	return err;
}

Error Replicator::syncMetaForced(Namespace::Ptr slaveNs, string_view nsName) {
	vector<string> keys;
	auto err = master_->EnumMeta(nsName, keys);

	for (auto &key : keys) {
		string data;
		err = master_->GetMeta(nsName, key, data);
		if (!err.ok()) {
			logPrintf(LogError, "[repl:%s] Error get meta '%s': %s", nsName, key, err.what());
			continue;
		}
		try {
			slaveNs->PutMeta(key, data, NsContext(dummyCtx_).Lsn(1));
		} catch (const Error &e) {
			logPrintf(LogError, "[repl:%s] Error set meta '%s': %s", slaveNs->GetName(), key, e.what());
		}
	}
	return errOK;
}

// Callback from WAL updates pusher
void Replicator::OnWALUpdate(int64_t lsn, string_view nsName, const WALRecord &wrec) {
	if (!canApplyUpdate(lsn, nsName)) return;

	Error err;
	auto slaveNs = slave_->getNamespaceNoThrow(nsName, dummyCtx_);

	SyncStat stat;

	try {
		err = applyWALRecord(lsn, nsName, slaveNs, wrec, stat);
	} catch (const Error &e) {
		err = e;
	}
	if (err.ok() && slaveNs && wrec.type != WalNamespaceDrop) {
		slaveNs->SetSlaveLSN(lsn, dummyCtx_);
	} else if (!err.ok()) {
		if (slaveNs) {
			slaveNs->SetSlaveReplStatus(ReplicationState::Status::Fatal, err, dummyCtx_);
		}
		auto lastErrIt = lastNsErrMsg_.find(nsName);
		if (lastErrIt == lastNsErrMsg_.end()) {
			lastErrIt = lastNsErrMsg_.emplace(string(nsName), NsErrorMsg{}).first;
		}
		auto &lastErr = lastErrIt->second;
		bool isDifferentError = lastErr.err.what() != err.what();
		if (isDifferentError || lastErr.count == static_cast<uint64_t>(config_.onlineReplErrorsThreshold)) {
			if (!lastErr.err.ok() && lastErr.count > 1) {
				logPrintf(LogError, "[repl:%s] Error apply WAL update: %s. Repeated %d times", nsName, err.what(), lastErr.count - 1);
			} else {
				logPrintf(LogError, "[repl:%s] Error apply WAL update: %s", nsName, err.what());
			}
			lastErr.count = 1;
			if (!isDifferentError) {
				lastErr.err = err;
			}
		} else {
			++lastErr.count;
		}

		if (config_.forceSyncOnLogicError) {
			resync_.send();
		}
	}
}

void Replicator::OnConnectionState(const Error &err) {
	if (err.ok()) {
		logPrintf(LogTrace, "[repl:] OnConnectionState connected");
		std::unique_lock<std::mutex> lck(syncMtx_);
		state_.store(StateInit, std::memory_order_release);
		resync_.send();
	} else {
		logPrintf(LogTrace, "[repl:] OnConnectionState closed, reason: %s", err.what());
	}
}

bool Replicator::canApplyUpdate(int64_t lsn, string_view nsName) {
	if (!isSyncEnabled(nsName)) return false;

	if (terminate_.load(std::memory_order_acquire)) {
		logPrintf(LogTrace, "[repl:%s] Skipping update due to replicator shutdown is in progress lsn %ld", nsName, lsn);
		return false;
	}

	if (state_.load(std::memory_order_acquire) == StateIdle) return true;

	std::unique_lock<std::mutex> lck(syncMtx_);
	auto state = state_.load(std::memory_order_acquire);
	if (state == StateIdle) return true;
	bool terminate = terminate_.load(std::memory_order_acquire);
	if (state == StateInit || terminate) {
		logPrintf(LogTrace, "[repl:%s] Skipping update due to replicator %s is in progress lsn %ld", nsName,
				  terminate ? "shutdown" : "startup", lsn);
		return false;
	}

	// sync is in progress, and ns is not processed
	auto mIt = maxLsns_.find(nsName);

	// ns is already synced
	if (mIt == maxLsns_.end()) return true;

	logPrintf(LogTrace, "[repl:%s] Skipping update due to concurrent sync lsn %ld, maxLsn %ld", nsName, lsn, mIt->second);
	if (lsn > mIt->second) mIt->second = lsn;

	return false;
}

bool Replicator::isSyncEnabled(string_view nsName) {
	// SKip system ns
	if (nsName.size() && nsName[0] == '#') return false;

	// skip non enabled namespaces
	if (config_.namespaces.size() && config_.namespaces.find(nsName) == config_.namespaces.end()) return false;
	return true;
}

}  // namespace reindexer
