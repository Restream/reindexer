
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

Replicator::Replicator(ReindexerImpl *slave)
	: slave_(slave), terminate_(false), state_(StateInit), enabled_(false), dummyCtx_(true, LSNPair(lsn_t(), lsn_t())) {
	stop_.set(loop_);
	resync_.set(loop_);
	resyncTimer_.set(loop_);
	walForcesyncAsync_.set(loop_);
}

Replicator::~Replicator() { Stop(); }

Error Replicator::Start() {
	std::lock_guard<std::mutex> lck(masterMtx_);
	if (master_) {
		return Error(errLogic, "Replicator is already started");
	}

	if (!(config_.role == ReplicationSlave)) return errOK;

	master_.reset(new client::Reindexer(
		client::ReindexerConfig(config_.connPoolSize, config_.workerThreads, 10000, 0, std::chrono::seconds(config_.timeoutSec),
								std::chrono::seconds(config_.timeoutSec), config_.enableCompression, config_.appName)));

	auto err = master_->Connect(config_.masterDSN, client::ConnectOpts().WithExpectedClusterID(config_.clusterID));
	if (err.ok()) err = master_->Status();
	if (err.code() == errOK || err.code() == errNetwork) {
		err = errOK;
		terminate_ = false;
	}
	if (err.ok()) {
		if (thread_.joinable()) {
			logPrintf(LogError, "Start thread, not joined");
			terminate_ = true;
			stop_.send();
			thread_.join();
		}
		thread_ = std::thread([this]() { this->run(); });
	}
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

	if (config_.namespaces.empty()) {
		master_->SubscribeUpdates(this, UpdatesFilters());
	} else {
		// Just to get any subscription for add/delete updates
		UpdatesFilters filters;
		filters.AddFilter(*config_.namespaces.begin(), UpdatesFilters::Filter());
		master_->SubscribeUpdates(this, filters, SubscriptionOpts().IncrementSubscription());
	}
	{
		std::lock_guard<std::mutex> lck(syncMtx_);
		state_.store(StateInit, std::memory_order_release);
	}

	resync_.set([this](ev::async &) { syncDatabase(); });
	resync_.start();
	resyncTimer_.set([this](ev::timer &, int) { syncDatabase(); });
	walForcesyncAsync_.set([this](ev::async &) {
		NamespaceDef nsDef;
		while (forcesyncQuery_.Pop(nsDef)) syncNamespaceForced(nsDef, "Upstream node call force sync.");
	});
	walForcesyncAsync_.start();

	syncDatabase();

	while (!terminate_) {
		loop_.run();
	}

	resync_.stop();
	stop_.stop();
	resyncTimer_.stop();
	walForcesyncAsync_.stop();
	master_->UnsubscribeUpdates(this);

	logPrintf(LogInfo, "[repl] Replicator with %s stopped", config_.masterDSN);
}

static bool errorIsFatal(Error err) {
	switch (err.code()) {
		case errOK:
		case errNetwork:
		case errTimeout:
		case errCanceled:
			return false;
		default:
			return true;
	}
}

bool Replicator::retryIfNetworkError(const Error &err) {
	if (err.ok()) return false;
	if (!errorIsFatal(err)) {
		state_.store(StateInit, std::memory_order_release);
		resyncTimer_.start(config_.retrySyncIntervalSec);
		logPrintf(LogInfo, "[repl:%s] Sync done with errors, resync is scheduled", slave_->storagePath_);
		return true;
	}
	return false;
}

static bool shouldUpdateLsn(const WALRecord &wrec) {
	return wrec.type != WalNamespaceDrop && (!wrec.inTransaction || wrec.type == WalCommitTransaction);
}

Error Replicator::syncNamespace(const NamespaceDef &ns, string_view forceSyncReason) {
	Error err = errOK;
	logPrintf(LogTrace, "[repl:%s:%s] ===Starting WAL synchronization.====", ns.name, slave_->storagePath_);
	do {
		// Perform startup sync
		if (forceSyncReason.empty()) {
			err = syncNamespaceByWAL(ns);
		} else {
			err = syncNamespaceForced(ns, forceSyncReason);
			forceSyncReason = string_view();
		}
		auto slaveNs = slave_->getNamespaceNoThrow(ns.name, dummyCtx_);
		if (err.ok() && slaveNs) {
			// Apply pended WAL updates
			UpdatesContainer walUpdates;
			SyncStat stat;
			auto replState = slaveNs->GetReplState(dummyCtx_);
			LSNPair lastLsn;
			while (err.ok() && !terminate_) {
				{
					std::unique_lock<std::mutex> lck(syncMtx_);
					auto updatesIt = pendedUpdates_.find(ns.name);
					if (updatesIt != pendedUpdates_.end()) {
						std::swap(walUpdates, updatesIt.value());
					}
					logPrintf(LogTrace, "[repl:%s]: %d new updates", ns.name, walUpdates.size());
					if (walUpdates.empty()) {
						pendedUpdates_.erase(ns.name);
						slaveNs->SetSlaveReplStatus(ReplicationState::Status::Idle, errOK, dummyCtx_);
						syncedNamespaces_.emplace(std::move(currentSyncNs_));
						currentSyncNs_.clear();
						lck.unlock();
						WrSerializer ser;
						stat.Dump(ser) << "lsn #" << int64_t(lastLsn.upstreamLSN_);
						logPrintf(!stat.lastError.ok() ? LogError : LogInfo, "[repl:%s:%s]:%d Updates applying done: %s", ns.name,
								  slave_->storagePath_, config_.serverId, ser.Slice());
						break;
					}
				}
				try {
					for (auto &rec : walUpdates) {
						bool forceApply = lastLsn.upstreamLSN_.isEmpty()
											  ? replState.lastUpstreamLSN.isEmpty() ||
													rec.first.upstreamLSN_.Counter() > replState.lastUpstreamLSN.Counter()
											  : false;
						if (terminate_) {
							logPrintf(LogTrace, "[repl:%s:%s]:%d Terminationg updates applying cycle due to termination flag", ns.name,
									  slave_->storagePath_, config_.serverId);
							break;
						}
						if (forceApply || rec.first.upstreamLSN_.Counter() > lastLsn.upstreamLSN_.Counter()) {
							WALRecord wrec(span<uint8_t>(rec.second));
							err = applyWALRecord(rec.first, ns.name, slaveNs, wrec, stat);
							if (!err.ok()) {
								break;
							}
							if (shouldUpdateLsn(wrec)) {
								lastLsn = rec.first;
							}
						} else {
							logPrintf(LogTrace, "[repl:%s:%s]:%d Dropping update with lsn: %d. Last lsn: %d", ns.name, slave_->storagePath_,
									  config_.serverId, rec.first.upstreamLSN_.Counter(), lastLsn.upstreamLSN_.Counter());
						}
					}
				} catch (const Error &e) {
					err = e;
				}
				walUpdates.clear();
				if (!lastLsn.upstreamLSN_.isEmpty()) {
					logPrintf(LogTrace, "[repl:%s] Setting new lsn %d after updates apply", ns.name, lastLsn.upstreamLSN_.Counter());
					slaveNs->SetReplLSNs(lastLsn, dummyCtx_);
				}
			}
		}
		if (!err.ok() && !terminate_) {
			if ((err.code() == errDataHashMismatch && config_.forceSyncOnWrongDataHash) || config_.forceSyncOnLogicError) {
				forceSyncReason = err.what();
			}
		}
	} while (err.ok() && !forceSyncReason.empty() && !terminate_);

	logPrintf(LogTrace, "[repl:%s:%s] ===End WAL synchronization.====", ns.name, slave_->storagePath_);
	return err;
}

Error Replicator::syncDatabase() {
	vector<NamespaceDef> nses;
	logPrintf(LogInfo, "[repl:%s]:%d Starting sync from '%s' state=%d", slave_->storagePath_, config_.serverId, config_.masterDSN,
			  state_.load());

	{
		std::lock_guard<std::mutex> lck(syncMtx_);
		state_.store(StateSyncing, std::memory_order_release);
		transactions_.clear();
		syncedNamespaces_.clear();
		currentSyncNs_.clear();
		pendedUpdates_.clear();
	}

	Error err = master_->EnumNamespaces(nses, EnumNamespacesOpts());
	if (!err.ok()) {
		logPrintf(LogError, "[repl:%s] EnumNamespaces error: %s%s", slave_->storagePath_, err.what(),
				  terminate_ ? ", terminating replication"_sv : ""_sv);
		if (err.code() == errForbidden || err.code() == errReplParams) {
			terminate_ = true;
		} else {
			retryIfNetworkError(err);
		}
		logPrintf(LogTrace, "[repl:%s] return error", slave_->storagePath_);
		state_.store(StateInit, std::memory_order_release);
		return err;
	}

	resyncTimer_.stop();
	// Loop for all master namespaces
	for (auto &ns : nses) {
		logPrintf(LogTrace, "[repl:%s:%s]:%d Loop for all master namespaces state=%d", ns.name, slave_->storagePath_, config_.serverId,
				  state_.load());
		// skip system & non enabled namespaces
		if (!isSyncEnabled(ns.name)) continue;
		// skip temporary namespaces (namespace from upstream slave node)
		if (ns.isTemporary) continue;
		if (terminate_) break;

		if (config_.namespaces.find(ns.name) != config_.namespaces.end()) {
			UpdatesFilters filters;
			filters.AddFilter(ns.name, UpdatesFilters::Filter());
			master_->SubscribeUpdates(this, filters, SubscriptionOpts().IncrementSubscription());
		}

		{
			std::lock_guard<std::mutex> lck(syncMtx_);
			currentSyncNs_ = ns.name;
		}

		ReplicationState replState;
		err = slave_->OpenNamespace(ns.name, StorageOpts().Enabled().SlaveMode());
		auto slaveNs = slave_->getNamespaceNoThrow(ns.name, dummyCtx_);
		if (err.ok() && slaveNs) {
			replState = slaveNs->GetReplState(dummyCtx_);
			slaveNs->SetSlaveReplStatus(ReplicationState::Status::Syncing, errOK, dummyCtx_);
		} else if (retryIfNetworkError(err)) {
			logPrintf(LogTrace, "[repl:%s] return error", slave_->storagePath_);
			return err;
		} else if (err.code() != errNotFound) {
			logPrintf(LogWarning, "[repl:%s]:%d Error: %s", ns.name, config_.serverId, err.what());
		}

		// If there are open error or fatal error in state, then force full sync
		bool forceSync = !err.ok() || (config_.forceSyncOnLogicError && replState.status == ReplicationState::Status::Fatal);
		string_view forceSyncReason;
		if (forceSync) {
			forceSyncReason = !replState.replError.ok() ? replState.replError.what() : "Namespace doesn't exists"_sv;
		}
		err = syncNamespace(ns, forceSyncReason);
		if (!err.ok()) {
			slaveNs = slave_->getNamespaceNoThrow(ns.name, dummyCtx_);
			if (slaveNs) {
				slaveNs->SetSlaveReplStatus(errorIsFatal(err) ? ReplicationState::Status::Fatal : ReplicationState::Status::Error, err,
											dummyCtx_);
			}
			if (retryIfNetworkError(err)) return err;
		}
	}

	state_.store(StateIdle, std::memory_order_release);
	logPrintf(LogInfo, "[repl:%s]:%d Done sync with '%s'", slave_->storagePath_, config_.serverId, config_.masterDSN);

	return err;
}

Error Replicator::syncNamespaceByWAL(const NamespaceDef &nsDef) {
	auto slaveNs = slave_->getNamespaceNoThrow(nsDef.name, dummyCtx_);
	if (!slaveNs) return Error(errNotFound, "Namespace %s not found", nsDef.name);

	lsn_t lsn = slaveNs->GetReplState(dummyCtx_).lastUpstreamLSN;

	if (lsn.isEmpty()) {
		return syncNamespaceForced(nsDef, "Empty lsn in replication state");
	}

	logPrintf(LogTrace, "[repl:%s:%s]:%d Start sync items, Query lsn = %s", nsDef.name, slave_->storagePath_, config_.serverId, lsn);
	//  Make query to master's WAL
	client::QueryResults qr(kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
	Error err = master_->Select(Query(nsDef.name).Where("#lsn", CondGt, int64_t(lsn)), qr);

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
	logPrintf(LogWarning, "[repl:%s:%s] Start FORCED sync: %s", ns.name, slave_->storagePath_, reason);

	// Create temporary namespace
	NamespaceDef tmpNsDef;
	tmpNsDef.isTemporary = true;
	if (ns.storage.IsEnabled())
		tmpNsDef.storage = StorageOpts().Enabled().CreateIfMissing().SlaveMode();
	else
		tmpNsDef.storage = StorageOpts().SlaveMode();
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
	if (err.ok()) err = syncSchemaForced(tmpNs, ns.name);

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
	if (err.ok()) {
		err = slave_->RenameNamespace(tmpNsDef.name, ns.name);
		slave_->forceSyncDownstream(ns.name);
	}

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
	lsn_t upstreamLSN = slaveNs->GetReplState(dummyCtx_).lastUpstreamLSN;
	logPrintf(LogTrace, "[repl:%s:%s]:%d applyWAL  lastUpstreamLSN = %s walRecordCount = %d", nsName, slave_->storagePath_,
			  config_.serverId, upstreamLSN, qr.Count());
	for (auto it : qr) {
		if (terminate_) break;
		if (qr.Status().ok()) {
			try {
				if (it.IsRaw()) {
					err = applyWALRecord(LSNPair(), nsName, slaveNs, WALRecord(it.GetRaw()), stat);
				} else {
					// Simple item updated
					ser.Reset();
					err = it.GetCJSON(ser, false);
					if (err.ok()) err = modifyItem(LSNPair(), slaveNs, ser.Slice(), ModeUpsert, qr.getTagsMatcher(0), stat);
				}
			} catch (const Error &e) {
				err = e;
			}
			if (!err.ok()) {
				logPrintf(LogTrace, "[repl:%s]:%d Error process WAL record with LSN #%s : %s", nsName, config_.serverId, lsn_t(it.GetLSN()),
						  err.what());
				stat.lastError = err;
				stat.errors++;
			}
			stat.processed++;
		} else {
			stat.lastError = qr.Status();
			logPrintf(LogInfo, "[repl:%s]:%d Error executing WAL query: %s", nsName, config_.serverId, stat.lastError.what());
			break;
		}
	}

	ReplicationState slaveState = slaveNs->GetReplState(dummyCtx_);

	// Check data hash, if operation successfull
	if (!stat.masterState.lastLsn.isEmpty() && stat.lastError.ok() && !terminate_ && slaveState.dataHash != stat.masterState.dataHash) {
		stat.lastError =
			Error(errDataHashMismatch, "[repl:%s]:%d dataHash mismatch with master %u != %u; itemsCount %d %d;", nsName, config_.serverId,
				  stat.masterState.dataHash, slaveState.dataHash, stat.masterState.dataCount, slaveState.dataCount);
	}

	if (stat.lastError.ok() && !terminate_) {
		logPrintf(LogTrace, "[repl:%s]:%d applyWal SetReplLSNs upstreamLsn = %s originLsn = %s", nsName, config_.serverId,
				  stat.masterState.lastLsn, stat.masterState.originLSN);
		// counters from the upstream node (from WalReplState)
		slaveNs->SetReplLSNs(LSNPair(stat.masterState.lastLsn, stat.masterState.originLSN), dummyCtx_);
	}

	ser.Reset();
	stat.Dump(ser) << "lsn #" << int64_t(slaveState.lastLsn);

	logPrintf(!stat.lastError.ok() ? LogError : LogInfo, "[repl:%s:%s]:%d Sync %s: %s", nsName, slave_->storagePath_, config_.serverId,
			  terminate_ ? "terminated" : "done", ser.Slice());

	if (terminate_) return Error(errCanceled, "terminated");
	return stat.lastError;
}

Error Replicator::applyTxWALRecord(LSNPair LSNs, string_view nsName, Namespace::Ptr slaveNs, const WALRecord &rec) {
	switch (rec.type) {
		// Modify item
		case WalItemModify: {
			std::lock_guard<std::mutex> lck(syncMtx_);
			Transaction &tx = transactions_[slaveNs.get()];
			if (tx.IsFree()) return Error(errLogic, "[repl:%s]:%d Transaction was not initiated.", nsName, config_.serverId);
			Item item = tx.NewItem();
			const Error err = unpackItem(item, LSNs.upstreamLSN_, rec.itemModify.itemCJson, master_->NewItem(nsName).impl_->tagsMatcher());
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
			if (tx.IsFree()) return Error(errLogic, "[repl:%s]:%d Transaction was not initiated.", nsName, config_.serverId);
			tx.Modify(std::move(q));
		} break;
		case WalInitTransaction: {
			std::lock_guard<std::mutex> lck(syncMtx_);
			Transaction &tx = transactions_[slaveNs.get()];
			if (!tx.IsFree()) logPrintf(LogError, "[repl:%s]:%d Init transaction befor commit of previous one.", nsName, config_.serverId);
			RdxContext rdxContext(true, LSNs);
			tx = slaveNs->NewTransaction(rdxContext);
		} break;
		case WalCommitTransaction: {
			QueryResults res;
			std::lock_guard<std::mutex> lck(syncMtx_);
			Transaction &tx = transactions_[slaveNs.get()];
			if (tx.IsFree()) return Error(errLogic, "[repl:%s]:%d Commit of transaction befor initiate it.", nsName, config_.serverId);
			RdxContext rdxContext(true, LSNs);
			slaveNs->CommitTransaction(tx, res, rdxContext);
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
		logPrintf(LogError, "[repl:%s]:%d Transaction started but not commited", nsName, config_.serverId);
		tx = Transaction{};
	}
}

Error Replicator::applyWALRecord(LSNPair LSNs, string_view nsName, Namespace::Ptr slaveNs, const WALRecord &rec, SyncStat &stat) {
	Error err;
	IndexDef iDef;

	if (!slaveNs && !(rec.type == WalNamespaceAdd || rec.type == WalForceSync)) {
		return Error(errParams, "Namespace %s not found", nsName);
	}

	if (rec.inTransaction) {
		return applyTxWALRecord(LSNs, nsName, slaveNs, rec);
	}
	RdxContext rdxContext(true, LSNs);
	switch (rec.type) {
		// Modify item
		case WalItemModify:
			checkNoOpenedTransaction(nsName, slaveNs);
			err = modifyItem(LSNs, slaveNs, rec.itemModify.itemCJson, rec.itemModify.modifyMode,
							 master_->NewItem(nsName).impl_->tagsMatcher(), stat);
			break;
		// Index added
		case WalIndexAdd:
			err = iDef.FromJSON(giftStr(rec.data));
			if (err.ok()) slaveNs->AddIndex(iDef, rdxContext);
			stat.updatedIndexes++;
			break;
		// Index dropped
		case WalIndexDrop:
			err = iDef.FromJSON(giftStr(rec.data));
			if (err.ok()) slaveNs->DropIndex(iDef, rdxContext);
			stat.deletedIndexes++;
			break;
		// Index updated
		case WalIndexUpdate:
			err = iDef.FromJSON(giftStr(rec.data));
			if (err.ok()) slaveNs->UpdateIndex(iDef, rdxContext);
			stat.updatedIndexes++;
			break;
		// Metadata updated
		case WalPutMeta:
			slaveNs->PutMeta(string(rec.putMeta.key), rec.putMeta.value, NsContext(dummyCtx_));
			stat.updatedMeta++;
			break;
		// Update query
		case WalUpdateQuery: {
			logPrintf(LogTrace, "[repl:%s]:%d WalUpdateQuery", nsName, config_.serverId);
			checkNoOpenedTransaction(nsName, slaveNs);
			QueryResults result;
			Query q;
			q.FromSQL(rec.data);
			const auto nsCtx = NsContext(rdxContext);
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
		case WalNamespaceAdd: {
			err = slave_->OpenNamespace(nsName, StorageOpts().Enabled().CreateIfMissing().SlaveMode());
			break;
		}
		// Drop namespace
		case WalNamespaceDrop:
			err = slave_->closeNamespace(nsName, rdxContext, true, true);
			break;
		// Rename namespace
		case WalNamespaceRename:
			err = slave_->RenameNamespace(nsName, string(rec.data));
			break;
		// force sync namespace
		case WalForceSync: {
			{
				NamespaceDef nsDef;
				nsDef.FromJSON(giftStr(rec.data));
				forcesyncQuery_.Push(nsDef.name, nsDef);
				walForcesyncAsync_.send();
				break;
			}
		}
		// Replication state
		case WalReplState: {  // last record in query
			stat.processed--;
			stat.masterState.FromJSON(giftStr(rec.data));
			MasterState masterState;
			masterState.dataCount = stat.masterState.dataCount;
			masterState.dataHash = stat.masterState.dataHash;
			masterState.lastUpstreamLSNm = stat.masterState.lastLsn;
			masterState.updatedUnixNano = stat.masterState.updatedUnixNano;
			slaveNs->SetSlaveReplMasterState(masterState, dummyCtx_);
			break;
		}
		case WalSetSchema:
			slaveNs->SetSchema(rec.data, dummyCtx_);
			stat.schemasSet++;
			break;
		default:
			return Error(errLogic, "Unexpected WAL rec type %d\n", int(rec.type));
	}
	return err;
}

Error Replicator::unpackItem(Item &item, lsn_t lsn, string_view cjson, const TagsMatcher &tm) {
	if (item.impl_->tagsMatcher().size() < tm.size()) {
		const bool res = item.impl_->tagsMatcher().try_merge(tm);
		if (!res) {
			return Error(errNotValid, "Can't merge tagsmatcher of item with lsn ");
		}
	}
	item.setLSN(int64_t(lsn));
	return item.FromCJSON(cjson);
}

Error Replicator::modifyItem(LSNPair LSNs, Namespace::Ptr slaveNs, string_view cjson, int modifyMode, const TagsMatcher &tm,
							 SyncStat &stat) {
	Item item = slaveNs->NewItem(dummyCtx_);
	Error err = unpackItem(item, LSNs.upstreamLSN_, cjson, tm);

	if (err.ok()) {
		RdxContext rdxContext(true, LSNs);
		auto nsCtx = NsContext(rdxContext);
		switch (modifyMode) {
			case ModeDelete:
				slaveNs->Delete(item, nsCtx);
				stat.deleted++;
				break;
			case ModeInsert:
				slaveNs->Insert(item, nsCtx);
				stat.updated++;
				break;
			case ModeUpsert:
				slaveNs->Upsert(item, nsCtx);
				stat.updated++;
				break;
			case ModeUpdate:
				slaveNs->Update(item, nsCtx);
				stat.updated++;
				break;
			default:
				return Error(errNotValid, "Unknown modify mode %d of item with lsn %ul", modifyMode, int64_t(LSNs.upstreamLSN_));
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
	if (schemasSet) ser << "New schema was set; ";
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

Error Replicator::syncSchemaForced(Namespace::Ptr slaveNs, const NamespaceDef &masterNsDef) {
	const string &nsName = masterNsDef.name;

	Error err = errOK;
	logPrintf(LogTrace, "[repl:%s] Setting schema", nsName);
	try {
		slaveNs->SetSchema(masterNsDef.schemaJson, dummyCtx_);
	} catch (const Error &e) {
		logPrintf(LogError, "[repl:%s] Error in set schema: %s", nsName, err.what());
		err = e;
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
			logPrintf(LogError, "[repl:%s]:%d Error get meta '%s': %s", nsName, config_.serverId, key, err.what());
			continue;
		}
		try {
			slaveNs->PutMeta(key, data, NsContext(dummyCtx_));
		} catch (const Error &e) {
			logPrintf(LogError, "[repl:%s]:%d Error set meta '%s': %s", slaveNs->GetName(), config_.serverId, key, e.what());
		}
	}
	return errOK;
}

// Callback from WAL updates pusher
void Replicator::OnWALUpdate(LSNPair LSNs, string_view nsName, const WALRecord &wrec) {
	auto sId = LSNs.originLSN_.Server();
	if (sId != 0) {	 // sId = 0 for configurations without specifying a server id
		if (sId == config_.serverId) {
			logPrintf(LogTrace, "[repl:%s]:%d OnWALUpdate equal serverId=%d originLSN=%lX serverIdFromLSN=%d", nsName, config_.serverId,
					  config_.serverId, int64_t(LSNs.originLSN_), LSNs.originLSN_.Server());
			return;
		}
	}
	logPrintf(LogTrace, "[repl:%s:%s]:%d OnWALUpdate state = %d upstreamLSN = %s", nsName, slave_->storagePath_, config_.serverId,
			  state_.load(std::memory_order_acquire), LSNs.upstreamLSN_);

	if (!canApplyUpdate(LSNs, nsName, wrec)) return;

	Error err;
	auto slaveNs = slave_->getNamespaceNoThrow(nsName, dummyCtx_);

	// necessary for cutting off onWALUpdate already arrived in applyWal (it is possible!)
	if (slaveNs && !LSNs.upstreamLSN_.isEmpty()) {
		auto replState = slaveNs->GetReplState(dummyCtx_);
		if (!replState.lastUpstreamLSN.isEmpty()) {
			if (replState.lastUpstreamLSN >= LSNs.upstreamLSN_) {
				logPrintf(LogTrace, "[repl:%s:%s]:%d OnWALUpdate old record state = %d upstreamLSN = %s", nsName, slave_->storagePath_,
						  config_.serverId, state_.load(std::memory_order_acquire), LSNs.upstreamLSN_);
				return;
			}
		}
	}

	SyncStat stat;

	try {
		err = applyWALRecord(LSNs, nsName, slaveNs, wrec, stat);
	} catch (const Error &e) {
		err = e;
	}
	if (err.ok()) {
		if (slaveNs && shouldUpdateLsn(wrec)) {
			if (!LSNs.upstreamLSN_.isEmpty()) slaveNs->SetReplLSNs(LSNs, dummyCtx_);
		}
	} else {
		if (slaveNs) {
			if (slaveNs->GetReplState(dummyCtx_).status != ReplicationState::Status::Fatal) {
				slaveNs->SetSlaveReplStatus(ReplicationState::Status::Fatal, err, dummyCtx_);
			}
		}
		auto lastErrIt = lastNsErrMsg_.find(nsName);
		if (lastErrIt == lastNsErrMsg_.end()) {
			lastErrIt = lastNsErrMsg_.emplace(string(nsName), NsErrorMsg{}).first;
		}
		auto &lastErr = lastErrIt->second;
		bool isDifferentError = lastErr.err.what() != err.what();
		if (isDifferentError || lastErr.count == static_cast<uint64_t>(config_.onlineReplErrorsThreshold)) {
			if (!lastErr.err.ok() && lastErr.count > 1) {
				logPrintf(LogError, "[repl:%s]:%d Error apply WAL update: %s. Repeated %d times", nsName, config_.serverId, err.what(),
						  lastErr.count - 1);
			} else {
				logPrintf(LogError, "[repl:%s]:%d Error apply WAL update: %s", nsName, config_.serverId, err.what());
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
		logPrintf(LogInfo, "[repl:] OnConnectionState connected");
	} else {
		logPrintf(LogInfo, "[repl:] OnConnectionState closed, reason: %s", err.what());
	}
	std::unique_lock<std::mutex> lck(syncMtx_);
	state_.store(StateInit, std::memory_order_release);
	resync_.send();
}

bool Replicator::canApplyUpdate(LSNPair LSNs, string_view nsName, const WALRecord &wrec) {
	if (!isSyncEnabled(nsName)) return false;

	if (terminate_.load(std::memory_order_acquire)) {
		logPrintf(LogTrace, "[repl:%s]:%d Skipping update due to replicator shutdown is in progress upstreamLSN %s", nsName,
				  config_.serverId, LSNs.upstreamLSN_);
		return false;
	}

	if (state_.load(std::memory_order_acquire) == StateIdle) return true;

	std::unique_lock<std::mutex> lck(syncMtx_);
	auto state = state_.load(std::memory_order_acquire);
	if (state == StateIdle) return true;
	bool terminate = terminate_.load(std::memory_order_acquire);
	if (state == StateInit || terminate) {
		logPrintf(LogTrace, "[repl:%s]:%d Skipping update due to replicator %s is in progress upstreamLSN %s", nsName, config_.serverId,
				  terminate ? "shutdown" : "startup", LSNs.upstreamLSN_);
		return false;
	}

	if (string_view(currentSyncNs_) != nsName) {
		if (syncedNamespaces_.find(nsName) != syncedNamespaces_.end()) {
			return true;
		} else {
			logPrintf(LogTrace, "[repl:%s]:%d Skipping update - namespace was not synced yet, upstreamLSN %s", nsName, config_.serverId,
					  LSNs.upstreamLSN_);
			if (wrec.type == WalNamespaceAdd || wrec.type == WalNamespaceDrop || wrec.type == WalNamespaceRename) {
				logPrintf(LogInfo, "[repl:%s]:%d Scheduling resync due to concurrent ns add/delete: %d", nsName, config_.serverId,
						  int(wrec.type));
				resync_.send();
			}
			return false;
		}
	}

	logPrintf(LogTrace, "[repl:%s]:%d Pending update due to concurrent sync upstreamLSN %s", nsName, config_.serverId, LSNs.upstreamLSN_);
	PackedWALRecord pwrec;
	pwrec.Pack(wrec);
	auto updatesIt = pendedUpdates_.find(nsName);
	if (updatesIt == pendedUpdates_.end()) {
		UpdatesContainer updates;
		updates.emplace_back(std::make_pair(LSNs, std::move(pwrec)));
		pendedUpdates_.emplace(string(nsName), std::move(updates));
	} else {
		auto &updates = updatesIt.value();
		updates.emplace_back(std::make_pair(LSNs, std::move(pwrec)));
	}

	return false;
}

bool Replicator::isSyncEnabled(string_view nsName) {
	// SKip system ns
	if (nsName.size() && nsName[0] == '#') return false;

	// skip non enabled namespaces
	if (config_.namespaces.size() && config_.namespaces.find(nsName) == config_.namespaces.end()) {
		return false;
	}
	return true;
}

void Replicator::ForceSyncQuery::Push(std::string &nsName, NamespaceDef &nsDef) {
	std::unique_lock<std::mutex> lock(mtx_);
	query_[nsName] = nsDef;
}
bool Replicator::ForceSyncQuery::Pop(NamespaceDef &def) {
	std::unique_lock<std::mutex> lock(mtx_);
	if (!query_.empty()) {
		auto it = query_.begin();
		def = it->second;
		query_.erase(it);
		return true;
	}
	return false;
}

}  // namespace reindexer
