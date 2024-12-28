#include "replicator.h"
#include "client/itemimpl.h"
#include "client/reindexer.h"
#include "core/formatters/lsn_fmt.h"
#include "core/namespace/namespaceimpl.h"
#include "core/reindexer_impl/reindexerimpl.h"
#include "tools/logger.h"

namespace reindexer {

using namespace net;
using namespace std::string_view_literals;

Replicator::Replicator(ReindexerImpl* slave)
	: slave_(slave),
	  resyncUpdatesLostFlag_(false),
	  terminate_(false),
	  state_(StateInit),
	  enabled_(false),
	  dummyCtx_(true, LSNPair(lsn_t(), lsn_t())),
	  syncQueue_(syncMtx_) {
	stop_.set(loop_);
	resync_.set(loop_);
	resyncTimer_.set(loop_);
	walSyncAsync_.set(loop_);
	resyncUpdatesLostAsync_.set(loop_);
}

Replicator::~Replicator() { Stop(); }

Error Replicator::Start() {
	std::lock_guard lck(masterMtx_);
	if (master_) {
		return Error(errLogic, "Replicator is already started");
	}

	if (!(config_.role == ReplicationSlave)) {
		return errOK;
	}

	master_.reset(new client::Reindexer(
		client::ReindexerConfig(config_.connPoolSize, config_.workerThreads, 10000, 0, std::chrono::seconds(config_.timeoutSec),
								std::chrono::seconds(config_.timeoutSec), config_.enableCompression, false, config_.appName)));

	auto err = master_->Connect(config_.masterDSN, client::ConnectOpts().WithExpectedClusterID(config_.clusterID));
	if (err.ok()) {
		err = master_->Status();
	}
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
			terminate_ = false;
		}
		thread_ = std::thread([this]() { this->run(); });
	}
	return err;
}

bool Replicator::Configure(const ReplicationConfigData& config) {
	if (!enabled_.load(std::memory_order_acquire)) {
		return false;
	}
	std::unique_lock lck(masterMtx_);
	bool changed = (config_ != config);

	if (changed) {
		if (master_) {
			stop();
		}
		config_ = config;
	}

	return changed || !master_;
}

void Replicator::Stop() {
	std::unique_lock lck(masterMtx_);
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
	stop_.set([&](ev::async& sig) { sig.loop.break_loop(); });
	stop_.start();
	logPrintf(LogInfo, "[repl] Replicator with %s started", config_.masterDSN);

	if (config_.namespaces.empty()) {
		const auto err = master_->SubscribeUpdates(this, UpdatesFilters());
		if (!err.ok()) {
			logPrintf(LogError, "[repl] SubscribeUpdates error: %s", err.what());
		}
	} else {
		// Just to get any subscription for add/delete updates
		UpdatesFilters filters;
		filters.AddFilter(*config_.namespaces.begin(), UpdatesFilters::Filter());
		const auto err = master_->SubscribeUpdates(this, filters, SubscriptionOpts().IncrementSubscription());
		if (!err.ok()) {
			logPrintf(LogError, "[repl] SubscribeUpdates error: %s", err.what());
		}
	}
	{
		std::lock_guard lck(syncMtx_);
		state_.store(StateInit, std::memory_order_release);
	}

	resync_.set([this](ev::async&) {
		auto err = syncDatabase("Resync");
		(void)err;	// ignore
	});
	resync_.start();
	resyncTimer_.set([this](ev::timer&, int) {
		auto err = syncDatabase("Reconnect");
		(void)err;	// ignore
	});
	walSyncAsync_.set([this](ev::async&) {
		// check status of namespace
		NamespaceDef nsDef;
		bool forced;
		while (syncQueue_.Get(nsDef, forced)) {
			subscribeUpdatesIfRequired(nsDef.name);
			auto [err, _] = syncNamespace(nsDef, forced ? "Upstream node call force sync" : std::string_view(), &syncQueue_, "WAL");
			(void)_;
			if (!err.ok()) {
				if (!terminate_) {
					resync_.send();
				}
				logPrintf(LogError, "[repl:%s]:%d Error on namespace downstream resync: %s. %s", nsDef.name, config_.serverId, err.what(),
						  terminate_ ? "Skipping full resync - replicator is already terminating" : "Full database resync was scheduled");
				return;
			}
		}
	});
	walSyncAsync_.start();
	resyncUpdatesLostAsync_.set([this](ev::async&) {
		auto err = syncDatabase("UpdateLost");
		(void)err;	// ignore
	});
	resyncUpdatesLostAsync_.start();
	if (auto err = syncDatabase("None"); !err.ok()) {
		logPrintf(LogError, "[repl] syncDatabase error: %s", err.what());
	}

	while (!terminate_) {
		loop_.run();
	}

	resync_.stop();
	stop_.stop();
	resyncTimer_.stop();
	walSyncAsync_.stop();
	resyncUpdatesLostAsync_.stop();
	if (auto err = master_->UnsubscribeUpdates(this); !err.ok()) {
		logPrintf(LogError, "[repl] UnsubscribeUpdates error: %s", err.what());
	}
	logPrintf(LogInfo, "[repl] Replicator with %s stopped", config_.masterDSN);
}

static bool errorIsFatal(const Error& err) {
	switch (err.code()) {
		case errOK:
		case errNetwork:
		case errTimeout:
		case errCanceled:
			return false;
		case errParseSQL:
		case errQueryExec:
		case errParams:
		case errLogic:
		case errParseJson:
		case errParseDSL:
		case errConflict:
		case errParseBin:
		case errForbidden:
		case errWasRelock:
		case errNotValid:
		case errNotFound:
		case errStateInvalidated:
		case errBadTransaction:
		case errOutdatedWAL:
		case errNoWAL:
		case errDataHashMismatch:
		case errTagsMissmatch:
		case errReplParams:
		case errNamespaceInvalidated:
		case errParseMsgPack:
		case errParseProtobuf:
		case errUpdatesLost:
		case errWrongReplicationData:
		case errUpdateReplication:
		case errClusterConsensus:
		case errTerminated:
		case errTxDoesNotExist:
		case errAlreadyConnected:
		case errTxInvalidLeader:
		case errAlreadyProxied:
		case errStrictMode:
		case errQrUIDMissmatch:
		case errSystem:
		case errAssert:
		default:
			return true;
	}
}

bool Replicator::retryIfNetworkError(const Error& err) {
	if (err.ok()) {
		return false;
	}
	if (!errorIsFatal(err)) {
		state_.store(StateInit, std::memory_order_release);
		resyncTimer_.start(config_.retrySyncIntervalSec);
		logPrintf(LogInfo, "[repl:%s] Sync done with errors, resync is scheduled", slave_->storagePath_);
		return true;
	}
	return false;
}

void Replicator::subscribeUpdatesIfRequired(const std::string& nsName) {
	if (config_.namespaces.find(nsName) != config_.namespaces.end()) {
		UpdatesFilters filters;
		filters.AddFilter(nsName, UpdatesFilters::Filter());
		const auto err = master_->SubscribeUpdates(this, filters, SubscriptionOpts().IncrementSubscription());
		if (!err.ok()) {
			logPrintf(LogError, "[repl] SubscribeUpdates error: %s", err.what());
		}
	}
}

static bool shouldUpdateLsn(const WALRecord& wrec) {
	return wrec.type != WalNamespaceDrop && (!wrec.inTransaction || wrec.type == WalCommitTransaction);
}

Replicator::SyncNsResult Replicator::syncNamespace(const NamespaceDef& ns, std::string_view forceSyncReason, SyncQueue* sourceQueue,
												   std::string_view initiator) {
	Error err = errOK;
	logPrintf(LogInfo, "[repl:%s:%s] ===Starting WAL synchronization. (initiated by %s)====", ns.name, slave_->storagePath_, initiator);
	auto onPendedUpdatesApplied = [this, sourceQueue](std::string_view nsName, const std::unique_lock<std::mutex>& lck) noexcept {
		pendedUpdates_.erase(nsName);
		if (sourceQueue) {
			sourceQueue->Pop(nsName, lck);
		}
	};
	do {
		// Perform startup sync
		if (forceSyncReason.empty()) {
			err = syncNamespaceByWAL(ns);
		} else {
			err = syncNamespaceForced(ns, forceSyncReason);
			forceSyncReason = std::string_view();
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
					std::unique_lock lck(syncMtx_);
					auto updatesIt = pendedUpdates_.find(ns.name);
					if (updatesIt != pendedUpdates_.end()) {
						if (updatesIt.value().UpdatesLost) {
							err = Error(errUpdatesLost, "Updates Lost %s", ns.name);
							logPrintf(LogWarning, "[repl:%s]: Updates Lost", ns.name);
							onPendedUpdatesApplied(ns.name, lck);
							break;
						}
						std::swap(walUpdates, updatesIt.value().container);
					}
					logPrintf(LogTrace, "[repl:%s:%s]: %d new updates", ns.name, slave_->storagePath_, walUpdates.size());
					if (walUpdates.empty()) {
						onPendedUpdatesApplied(ns.name, lck);
						if (replState.replicatorEnabled) {
							slaveNs->SetSlaveReplStatus(ReplicationState::Status::Idle, errOK, dummyCtx_);
						} else {
							logPrintf(
								LogError,
								"[repl:%s:%s]:%d Sync namespace logical error. Set status Idle. Replication not allowed for namespace.",
								ns.name, slave_->storagePath_, config_.serverId);
							err = Error(errLogic, "Replication not allowed for namespace.");
							break;
						}
						syncedNamespaces_.emplace(std::move(currentSyncNs_));
						currentSyncNs_.clear();
						lck.unlock();
						WrSerializer ser;
						stat.Dump(ser) << "lsn #" << int64_t(lastLsn.upstreamLSN_);
						logPrintf((!stat.lastError.ok() ? LogError : LogInfo), "[repl:%s:%s]:%d Updates applying done: %s", ns.name,
								  slave_->storagePath_, config_.serverId, ser.Slice());
						break;
					}
				}
				try {
					for (auto& rec : walUpdates) {
						bool forceApply = lastLsn.upstreamLSN_.isEmpty()
											  ? replState.lastUpstreamLSN.isEmpty() ||
													rec.first.upstreamLSN_.Counter() > replState.lastUpstreamLSN.Counter()
											  : false;
						if (terminate_) {
							logPrintf(LogTrace, "[repl:%s:%s]:%d Terminating updates applying cycle due to termination flag", ns.name,
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
				} catch (const Error& e) {
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

	logPrintf(LogInfo, "[repl:%s:%s] ===End %s synchronization.====", ns.name, slave_->storagePath_,
			  forceSyncReason.empty() ? "WAL" : "FORCE");
	return {err, forceSyncReason};
}

Error Replicator::syncDatabase(std::string_view initiator) {
	std::vector<NamespaceDef> nses;
	logPrintf(LogInfo, "[repl:%s]:%d Starting sync from '%s' state=%d, initiated by %s", slave_->storagePath_, config_.serverId,
			  config_.masterDSN, int(state_.load()), initiator);

	{
		std::lock_guard lck(syncMtx_);
		state_.store(StateSyncing, std::memory_order_release);
		resyncUpdatesLostFlag_ = false;
		transactions_.clear();
		syncedNamespaces_.clear();
		currentSyncNs_.clear();
		pendedUpdates_.clear();
		syncQueue_.Clear();
	}

	Error err = master_->EnumNamespaces(nses, EnumNamespacesOpts());
	if (!err.ok()) {
		if (err.code() == errForbidden || err.code() == errReplParams) {
			terminate_ = true;
		} else {
			retryIfNetworkError(err);
		}
		logPrintf(LogTrace, "[repl:%s] return error", slave_->storagePath_);
		state_.store(StateInit, std::memory_order_release);
		logPrintf(LogError, "[repl:%s] EnumNamespaces error: %s%s", slave_->storagePath_, err.what(),
				  terminate_ ? ", terminating replication"sv : ""sv);
		return err;
	}

	if (config_.namespaces.empty()) {
		err = master_->SubscribeUpdates(this, UpdatesFilters{});
		if (!err.ok()) {
			bool exit = false;
			if (err.code() == errForbidden || err.code() == errReplParams) {
				terminate_ = true;
				exit = true;
			} else if (retryIfNetworkError(err)) {
				exit = true;
			}
			logPrintf(LogError, "[repl:%s] SubscribeUpdates error: %s%s", slave_->storagePath_, err.what(),
					  terminate_ ? ", terminating replication"sv : ""sv);
			if (exit) {
				state_.store(StateInit, std::memory_order_release);
				return err;
			}
		}
	}

	resyncTimer_.stop();
	// Loop for all master namespaces
	for (auto& ns : nses) {
		logPrintf(LogTrace, "[repl:%s:%s]:%d Loop for all master namespaces state=%d", ns.name, slave_->storagePath_, config_.serverId,
				  int(state_.load()));
		// skip system & non enabled namespaces
		if (!isSyncEnabled(ns.name)) {
			continue;
		}
		// skip temporary namespaces (namespace from upstream slave node)
		if (ns.isTemporary) {
			continue;
		}
		if (terminate_) {
			break;
		}

		subscribeUpdatesIfRequired(ns.name);

		{
			std::lock_guard lck(syncMtx_);
			currentSyncNs_ = ns.name;
		}

		ReplicationState replState;
		err = slave_->openNamespace(ns.name, StorageOpts().Enabled().SlaveMode(), dummyCtx_);
		auto slaveNs = slave_->getNamespaceNoThrow(ns.name, dummyCtx_);
		if (err.ok() && slaveNs) {
			replState = slaveNs->GetReplState(dummyCtx_);
			if (replState.replicatorEnabled) {
				slaveNs->SetSlaveReplStatus(ReplicationState::Status::Syncing, errOK, dummyCtx_);
			} else {
				logPrintf(LogError,
						  "[repl:%s:%s]:%d Sync namespace logical error. Set status Syncing. Replication not allowed for namespace.",
						  ns.name, slave_->storagePath_, config_.serverId);
				return Error(errLogic, "Replication not allowed for namespace.");
			}

		} else if (retryIfNetworkError(err)) {
			logPrintf(LogTrace, "[repl:%s] return error", slave_->storagePath_);
			return err;
		} else if (err.code() != errNotFound) {
			logPrintf(LogWarning, "[repl:%s]:%d Error: %s", ns.name, config_.serverId, err.what());
		}

		// If there are open error or fatal error in state, then force full sync
		bool forceSync = !err.ok() || (config_.forceSyncOnLogicError && replState.status == ReplicationState::Status::Fatal);
		std::string_view forceSyncReason;
		if (forceSync) {
			forceSyncReason = !replState.replError.ok() ? replState.replError.what() : "Namespace doesn't exists"sv;
		}

		do {
			auto syncNsRes = syncNamespace(ns, forceSyncReason, nullptr, initiator);
			std::tie(err, forceSyncReason) = {syncNsRes.error, syncNsRes.forceSyncReason};
		} while (err.code() == errUpdatesLost);

		if (!err.ok()) {
			slaveNs = slave_->getNamespaceNoThrow(ns.name, dummyCtx_);
			if (slaveNs) {
				replState = slaveNs->GetReplState(dummyCtx_);
				if (replState.replicatorEnabled) {
					slaveNs->SetSlaveReplStatus(errorIsFatal(err) ? ReplicationState::Status::Fatal : ReplicationState::Status::Error, err,
												dummyCtx_);
				} else {
					logPrintf(
						LogError,
						"[repl:%s:%s]:%d Sync namespace logical error. Set status Fatal. Replication not allowed for namespace. Err= %s",
						ns.name, slave_->storagePath_, config_.serverId, err.what());
				}
			}
			if (retryIfNetworkError(err)) {
				return err;
			}
		}
	}
	state_.store(StateIdle, std::memory_order_release);
	logPrintf(LogInfo, "[repl:%s]:%d Done sync with '%s'", slave_->storagePath_, config_.serverId, config_.masterDSN);
	return err;
}

Error Replicator::syncNamespaceByWAL(const NamespaceDef& nsDef) {
	auto slaveNs = slave_->getNamespaceNoThrow(nsDef.name, dummyCtx_);
	if (!slaveNs) {
		return Error(errNotFound, "Namespace %s not found", nsDef.name);
	}

	lsn_t lsn = slaveNs->GetReplState(dummyCtx_).lastUpstreamLSN;

	if (lsn.isEmpty()) {
		return syncNamespaceForced(nsDef, "Empty lsn in replication state");
	}

	logPrintf(LogTrace, "[repl:%s:%s]:%d Start sync items, Query lsn = %s", nsDef.name, slave_->storagePath_, config_.serverId, lsn);
	//   Make query to master's WAL
	client::QueryResults qr(kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
	Error err = master_->Select(Query(nsDef.name).Where("#lsn", CondGt, int64_t(lsn)), qr);

	switch (err.code()) {
		case errOutdatedWAL:
			// Check if WAL has been outdated, if yes, then force resync
			return syncNamespaceForced(nsDef, err.what());
		case errOK:
			err = applyWAL(slaveNs, qr);
			if (err.ok()) {
				err = slave_->syncDownstream(nsDef.name, false);
			}
			return err;
		case errNoWAL:
			terminate_ = true;
			return err;
		case errNetwork:
		case errTimeout:
		case errCanceled:
		case errParseSQL:
		case errQueryExec:
		case errParams:
		case errLogic:
		case errParseJson:
		case errParseDSL:
		case errConflict:
		case errParseBin:
		case errForbidden:
		case errWasRelock:
		case errNotValid:
		case errNotFound:
		case errStateInvalidated:
		case errBadTransaction:
		case errDataHashMismatch:
		case errTagsMissmatch:
		case errReplParams:
		case errNamespaceInvalidated:
		case errParseMsgPack:
		case errParseProtobuf:
		case errUpdatesLost:
		case errWrongReplicationData:
		case errUpdateReplication:
		case errClusterConsensus:
		case errTerminated:
		case errTxDoesNotExist:
		case errAlreadyConnected:
		case errTxInvalidLeader:
		case errAlreadyProxied:
		case errStrictMode:
		case errQrUIDMissmatch:
		case errSystem:
		case errAssert:
			break;
	}
	return err;
}

// Forced namespace sync
// This will completely drop slave namespace
// read all indexes and data from master, then apply to slave
Error Replicator::syncNamespaceForced(const NamespaceDef& ns, std::string_view reason) {
	logPrintf(LogWarning, "[repl:%s:%s] Start FORCED sync: %s", ns.name, slave_->storagePath_, reason);

	// Create temporary namespace
	NamespaceDef tmpNsDef;
	tmpNsDef.isTemporary = true;
	if (ns.storage.IsEnabled()) {
		tmpNsDef.storage = StorageOpts().Enabled().CreateIfMissing().SlaveMode();
	} else {
		tmpNsDef.storage = StorageOpts().SlaveMode();
	}

	tmpNsDef.name = createTmpNamespaceName(ns.name);
	auto dropTmpNs = [this, &tmpNsDef] {
		auto tmpNs = slave_->getNamespaceNoThrow(tmpNsDef.name, dummyCtx_);
		if (tmpNs) {
			auto dropErr = slave_->closeNamespace(tmpNsDef.name, dummyCtx_, true, true);
			if (!dropErr.ok()) {
				logPrintf(LogWarning, "Unable to drop temporary namespace %s: %s", tmpNsDef.name, dropErr.what());
			}
		}
	};

	auto err = slave_->addNamespace(tmpNsDef, RdxContext());
	if (!err.ok()) {
		logPrintf(LogWarning, "Unable to create temporary namespace %s for the force sync: %s", tmpNsDef.name, err.what());
		dropTmpNs();
		return err;
	}
	Namespace::Ptr tmpNs = slave_->getNamespaceNoThrow(tmpNsDef.name, dummyCtx_);
	if (!tmpNs) {
		logPrintf(LogWarning, "Unable to get temporary namespace %s for the force sync:", tmpNsDef.name);
		return Error(errNotFound, "Namespace %s not found", tmpNsDef.name);
	}
	auto replState = tmpNs->GetReplState(dummyCtx_);
	if (replState.replicatorEnabled) {
		tmpNs->SetSlaveReplStatus(ReplicationState::Status::Syncing, errOK, dummyCtx_);
	} else {
		logPrintf(LogError, "[repl:%s:%s] Sync namespace logical error. Set status Syncing tmpNs. Replication not allowed for namespace.",
				  ns.name, slave_->storagePath_);
		dropTmpNs();
		return Error(errLogic, "Replication not allowed for namespace.");
	}

	err = syncMetaForced(tmpNs, ns.name);
	if (err.ok()) {
		err = syncSchemaForced(tmpNs, NamespaceDef(ns.name));
	}

	//  Make query to complete master's namespace data
	client::QueryResults qr(kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
	if (err.ok()) {
		err = master_->Select(Query(ns.name).Where("#lsn", CondAny, VariantArray{}), qr);
	}
	if (err.ok()) {
		ForceSyncContext fsyncCtx{.nsDef = ns, .replaceTagsMatcher = [&] {
									  if (auto err = tmpNs->ReplaceTagsMatcher(qr.getTagsMatcher(0), dummyCtx_); !err.ok()) {
										  throw err;
									  }
								  }};
		err = applyWAL(tmpNs, qr, &fsyncCtx);
		if (err.code() == errDataHashMismatch) {
			logPrintf(LogError, "[repl:%s] Internal error. dataHash mismatch while fullSync!", ns.name, err.what());
			err = errOK;
		}
	}
	if (err.ok()) {
		err = slave_->renameNamespace(tmpNsDef.name, ns.name, true);
	}
	if (err.ok()) {
		err = slave_->syncDownstream(ns.name, true);
	}

	if (!err.ok()) {
		logPrintf(LogError, "[repl:%s] FORCED sync error: %s", ns.name, err.what());
		dropTmpNs();
	}

	return err;
}

Error Replicator::applyWAL(Namespace::Ptr& slaveNs, client::QueryResults& qr, const ForceSyncContext* fsyncCtx) {
	Error err;
	SyncStat stat;
	WrSerializer ser;
	const auto nsName = slaveNs->GetName(dummyCtx_);
	// process WAL
	auto replSt = slaveNs->GetReplState(dummyCtx_);
	logPrintf(LogTrace, "[repl:%s:%s]:%d applyWAL  lastUpstreamLSN = %s walRecordCount = %d", nsName, slave_->storagePath_,
			  config_.serverId, replSt.lastUpstreamLSN, qr.Count());
	if (fsyncCtx) {
		assertrx(fsyncCtx->replaceTagsMatcher);
		bool hasRawIndexes = qr.Count() && qr.begin().IsRaw() && WALRecord(qr.begin().GetRaw()).type == WalIndexAdd;
		if (!hasRawIndexes) {
			err = syncIndexesForced(slaveNs, *fsyncCtx);
			if (!err.ok()) {
				logPrintf(LogInfo, "[repl:%s]:%d Sync indexes error '%s'", nsName, config_.serverId, err.what());
			}
			fsyncCtx = nullptr;
		}
	}
	if (err.ok()) {
		for (auto it : qr) {
			if (terminate_) {
				break;
			}
			if (qr.Status().ok()) {
				try {
					if (it.IsRaw()) {
						WALRecord wrec(it.GetRaw());
						if (fsyncCtx && wrec.type != WalIndexAdd) {
							fsyncCtx->replaceTagsMatcher();
							fsyncCtx = nullptr;
						}
						err = applyWALRecord(LSNPair(), nsName, slaveNs, wrec, stat);
					} else {
						// Simple item updated
						if (fsyncCtx) {
							fsyncCtx->replaceTagsMatcher();
							fsyncCtx = nullptr;
						}
						ser.Reset();
						err = it.GetCJSON(ser, false);
						if (err.ok()) {
							std::unique_lock lck(syncMtx_);
							if (auto txIt = transactions_.find(slaveNs.get()); txIt == transactions_.end() || txIt->second.IsFree()) {
								lck.unlock();
								err = modifyItem(LSNPair(), slaveNs, ser.Slice(), ModeUpsert, qr.getTagsMatcher(0), stat);
							} else {
								err = modifyItemTx(LSNPair(), txIt->second, ser.Slice(), ModeUpsert, qr.getTagsMatcher(0), stat);
							}
						}
					}
				} catch (const Error& e) {
					err = e;
				}
				if (!err.ok()) {
					logPrintf(LogTrace, "[repl:%s]:%d Error process WAL record with LSN #%s : %s", nsName, config_.serverId,
							  lsn_t(it.GetLSN()), err.what());
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
	}

	ReplicationState slaveState = slaveNs->GetReplState(dummyCtx_);

	// Check data hash, if operation successfull
	if (!stat.masterState.lastLsn.isEmpty() && stat.lastError.ok() && !terminate_ &&
		(slaveState.dataHash != stat.masterState.dataHash || slaveState.dataCount != stat.masterState.dataCount)) {
		stat.lastError = Error(
			errDataHashMismatch, "[repl:%s]:%d dataHash mismatch with master (m: %u; s: %u) or data count missmatch (m: %d; s: %d)", nsName,
			config_.serverId, stat.masterState.dataHash, slaveState.dataHash, stat.masterState.dataCount, slaveState.dataCount);
	}

	if (stat.txStarts != stat.txEnds) {
		logPrintf(LogWarning,
				  "[repl:%s]:%d The discrepancy between the number of started transactions and the number of completed transactions(%d:%d)",
				  nsName, config_.serverId, stat.txStarts, stat.txEnds);
	}

	if (stat.lastError.ok() && !terminate_) {
		logPrintf(LogTrace, "[repl:%s]:%d applyWal SetReplLSNs upstreamLsn = %s originLsn = %s", nsName, config_.serverId,
				  stat.masterState.lastLsn, stat.masterState.originLSN);
		//  counters from the upstream node (from WalReplState)
		slaveNs->SetReplLSNs(LSNPair(stat.masterState.lastLsn, stat.masterState.originLSN), dummyCtx_);
	}

	ser.Reset();
	stat.Dump(ser) << "lsn #" << int64_t(slaveState.lastLsn);

	logPrintf((!stat.lastError.ok() ? LogError : LogInfo), "[repl:%s:%s]:%d Sync %s: %s", nsName, slave_->storagePath_, config_.serverId,
			  terminate_ ? "terminated" : "done", ser.Slice());

	if (terminate_) {
		return Error(errCanceled, "terminated");
	}
	return stat.lastError;
}

Error Replicator::applyTxWALRecord(LSNPair LSNs, std::string_view nsName, Namespace::Ptr& slaveNs, const WALRecord& rec, SyncStat& stat) {
	switch (rec.type) {
		// Modify item
		case WalItemModify: {
			std::lock_guard lck(syncMtx_);
			Transaction& tx = transactions_[slaveNs.get()];
			if (tx.IsFree()) {
				return Error(errLogic, "[repl:%s]:%d Transaction was not initiated.", nsName, config_.serverId);
			}
			Item item = tx.NewItem();
			Error err = unpackItem(item, LSNs.upstreamLSN_, rec.itemModify.itemCJson, master_->NewItem(nsName).impl_->tagsMatcher());
			if (!err.ok()) {
				return err;
			}
			try {
				tx.Modify(std::move(item), static_cast<ItemModifyMode>(rec.itemModify.modifyMode));
			} catch (Error& e) {
				return e;
			}
		} break;
		// Update query
		case WalUpdateQuery: {
			QueryResults result;
			Query q = Query::FromSQL(rec.data);
			std::lock_guard lck(syncMtx_);
			Transaction& tx = transactions_[slaveNs.get()];
			if (tx.IsFree()) {
				return Error(errLogic, "[repl:%s]:%d Transaction was not initiated.", nsName, config_.serverId);
			}
			try {
				tx.Modify(std::move(q));
			} catch (Error& e) {
				return e;
			}
		} break;
		case WalInitTransaction: {
			std::lock_guard lck(syncMtx_);
			Transaction& tx = transactions_[slaveNs.get()];
			if (!tx.IsFree()) {
				logPrintf(LogError, "[repl:%s]:%d Init transaction befor commit of previous one.", nsName, config_.serverId);
			}
			RdxContext rdxContext(true, LSNs);
			tx = slaveNs->NewTransaction(rdxContext);
			if (!tx.Status().ok()) {
				return tx.Status();
			}
			stat.txStarts++;
		} break;
		case WalCommitTransaction: {
			QueryResults res;
			std::lock_guard lck(syncMtx_);
			Transaction& tx = transactions_[slaveNs.get()];
			if (tx.IsFree()) {
				return Error(errLogic, "[repl:%s]:%d Commit of transaction befor initiate it.", nsName, config_.serverId);
			}
			RdxContext rdxContext(true, LSNs);
			slaveNs->CommitTransaction(tx, res, rdxContext);
			stat.txEnds++;
			tx = Transaction{};
		} break;
		case WalTagsMatcher: {
			TagsMatcher tm;
			Serializer ser(rec.data.data(), rec.data.size());
			const auto version = ser.GetVarint();
			const auto stateToken = ser.GetVarint();
			tm.deserialize(ser, version, stateToken);
			logPrintf(LogInfo, "[repl:%s]:%d Got new tagsmatcher replicated via tx: { state_token: %08X, version: %d }", nsName,
					  config_.serverId, stateToken, version);

			std::lock_guard lck(syncMtx_);
			Transaction& tx = transactions_[slaveNs.get()];
			if (tx.IsFree()) {
				return Error(errLogic, "[repl:%s]:%d Transaction was not initiated.", nsName, config_.serverId);
			}
			try {
				tx.MergeTagsMatcher(std::move(tm));
			} catch (Error& e) {
				return e;
			}
		} break;
		case WalPutMeta: {
			std::lock_guard lck(syncMtx_);
			Transaction& tx = transactions_[slaveNs.get()];
			if (tx.IsFree()) {
				return Error(errLogic, "[repl:%s]:%d Transaction was not initiated.", nsName, config_.serverId);
			}

			try {
				tx.PutMeta(std::string(rec.itemMeta.key), rec.itemMeta.value);
			} catch (Error& e) {
				return e;
			}
		} break;
		case WalEmpty:
		case WalReplState:
		case WalItemUpdate:
		case WalIndexAdd:
		case WalIndexDrop:
		case WalIndexUpdate:
		case WalNamespaceAdd:
		case WalNamespaceDrop:
		case WalNamespaceRename:
		case WalForceSync:
		case WalSetSchema:
		case WalWALSync:
		case WalResetLocalWal:
		case WalRawItem:
		case WalShallowItem:
		case WalDeleteMeta:
			return Error(errLogic, "Unexpected for transaction WAL rec type %d\n", int(rec.type));
	}
	return {};
}

void Replicator::checkNoOpenedTransaction(std::string_view nsName, Namespace::Ptr& slaveNs) {
	std::lock_guard lck(syncMtx_);
	auto txIt = transactions_.find(slaveNs.get());
	if (txIt != transactions_.end() && !txIt->second.IsFree()) {
		logPrintf(LogError, "[repl:%s]:%d Transaction started but not commited", nsName, config_.serverId);
		throw Error(errLogic, "Transaction for '%s' was started but was not commited", nsName);
	}
}

Error Replicator::applyWALRecord(LSNPair LSNs, std::string_view nsName, Namespace::Ptr& slaveNs, const WALRecord& rec, SyncStat& stat) {
	Error err;
	IndexDef iDef;

	if (!slaveNs && !(rec.type == WalNamespaceAdd || rec.type == WalForceSync)) {
		return Error(errParams, "Namespace %s not found", nsName);
	}

	if (rec.inTransaction) {
		return applyTxWALRecord(LSNs, nsName, slaveNs, rec, stat);
	}

	checkNoOpenedTransaction(nsName, slaveNs);

	auto sendSyncAsync = [this](const WALRecord& rec, bool forced) {
		NamespaceDef nsDef;
		Error err = nsDef.FromJSON(giftStr(rec.data));
		if (!err.ok()) {
			return err;
		}
		syncQueue_.Push(nsDef.name, std::move(nsDef), forced);
		walSyncAsync_.send();
		return err;
	};
	RdxContext rdxContext(true, LSNs);
	switch (rec.type) {
		// Modify item
		case WalItemModify:
			err = modifyItem(LSNs, slaveNs, rec.itemModify.itemCJson, rec.itemModify.modifyMode,
							 master_->NewItem(nsName).impl_->tagsMatcher(), stat);
			break;
		// Index added
		case WalIndexAdd:
			err = iDef.FromJSON(giftStr(rec.data));
			if (err.ok()) {
				slaveNs->AddIndex(iDef, rdxContext);
			}
			stat.updatedIndexes++;
			break;
		// Index dropped
		case WalIndexDrop:
			err = iDef.FromJSON(giftStr(rec.data));
			if (err.ok()) {
				slaveNs->DropIndex(iDef, rdxContext);
			}
			stat.deletedIndexes++;
			break;
		// Index updated
		case WalIndexUpdate:
			err = iDef.FromJSON(giftStr(rec.data));
			if (err.ok()) {
				slaveNs->UpdateIndex(iDef, rdxContext);
			}
			stat.updatedIndexes++;
			break;
		// Metadata updated
		case WalPutMeta:
			slaveNs->PutMeta(std::string(rec.itemMeta.key), rec.itemMeta.value, dummyCtx_);
			stat.updatedMeta++;
			break;
		// Metadata deleted
		case WalDeleteMeta:
			slaveNs->DeleteMeta(std::string(rec.itemMeta.key), dummyCtx_);
			stat.updatedMeta++;
			break;
		// Update query
		case WalUpdateQuery: {
			logPrintf(LogTrace, "[repl:%s]:%d WalUpdateQuery", nsName, config_.serverId);
			QueryResults result;
			Query q = Query::FromSQL(rec.data);
			switch (q.type_) {
				case QueryDelete:
					slaveNs->Delete(q, result, rdxContext);
					break;
				case QueryUpdate:
					slaveNs->Update(q, result, rdxContext);
					break;
				case QueryTruncate:
					slaveNs->Truncate(rdxContext);
					break;
				case QuerySelect:
					break;
			}
			break;
		}
		// New namespace
		case WalNamespaceAdd: {
			err = slave_->openNamespace(nsName, StorageOpts().Enabled().CreateIfMissing().SlaveMode(), dummyCtx_);
			break;
		}
		// Drop namespace
		case WalNamespaceDrop:
			err = slave_->closeNamespace(nsName, rdxContext, true, true);
			break;
		// Rename namespace
		case WalNamespaceRename:
			err = slave_->renameNamespace(nsName, std::string(rec.data), true);
			break;
		// force sync namespace
		case WalForceSync:
			logPrintf(LogTrace, "[repl:%s]:%d WalForceSync: Scheduling force-sync for the namespace", nsName, config_.serverId);
			return sendSyncAsync(rec, true);
			break;
		case WalWALSync:
			logPrintf(LogTrace, "[repl:%s]:%d WalWALSync: Scheduling WAL-sync for the namespace", nsName, config_.serverId);
			return sendSyncAsync(rec, false);
			break;

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
			slaveNs->SetSchema(rec.data, rdxContext);
			stat.schemasSet++;
			break;
		case WalTagsMatcher: {
			TagsMatcher tm;
			Serializer ser(rec.data.data(), rec.data.size());
			const auto version = ser.GetVarint();
			const auto stateToken = ser.GetVarint();
			tm.deserialize(ser, version, stateToken);
			logPrintf(LogInfo, "[repl:%s]:%d Got new tagsmatcher replicated via single record: { state_token: %08X, version: %d }", nsName,
					  config_.serverId, stateToken, version);
			slaveNs->MergeTagsMatcher(tm, rdxContext);
		} break;
		case WalEmpty:
		case WalItemUpdate:
		case WalInitTransaction:
		case WalCommitTransaction:
		case WalResetLocalWal:
		case WalRawItem:
		case WalShallowItem:
		default:
			return Error(errLogic, "Unexpected WAL rec type %d\n", int(rec.type));
	}
	return err;
}

Error Replicator::unpackItem(Item& item, lsn_t lsn, std::string_view cjson, const TagsMatcher& tm) {
	if (item.impl_->tagsMatcher().size() < tm.size()) {
		const bool res = item.impl_->tagsMatcher().try_merge(tm);
		if (!res) {
			return Error(errNotValid, "Can't merge tagsmatcher of item with lsn ");
		}
	}
	item.setLSN(int64_t(lsn));
	return item.FromCJSON(cjson);
}

void Replicator::pushPendingUpdate(LSNPair LSNs, std::string_view nsName, const WALRecord& wrec) {
	PackedWALRecord pwrec;
	pwrec.Pack(wrec);
	auto updatesIt = pendedUpdates_.find(nsName);
	if (updatesIt == pendedUpdates_.end()) {
		UpdatesData updates;
		updates.container.emplace_back(std::make_pair(LSNs, std::move(pwrec)));
		pendedUpdates_.emplace(std::string(nsName), std::move(updates));
	} else {
		auto& updates = updatesIt.value().container;
		updates.emplace_back(std::make_pair(LSNs, std::move(pwrec)));
	}
}

Error Replicator::modifyItem(LSNPair LSNs, Namespace::Ptr& slaveNs, std::string_view cjson, int modifyMode, const TagsMatcher& tm,
							 SyncStat& stat) {
	Item item = slaveNs->NewItem(dummyCtx_);
	Error err = unpackItem(item, LSNs.upstreamLSN_, cjson, tm);

	if (err.ok()) {
		RdxContext rdxContext(true, LSNs);
		switch (modifyMode) {
			case ModeDelete:
				slaveNs->Delete(item, rdxContext);
				stat.deleted++;
				break;
			case ModeInsert:
				slaveNs->Insert(item, rdxContext);
				stat.updated++;
				break;
			case ModeUpsert:
				slaveNs->Upsert(item, rdxContext);
				stat.updated++;
				break;
			case ModeUpdate:
				slaveNs->Update(item, rdxContext);
				stat.updated++;
				break;
			default:
				return Error(errNotValid, "Unknown modify mode %d of item with lsn %ul", modifyMode, int64_t(LSNs.upstreamLSN_));
		}
	}
	return err;
}

Error Replicator::modifyItemTx(LSNPair LSNs, Transaction& tx, std::string_view cjson, int modifyMode, const TagsMatcher& tm,
							   SyncStat& stat) {
	Item item = tx.NewItem();
	Error err = unpackItem(item, LSNs.upstreamLSN_, cjson, tm);

	if (err.ok()) {
		switch (modifyMode) {
			case ModeDelete:
				tx.Delete(std::move(item));
				stat.deleted++;
				break;
			case ModeInsert:
				tx.Insert(std::move(item));
				stat.updated++;
				break;
			case ModeUpsert:
				tx.Upsert(std::move(item));
				stat.updated++;
				break;
			case ModeUpdate:
				tx.Update(std::move(item));
				stat.updated++;
				break;
			default:
				return Error(errNotValid, "Unknown modify mode %d of tx item with lsn %ul", modifyMode, int64_t(LSNs.upstreamLSN_));
		}
	}
	return err;
}

WrSerializer& Replicator::SyncStat::Dump(WrSerializer& ser) {
	if (updated) {
		ser << updated << " items updated; ";
	}
	if (deleted) {
		ser << deleted << " items deleted; ";
	}
	if (updatedIndexes) {
		ser << updatedIndexes << " indexes updated; ";
	}
	if (deletedIndexes) {
		ser << deletedIndexes << " indexes deleted; ";
	}
	if (updatedMeta) {
		ser << updatedMeta << " meta updated; ";
	}
	if (schemasSet) {
		ser << "New schema was set; ";
	}
	if (txStarts || txEnds) {
		ser << " started " << txStarts << ", completed " << txEnds << " transactions; ";
	}
	if (errors || !lastError.ok()) {
		ser << errors << " errors (" << lastError.what() << ") ";
	}
	if (!ser.Len()) {
		ser << "Up to date; ";
	}
	if (processed) {
		ser << "processed " << processed << " WAL records ";
	}
	return ser;
}

Error Replicator::syncIndexesForced(Namespace::Ptr& slaveNs, const ForceSyncContext& fsyncCtx) {
	const std::string& nsName = fsyncCtx.nsDef.name;

	Error err;
	for (auto& idx : fsyncCtx.nsDef.indexes) {
		logPrintf(LogTrace, "[repl:%s] Updating index '%s'", nsName, idx.name_);
		try {
			slaveNs->AddIndex(idx, dummyCtx_);
		} catch (const Error& e) {
			logPrintf(LogError, "[repl:%s] Error add index '%s': %s", nsName, idx.name_, err.what());
			err = e;
		}
	}
	try {
		assertrx(fsyncCtx.replaceTagsMatcher);
		fsyncCtx.replaceTagsMatcher();
	} catch (const Error& e) {
		logPrintf(LogError, "[repl:%s] Error on TagsMatcher replace call: %s", nsName, err.what());
		err = e;
	}

	return err;
}

Error Replicator::syncSchemaForced(Namespace::Ptr& slaveNs, const NamespaceDef& masterNsDef) {
	const std::string& nsName = masterNsDef.name;

	Error err = errOK;
	logPrintf(LogTrace, "[repl:%s] Setting schema", nsName);
	try {
		slaveNs->SetSchema(masterNsDef.schemaJson, dummyCtx_);
	} catch (const Error& e) {
		logPrintf(LogError, "[repl:%s] Error in set schema: %s", nsName, err.what());
		err = e;
	}

	return err;
}

Error Replicator::syncMetaForced(Namespace::Ptr& slaveNs, std::string_view nsName) {
	std::vector<std::string> keys;
	auto err = master_->EnumMeta(nsName, keys);

	for (auto& key : keys) {
		std::string data;
		err = master_->GetMeta(nsName, key, data);
		if (!err.ok()) {
			logPrintf(LogError, "[repl:%s]:%d Error get meta '%s': %s", nsName, config_.serverId, key, err.what());
			continue;
		}
		try {
			slaveNs->PutMeta(key, data, dummyCtx_);
		} catch (const Error& e) {
			logPrintf(LogError, "[repl:%s]:%d Error set meta '%s': %s", slaveNs->GetName(dummyCtx_), config_.serverId, key, e.what());
		}
	}
	return errOK;
}

// Callback from WAL updates pusher
void Replicator::OnWALUpdate(LSNPair LSNs, std::string_view nsName, const WALRecord& wrec) {
	try {
		onWALUpdateImpl(LSNs, nsName, wrec);
	} catch (Error& e) {
		logPrintf(LogError, "[repl:%s]:%d Exception on WAL update: %s", nsName, config_.serverId, e.what());
		assertrx_dbg(false);
		resync_.send();
	} catch (std::exception& e) {
		logPrintf(LogError, "[repl:%s]:%d Exception on WAL update (std::exception): %s", nsName, config_.serverId, e.what());
		assertrx_dbg(false);
		resync_.send();
	} catch (...) {
		logPrintf(LogError, "[repl:%s]:%d Exception on WAL update: <unknow exception type>", nsName, config_.serverId);
		assertrx_dbg(false);
		resync_.send();
	}
}

void Replicator::onWALUpdateImpl(LSNPair LSNs, std::string_view nsName, const WALRecord& wrec) {
	auto sId = LSNs.originLSN_.Server();
	if (sId != 0) {	 // sId = 0 for configurations without specifying a server id
		if (sId == config_.serverId) {
			logPrintf(LogTrace, "[repl:%s]:%d OnWALUpdate equal serverId=%d originLSN=%lX serverIdFromLSN=%d", nsName, config_.serverId,
					  config_.serverId, int64_t(LSNs.originLSN_), LSNs.originLSN_.Server());
			return;
		}
	}
	logPrintf(LogTrace, "[repl:%s:%s]:%d OnWALUpdate state = %d upstreamLSN = %s", nsName, slave_->storagePath_, config_.serverId,
			  int(state_.load(std::memory_order_acquire)), LSNs.upstreamLSN_);
	if (!canApplyUpdate(LSNs, nsName, wrec)) {
		return;
	}
	Error err;
	auto slaveNs = slave_->getNamespaceNoThrow(nsName, dummyCtx_);

	// necessary for cutting off onWALUpdate already arrived in applyWal (it is possible!)
	if (slaveNs && !LSNs.upstreamLSN_.isEmpty()) {
		auto replState = slaveNs->GetReplState(dummyCtx_);
		if (!replState.lastUpstreamLSN.isEmpty()) {
			if (replState.lastUpstreamLSN.Counter() >= LSNs.upstreamLSN_.Counter()) {
				logPrintf(LogTrace,
						  "[repl:%s:%s]:%d OnWALUpdate old record state = %d upstreamLSN = %s replState.lastUpstreamLSN=%s wrec.type = %d",
						  nsName, slave_->storagePath_, config_.serverId, int(state_.load(std::memory_order_acquire)), LSNs.upstreamLSN_,
						  replState.lastUpstreamLSN, wrec.type);
				return;
			}
		}
	}

	SyncStat stat;
	try {
		err = applyWALRecord(LSNs, nsName, slaveNs, wrec, stat);
	} catch (const Error& e) {
		err = e;
	}
	if (err.ok()) {
		if (slaveNs && shouldUpdateLsn(wrec) && !LSNs.upstreamLSN_.isEmpty()) {
			slaveNs->SetReplLSNs(LSNs, dummyCtx_);
		}
	} else {
		if (slaveNs) {
			auto replState = slaveNs->GetReplState(dummyCtx_);
			if (replState.status != ReplicationState::Status::Fatal) {
				if (replState.replicatorEnabled) {
					slaveNs->SetSlaveReplStatus(ReplicationState::Status::Fatal, err, dummyCtx_);
				} else {
					logPrintf(LogError, "[repl:%s:%s]:%d OnWALUpdate logical error. Replication not allowed for nanespace. Err = %s",
							  nsName, slave_->storagePath_, config_.serverId, err.what());
				}
			}
		}
		auto lastErrIt = lastNsErrMsg_.find(nsName);
		if (lastErrIt == lastNsErrMsg_.end()) {
			lastErrIt = lastNsErrMsg_.emplace(std::string(nsName), NsErrorMsg{}).first;
		}
		auto& lastErr = lastErrIt->second;
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

void Replicator::OnUpdatesLost(std::string_view nsName) {
	std::unique_lock lck(syncMtx_);
	auto updatesIt = pendedUpdates_.find(nsName);
	if (updatesIt == pendedUpdates_.end()) {
		UpdatesData updates;
		updates.UpdatesLost = true;
		pendedUpdates_.emplace(std::string(nsName), std::move(updates));
		logPrintf(LogTrace, "[repl:%s:%s]:%d Lost updates add.", nsName, slave_->storagePath_, config_.serverId);
	} else {
		logPrintf(LogTrace, "[repl:%s:%s]:%d Lost updates set TRUE.", nsName, slave_->storagePath_, config_.serverId);
		updatesIt.value().UpdatesLost = true;
	}

	resyncUpdatesLostFlag_ = true;
	resyncUpdatesLostAsync_.send();
}

void Replicator::OnConnectionState(const Error& err) {
	if (err.ok()) {
		logPrintf(LogInfo, "[repl:] OnConnectionState connected");
	} else {
		logPrintf(LogInfo, "[repl:] OnConnectionState closed, reason: %s", err.what());
	}
	std::unique_lock lck(syncMtx_);
	state_.store(StateInit, std::memory_order_release);
	resync_.send();
}

bool Replicator::canApplyUpdate(LSNPair LSNs, std::string_view nsName, const WALRecord& wrec) {
	if (!isSyncEnabled(nsName)) {
		return false;
	}

	if (terminate_.load(std::memory_order_acquire)) {
		logPrintf(LogTrace, "[repl:%s]:%d Skipping update due to replicator shutdown is in progress upstreamLSN %s (%d)", nsName,
				  config_.serverId, LSNs.upstreamLSN_, int(wrec.type));
		return false;
	}

	if (state_.load(std::memory_order_acquire) == StateIdle && !resyncUpdatesLostFlag_ && syncQueue_.Size() == 0) {
		logPrintf(LogTrace, "[repl:%s]:%d apply update upstreamLSN %s (%d)", nsName, config_.serverId, LSNs.upstreamLSN_, int(wrec.type));
		return true;
	}

	std::lock_guard lck(syncMtx_);
	if (syncQueue_.Size() != 0 && syncQueue_.Contains(nsName)) {
		logPrintf(LogTrace,
				  "[repl:%s]:%d Skipping update due to scheduled namespace resync %s (%d). Submitting this update to the pending queue",
				  nsName, config_.serverId, LSNs.upstreamLSN_, int(wrec.type));
		pushPendingUpdate(LSNs, nsName, wrec);
		return false;
	}

	auto state = state_.load(std::memory_order_acquire);
	if (state == StateIdle) {
		if (!resyncUpdatesLostFlag_) {
			logPrintf(LogTrace, "[repl:%s]:%d apply update upstreamLSN %s (%d)", nsName, config_.serverId, LSNs.upstreamLSN_,
					  int(wrec.type));
			return true;
		} else {
			auto it = pendedUpdates_.find(nsName);
			if (it != pendedUpdates_.end()) {
				if (it->second.UpdatesLost) {
					logPrintf(LogTrace, "[repl:%s]:%d Skipping update (lsn: %d; type: %d): there are lost updates for namespace", nsName,
							  config_.serverId, LSNs.upstreamLSN_, int(wrec.type));
					return false;
				} else {
					logPrintf(LogTrace, "[repl:%s]:%d apply update pendeded not empty %s (%d)", nsName, config_.serverId, LSNs.upstreamLSN_,
							  int(wrec.type));
					return true;
				}
			} else {
				logPrintf(LogTrace, "[repl:%s]:%d apply update pendeded empty %s (%d)", nsName, config_.serverId, LSNs.upstreamLSN_,
						  int(wrec.type));
				return true;
			}
		}
	}
	bool terminate = terminate_.load(std::memory_order_acquire);
	if (state == StateInit || terminate) {
		logPrintf(LogTrace, "[repl:%s]:%d Skipping update due to replicator %s is in progress upstreamLSN %s (%d)", nsName,
				  config_.serverId, terminate ? "shutdown" : "startup", LSNs.upstreamLSN_, int(wrec.type));
		return false;
	}

	if (wrec.type == WalWALSync || wrec.type == WalForceSync) {
		return true;
	}

	if (std::string_view(currentSyncNs_) != nsName) {
		if (syncedNamespaces_.find(nsName) != syncedNamespaces_.end()) {
			logPrintf(LogTrace, "[repl:%s]:%d applying update for synced ns %s (%d)", nsName, config_.serverId, LSNs.upstreamLSN_,
					  int(wrec.type));
			return true;
		} else {
			logPrintf(LogTrace, "[repl:%s]:%d Skipping update - namespace was not synced yet, upstreamLSN %s (%d)", nsName,
					  config_.serverId, LSNs.upstreamLSN_, int(wrec.type));
			if (wrec.type == WalNamespaceAdd || wrec.type == WalNamespaceDrop || wrec.type == WalNamespaceRename) {
				logPrintf(LogInfo, "[repl:%s]:%d Scheduling resync due to concurrent ns add/delete: %d", nsName, config_.serverId,
						  int(wrec.type));
				resync_.send();
			}
			return false;
		}
	}
	logPrintf(LogTrace, "[repl:%s:%s]:%d Pending update due to concurrent sync upstreamLSN %s", nsName, slave_->storagePath_,
			  config_.serverId, LSNs.upstreamLSN_);
	pushPendingUpdate(LSNs, nsName, wrec);
	return false;
}

bool Replicator::isSyncEnabled(std::string_view nsName) {
	// SKip system ns
	if (isSystemNamespaceNameFast(nsName)) {
		return false;
	}

	// skip non enabled namespaces
	if (config_.namespaces.size() && config_.namespaces.find(nsName) == config_.namespaces.end()) {
		return false;
	}
	return true;
}

void Replicator::SyncQueue::Push(const std::string& nsName, NamespaceDef&& nsDef, bool forced) {
	std::lock_guard lock(mtx_);
	auto& val = queue_[nsName];
	val = recordData(std::move(nsDef), val.forced || forced);
	size_.fetch_add(1, std::memory_order_release);
}

bool Replicator::SyncQueue::Get(NamespaceDef& def, bool& forced) const {
	std::lock_guard lock(mtx_);
	if (!queue_.empty()) {
		auto it = queue_.begin();
		def = it->second.def;
		forced = it->second.forced;
		return true;
	}
	return false;
}

bool Replicator::SyncQueue::Pop(std::string_view nsName, const std::unique_lock<std::mutex>& replicatorLock) noexcept {
	std::lock_guard lock(mtx_);
	// Pop() must be called under top level replicator's sync mutex
	assertrx(replicatorLock.owns_lock());
	assertrx(replicatorLock.mutex() == &replicatorMtx_);
	(void)replicatorMtx_;
	(void)replicatorLock;
	auto cnt = queue_.erase(nsName);
	if (cnt) {
		size_.fetch_sub(cnt, std::memory_order_release);
	}
	return false;
}

bool Replicator::SyncQueue::Contains(std::string_view nsName) noexcept {
	std::lock_guard lock(mtx_);
	return queue_.find(nsName) != queue_.end();
}

void Replicator::SyncQueue::Clear() noexcept {
	std::lock_guard lock(mtx_);
	queue_.clear();
	size_.store(0, std::memory_order_release);
}

}  // namespace reindexer
