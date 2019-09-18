
#include "replicator.h"
#include "client/itemimpl.h"
#include "client/reindexer.h"
#include "core/itemimpl.h"
#include "core/namespace.h"
#include "core/namespacedef.h"
#include "core/reindexerimpl.h"
#include "tools/logger.h"
#include "walrecord.h"

namespace reindexer {

using namespace net;

Replicator::Replicator(ReindexerImpl *slave) : slave_(slave), terminate_(false), state_(StateInit) {
	stop_.set(loop_);
	resync_.set(loop_);
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

	auto err = master_->Connect(config_.masterDSN);
	terminate_ = false;
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
	state_ = StateInit;
	syncMtx_.unlock();

	master_->SubscribeUpdates(this, true);

	resync_.set([this](ev::async &) { syncDatabase(); });
	resync_.start();

	syncDatabase();

	while (!terminate_) {
		loop_.run();
	}

	resync_.stop();
	stop_.stop();
	logPrintf(LogInfo, "[repl] Replicator with %s stopped", config_.masterDSN);
}

// Sync database
Error Replicator::syncDatabase() {
	const static RdxContext dummyCtx;
	vector<NamespaceDef> nses;
	logPrintf(LogInfo, "[repl] Starting sync from '%s'", config_.masterDSN);

	Error err = master_->EnumNamespaces(nses, false);
	if (!err.ok()) {
		logPrintf(LogError, "[repl] EnumNamespaces error: %s", err.what());
		return err;
	}

	syncMtx_.lock();
	for (auto &ns : nses) maxLsns_[ns.name] = -1;
	state_ = StateSyncing;
	syncMtx_.unlock();

	// Loop for all master namespaces
	for (auto &ns : nses) {
		// skip system & non enabled namespaces
		if (!isSyncEnabled(ns.name)) continue;

		if (terminate_) break;

		err = slave_->OpenNamespace(ns.name, StorageOpts().Enabled().CreateIfMissing().SlaveMode());
		if (!err.ok()) logPrintf(LogError, "[repl:%s] Error: %s", ns.name, err.what());

		// Protect for concurent updates stream of same namespace
		// if state is StateSync is set, then concurent updates will not modify data, but just set maxLsn_

		for (bool done = false; err.ok() && !done && !terminate_;) {
			err = syncNamespaceByWAL(ns);
			if (!err.ok()) {
				logPrintf(LogError, "[repl:%s] syncNamespace error: %s", ns.name, err.what());
				if (err.code() == errDataHashMismatch && !terminate_) {
					if (config_.forceSyncOnWrongDataHash) {
						err = syncNamespaceForced(ns, "DataHash mismatch");
					} else {
						err = errOK;
					}
				} else if (err.code() != errNetwork && !terminate_ && config_.forceSyncOnLogicError) {
					err = syncNamespaceForced(ns, "Logic error occurried");
				} else
					break;
				if (!err.ok()) {
					logPrintf(LogError, "[repl:%s] syncNamespace error: %s", ns.name, err.what());
					break;
				}
			}
			int64_t curLSN = slave_->getNamespace(ns.name, dummyCtx)->GetReplState(dummyCtx).lastLsn;
			syncMtx_.lock();
			// Check, if concurrent update attempt happened with LSN bigger, than current LSN
			// In this case retry sync
			if (maxLsns_[ns.name] <= curLSN) {
				done = true;
				maxLsns_.erase(ns.name);
			}
			syncMtx_.unlock();
		}
	};
	state_ = StateIdle;

	return err;
}

// Foced namespace sync
// This will completely drop slave namespace
// read all indexes and data from master, then apply to slave
Error Replicator::syncNamespaceByWAL(const NamespaceDef &ns) {
	const static RdxContext dummyCtx;
	auto slaveNs = slave_->getNamespace(ns.name, dummyCtx);
	int64_t lsn = slaveNs->GetReplState(dummyCtx).lastLsn;

	logPrintf(LogTrace, "[repl:%s] Start sync items, lsn %ld", ns.name, lsn);

	//  Make query to master's WAL
	client::QueryResults qr(kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
	Error err = master_->Select(Query(ns.name).Where("#lsn", CondGt, lsn), qr);

	switch (err.code()) {
		case errOutdatedWAL:
			// Check if WAL has been outdated, if yes, then force resync
			return syncNamespaceForced(ns, err.what());
		case errOK:
			return applyWAL(ns.name, qr);
		case errNoWAL:
			terminate_ = true;
			return err;
		default:
			return err;
	}
}

// Foced namespace sync
// This will completely drop slave namespace
// read all indexes and data from master, then apply to slave
Error Replicator::syncNamespaceForced(const NamespaceDef &ns, string_view reason) {
	const static RdxContext dummyCtx;
	logPrintf(LogWarning, "[repl:%s] Start FORCED sync: %s", ns.name, reason);

	// Drop and recreate namespace
	auto err = slave_->closeNamespace(ns.name, dummyCtx, true, true);
	if (err.ok() || err.code() == errNotFound) {
		err = slave_->OpenNamespace(ns.name, StorageOpts().Enabled().CreateIfMissing().SlaveMode());
	}
	if (err.ok()) err = syncIndexesForced(ns);
	if (err.ok()) err = syncMetaForced(ns.name);

	//  Make query to complete master's namespace data
	client::QueryResults qr(kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
	if (err.ok()) err = master_->Select(Query(ns.name).Where("#lsn", CondAny, {}), qr);
	if (err.ok()) {
		slave_->getNamespace(ns.name, dummyCtx)->ReplaceTagsMatcher(qr.getTagsMatcher(0), dummyCtx);
		err = applyWAL(ns.name, qr);
	}
	return err;
}

Error Replicator::applyWAL(string_view nsName, client::QueryResults &qr) {
	const static RdxContext dummyCtx;
	Error err;
	SyncStat stat;

	auto slaveNs = slave_->getNamespace(nsName, dummyCtx);

	WrSerializer ser;
	// process WAL
	int64_t slaveLSN = slaveNs->GetReplState(dummyCtx).lastLsn;
	for (auto it : qr) {
		if (terminate_) break;
		try {
			int64_t lsn = it.GetLSN();
			slaveLSN = std::max(lsn, slaveLSN);
			if (it.IsRaw()) {
				err = applyWALRecord(lsn, nsName, slaveNs, WALRecord(it.GetRaw()), stat);
			} else {
				// Simple item updated
				ser.Reset();
				err = it.GetCJSON(ser, false);
				if (err.ok()) err = applyItemCJson(lsn, slaveNs, ser.Slice(), ModeUpsert, qr.getTagsMatcher(0), stat);
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
	}
	if (!qr.Status().ok()) {
		stat.lastError = qr.Status();
		logPrintf(LogTrace, "[repl:%s] Error executing WAL query: %s", nsName, stat.lastError.what());
	}

	if (stat.lastError.ok() && !terminate_) {
		// Set slave LSN if operation successfull
		slaveLSN = std::max(stat.masterState.lastLsn, slaveLSN);
		slaveNs->SetSlaveLSN(slaveLSN, dummyCtx);
	}
	ReplicationState slaveState = slaveNs->GetReplState(dummyCtx);

	// Check data hash, if operation successfull
	if (stat.masterState.lastLsn >= 0 && stat.lastError.ok() && !terminate_ && slaveState.dataHash != stat.masterState.dataHash) {
		err = stat.lastError = Error(errDataHashMismatch, "[repl:%s] dataHash mismatch with master %u != %u; itemsCount %d %d; lsn %d %d",
									 nsName, stat.masterState.dataHash, slaveState.dataHash, stat.masterState.dataCount,
									 slaveState.dataCount, stat.masterState.lastLsn, slaveLSN);
	}

	ser.Reset();

	stat.Dump(ser) << "lsn #" << slaveState.lastLsn;

	logPrintf(stat.errors ? LogError : LogInfo, "[repl:%s] Sync %s: %s", nsName, terminate_ ? "terminated" : "done", ser.c_str());

	return err;
}

Error Replicator::applyWALRecord(int64_t lsn, string_view nsName, std::shared_ptr<Namespace> slaveNs, const WALRecord &rec,
								 SyncStat &stat) {
	const static RdxContext dummyCtx;
	Error err;
	IndexDef iDef;

	if (!slaveNs && rec.type != WalNamespaceAdd) {
		return Error(errParams, "Namespace %s not found", nsName);
	}

	switch (rec.type) {
		// Modify item
		case WalItemModify:
			err = applyItemCJson(lsn, slaveNs, rec.itemModify.itemCJson, rec.itemModify.modifyMode,
								 master_->NewItem(nsName).impl_->tagsMatcher(), stat);
			break;
		// Index added
		case WalIndexAdd:
			err = iDef.FromJSON(giftStr(rec.data));
			if (err.ok()) slaveNs->AddIndex(iDef, dummyCtx);
			stat.updatedIndexes++;
			break;
		// Index dropped
		case WalIndexDrop:
			err = iDef.FromJSON(giftStr(rec.data));
			if (err.ok()) slaveNs->DropIndex(iDef, dummyCtx);
			stat.deletedIndexes++;
			break;
		// Index updated
		case WalIndexUpdate:
			err = iDef.FromJSON(giftStr(rec.data));
			if (err.ok()) slaveNs->UpdateIndex(iDef, dummyCtx);
			stat.updatedIndexes++;
			break;
		// Metadata updated
		case WalPutMeta:
			slaveNs->PutMeta(string(rec.putMeta.key), rec.putMeta.value, dummyCtx, lsn);
			stat.updatedMeta++;
			break;
		// Update query
		case WalUpdateQuery: {
			QueryResults result;
			Query q;
			q.FromSQL(rec.data);
			switch (q.type_) {
				case QueryDelete:
					slaveNs->Delete(q, result, dummyCtx, lsn);
					break;
				case QueryUpdate:
					slaveNs->Update(q, result, dummyCtx, lsn);
					break;
				case QueryTruncate:
					slaveNs->Truncate(dummyCtx, lsn);
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
			err = slave_->closeNamespace(nsName, dummyCtx, true, true);
			break;
		// Replication state
		case WalReplState:
			stat.processed--;
			stat.masterState.FromJSON(giftStr(rec.data));
			if (stat.masterState.clusterID != config_.clusterID) {
				terminate_ = true;
				return Error(errLogic, "Wrong cluster ID expect %d, got %d from master. Terminating replicator.", config_.clusterID,
							 stat.masterState.clusterID);
			}
			break;
		default:
			break;
	}
	return err;
}

Error Replicator::applyItemCJson(int64_t lsn, std::shared_ptr<Namespace> slaveNs, string_view cjson, int modifyMode, const TagsMatcher &tm,
								 SyncStat &stat) {
	const static RdxContext dummyCtx;
	// break;
	Item item = slaveNs->NewItem(dummyCtx);

	if (item.impl_->tagsMatcher().size() < tm.size()) {
		bool res = item.impl_->tagsMatcher().try_merge(tm);
		if (!res) {
			return Error(errNotValid, "Can't merge tagsmatcher of item with lsn %ul", lsn);
		}
	}

	item.setLSN(lsn);
	Error err = item.FromCJSON(cjson);
	if (err.ok()) {
		switch (modifyMode) {
			case ModeDelete:
				slaveNs->Delete(item, dummyCtx);
				stat.deleted++;
				break;
			case ModeInsert:
				slaveNs->Insert(item, dummyCtx);
				stat.updated++;
				break;
			case ModeUpsert:
				slaveNs->Upsert(item, dummyCtx);
				stat.updated++;
				break;
			case ModeUpdate:
				slaveNs->Update(item, dummyCtx);
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
	if (errors) ser << errors << " errors (" << lastError.what() << ") ";
	if (!ser.Len()) ser << "Up to date; ";
	if (processed) ser << "processed " << processed << " WAL records ";
	return ser;
}

Error Replicator::syncIndexesForced(const NamespaceDef &masterNsDef) {
	const static RdxContext dummyCtx;
	const string &nsName = masterNsDef.name;
	auto ns = slave_->getNamespace(nsName, dummyCtx);

	Error err = errOK;
	for (auto &idx : masterNsDef.indexes) {
		logPrintf(LogTrace, "[repl:%s] Updating index '%s'", nsName, idx.name_);
		try {
			ns->AddIndex(idx, dummyCtx);
		} catch (const Error &e) {
			logPrintf(LogError, "[repl:%s] Error add index '%s': %s", nsName, idx.name_, err.what());
			err = e;
		}
	}

	return err;
}

Error Replicator::syncMetaForced(string_view nsName) {
	const static RdxContext dummyCtx;
	vector<string> keys;
	auto err = master_->EnumMeta(nsName, keys);
	auto ns = slave_->getNamespace(nsName, dummyCtx);
	if (!err.ok()) return err;

	for (auto &key : keys) {
		string data;
		err = master_->GetMeta(nsName, key, data);
		if (!err.ok()) {
			logPrintf(LogError, "[repl:%s] Error get meta '%s': %s", nsName, key, err.what());
			continue;
		}
		try {
			ns->PutMeta(key, data, dummyCtx, 1);
		} catch (const Error &e) {
			logPrintf(LogError, "[repl:%s] Error set meta '%s': %s", nsName, key, e.what());
		}
	}
	return errOK;
}

// Callback from WAL updates pusher
void Replicator::OnWALUpdate(int64_t lsn, string_view nsName, const WALRecord &wrec) {
	if (!canApplyUpdate(lsn, nsName)) return;

	std::shared_ptr<Namespace> slaveNs;

	Error err;
	try {
		slaveNs = slave_->getNamespace(nsName, RdxContext());
	} catch (const Error &) {
	}

	SyncStat stat;

	try {
		err = applyWALRecord(lsn, nsName, slaveNs, wrec, stat);
	} catch (const Error &e) {
		err = e;
	}
	if (err.ok() && slaveNs && wrec.type != WalNamespaceDrop) {
		slaveNs->SetSlaveLSN(lsn, RdxContext());
	} else if (!err.ok()) {
		logPrintf(LogError, "[repl:%s] Error apply WAL update: %s", nsName, err.what());
	}
}

void Replicator::OnConnectionState(const Error &err) {
	if (err.ok()) {
		logPrintf(LogTrace, "[repl:] OnConnectionState connected");
		std::unique_lock<std::mutex> lck(syncMtx_);
		state_ = StateInit;
		resync_.send();
	} else {
		logPrintf(LogTrace, "[repl:] OnConnectionState closed, reason: %s", err.what());
	}
};

bool Replicator::canApplyUpdate(int64_t lsn, string_view nsName) {
	if (!isSyncEnabled(nsName)) return false;

	if (state_ == StateIdle) return true;
	std::unique_lock<std::mutex> lck(syncMtx_);
	if (state_ == StateIdle) return true;
	if (state_ == StateInit || terminate_) {
		logPrintf(LogTrace, "[repl:%s] Skipping update due to replicator %s is in progress lsn %ld", nsName,
				  terminate_ ? "shutdown" : "startup", lsn);
		return false;
	}

	// sync is in progress, and ns is not processed
	auto mIt = maxLsns_.find(nsName);

	// ns is already synced
	if (maxLsns_.find(nsName) == maxLsns_.end()) return true;

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
