#include "namespace.h"
#include "core/querystat.h"
#include "snapshot/snapshothandler.h"
#include "snapshot/snapshotrecord.h"
#include "tools/flagguard.h"
#include "tools/fsops.h"
#include "tools/logger.h"

namespace reindexer {

void Namespace::CommitTransaction(LocalTransaction& tx, LocalQueryResults& result, const NsContext& ctx) {
	auto nsl = atomicLoadMainNs();
	bool enablePerfCounters = nsl->enablePerfCounters_.load(std::memory_order_relaxed);
	if (enablePerfCounters) {
		txStatsCounter_.Count(tx);
	}
	bool wasCopied = false;	 // NOLINT(*deadcode.DeadStores)
	auto params = longTxLoggingParams_.load(std::memory_order_relaxed);
	QueryStatCalculator statCalculator(long_actions::MakeLogger(tx, params, wasCopied), params.thresholdUs >= 0);

	PerfStatCalculatorMT txCommitCalc(commitStatsCounter_, enablePerfCounters);
	if (needNamespaceCopy(nsl, tx)) {
		PerfStatCalculatorMT calc(nsl->updatePerfCounter_, enablePerfCounters);

		auto clonerLck = statCalculator.CreateLock<contexted_unique_lock>(clonerMtx_, ctx.rdxContext);

		nsl = ns_;
		if (needNamespaceCopy(nsl, tx)) {
			PerfStatCalculatorMT nsCopyCalc(copyStatsCounter_, enablePerfCounters);
			calc.SetCounter(nsl->updatePerfCounter_);
			calc.LockHit();
			logPrintf(LogTrace, "Namespace::CommitTransaction creating copy for (%s)", nsl->name_);
			hasCopy_.store(true, std::memory_order_release);
			CounterGuardAIR32 cg(nsl->cancelCommitCnt_);
			try {
				auto nsRlck = statCalculator.CreateLock(*nsl, &NamespaceImpl::rLock, ctx.rdxContext);
				tx.ValidatePK(nsl->pkFields());

				auto storageLock = statCalculator.CreateLock(nsl->storage_, &AsyncStorage::FullLock);

				cg.Reset();
				nsCopy_.reset(new NamespaceImpl(*nsl, storageLock));
				nsCopyCalc.HitManualy();
				NsContext nsCtx(ctx);
				nsCtx.CopiedNsRequest();
				nsCopy_->CommitTransaction(tx, result, nsCtx, statCalculator);
				nsCopy_->optimizeIndexes(nsCtx);
				nsCopy_->warmupFtIndexes();
				try {
					nsCopy_->storage_.InheritUpdatesFrom(nsl->storage_,
														 storageLock);	// Updates can not be flushed until tx is commited into ns copy
				} catch (Error& e) {
					// This exception should never be seen - there are no good ways to recover from it
					assertf(false, "Error during storage moving in namespace (%s) copying: %s", nsl->name_, e.what());
				}

				calc.SetCounter(nsCopy_->updatePerfCounter_);
				nsl->markReadOnly();
				atomicStoreMainNs(nsCopy_.release());
				wasCopied = true;  // NOLINT(*deadcode.DeadStores)
				hasCopy_.store(false, std::memory_order_release);
				if (!nsl->repl_.temporary && !nsCtx.inSnapshot) {
					// If commit happens in ns copy, than the copier have to handle replication
					auto err = ns_->clusterizator_.Replicate(
						cluster::UpdateRecord{cluster::UpdateRecord::Type::CommitTx, ns_->name_, ns_->wal_.LastLSN(), ns_->repl_.nsVersion,
											  ctx.rdxContext.EmmiterServerId()},
						[&clonerLck, &storageLock, &nsRlck]() {
							storageLock.unlock();
							nsRlck.unlock();
							clonerLck.unlock();
						},
						ctx.rdxContext);
					if (!err.ok()) {
						throw Error(errUpdateReplication, err.what());
					}
				}
			} catch (Error& e) {
				logPrintf(LogTrace, "Namespace::CommitTransaction copying tx for (%s) was terminated by exception:'%s'", nsl->name_,
						  e.what());
				calc.enable_ = false;
				nsCopy_.reset();
				hasCopy_.store(false, std::memory_order_release);
				throw;
			} catch (...) {
				logPrintf(LogTrace, "Namespace::CommitTransaction copying tx for (%s) was terminated by unknown exception", nsl->name_);
				calc.enable_ = false;
				nsCopy_.reset();
				hasCopy_.store(false, std::memory_order_release);
				throw;
			}
			logPrintf(LogTrace, "Namespace::CommitTransaction copying tx for (%s) has succeed", nsl->name_);
			if (clonerLck.owns_lock()) {
				nsl = ns_;
				clonerLck.unlock();
				statCalculator.LogFlushDuration(nsl->storage_, &AsyncStorage::TryForceFlush);
			} else {
				statCalculator.LogFlushDuration(getMainNs()->storage_, &AsyncStorage::TryForceFlush);
			}
			return;
		}
	}
	nsFuncWrapper<&NamespaceImpl::CommitTransaction>(tx, result, ctx, statCalculator);
}

NamespacePerfStat Namespace::GetPerfStat(const RdxContext& ctx) {
	NamespacePerfStat stats = nsFuncWrapper<&NamespaceImpl::GetPerfStat>(ctx);
	stats.transactions = txStatsCounter_.Get();
	auto copyStats = copyStatsCounter_.Get<PerfStat>();
	stats.transactions.totalCopyCount = copyStats.totalHitCount;
	stats.transactions.minCopyTimeUs = copyStats.minTimeUs;
	stats.transactions.maxCopyTimeUs = copyStats.maxTimeUs;
	stats.transactions.avgCopyTimeUs = copyStats.totalTimeUs / (copyStats.totalHitCount ? copyStats.totalHitCount : 1);
	auto commitStats = commitStatsCounter_.Get<PerfStat>();
	stats.transactions.totalCount = commitStats.totalHitCount;
	stats.transactions.minCommitTimeUs = commitStats.minTimeUs;
	stats.transactions.maxCommitTimeUs = commitStats.maxTimeUs;
	stats.transactions.avgCommitTimeUs = commitStats.totalTimeUs / (commitStats.totalHitCount ? commitStats.totalHitCount : 1);
	return stats;
}

void Namespace::ApplySnapshotChunk(const SnapshotChunk& ch, bool isInitialLeaderSync, const RdxContext& ctx) {
	if (!ch.IsTx() || ch.IsShallow() || !ch.IsWAL()) {
		return nsFuncWrapper<&NamespaceImpl::ApplySnapshotChunk>(ch, isInitialLeaderSync, ctx);
	} else {
		SnapshotTxHandler handler(*this);
		handler.ApplyChunk(ch, isInitialLeaderSync, ctx);
	}
}

bool Namespace::needNamespaceCopy(const NamespaceImpl::Ptr& ns, const LocalTransaction& tx) const noexcept {
	auto stepsCount = tx.GetSteps().size();
	auto startCopyPolicyTxSize = static_cast<uint32_t>(startCopyPolicyTxSize_.load(std::memory_order_relaxed));
	auto copyPolicyMultiplier = static_cast<uint32_t>(copyPolicyMultiplier_.load(std::memory_order_relaxed));
	auto txSizeToAlwaysCopy = static_cast<uint32_t>(txSizeToAlwaysCopy_.load(std::memory_order_relaxed));
	return ((stepsCount >= startCopyPolicyTxSize) && (ns->GetItemsCapacity() <= copyPolicyMultiplier * stepsCount)) ||
		   (stepsCount >= txSizeToAlwaysCopy);
}

void Namespace::doRename(const Namespace::Ptr& dst, const std::string& newName, const std::string& storagePath,
						 const std::function<void(std::function<void()>)>& replicateCb, const RdxContext& ctx) {
	logPrintf(LogTrace, "[rename] Trying to rename namespace '%s'...", GetName(ctx));
	std::string dbpath;
	const auto flushOpts = StorageFlushOpts().WithImmediateReopen();
	auto lck = nsFuncWrapper<&NamespaceImpl::dataWLock>(ctx, true);
	auto srcNsPtr = atomicLoadMainNs();
	auto& srcNs = *srcNsPtr;
	srcNs.storage_.Flush(flushOpts);  // Repeat flush, to raise any disk errors before attempt to close storage
	auto storageStatus = srcNs.storage_.GetStatusCached();
	if (!storageStatus.err.ok()) {
		throw Error(storageStatus.err.code(), "Unable to flush storage before rename: %s", storageStatus.err.what());
	}
	NamespaceImpl::Locker::WLockT dstLck;
	NamespaceImpl::Ptr dstNs;
	if (dst) {
		while (true) {
			try {
				dstNs = dst->awaitMainNs(ctx);
				dstLck = dstNs->dataWLock(ctx, true);
				break;
			} catch (const Error& e) {
				if (e.code() != errNamespaceInvalidated) {
					throw;
				} else {
					std::this_thread::yield();
				}
			}
		}
		dstNs->checkClusterRole(ctx);
		dbpath = dstNs->storage_.GetPath();
	} else if (newName == srcNs.name_) {
		return;
	}
	srcNs.checkClusterRole(ctx);

	if (dbpath.empty()) {
		dbpath = fs::JoinPath(storagePath, newName);
	} else {
		dstNs->storage_.Destroy();
	}

	const bool hadStorage = (srcNs.storage_.IsValid());
	auto storageType = StorageType::LevelDB;
	const auto srcDbpath = srcNs.storage_.GetPath();
	if (hadStorage) {
		storageType = srcNs.storage_.GetType();
		srcNs.storage_.Close();
		fs::RmDirAll(dbpath);
		int renameRes = fs::Rename(srcDbpath, dbpath);
		if (renameRes < 0) {
			if (dst) {
				assertrx(dstLck.owns_lock());
				dstLck.unlock();
			}
			srcNs.storage_.Open(storageType, srcNs.name_, srcDbpath, srcNs.storageOpts_);
			throw Error(errParams, "Unable to rename '%s' to '%s'", srcDbpath, dbpath);
		}
	}

	if (dst) {
		logPrintf(LogInfo, "[rename] Rename namespace '%s' to '%s'", srcNs.name_, dstNs->name_);
		srcNs.name_ = dstNs->name_;
		assertrx(dstLck.owns_lock());
		dstLck.unlock();
	} else {
		logPrintf(LogInfo, "[rename] Rename namespace '%s' to '%s'", srcNs.name_, newName);
		srcNs.name_ = newName;
	}
	srcNs.payloadType_.SetName(srcNs.name_);

	if (hadStorage) {
		logPrintf(LogTrace, "[rename] Storage was moved from %s to %s", srcDbpath, dbpath);
		auto status = srcNs.storage_.Open(storageType, srcNs.name_, dbpath, srcNs.storageOpts_);
		if (!status.ok()) {
			srcNs.storage_.Close();
			throw status;
		}
	}
	if (srcNs.repl_.temporary) {
		srcNs.repl_.temporary = false;
		srcNs.saveReplStateToStorage();
	}

	if (replicateCb) {
		replicateCb([&lck] {
			assertrx(lck.isClusterLck());
			lck.unlock();
		});
	}
}

}  // namespace reindexer
