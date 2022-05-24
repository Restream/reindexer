#include "namespace.h"
#include "core/storage/storagefactory.h"
#include "snapshot/snapshothandler.h"
#include "snapshot/snapshotrecord.h"
#include "tools/flagguard.h"
#include "tools/fsops.h"
#include "tools/logger.h"

namespace reindexer {

#define handleInvalidation(Fn) nsFuncWrapper<decltype(&Fn), &Fn>

void Namespace::CommitTransaction(Transaction& tx, LocalQueryResults& result, const NsContext& ctx) {
	auto ns = atomicLoadMainNs();
	bool enablePerfCounters = ns->enablePerfCounters_.load(std::memory_order_relaxed);
	if (enablePerfCounters) {
		txStatsCounter_.Count(tx);
	}
	PerfStatCalculatorMT txCommitCalc(commitStatsCounter_, enablePerfCounters);
	if (needNamespaceCopy(ns, tx)) {
		PerfStatCalculatorMT calc(ns->updatePerfCounter_, enablePerfCounters);
		contexted_unique_lock<Mutex, const RdxContext> clonerLck(clonerMtx_, &ctx.rdxContext);
		ns = ns_;
		if (needNamespaceCopy(ns, tx)) {
			PerfStatCalculatorMT nsCopyCalc(copyStatsCounter_, enablePerfCounters);
			calc.SetCounter(ns->updatePerfCounter_);
			calc.LockHit();
			logPrintf(LogTrace, "Namespace::CommitTransaction creating copy for (%s)", ns->name_);
			hasCopy_.store(true, std::memory_order_release);
			CounterGuardAIR32 cg(ns->cancelCommitCnt_);
			try {
				auto nsRlck = ns->rLock(ctx.rdxContext);
				auto storageLock = ns->locker_.StorageLock();
				cg.Reset();
				nsCopy_.reset(new NamespaceImpl(*ns));
				nsCopyCalc.HitManualy();
				NsContext nsCtx(ctx);
				nsCtx.CopiedNsRequest();
				nsCopy_->CommitTransaction(tx, result, nsCtx);
				nsCopy_->optimizeIndexes(nsCtx);
				nsCopy_->warmupFtIndexes();
				calc.SetCounter(nsCopy_->updatePerfCounter_);
				ns->markReadOnly();
				atomicStoreMainNs(nsCopy_.release());
				hasCopy_.store(false, std::memory_order_release);
				if (!ns->repl_.temporary && !nsCtx.inSnapshot) {
					// If commit happens in ns copy, than the copier have to handle replication
					auto err = ns_->clusterizator_->Replicate(
						cluster::UpdateRecord{cluster::UpdateRecord::Type::CommitTx, ns_->name_, ns_->wal_.LastLSN(), ns_->repl_.nsVersion,
											  ctx.rdxContext.emmiterServerId_},
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
				logPrintf(LogTrace, "Namespace::CommitTransaction copying tx for (%s) was terminated by exception:'%s'", ns->name_,
						  e.what());
				calc.enable_ = false;
				nsCopy_.reset();
				hasCopy_.store(false, std::memory_order_release);
				throw;
			} catch (...) {
				logPrintf(LogTrace, "Namespace::CommitTransaction copying tx for (%s) was terminated by unknown exception", ns->name_);
				calc.enable_ = false;
				nsCopy_.reset();
				hasCopy_.store(false, std::memory_order_release);
				throw;
			}
			logPrintf(LogTrace, "Namespace::CommitTransaction copying tx for (%s) has succeed", ns->name_);
			return;
		}
	}
	handleInvalidation(NamespaceImpl::CommitTransaction)(tx, result, ctx);
}

NamespacePerfStat Namespace::GetPerfStat(const RdxContext& ctx) {
	NamespacePerfStat stats = handleInvalidation(NamespaceImpl::GetPerfStat)(ctx);
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
		return handleInvalidation(NamespaceImpl::ApplySnapshotChunk)(ch, isInitialLeaderSync, ctx);
	} else {
		SnapshotTxHandler handler(*this);
		handler.ApplyChunk(ch, isInitialLeaderSync, ctx);
	}
}

bool Namespace::needNamespaceCopy(const NamespaceImpl::Ptr& ns, const Transaction& tx) const noexcept {
	auto stepsCount = tx.GetSteps().size();
	auto startCopyPolicyTxSize = static_cast<uint32_t>(startCopyPolicyTxSize_.load(std::memory_order_relaxed));
	auto copyPolicyMultiplier = static_cast<uint32_t>(copyPolicyMultiplier_.load(std::memory_order_relaxed));
	auto txSizeToAlwaysCopy = static_cast<uint32_t>(txSizeToAlwaysCopy_.load(std::memory_order_relaxed));
	return ((stepsCount >= startCopyPolicyTxSize) && (ns->GetItemsCapacity() <= copyPolicyMultiplier * stepsCount)) ||
		   (stepsCount >= txSizeToAlwaysCopy);
}

void Namespace::doRename(Namespace::Ptr dst, const std::string& newName, const std::string& storagePath,
						 std::function<void(std::function<void()>)> replicateCb, const RdxContext& ctx) {
	std::string dbpath;
	handleInvalidation(NamespaceImpl::flushStorage)(ctx);
	auto lck = handleInvalidation(NamespaceImpl::dataWLock)(ctx, true);
	auto srcNsPtr = atomicLoadMainNs();
	auto& srcNs = *srcNsPtr;
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
		dbpath = dstNs->dbpath_;
	} else if (newName == srcNs.name_) {
		return;
	}
	srcNs.checkClusterRole(ctx);

	if (dbpath.empty()) {
		dbpath = fs::JoinPath(storagePath, newName);
	} else {
		dstNs->deleteStorage();
	}

	bool hadStorage = (srcNs.storage_ != nullptr);
	auto storageType = StorageType::LevelDB;
	if (hadStorage) {
		storageType = srcNs.storage_->Type();
		srcNs.storage_.reset();
		fs::RmDirAll(dbpath);
		int renameRes = fs::Rename(srcNs.dbpath_, dbpath);
		if (renameRes < 0) {
			if (dst) {
				assertrx(dstLck.owns_lock());
				dstLck.unlock();
			}
			throw Error(errParams, "Unable to rename '%s' to '%s'", srcNs.dbpath_, dbpath);
		}
	}

	if (dst) {
		logPrintf(LogInfo, "Rename namespace '%s' to '%s'", srcNs.name_, dstNs->name_);
		srcNs.name_ = dstNs->name_;
		assertrx(dstLck.owns_lock());
		dstLck.unlock();
	} else {
		logPrintf(LogInfo, "Rename namespace '%s' to '%s'", srcNs.name_, newName);
		srcNs.name_ = newName;
	}
	srcNs.payloadType_.SetName(srcNs.name_);

	if (hadStorage) {
		logPrintf(LogTrace, "Storage was moved from %s to %s", srcNs.dbpath_, dbpath);
		srcNs.dbpath_ = std::move(dbpath);
		srcNs.storage_.reset(datastorage::StorageFactory::create(storageType));
		auto status = srcNs.storage_->Open(srcNs.dbpath_, srcNs.storageOpts_);
		if (!status.ok()) {
			throw status;
		}
		srcNs.wal_.SetStorage(srcNs.storage_);
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
