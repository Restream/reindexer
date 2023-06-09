#pragma once

#include <thread>
#include <type_traits>
#include "core/queryresults/queryresults.h"
#include "core/transaction/txstats.h"
#include "estl/shared_mutex.h"
#include "namespaceimpl.h"
#include "tools/flagguard.h"

namespace reindexer {

#define handleInvalidation(Fn) nsFuncWrapper<decltype(&Fn), &Fn>

class Namespace {
public:
	Namespace(const std::string &name, std::optional<int32_t> stateToken, cluster::INsDataReplicator *clusterizator)
		: ns_(std::make_shared<NamespaceImpl>(name, std::move(stateToken), clusterizator)) {}
	Namespace(NamespaceImpl::Ptr ns) : ns_(std::move(ns)) {}
	typedef shared_ptr<Namespace> Ptr;

	void CommitTransaction(LocalTransaction &tx, LocalQueryResults &result, const NsContext &ctx);
	std::string GetName(const RdxContext &ctx) const { return handleInvalidation(NamespaceImpl::GetName)(ctx); }
	bool IsSystem(const RdxContext &ctx) const { return handleInvalidation(NamespaceImpl::IsSystem)(ctx); }
	bool IsTemporary(const RdxContext &ctx) const { return handleInvalidation(NamespaceImpl::IsTemporary)(ctx); }
	void SetNsVersion(lsn_t version, const RdxContext &ctx) { handleInvalidation(NamespaceImpl::SetNsVersion)(version, ctx); }
	void EnableStorage(const std::string &path, StorageOpts opts, StorageType storageType, const RdxContext &ctx) {
		handleInvalidation(NamespaceImpl::EnableStorage)(path, opts, storageType, ctx);
	}
	void LoadFromStorage(unsigned threadsCount, const RdxContext &ctx) {
		handleInvalidation(NamespaceImpl::LoadFromStorage)(threadsCount, ctx);
	}
	void DeleteStorage(const RdxContext &ctx) { handleInvalidation(NamespaceImpl::DeleteStorage)(ctx); }
	uint32_t GetItemsCount() { return handleInvalidation(NamespaceImpl::GetItemsCount)(); }
	void AddIndex(const IndexDef &indexDef, const RdxContext &ctx) { handleInvalidation(NamespaceImpl::AddIndex)(indexDef, ctx); }
	void UpdateIndex(const IndexDef &indexDef, const RdxContext &ctx) { handleInvalidation(NamespaceImpl::UpdateIndex)(indexDef, ctx); }
	void DropIndex(const IndexDef &indexDef, const RdxContext &ctx) { handleInvalidation(NamespaceImpl::DropIndex)(indexDef, ctx); }
	void SetSchema(std::string_view schema, const RdxContext &ctx) { handleInvalidation(NamespaceImpl::SetSchema)(schema, ctx); }
	std::string GetSchema(int format, const RdxContext &ctx) { return handleInvalidation(NamespaceImpl::GetSchema)(format, ctx); }
	std::shared_ptr<const Schema> GetSchemaPtr(const RdxContext &ctx) { return handleInvalidation(NamespaceImpl::GetSchemaPtr)(ctx); }
	void Insert(Item &item, const RdxContext &ctx) { handleInvalidation(NamespaceImpl::Insert)(item, ctx); }
	void Insert(Item &item, LocalQueryResults &qr, const RdxContext &ctx) {
		nsFuncWrapper<&NamespaceImpl::modifyItem, ModeInsert>(item, qr, ctx);
	}
	void Update(Item &item, const RdxContext &ctx) {
		nsFuncWrapper<void (NamespaceImpl::*)(Item &, const RdxContext &), &NamespaceImpl::Update>(item, ctx);
	}
	void Update(Item &item, LocalQueryResults &qr, const RdxContext &ctx) {
		nsFuncWrapper<&NamespaceImpl::modifyItem, ModeUpdate>(item, qr, ctx);
	}
	void Update(const Query &query, LocalQueryResults &result, const RdxContext &ctx) {
		nsFuncWrapper<&NamespaceImpl::doUpdate>(query, result, ctx);
	}
	void Upsert(Item &item, const RdxContext &ctx) { handleInvalidation(NamespaceImpl::Upsert)(item, ctx); }
	void Upsert(Item &item, LocalQueryResults &qr, const RdxContext &ctx) {
		nsFuncWrapper<&NamespaceImpl::modifyItem, ModeUpsert>(item, qr, ctx);
	}
	void Delete(Item &item, const RdxContext &ctx) {
		nsFuncWrapper<void (NamespaceImpl::*)(Item &, const RdxContext &), &NamespaceImpl::Delete>(item, ctx);
	}
	void Delete(Item &item, LocalQueryResults &qr, const RdxContext &ctx) {
		nsFuncWrapper<&NamespaceImpl::modifyItem, ModeDelete>(item, qr, ctx);
	}
	void Delete(const Query &query, LocalQueryResults &result, const RdxContext &ctx) {
		nsFuncWrapper<&NamespaceImpl::doDelete>(query, result, ctx);
	}
	void Truncate(const RdxContext &ctx) { handleInvalidation(NamespaceImpl::Truncate)(ctx); }
	void Select(LocalQueryResults &result, SelectCtx &params, const RdxContext &ctx) {
		handleInvalidation(NamespaceImpl::Select)(result, params, ctx);
	}
	NamespaceDef GetDefinition(const RdxContext &ctx) { return handleInvalidation(NamespaceImpl::GetDefinition)(ctx); }
	NamespaceMemStat GetMemStat(const RdxContext &ctx) { return handleInvalidation(NamespaceImpl::GetMemStat)(ctx); }
	NamespacePerfStat GetPerfStat(const RdxContext &ctx);
	void ResetPerfStat(const RdxContext &ctx) {
		txStatsCounter_.Reset();
		commitStatsCounter_.Reset();
		copyStatsCounter_.Reset();
		handleInvalidation(NamespaceImpl::ResetPerfStat)(ctx);
	}
	std::vector<std::string> EnumMeta(const RdxContext &ctx) { return handleInvalidation(NamespaceImpl::EnumMeta)(ctx); }
	void BackgroundRoutine(RdxActivityContext *ctx) {
		if (hasCopy_.load(std::memory_order_acquire)) {
			return;
		}
		handleInvalidation(NamespaceImpl::BackgroundRoutine)(ctx);
	}
	void StorageFlushingRoutine() {
		if (hasCopy_.load(std::memory_order_acquire)) {
			return;
		}
		handleInvalidation(NamespaceImpl::StorageFlushingRoutine)();
	}
	void CloseStorage(const RdxContext &ctx) { handleInvalidation(NamespaceImpl::CloseStorage)(ctx); }
	LocalTransaction NewTransaction(const RdxContext &ctx) { return handleInvalidation(NamespaceImpl::NewTransaction)(ctx); }

	Item NewItem(const RdxContext &ctx) { return handleInvalidation(NamespaceImpl::NewItem)(ctx); }
	void ToPool(ItemImpl *item) { handleInvalidation(NamespaceImpl::ToPool)(item); }
	std::string GetMeta(const std::string &key, const RdxContext &ctx) { return handleInvalidation(NamespaceImpl::GetMeta)(key, ctx); }
	void PutMeta(const std::string &key, std::string_view data, const RdxContext &ctx) {
		handleInvalidation(NamespaceImpl::PutMeta)(key, data, ctx);
	}
	int getIndexByName(std::string_view index) const {
		return nsFuncWrapper<int (NamespaceImpl::*)(std::string_view) const, &NamespaceImpl::getIndexByName>(index);
	}
	bool getIndexByName(std::string_view name, int &index) const {
		return nsFuncWrapper<bool (NamespaceImpl::*)(std::string_view, int &) const, &NamespaceImpl::getIndexByName>(name, index);
	}
	void FillResult(LocalQueryResults &result, const IdSet &ids) const { handleInvalidation(NamespaceImpl::FillResult)(result, ids); }
	void EnablePerfCounters(bool enable = true) { handleInvalidation(NamespaceImpl::EnablePerfCounters)(enable); }
	ReplicationState GetReplState(const RdxContext &ctx) const { return handleInvalidation(NamespaceImpl::GetReplState)(ctx); }
	ReplicationStateV2 GetReplStateV2(const RdxContext &ctx) const { return handleInvalidation(NamespaceImpl::GetReplStateV2)(ctx); }
	void Rename(const Namespace::Ptr &dst, const std::string &storagePath, const std::function<void(std::function<void()>)> &replicateCb,
				const RdxContext &ctx) {
		if (this == dst.get() || dst == nullptr) {
			return;
		}
		doRename(dst, std::string(), storagePath, replicateCb, ctx);
	}
	void Rename(const std::string &newName, const std::string &storagePath, std::function<void(std::function<void()>)> replicateCb,
				const RdxContext &ctx) {
		if (newName.empty()) {
			return;
		}
		doRename(nullptr, newName, storagePath, std::move(replicateCb), ctx);
	}
	void OnConfigUpdated(DBConfigProvider &configProvider, const RdxContext &ctx) {
		NamespaceConfigData configData;
		configProvider.GetNamespaceConfig(GetName(ctx), configData);
		startCopyPolicyTxSize_.store(configData.startCopyPolicyTxSize, std::memory_order_relaxed);
		copyPolicyMultiplier_.store(configData.copyPolicyMultiplier, std::memory_order_relaxed);
		txSizeToAlwaysCopy_.store(configData.txSizeToAlwaysCopy, std::memory_order_relaxed);
		longTxLoggingParams_.store(configProvider.GetTxLoggingParams(), std::memory_order_relaxed);
		handleInvalidation(NamespaceImpl::OnConfigUpdated)(configProvider, ctx);
	}
	StorageOpts GetStorageOpts(const RdxContext &ctx) { return handleInvalidation(NamespaceImpl::GetStorageOpts)(ctx); }
	void Refill(std::vector<Item> &items, const RdxContext &ctx) { handleInvalidation(NamespaceImpl::Refill)(items, ctx); }
	Error SetClusterizationStatus(ClusterizationStatus &&status, const RdxContext &ctx) {
		return handleInvalidation(NamespaceImpl::SetClusterizationStatus)(std::move(status), ctx);
	}
	void GetSnapshot(Snapshot &snapshot, const SnapshotOpts &opts, const RdxContext &ctx) {
		return handleInvalidation(NamespaceImpl::GetSnapshot)(snapshot, opts, ctx);
	}
	void ApplySnapshotChunk(const SnapshotChunk &ch, bool isInitialLeaderSync, const RdxContext &ctx);
	void SetTagsMatcher(TagsMatcher &&tm, const RdxContext &ctx) {
		return handleInvalidation(NamespaceImpl::SetTagsMatcher)(std::move(tm), ctx);
	}

	std::set<std::string> GetFTIndexes(const RdxContext &ctx) const {
		return nsFuncWrapper<std::set<std::string> (NamespaceImpl::*)(const RdxContext &) const, &NamespaceImpl::GetFTIndexes>(ctx);
	}

	void DumpIndex(std::ostream &os, std::string_view index, const RdxContext &ctx) {
		return handleInvalidation(NamespaceImpl::DumpIndex)(os, index, ctx);
	}
	void SetDestroyFlag() { return handleInvalidation(NamespaceImpl::SetDestroyFlag)(); }

protected:
	friend class ReindexerImpl;
	friend class LocalQueryResults;
	friend class ShardingProxy;
	void updateSelectTime() const { handleInvalidation(NamespaceImpl::updateSelectTime)(); }
	NamespaceImpl::Ptr getMainNs() const { return atomicLoadMainNs(); }
	NamespaceImpl::Ptr awaitMainNs(const RdxContext &ctx) const {
		if (hasCopy_.load(std::memory_order_acquire)) {
			contexted_unique_lock<Mutex, const RdxContext> lck(clonerMtx_, &ctx);
			assertrx(!hasCopy_.load(std::memory_order_acquire));
			return ns_;
		}
		return atomicLoadMainNs();
	}

	PayloadType getPayloadType(const RdxContext &ctx) const {
		return nsFuncWrapper<PayloadType (NamespaceImpl::*)(const RdxContext &ctx) const, &NamespaceImpl::getPayloadType>(ctx);
	}

private:
	template <typename Fn, Fn fn, typename... Args>
	typename std::invoke_result<Fn, NamespaceImpl, Args...>::type nsFuncWrapper(Args &&...args) const {
		while (true) {
			try {
				auto ns = atomicLoadMainNs();
				return (*ns.*fn)(std::forward<Args>(args)...);
			} catch (const Error &e) {
				if (e.code() != errNamespaceInvalidated) {
					throw;
				} else {
					std::this_thread::yield();
				}
			}
		}
	}

	template <void (NamespaceImpl::*fn)(Item &, int, NamespaceImpl::UpdatesContainer &, const NsContext &), int mode>
	void nsFuncWrapper(Item &item, LocalQueryResults &qr, const RdxContext &ctx) const {
		nsFuncWrapper<Item, void (NamespaceImpl::*)(Item &, int, NamespaceImpl::UpdatesContainer &, const NsContext &), fn, mode>(item, qr,
																																  ctx);
	}
	template <void (NamespaceImpl::*fn)(const Query &, LocalQueryResults &, NamespaceImpl::UpdatesContainer &, const NsContext &)>
	void nsFuncWrapper(const Query &query, LocalQueryResults &qr, const RdxContext &ctx) const {
		nsFuncWrapper<const Query,
					  void (NamespaceImpl::*)(const Query &, LocalQueryResults &, NamespaceImpl::UpdatesContainer &, const NsContext &),
					  fn>(query, qr, ctx);
	}
	template <typename T, typename FN, FN fn, int mode = 0>
	void nsFuncWrapper(T &v, LocalQueryResults &qr, const RdxContext &ctx) const {
		NsContext nsCtx(ctx);
		while (true) {
			std::shared_ptr<NamespaceImpl> ns;
			bool added = false;
			try {
				ns = atomicLoadMainNs();

				PerfStatCalculatorMT calc(ns->updatePerfCounter_, ns->enablePerfCounters_);
				NamespaceImpl::UpdatesContainer pendedRepl;

				CounterGuardAIR32 cg(ns->cancelCommitCnt_);
				auto wlck = ns->dataWLock(nsCtx.rdxContext);
				cg.Reset();

				qr.AddNamespace(ns, true, nsCtx.rdxContext);
				added = true;
				if constexpr (std::is_same_v<T, Item>) {
					(*ns.*fn)(v, mode, pendedRepl, nsCtx);
					qr.AddItem(v, true, false);
				} else {
					(*ns.*fn)(v, qr, pendedRepl, nsCtx);
				}
				ns->replicate(std::move(pendedRepl), std::move(wlck), true, nsCtx);
				return;
			} catch (const Error &e) {
				if (e.code() != errNamespaceInvalidated) {
					throw;
				} else {
					if (added) qr.RemoveNamespace(ns.get());
					std::this_thread::yield();
				}
			}
		}
	}

	bool needNamespaceCopy(const NamespaceImpl::Ptr &ns, const LocalTransaction &tx) const noexcept;
	void doRename(const Namespace::Ptr &dst, const std::string &newName, const std::string &storagePath,
				  const std::function<void(std::function<void()>)> &replicateCb, const RdxContext &ctx);
	NamespaceImpl::Ptr atomicLoadMainNs() const {
		std::lock_guard<spinlock> lck(nsPtrSpinlock_);
		return ns_;
	}
	void atomicStoreMainNs(NamespaceImpl *ns) {
		std::lock_guard<spinlock> lck(nsPtrSpinlock_);
		ns_.reset(ns);
	}

	std::shared_ptr<NamespaceImpl> ns_;
	std::unique_ptr<NamespaceImpl> nsCopy_;
	std::atomic<bool> hasCopy_ = {false};
	using Mutex = MarkedMutex<std::timed_mutex, MutexMark::Namespace>;
	mutable Mutex clonerMtx_;
	mutable spinlock nsPtrSpinlock_;
	std::atomic<int> startCopyPolicyTxSize_;
	std::atomic<int> copyPolicyMultiplier_;
	std::atomic<int> txSizeToAlwaysCopy_;
	TxStatCounter txStatsCounter_{};
	PerfStatCounterMT commitStatsCounter_;
	PerfStatCounterMT copyStatsCounter_;
	std::atomic<LongTxLoggingParams> longTxLoggingParams_;
};

#undef handleInvalidation

}  // namespace reindexer
