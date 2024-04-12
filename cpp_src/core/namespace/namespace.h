#pragma once

#include <thread>
#include <type_traits>
#include "bgnamespacedeleter.h"
#include "core/querystat.h"
#include "core/txstats.h"

namespace reindexer {

class Namespace {
	template <auto fn, typename... Args>
	auto nsFuncWrapper(Args &&...args) const {
		while (true) {
			try {
				auto ns = atomicLoadMainNs();
				if (!ns) {
					throw Error(errLogic, "Ns is nullptr");
				}
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

	template <void (NamespaceImpl::*fn)(Item &, ItemModifyMode, const NsContext &), ItemModifyMode mode>
	void nsFuncWrapper(Item &item, QueryResults &qr, NsContext ctx) const {
		nsFuncWrapper<Item, fn, mode>(item, qr, ctx);
	}

	template <void (NamespaceImpl::*fn)(const Query &, QueryResults &, const NsContext &), QueryType queryType>
	void nsFuncWrapper(const Query &query, QueryResults &qr, NsContext ctx) const {
		nsFuncWrapper<const Query, fn, queryType>(query, qr, ctx);
	}

	template <typename T, auto fn, auto enumVal>
	void nsFuncWrapper(T &v, QueryResults &qr, NsContext ctx) const {
		while (true) {
			NamespaceImpl::Ptr ns;
			bool added = false;
			try {
				ns = atomicLoadMainNs();
				PerfStatCalculatorMT calc(ns->updatePerfCounter_, ns->enablePerfCounters_);
				CounterGuardAIR32 cg(ns->cancelCommitCnt_);
				if constexpr (std::is_same_v<T, Item>) {
					auto locker{ns->wLock(ctx.rdxContext)};
					calc.LockHit();
					cg.Reset();
					ns->checkApplySlaveUpdate(ctx.rdxContext.fromReplication_);
					qr.AddNamespace(ns, true);
					added = true;
					(*ns.*fn)(v, enumVal, ctx);
					qr.AddItem(v, true, false);
					ns->tryForceFlush(std::move(locker));
				} else if constexpr (std::is_same_v<T, const Query>) {
					auto params = longUpdDelLoggingParams_.load(std::memory_order_relaxed);
					const bool isEnabled = params.thresholdUs >= 0 && !isSystemNamespaceNameFast(v.NsName());
					auto statCalculator = QueryStatCalculator(long_actions::MakeLogger<enumVal>(v, std::move(params)), isEnabled);
					auto locker = statCalculator.CreateLock(*ns, &NamespaceImpl::wLock, ctx.rdxContext);
					calc.LockHit();
					cg.Reset();
					ns->checkApplySlaveUpdate(ctx.rdxContext.fromReplication_);
					qr.AddNamespace(ns, true);
					added = true;
					(*ns.*fn)(v, qr, ctx);
					statCalculator.LogFlushDuration(*ns, &NamespaceImpl::tryForceFlush, std::move(locker));
				} else {
					static_assert(std::is_same_v<T, Item> || std::is_same_v<T, const Query>);
				}
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

public:
	using Ptr = shared_ptr<Namespace>;

	Namespace(const std::string &name, UpdatesObservers &observers, BackgroundNamespaceDeleter &bgDeleter)
		: ns_(make_intrusive<NamespaceImpl>(name, observers)), bgDeleter_(bgDeleter) {}

	void CommitTransaction(Transaction &tx, QueryResults &result, const RdxContext &ctx);
	std::string GetName(const RdxContext &ctx) const { return nsFuncWrapper<&NamespaceImpl::GetName>(ctx); }
	bool IsSystem(const RdxContext &ctx) const { return nsFuncWrapper<&NamespaceImpl::IsSystem>(ctx); }
	bool IsTemporary(const RdxContext &ctx) const { return nsFuncWrapper<&NamespaceImpl::IsTemporary>(ctx); }
	void EnableStorage(const std::string &path, StorageOpts opts, StorageType storageType, const RdxContext &ctx) {
		nsFuncWrapper<&NamespaceImpl::EnableStorage>(path, opts, storageType, ctx);
	}
	void LoadFromStorage(unsigned threadsCount, const RdxContext &ctx) {
		nsFuncWrapper<&NamespaceImpl::LoadFromStorage>(threadsCount, ctx);
	}
	void DeleteStorage(const RdxContext &ctx) { nsFuncWrapper<&NamespaceImpl::DeleteStorage>(ctx); }
	uint32_t GetItemsCount() { return nsFuncWrapper<&NamespaceImpl::GetItemsCount>(); }
	void AddIndex(const IndexDef &indexDef, const RdxContext &ctx) { nsFuncWrapper<&NamespaceImpl::AddIndex>(indexDef, ctx); }
	void UpdateIndex(const IndexDef &indexDef, const RdxContext &ctx) { nsFuncWrapper<&NamespaceImpl::UpdateIndex>(indexDef, ctx); }
	void DropIndex(const IndexDef &indexDef, const RdxContext &ctx) { nsFuncWrapper<&NamespaceImpl::DropIndex>(indexDef, ctx); }
	void SetSchema(std::string_view schema, const RdxContext &ctx) { nsFuncWrapper<&NamespaceImpl::SetSchema>(schema, ctx); }
	std::string GetSchema(int format, const RdxContext &ctx) { return nsFuncWrapper<&NamespaceImpl::GetSchema>(format, ctx); }
	std::shared_ptr<const Schema> GetSchemaPtr(const RdxContext &ctx) { return nsFuncWrapper<&NamespaceImpl::GetSchemaPtr>(ctx); }
	void Insert(Item &item, const RdxContext &ctx) { nsFuncWrapper<&NamespaceImpl::Insert>(item, ctx); }
	void Insert(Item &item, QueryResults &qr, const RdxContext &ctx) {
		nsFuncWrapper<&NamespaceImpl::modifyItem, ItemModifyMode::ModeInsert>(item, qr, ctx);
	}
	void Update(Item &item, const RdxContext &ctx) { nsFuncWrapper<&NamespaceImpl::Update>(item, ctx); }
	void Update(Item &item, QueryResults &qr, const RdxContext &ctx) {
		nsFuncWrapper<&NamespaceImpl::modifyItem, ItemModifyMode::ModeUpdate>(item, qr, ctx);
	}
	void Update(const Query &query, QueryResults &result, const RdxContext &ctx) {
		nsFuncWrapper<&NamespaceImpl::doUpdate, QueryType::QueryUpdate>(query, result, ctx);
	}
	void Upsert(Item &item, const RdxContext &ctx) { nsFuncWrapper<&NamespaceImpl::Upsert>(item, ctx); }
	void Upsert(Item &item, QueryResults &qr, const RdxContext &ctx) {
		nsFuncWrapper<&NamespaceImpl::modifyItem, ItemModifyMode::ModeUpsert>(item, qr, ctx);
	}
	void Delete(Item &item, const RdxContext &ctx) { nsFuncWrapper<&NamespaceImpl::Delete>(item, ctx); }
	void Delete(Item &item, QueryResults &qr, const RdxContext &ctx) {
		nsFuncWrapper<&NamespaceImpl::modifyItem, ItemModifyMode::ModeDelete>(item, qr, ctx);
	}
	void Delete(const Query &query, QueryResults &result, const RdxContext &ctx) {
		nsFuncWrapper<&NamespaceImpl::doDelete, QueryType::QueryDelete>(query, result, ctx);
	}
	void Truncate(const RdxContext &ctx) { nsFuncWrapper<&NamespaceImpl::Truncate>(ctx); }
	template <typename JoinPreResultCtx>
	void Select(QueryResults &result, SelectCtxWithJoinPreSelect<JoinPreResultCtx> &params, const RdxContext &ctx) {
		nsFuncWrapper<&NamespaceImpl::Select>(result, params, ctx);
	}
	NamespaceDef GetDefinition(const RdxContext &ctx) { return nsFuncWrapper<&NamespaceImpl::GetDefinition>(ctx); }
	NamespaceMemStat GetMemStat(const RdxContext &ctx) { return nsFuncWrapper<&NamespaceImpl::GetMemStat>(ctx); }
	NamespacePerfStat GetPerfStat(const RdxContext &ctx);
	void ResetPerfStat(const RdxContext &ctx) {
		txStatsCounter_.Reset();
		commitStatsCounter_.Reset();
		copyStatsCounter_.Reset();
		nsFuncWrapper<&NamespaceImpl::ResetPerfStat>(ctx);
	}
	std::vector<std::string> EnumMeta(const RdxContext &ctx) { return nsFuncWrapper<&NamespaceImpl::EnumMeta>(ctx); }
	void BackgroundRoutine(RdxActivityContext *ctx) {
		if (hasCopy_.load(std::memory_order_acquire)) {
			return;
		}
		nsFuncWrapper<&NamespaceImpl::BackgroundRoutine>(ctx);
	}
	void StorageFlushingRoutine() {
		if (hasCopy_.load(std::memory_order_acquire)) {
			return;
		}
		nsFuncWrapper<&NamespaceImpl::StorageFlushingRoutine>();
	}
	void CloseStorage(const RdxContext &ctx) { nsFuncWrapper<&NamespaceImpl::CloseStorage>(ctx); }
	Transaction NewTransaction(const RdxContext &ctx) { return nsFuncWrapper<&NamespaceImpl::NewTransaction>(ctx); }

	Item NewItem(const RdxContext &ctx) { return nsFuncWrapper<&NamespaceImpl::NewItem>(ctx); }
	void ToPool(ItemImpl *item) { nsFuncWrapper<&NamespaceImpl::ToPool>(item); }
	std::string GetMeta(const std::string &key, const RdxContext &ctx) { return nsFuncWrapper<&NamespaceImpl::GetMeta>(key, ctx); }
	void PutMeta(const std::string &key, std::string_view data, const RdxContext &ctx) {
		nsFuncWrapper<&NamespaceImpl::PutMeta>(key, data, ctx);
	}
	void DeleteMeta(const std::string &key, const RdxContext &ctx) { nsFuncWrapper<&NamespaceImpl::DeleteMeta>(key, ctx); }
	int64_t GetSerial(const std::string &field) { return nsFuncWrapper<&NamespaceImpl::GetSerial>(field); }
	int getIndexByName(std::string_view index) const { return nsFuncWrapper<&NamespaceImpl::getIndexByName>(index); }
	bool getIndexByName(std::string_view name, int &index) const { return nsFuncWrapper<&NamespaceImpl::tryGetIndexByName>(name, index); }
	void FillResult(QueryResults &result, const IdSet &ids) const { nsFuncWrapper<&NamespaceImpl::FillResult>(result, ids); }
	void EnablePerfCounters(bool enable = true) { nsFuncWrapper<&NamespaceImpl::EnablePerfCounters>(enable); }
	ReplicationState GetReplState(const RdxContext &ctx) const { return nsFuncWrapper<&NamespaceImpl::GetReplState>(ctx); }
	void SetReplLSNs(LSNPair LSNs, const RdxContext &ctx) { nsFuncWrapper<&NamespaceImpl::SetReplLSNs>(LSNs, ctx); }
	void SetSlaveReplStatus(ReplicationState::Status status, const Error &error, const RdxContext &ctx) {
		nsFuncWrapper<&NamespaceImpl::SetSlaveReplStatus>(status, error, ctx);
	}
	void SetSlaveReplMasterState(MasterState state, const RdxContext &ctx) {
		nsFuncWrapper<&NamespaceImpl::SetSlaveReplMasterState>(state, ctx);
	}
	Error ReplaceTagsMatcher(const TagsMatcher &tm, const RdxContext &ctx) {
		return nsFuncWrapper<&NamespaceImpl::ReplaceTagsMatcher>(tm, ctx);
	}
	void Rename(const Namespace::Ptr &dst, const std::string &storagePath, const RdxContext &ctx) {
		if (this == dst.get() || dst == nullptr) {
			return;
		}
		doRename(dst, std::string(), storagePath, ctx);
	}
	void Rename(const std::string &newName, const std::string &storagePath, const RdxContext &ctx) {
		if (newName.empty()) {
			return;
		}
		doRename(nullptr, newName, storagePath, ctx);
	}
	void OnConfigUpdated(DBConfigProvider &configProvider, const RdxContext &ctx) {
		NamespaceConfigData configData;
		const auto nsName = GetName(ctx);
		std::string_view realNsName(nsName);
		if (isTmpNamespaceNameFast(nsName)) {
			realNsName = demangleTmpNamespaceName(realNsName);
		}
		configProvider.GetNamespaceConfig(realNsName, configData);
		startCopyPolicyTxSize_.store(configData.startCopyPolicyTxSize, std::memory_order_relaxed);
		copyPolicyMultiplier_.store(configData.copyPolicyMultiplier, std::memory_order_relaxed);
		txSizeToAlwaysCopy_.store(configData.txSizeToAlwaysCopy, std::memory_order_relaxed);
		longTxLoggingParams_.store(configProvider.GetTxLoggingParams(), std::memory_order_relaxed);
		longUpdDelLoggingParams_.store(configProvider.GetUpdDelLoggingParams(), std::memory_order_relaxed);
		nsFuncWrapper<&NamespaceImpl::OnConfigUpdated>(configProvider, ctx);
	}
	StorageOpts GetStorageOpts(const RdxContext &ctx) { return nsFuncWrapper<&NamespaceImpl::GetStorageOpts>(ctx); }
	void Refill(std::vector<Item> &items, const RdxContext &ctx) { nsFuncWrapper<&NamespaceImpl::Refill>(items, ctx); }
	void SetTagsMatcher(TagsMatcher &&tm, const RdxContext &ctx) {
		return nsFuncWrapper<&NamespaceImpl::SetTagsMatcher>(std::move(tm), ctx);
	}

	void DumpIndex(std::ostream &os, std::string_view index, const RdxContext &ctx) {
		return nsFuncWrapper<&NamespaceImpl::DumpIndex>(os, index, ctx);
	}

protected:
	friend class ReindexerImpl;
	friend class QueryResults;
	void updateSelectTime() const { nsFuncWrapper<&NamespaceImpl::updateSelectTime>(); }
	void setSlaveMode(const RdxContext &ctx) { nsFuncWrapper<&NamespaceImpl::setSlaveMode>(ctx); }
	NamespaceImpl::Ptr getMainNs() const { return atomicLoadMainNs(); }
	NamespaceImpl::Ptr awaitMainNs(const RdxContext &ctx) const {
		if (hasCopy_.load(std::memory_order_acquire)) {
			contexted_unique_lock<Mutex, const RdxContext> lck(clonerMtx_, ctx);
			assertrx(!hasCopy_.load(std::memory_order_acquire));
			return ns_;
		}
		return atomicLoadMainNs();
	}

private:
	bool needNamespaceCopy(const NamespaceImpl::Ptr &ns, const Transaction &tx) const noexcept;
	void doRename(const Namespace::Ptr &dst, const std::string &newName, const std::string &storagePath, const RdxContext &ctx);
	NamespaceImpl::Ptr atomicLoadMainNs() const {
		std::lock_guard<spinlock> lck(nsPtrSpinlock_);
		return ns_;
	}
	void atomicStoreMainNs(NamespaceImpl *ns) {
		std::lock_guard<spinlock> lck(nsPtrSpinlock_);
		ns_.reset(ns);
	}

	NamespaceImpl::Ptr ns_;
	std::unique_ptr<NamespaceImpl> nsCopy_;
	std::atomic<bool> hasCopy_ = {false};
	using Mutex = MarkedMutex<std::timed_mutex, MutexMark::CloneNs>;
	mutable Mutex clonerMtx_;
	mutable spinlock nsPtrSpinlock_;
	std::atomic<int> startCopyPolicyTxSize_;
	std::atomic<int> copyPolicyMultiplier_;
	std::atomic<int> txSizeToAlwaysCopy_;
	TxStatCounter txStatsCounter_;
	PerfStatCounterMT commitStatsCounter_;
	PerfStatCounterMT copyStatsCounter_;
	std::atomic<LongTxLoggingParams> longTxLoggingParams_;
	std::atomic<LongQueriesLoggingParams> longUpdDelLoggingParams_;
	BackgroundNamespaceDeleter &bgDeleter_;
};

}  // namespace reindexer
