#pragma once

#include <thread>
#include <type_traits>
#include "core/txstats.h"
#include "estl/shared_mutex.h"
#include "namespaceimpl.h"

namespace reindexer {

#define handleInvalidation(Fn) nsFuncWrapper<decltype(&Fn), &Fn>

class Namespace {
public:
	Namespace(const string &name, UpdatesObservers &observers) : ns_(std::make_shared<NamespaceImpl>(name, observers)) {}
	Namespace(NamespaceImpl::Ptr ns) : ns_(std::move(ns)) {}
	typedef shared_ptr<Namespace> Ptr;

	void CommitTransaction(Transaction &tx, QueryResults &result, const RdxContext &ctx);
	const string &GetName() const { return handleInvalidation(NamespaceImpl::GetName)(); }
	bool IsSystem(const RdxContext &ctx) const { return handleInvalidation(NamespaceImpl::IsSystem)(ctx); }
	bool IsTemporary(const RdxContext &ctx) const { return handleInvalidation(NamespaceImpl::IsTemporary)(ctx); }
	void EnableStorage(const string &path, StorageOpts opts, StorageType storageType, const RdxContext &ctx) {
		handleInvalidation(NamespaceImpl::EnableStorage)(path, opts, storageType, ctx);
	}
	void LoadFromStorage(const RdxContext &ctx) { handleInvalidation(NamespaceImpl::LoadFromStorage)(ctx); }
	void DeleteStorage(const RdxContext &ctx) { handleInvalidation(NamespaceImpl::DeleteStorage)(ctx); }
	uint32_t GetItemsCount() { return handleInvalidation(NamespaceImpl::GetItemsCount)(); }
	void AddIndex(const IndexDef &indexDef, const RdxContext &ctx) { handleInvalidation(NamespaceImpl::AddIndex)(indexDef, ctx); }
	void UpdateIndex(const IndexDef &indexDef, const RdxContext &ctx) { handleInvalidation(NamespaceImpl::UpdateIndex)(indexDef, ctx); }
	void DropIndex(const IndexDef &indexDef, const RdxContext &ctx) { handleInvalidation(NamespaceImpl::DropIndex)(indexDef, ctx); }
	void Insert(Item &item, const RdxContext &ctx) { handleInvalidation(NamespaceImpl::Insert)(item, ctx); }
	void Update(Item &item, const RdxContext &ctx) {
		nsFuncWrapper<void (NamespaceImpl::*)(Item &, const RdxContext &), &NamespaceImpl::Update>(item, ctx);
	}
	void Update(const Query &query, QueryResults &result, const NsContext &ctx) {
		nsFuncWrapper<void (NamespaceImpl::*)(const Query &, QueryResults &, const NsContext &ctx), &NamespaceImpl::Update>(query, result,
																															ctx);
	}
	void Upsert(Item &item, const NsContext &ctx) { handleInvalidation(NamespaceImpl::Upsert)(item, ctx); }
	void Delete(Item &item, const NsContext &ctx) {
		nsFuncWrapper<void (NamespaceImpl::*)(Item &, const NsContext &), &NamespaceImpl::Delete>(item, ctx);
	}
	void Delete(const Query &query, QueryResults &result, const NsContext &ctx) {
		nsFuncWrapper<void (NamespaceImpl::*)(const Query &, QueryResults &, const NsContext &), &NamespaceImpl::Delete>(query, result,
																														 ctx);
	}
	void Truncate(const NsContext &ctx) { handleInvalidation(NamespaceImpl::Truncate)(ctx); }
	void Select(QueryResults &result, SelectCtx &params, const RdxContext &ctx) {
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
	vector<string> EnumMeta(const RdxContext &ctx) { return handleInvalidation(NamespaceImpl::EnumMeta)(ctx); }
	void BackgroundRoutine(RdxActivityContext *ctx) {
		if (hasCopy_.load(std::memory_order_acquire)) {
			return;
		}
		handleInvalidation(NamespaceImpl::BackgroundRoutine)(ctx);
	}
	void CloseStorage(const RdxContext &ctx) { handleInvalidation(NamespaceImpl::CloseStorage)(ctx); }
	Transaction NewTransaction(const RdxContext &ctx) { return handleInvalidation(NamespaceImpl::NewTransaction)(ctx); }

	Item NewItem(const RdxContext &ctx) { return handleInvalidation(NamespaceImpl::NewItem)(ctx); }
	void ToPool(ItemImpl *item) { handleInvalidation(NamespaceImpl::ToPool)(item); }
	string GetMeta(const string &key, const RdxContext &ctx) { return handleInvalidation(NamespaceImpl::GetMeta)(key, ctx); }
	void PutMeta(const string &key, const string_view &data, const NsContext &ctx) {
		handleInvalidation(NamespaceImpl::PutMeta)(key, data, ctx);
	}
	int64_t GetSerial(const string &field) { return handleInvalidation(NamespaceImpl::GetSerial)(field); }
	int getIndexByName(const string &index) const {
		return nsFuncWrapper<int (NamespaceImpl::*)(const string &) const, &NamespaceImpl::getIndexByName>(index);
	}
	bool getIndexByName(const string &name, int &index) const {
		return nsFuncWrapper<bool (NamespaceImpl::*)(const string &, int &) const, &NamespaceImpl::getIndexByName>(name, index);
	}
	void FillResult(QueryResults &result, IdSet::Ptr ids) const { handleInvalidation(NamespaceImpl::FillResult)(result, ids); }
	void EnablePerfCounters(bool enable = true) { handleInvalidation(NamespaceImpl::EnablePerfCounters)(enable); }
	ReplicationState GetReplState(const RdxContext &ctx) const { return handleInvalidation(NamespaceImpl::GetReplState)(ctx); }
	void SetSlaveLSN(int64_t slaveLSN, const RdxContext &ctx) { handleInvalidation(NamespaceImpl::SetSlaveLSN)(slaveLSN, ctx); }
	void SetSlaveReplStatus(ReplicationState::Status status, const Error &error, const RdxContext &ctx) {
		handleInvalidation(NamespaceImpl::SetSlaveReplStatus)(status, error, ctx);
	}
	void SetSlaveReplMasterState(MasterState state, const RdxContext &ctx) {
		handleInvalidation(NamespaceImpl::SetSlaveReplMasterState)(state, ctx);
	}
	void ReplaceTagsMatcher(const TagsMatcher &tm, const RdxContext &ctx) {
		handleInvalidation(NamespaceImpl::ReplaceTagsMatcher)(tm, ctx);
	}
	void Rename(Namespace::Ptr dst, const std::string &storagePath, const RdxContext &ctx) {
		if (this == dst.get() || dst == nullptr) {
			return;
		}
		doRename(std::move(dst), std::string(), storagePath, ctx);
	}
	void Rename(const std::string &newName, const std::string &storagePath, const RdxContext &ctx) {
		if (newName.empty()) {
			return;
		}
		doRename(nullptr, newName, storagePath, ctx);
	}
	void OnConfigUpdated(DBConfigProvider &configProvider, const RdxContext &ctx) {
		NamespaceConfigData configData;
		configProvider.GetNamespaceConfig(GetName(), configData);
		startCopyPolicyTxSize_.store(configData.startCopyPolicyTxSize, std::memory_order_relaxed);
		copyPolicyMultiplier_.store(configData.copyPolicyMultiplier, std::memory_order_relaxed);
		txSizeToAlwaysCopy_.store(configData.txSizeToAlwaysCopy, std::memory_order_relaxed);
		handleInvalidation(NamespaceImpl::OnConfigUpdated)(configProvider, ctx);
	}
	void SetStorageOpts(StorageOpts opts, const RdxContext &ctx) { handleInvalidation(NamespaceImpl::SetStorageOpts)(opts, ctx); }
	StorageOpts GetStorageOpts(const RdxContext &ctx) { return handleInvalidation(NamespaceImpl::GetStorageOpts)(ctx); }
	void Refill(vector<Item> &items, const NsContext &ctx) { handleInvalidation(NamespaceImpl::Refill)(items, ctx); }

protected:
	friend class ReindexerImpl;
	bool tryToReload(const RdxContext &ctx) const { return handleInvalidation(NamespaceImpl::tryToReload)(ctx); }
	bool needToLoadData(const RdxContext &ctx) const { return handleInvalidation(NamespaceImpl::needToLoadData)(ctx); }
	void updateSelectTime() const { handleInvalidation(NamespaceImpl::updateSelectTime)(); }

	NamespaceImpl::Ptr getMainNs() const { return atomicLoadMainNs(); }
	NamespaceImpl::Ptr awaitMainNs(const RdxContext &ctx) const {
		if (hasCopy_.load(std::memory_order_acquire)) {
			contexted_unique_lock<Mutex, const RdxContext> lck(clonerMtx_, &ctx);
			assert(!hasCopy_.load(std::memory_order_acquire));
			return ns_;
		}
		return atomicLoadMainNs();
	}

private:
	template <typename Fn, Fn fn, typename... Args>
	typename std::result_of<Fn(NamespaceImpl, Args...)>::type nsFuncWrapper(Args &&... args) const {
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

	bool needNamespaceCopy(const NamespaceImpl::Ptr &ns, const Transaction &tx) const noexcept;
	void doRename(Namespace::Ptr dst, const std::string &newName, const std::string &storagePath, const RdxContext &ctx);
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
	TxStatCounter txStatsCounter_;
	PerfStatCounterMT commitStatsCounter_;
	PerfStatCounterMT copyStatsCounter_;
};

#undef handleInvalidation

}  // namespace reindexer
