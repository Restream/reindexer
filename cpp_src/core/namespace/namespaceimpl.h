#pragma once

#include <atomic>
#include <deque>
#include <memory>
#include <set>
#include <thread>
#include <vector>
#include "asyncstorage.h"
#include "cluster/insdatareplicator.h"
#include "core/cjson/tagsmatcher.h"
#include "core/dbconfig.h"
#include "core/index/keyentry.h"
#include "core/item.h"
#include "core/joincache.h"
#include "core/namespacedef.h"
#include "core/payload/payloadiface.h"
#include "core/perfstatcounter.h"
#include "core/querycache.h"
#include "core/schema.h"
#include "core/storage/idatastorage.h"
#include "core/storage/storagetype.h"
#include "core/transaction/localtransaction.h"
#include "estl/contexted_locks.h"
#include "estl/fast_hash_map.h"
#include "estl/shared_mutex.h"
#include "estl/smart_lock.h"
#include "estl/syncpool.h"
#include "stringsholder.h"
#include "wal/waltracker.h"

#ifdef kRxStorageItemPrefix
static_assert(false, "Redefinition of kRxStorageItemPrefix");
#endif	// kRxStorageItemPrefix
#define kRxStorageItemPrefix "I"

namespace reindexer {

using reindexer::datastorage::StorageType;

class Index;
struct SelectCtx;
struct JoinPreResult;
class DBConfigProvider;
class SelectLockUpgrader;
class QueryPreprocessor;
class SelectIteratorContainer;
class RdxContext;
class RdxActivityContext;
class ItemComparator;
class SortExpression;
class ProxiedSortExpression;
class ProtobufSchema;
class LocalQueryResults;
class SnapshotRecord;
class Snapshot;
struct SnapshotOpts;
namespace SortExprFuncs {
struct DistanceBetweenJoinedIndexesSameNs;
}  // namespace SortExprFuncs

class NsContext {
public:
	NsContext(const RdxContext &rdxCtx) : rdxContext(rdxCtx) {}
	NsContext &InTransaction(lsn_t stepLsn) noexcept {
		inTransaction = true;
		originLsn_ = stepLsn;
		return *this;
	}
	NsContext &CopiedNsRequest() noexcept {
		isCopiedNsRequest = true;
		return *this;
	}
	NsContext &InSnapshot(lsn_t stepLsn, bool wal, bool requireResync, bool initialLeaderSync) noexcept {
		inSnapshot = true;
		isWal = wal;
		originLsn_ = stepLsn;
		isRequireResync = requireResync;
		isInitialLeaderSync = initialLeaderSync;
		return *this;
	}
	lsn_t GetOriginLSN() const noexcept { return (inTransaction || inSnapshot) ? originLsn_ : rdxContext.GetOriginLSN(); }
	bool IsForceSyncItem() const noexcept { return inSnapshot && !isWal; }
	bool IsWalSyncItem() const noexcept { return inSnapshot && isWal; }

	const RdxContext &rdxContext;
	bool inTransaction = false;
	bool inSnapshot = false;
	bool isCopiedNsRequest = false;
	bool isWal = false;
	bool isRequireResync = false;
	bool isInitialLeaderSync = false;

private:
	lsn_t originLsn_;
};

class NamespaceImpl {  // NOLINT(*performance.Padding) Padding does not matter for this class
	class IndexesCacheCleaner {
	public:
		explicit IndexesCacheCleaner(NamespaceImpl &ns) : ns_{ns} {}
		IndexesCacheCleaner(const IndexesCacheCleaner &) = delete;
		IndexesCacheCleaner(IndexesCacheCleaner &&) = delete;
		IndexesCacheCleaner &operator=(const IndexesCacheCleaner &) = delete;
		IndexesCacheCleaner &operator=(IndexesCacheCleaner &&) = delete;
		void Add(SortType s) {
			if (s > 0) {
				sorts_.set(s);
			}
		}
		~IndexesCacheCleaner();

	private:
		NamespaceImpl &ns_;
		std::bitset<64> sorts_;
	};

protected:
	friend class NsSelecter;
	friend class JoinedSelector;
	friend class WALSelecter;
	friend class NsSelectFuncInterface;
	friend class QueryPreprocessor;
	friend class SelectIteratorContainer;
	friend class ItemComparator;
	friend class ItemModifier;
	friend class Namespace;
	friend SortExpression;
	friend ProxiedSortExpression;
	friend SortExprFuncs::DistanceBetweenJoinedIndexesSameNs;
	friend class ReindexerImpl;
	friend LocalQueryResults;
	friend class SnapshotHandler;
	friend class FieldComparator;
	friend class QueryResults;
	friend class ItemsLoader;
	friend class IndexInserters;

	class NSUpdateSortedContext : public UpdateSortedContext {
	public:
		NSUpdateSortedContext(const NamespaceImpl &ns, SortType curSortId)
			: ns_(ns), sorted_indexes_(ns_.getSortedIdxCount()), curSortId_(curSortId) {
			ids2Sorts_.reserve(ns.items_.size());
			ids2SortsMemSize_ = ids2Sorts_.capacity() * sizeof(SortType);
			ns.nsUpdateSortedContextMemory_.fetch_add(ids2SortsMemSize_);
			for (IdType i = 0; i < IdType(ns_.items_.size()); i++)
				ids2Sorts_.push_back(ns_.items_[i].IsFree() ? SortIdUnexists : SortIdUnfilled);
		}
		~NSUpdateSortedContext() override { ns_.nsUpdateSortedContextMemory_.fetch_sub(ids2SortsMemSize_); }
		int getSortedIdxCount() const noexcept override { return sorted_indexes_; }
		SortType getCurSortId() const noexcept override { return curSortId_; }
		const std::vector<SortType> &ids2Sorts() const noexcept override { return ids2Sorts_; }
		std::vector<SortType> &ids2Sorts() noexcept override { return ids2Sorts_; }

	protected:
		const NamespaceImpl &ns_;
		const int sorted_indexes_;
		const IdType curSortId_;
		std::vector<SortType> ids2Sorts_;
		int64_t ids2SortsMemSize_ = 0;
	};

	class IndexesStorage : public std::vector<std::unique_ptr<Index>> {
	public:
		using Base = std::vector<std::unique_ptr<Index>>;

		IndexesStorage(const NamespaceImpl &ns);

		IndexesStorage(const IndexesStorage &src) = delete;
		IndexesStorage &operator=(const IndexesStorage &src) = delete;

		IndexesStorage(IndexesStorage &&src) = delete;
		IndexesStorage &operator=(IndexesStorage &&src) noexcept = delete;

		int denseIndexesSize() const { return ns_.payloadType_.NumFields(); }
		int sparseIndexesSize() const { return ns_.sparseIndexesCount_; }
		int compositeIndexesSize() const { return totalSize() - denseIndexesSize() - sparseIndexesSize(); }
		void MoveBase(IndexesStorage &&src);
		int firstSparsePos() const { return ns_.payloadType_.NumFields(); }
		int firstCompositePos() const { return ns_.payloadType_.NumFields() + ns_.sparseIndexesCount_; }
		int firstCompositePos(const PayloadType &pt, int sparseIndexes) const { return pt.NumFields() + sparseIndexes; }

		int totalSize() const { return size(); }

	private:
		const NamespaceImpl &ns_;
	};

	class Items : public std::vector<PayloadValue> {
	public:
		bool exists(IdType id) const { return id < IdType(size()) && !at(id).IsFree(); }
	};

public:
	using UpdatesContainer = h_vector<cluster::UpdateRecord, 2>;
	enum OptimizationState : int { NotOptimized, OptimizedPartially, OptimizationCompleted };

	typedef shared_ptr<NamespaceImpl> Ptr;
	using Mutex = MarkedMutex<shared_timed_mutex, MutexMark::Namespace>;

	class Locker {
	public:
		class NsWLock {
		public:
			NsWLock() = default;
			NsWLock(Mutex &mtx, const RdxContext &ctx, bool isCL) : impl_(mtx, &ctx), isClusterLck_(isCL) {}
			void lock() { impl_.lock(); }
			void unlock() { impl_.unlock(); }
			bool owns_lock() const { return impl_.owns_lock(); }
			bool isClusterLck() const noexcept { return isClusterLck_; }

		private:
			contexted_unique_lock<Mutex, const RdxContext> impl_;
			bool isClusterLck_ = false;
		};
		typedef contexted_shared_lock<Mutex, const RdxContext> RLockT;
		typedef NsWLock WLockT;

		Locker(cluster::INsDataReplicator *clusterizator, NamespaceImpl &owner) : clusterizator_(clusterizator), owner_(owner) {}

		RLockT RLock(const RdxContext &ctx) const { return RLockT(mtx_, &ctx); }
		WLockT DataWLock(const RdxContext &ctx, bool skipClusterStatusCheck) const {
			using namespace std::string_view_literals;
			WLockT lck(mtx_, ctx, true);
			if (readonly_.load(std::memory_order_acquire)) {
				throw Error(errNamespaceInvalidated, "NS invalidated"sv);
			}
			const bool requireSync = clusterizator_ && !ctx.NoWaitSync() && ctx.GetOriginLSN().isEmpty() && !owner_.isSystem();
			const bool isFollowerNS = owner_.repl_.clusterStatus.role == ClusterizationStatus::Role::SimpleReplica ||
									  owner_.repl_.clusterStatus.role == ClusterizationStatus::Role::ClusterReplica;
			bool synchronized = isFollowerNS || !requireSync || clusterizator_->IsInitialSyncDone(owner_.name_);
			while (!synchronized) {
				// This is required in case of rename during sync wait
				auto name = owner_.name_;

				lck.unlock();
				clusterizator_->AwaitInitialSync(name, ctx);
				lck.lock();
				if (readonly_.load(std::memory_order_acquire)) {
					throw Error(errNamespaceInvalidated, "NS invalidated"sv);
				}
				synchronized = clusterizator_->IsInitialSyncDone(owner_.name_);
			}

			if (!skipClusterStatusCheck) {
				owner_.checkClusterStatus(ctx);	 // throw exception if false
			}

			return lck;
		}
		WLockT SimpleWLock(const RdxContext &ctx) const {
			using namespace std::string_view_literals;
			WLockT lck(mtx_, ctx, false);
			if (readonly_.load(std::memory_order_acquire)) {
				throw Error(errNamespaceInvalidated, "NS invalidated"sv);
			}
			return lck;
		}
		std::unique_lock<std::mutex> StorageLock() const {
			using namespace std::string_view_literals;
			std::unique_lock<std::mutex> lck(storageMtx_);
			if (readonly_.load(std::memory_order_acquire)) {
				throw Error(errNamespaceInvalidated, "NS invalidated"sv);
			}
			return lck;
		}
		void MarkReadOnly() { readonly_.store(true, std::memory_order_release); }
		std::atomic_bool &IsReadOnly() { return readonly_; }

	private:
		mutable Mutex mtx_;
		mutable std::mutex storageMtx_;
		std::atomic<bool> readonly_ = {false};
		cluster::INsDataReplicator *clusterizator_;
		NamespaceImpl &owner_;
	};

	NamespaceImpl(const std::string &_name, std::optional<int32_t> stateToken, cluster::INsDataReplicator *clusterizator);
	NamespaceImpl &operator=(const NamespaceImpl &) = delete;
	~NamespaceImpl();

	std::string GetName(const RdxContext &ctx) const {
		auto rlck = rLock(ctx);
		return name_;
	}
	bool IsSystem(const RdxContext &ctx) const {
		auto rlck = rLock(ctx);
		return isSystem();
	}
	bool IsTemporary(const RdxContext &ctx) const { return GetReplState(ctx).temporary; }
	void SetNsVersion(lsn_t version, const RdxContext &ctx);

	void EnableStorage(const std::string &path, StorageOpts opts, StorageType storageType, const RdxContext &ctx);
	void LoadFromStorage(unsigned threadsCount, const RdxContext &ctx);
	void DeleteStorage(const RdxContext &);

	uint32_t GetItemsCount() const { return itemsCount_.load(std::memory_order_relaxed); }
	uint32_t GetItemsCapacity() const { return itemsCapacity_.load(std::memory_order_relaxed); }
	void AddIndex(const IndexDef &indexDef, const RdxContext &ctx);
	void UpdateIndex(const IndexDef &indexDef, const RdxContext &ctx);
	void DropIndex(const IndexDef &indexDef, const RdxContext &ctx);
	void SetSchema(std::string_view schema, const RdxContext &ctx);
	std::string GetSchema(int format, const RdxContext &ctx);

	void Insert(Item &item, const RdxContext &ctx);
	void Update(Item &item, const RdxContext &ctx);
	void Update(const Query &query, LocalQueryResults &result, const RdxContext &);
	void Upsert(Item &item, const RdxContext &);
	void Delete(Item &item, const RdxContext &);
	void Delete(const Query &query, LocalQueryResults &result, const RdxContext &);
	void ModifyItem(Item &item, int mode, const RdxContext &ctx);
	void Truncate(const RdxContext &);
	void Refill(std::vector<Item> &, const RdxContext &);

	void Select(LocalQueryResults &result, SelectCtx &params, const RdxContext &);
	NamespaceDef GetDefinition(const RdxContext &ctx);
	NamespaceMemStat GetMemStat(const RdxContext &);
	NamespacePerfStat GetPerfStat(const RdxContext &);
	void ResetPerfStat(const RdxContext &);
	std::vector<std::string> EnumMeta(const RdxContext &ctx);

	void BackgroundRoutine(RdxActivityContext *);
	void StorageFlushingRoutine();
	void CloseStorage(const RdxContext &);

	LocalTransaction NewTransaction(const RdxContext &ctx);
	void CommitTransaction(LocalTransaction &tx, LocalQueryResults &result, const NsContext &ctx);

	Item NewItem(const RdxContext &ctx);
	void ToPool(ItemImpl *item);
	// Get meta data from storage by key
	std::string GetMeta(const std::string &key, const RdxContext &ctx);
	// Put meta data to storage by key
	void PutMeta(const std::string &key, std::string_view data, const RdxContext &);
	int64_t GetSerial(const std::string &field, UpdatesContainer &replUpdates, const NsContext &ctx);

	int getIndexByName(const std::string &index) const;
	bool getIndexByName(const std::string &name, int &index) const;
	PayloadType getPayloadType(const RdxContext &ctx) const;

	void FillResult(LocalQueryResults &result, const IdSet &ids) const;

	void EnablePerfCounters(bool enable = true) { enablePerfCounters_ = enable; }

	ReplicationState GetReplState(const RdxContext &) const;
	ReplicationStateV2 GetReplStateV2(const RdxContext &) const;

	void OnConfigUpdated(DBConfigProvider &configProvider, const RdxContext &ctx);
	StorageOpts GetStorageOpts(const RdxContext &);
	std::shared_ptr<const Schema> GetSchemaPtr(const RdxContext &ctx) const;
	IndexesCacheCleaner GetIndexesCacheCleaner() { return IndexesCacheCleaner{*this}; }
	Error SetClusterizationStatus(ClusterizationStatus &&status, const RdxContext &ctx);
	void ApplySnapshotChunk(const SnapshotChunk &ch, bool isInitialLeaderSync, const RdxContext &ctx);
	void GetSnapshot(Snapshot &snapshot, const SnapshotOpts &opts, const RdxContext &ctx);
	void SetTagsMatcher(TagsMatcher &&tm, const RdxContext &ctx);

protected:
	struct SysRecordsVersions {
		uint64_t idxVersion{0};
		uint64_t tagsVersion{0};
		uint64_t replVersion{0};
		uint64_t schemaVersion{0};
	};

	ReplicationState getReplState() const;
	std::string sysRecordName(std::string_view sysTag, uint64_t version);
	void writeSysRecToStorage(std::string_view data, std::string_view sysTag, uint64_t &version, bool direct);
	void saveIndexesToStorage();
	void saveSchemaToStorage();
	Error loadLatestSysRecord(std::string_view baseSysTag, uint64_t &version, std::string &content);
	bool loadIndexesFromStorage();
	void saveReplStateToStorage(bool direct = true);
	void saveTagsMatcherToStorage(bool clearUpdate);
	void loadReplStateFromStorage();

	void initWAL(int64_t minLSN, int64_t maxLSN);

	void markUpdated(bool forceOptimizeAllIndexes);
	Item newItem();
	void doUpdate(const Query &query, LocalQueryResults &result, UpdatesContainer &pendedRepl, const NsContext &);
	void doDelete(const Query &query, LocalQueryResults &result, UpdatesContainer &pendedRepl, const NsContext &);
	void doUpsert(ItemImpl *ritem, IdType id, bool doUpdate);
	void modifyItem(Item &item, int mode, UpdatesContainer &pendedRepl, const NsContext &ctx);
	void deleteItem(Item &item, UpdatesContainer &pendedRepl, const NsContext &ctx);
	void doModifyItem(Item &item, int mode, UpdatesContainer &pendedRepl, const NsContext &ctx, IdType suggestedId = -1);
	void updateTagsMatcherFromItem(ItemImpl *ritem, const NsContext &ctx);
	void updateItems(const PayloadType &oldPlType, const FieldsSet &changedFields, int deltaFields);
	void fillSparseIndex(Index &, std::string_view jsonPath);
	void doDelete(IdType id);
	void doTruncate(UpdatesContainer &pendedRepl, const NsContext &ctx);
	void optimizeIndexes(const NsContext &);
	void insertIndex(std::unique_ptr<Index> newIndex, int idxNo, const std::string &realName);
	void addIndex(const IndexDef &indexDef, bool disableTmVersionInc, bool skipEqualityCheck = false);
	void doAddIndex(const IndexDef &indexDef, bool skipEqualityCheck, UpdatesContainer &pendedRepl, const NsContext &ctx);
	void addCompositeIndex(const IndexDef &indexDef);
	bool checkIfSameIndexExists(const IndexDef &indexDef, bool discardConfig, bool *requireTtlUpdate = nullptr);
	void verifyUpdateIndex(const IndexDef &indexDef) const;
	void verifyUpdateCompositeIndex(const IndexDef &indexDef) const;
	void updateIndex(const IndexDef &indexDef, bool disableTmVersionInc);
	void doUpdateIndex(const IndexDef &indexDef, UpdatesContainer &pendedRepl, const NsContext &ctx);
	void dropIndex(const IndexDef &index, bool disableTmVersionInc);
	void doDropIndex(const IndexDef &index, UpdatesContainer &pendedRepl, const NsContext &ctx);
	void addToWAL(const IndexDef &indexDef, WALRecType type, const NsContext &ctx);
	void addToWAL(std::string_view json, WALRecType type, const NsContext &ctx);
	void removeExpiredItems(RdxActivityContext *);
	void removeExpiredStrings(RdxActivityContext *);
	void setSchema(std::string_view schema, UpdatesContainer &pendedRepl, const NsContext &ctx);
	void setTagsMatcher(TagsMatcher &&tm, UpdatesContainer &pendedRepl, const NsContext &ctx);
	void replicateItem(IdType itemId, const NsContext &ctx, bool statementReplication, uint64_t oldPlHash, size_t oldItemCapacity,
					   int oldTmVersion, UpdatesContainer &pendedRepl);

	void recreateCompositeIndexes(int startIdx, int endIdx);
	NamespaceDef getDefinition() const;
	IndexDef getIndexDefinition(const std::string &indexName) const;
	IndexDef getIndexDefinition(size_t) const;

	std::string getMeta(const std::string &key) const;
	void putMeta(const std::string &key, std::string_view data, UpdatesContainer &pendedRepl, const NsContext &ctx);
	std::pair<IdType, bool> findByPK(ItemImpl *ritem, bool inTransaction, const RdxContext &);
	int getSortedIdxCount() const;
	void updateSortedIdxCount();
	void setFieldsBasedOnPrecepts(ItemImpl *ritem, UpdatesContainer &replUpdates, const NsContext ctx);

	void putToJoinCache(JoinCacheRes &res, std::shared_ptr<JoinPreResult> preResult) const;
	void putToJoinCache(JoinCacheRes &res, JoinCacheVal &&val) const;
	void getFromJoinCache(JoinCacheRes &ctx) const;
	void getIndsideFromJoinCache(JoinCacheRes &ctx) const;

	const FieldsSet &pkFields();

	std::vector<std::string> enumMeta() const;

	void warmupFtIndexes();
	void updateSelectTime();
	void markReadOnly() { locker_.MarkReadOnly(); }
	Locker::WLockT simpleWLock(const RdxContext &ctx) const { return locker_.SimpleWLock(ctx); }
	Locker::WLockT dataWLock(const RdxContext &ctx, bool skipClusterStatusCheck = false) const {
		return locker_.DataWLock(ctx, skipClusterStatusCheck);
	}
	Locker::RLockT rLock(const RdxContext &ctx) const { return locker_.RLock(ctx); }
	void checkClusterRole(const RdxContext &ctx) const { checkClusterRole(ctx.GetOriginLSN()); }
	void checkClusterRole(lsn_t originLsn) const;
	void checkClusterStatus(const RdxContext &ctx) const { checkClusterStatus(ctx.GetOriginLSN()); }
	void checkClusterStatus(lsn_t originLsn) const;
	void replicateTmUpdateIfRequired(UpdatesContainer &pendedRepl, int oldTmVersion, const NsContext &ctx) noexcept;

	bool SortOrdersBuilt() const noexcept { return optimizationState_.load(std::memory_order_acquire) == OptimizationCompleted; }

	IndexesStorage indexes_;
	fast_hash_map<std::string, int, nocase_hash_str, nocase_equal_str> indexesNames_;
	// All items with data
	Items items_;
	std::vector<IdType> free_;
	// NamespaceImpl name
	std::string name_;
	// Payload types
	PayloadType payloadType_;

	// Tags matcher
	TagsMatcher tagsMatcher_;

	AsyncStorage storage_;
	std::atomic<unsigned> replStateUpdates_ = {0};

	std::unordered_map<std::string, std::string> meta_;

	shared_ptr<QueryTotalCountCache> queryTotalCountCache_;

	int sparseIndexesCount_ = 0;
	VariantArray krefs, skrefs;

	SysRecordsVersions sysRecordsVersions_;

	Locker locker_;
	std::shared_ptr<Schema> schema_;

	StringsHolderPtr StrHolder(bool noLock, const RdxContext &);
	std::set<std::string> GetFTIndexes(const RdxContext &) const;
	size_t ItemsCount() const noexcept { return items_.size() - free_.size(); }
	const NamespaceConfigData &Config() const noexcept { return config_; }

	void DumpIndex(std::ostream &os, std::string_view index, const RdxContext &ctx) const;

private:
	NamespaceImpl(const NamespaceImpl &src, AsyncStorage::FullLockT &storageLock);

	bool isSystem() const noexcept { return !name_.empty() && name_[0] == '#'; }
	IdType createItem(size_t realSize, IdType suggestedId);

	void processWalRecord(WALRecord &&wrec, const NsContext &ctx, lsn_t itemLsn = lsn_t(), Item *item = nullptr);
	void replicateAsync(cluster::UpdateRecord &&rec, const RdxContext &ctx);
	void replicateAsync(UpdatesContainer &&recs, const RdxContext &ctx);
	void replicate(cluster::UpdateRecord &&rec, Locker::WLockT &&wlck, bool tryForceFlush, const RdxContext &ctx);
	void replicate(UpdatesContainer &&recs, Locker::WLockT &&wlck, bool tryForceFlush, const NsContext &ctx);

	void setTemporary() noexcept { repl_.temporary = true; }

	void removeIndex(std::unique_ptr<Index> &);
	void dumpIndex(std::ostream &os, std::string_view index) const;
	void tryForceFlush(Locker::WLockT &&wlck) {
		if (wlck.owns_lock()) {
			wlck.unlock();
			storage_.TryForceFlush();
		}
	}

	JoinCache::Ptr joinCache_;

	PerfStatCounterMT updatePerfCounter_, selectPerfCounter_;
	std::atomic<bool> enablePerfCounters_;

	NamespaceConfigData config_;
	// Replication variables
	WALTracker wal_;
	ReplicationState repl_;

	StorageOpts storageOpts_;
	std::atomic<int64_t> lastSelectTime_;

	sync_pool<ItemImpl, 1024> pool_;
	std::atomic<int32_t> cancelCommitCnt_{0};
	std::atomic<int64_t> lastUpdateTime_;

	std::atomic<uint32_t> itemsCount_ = {0};
	std::atomic<uint32_t> itemsCapacity_ = {0};
	bool nsIsLoading_;

	size_t itemsDataSize_ = 0;

	std::atomic<int> optimizationState_{OptimizationState::NotOptimized};
	StringsHolderPtr strHolder_;
	std::deque<StringsHolderPtr> strHoldersWaitingToBeDeleted_;
	std::chrono::seconds lastExpirationCheckTs_;
	mutable std::atomic<int64_t> nsUpdateSortedContextMemory_ = {0};

	cluster::INsDataReplicator *clusterizator_;
};

}  // namespace reindexer
