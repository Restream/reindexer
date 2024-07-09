#pragma once

#include <atomic>
#include <deque>
#include <memory>
#include <set>
#include <thread>
#include <vector>
#include "asyncstorage.h"
#include "core/cjson/tagsmatcher.h"
#include "core/dbconfig.h"
#include "core/index/keyentry.h"
#include "core/item.h"
#include "core/joincache.h"
#include "core/namespacedef.h"
#include "core/payload/payloadiface.h"
#include "core/perfstatcounter.h"
#include "core/querycache.h"
#include "core/rollback.h"
#include "core/schema.h"
#include "core/selectkeyresult.h"
#include "core/storage/storagetype.h"
#include "core/transaction.h"
#include "estl/contexted_locks.h"
#include "estl/fast_hash_map.h"
#include "estl/shared_mutex.h"
#include "estl/syncpool.h"
#include "replicator/updatesobserver.h"
#include "replicator/waltracker.h"
#include "stringsholder.h"

#ifdef kRxStorageItemPrefix
static_assert(false, "Redefinition of kRxStorageItemPrefix");
#endif	// kRxStorageItemPrefix
#define kRxStorageItemPrefix "I"

namespace reindexer {

using reindexer::datastorage::StorageType;

class Index;
template <typename>
struct SelectCtxWithJoinPreSelect;
struct JoinPreResult;
class QueryResults;
class DBConfigProvider;
class QueryPreprocessor;
class RdxContext;
class RdxActivityContext;
class SortExpression;

namespace long_actions {
template <typename T>
struct Logger;
}  // namespace long_actions

template <typename T, template <typename> class>
class QueryStatCalculator;

namespace SortExprFuncs {
struct DistanceBetweenJoinedIndexesSameNs;
}  // namespace SortExprFuncs

struct NsContext {
	NsContext(const RdxContext &rdxCtx) noexcept : rdxContext{rdxCtx} {}
	NsContext &InTransaction() noexcept {
		inTransaction = true;
		return *this;
	}
	NsContext &CopiedNsRequest() noexcept {
		isCopiedNsRequest = true;
		return *this;
	}

	const RdxContext &rdxContext;
	bool isCopiedNsRequest{false};
	bool inTransaction{false};
};

namespace composite_substitution_helpers {
class CompositeSearcher;
}

enum class StoredValuesOptimizationStatus : int8_t {
	DisabledByCompositeIndex,
	DisabledByFullTextIndex,
	DisabledByJoinedFieldSort,
	Enabled
};

enum class IndexOptimization : int8_t { Partial, Full };

class NamespaceImpl final : public intrusive_atomic_rc_base {  // NOLINT(*performance.Padding) Padding does not
	// matter for this class
	class RollBack_insertIndex;
	class RollBack_addIndex;
	template <NeedRollBack needRollBack>
	class RollBack_recreateCompositeIndexes;
	template <NeedRollBack needRollBack>
	class RollBack_updateItems;
	class IndexesCacheCleaner {
	public:
		explicit IndexesCacheCleaner(NamespaceImpl &ns) noexcept : ns_{ns} {}
		IndexesCacheCleaner(const IndexesCacheCleaner &) = delete;
		IndexesCacheCleaner(IndexesCacheCleaner &&) = delete;
		IndexesCacheCleaner &operator=(const IndexesCacheCleaner &) = delete;
		IndexesCacheCleaner &operator=(IndexesCacheCleaner &&) = delete;
		void Add(SortType s) {
			if rx_unlikely (s >= sorts_.size()) {
				throw Error(errLogic, "Index sort type overflow: %d. Limit is %d", s, sorts_.size() - 1);
			}
			if (s > 0) {
				sorts_.set(s);
			}
		}
		~IndexesCacheCleaner();

	private:
		const NamespaceImpl &ns_;
		std::bitset<kMaxIndexes> sorts_;
	};

	friend class NsSelecter;
	friend class JoinedSelector;
	friend class WALSelecter;
	friend class NsSelectFuncInterface;
	friend class QueryPreprocessor;
	friend class composite_substitution_helpers::CompositeSearcher;
	friend class SelectIteratorContainer;
	friend class ItemComparator;
	friend class ItemModifier;
	friend class Namespace;
	friend SortExpression;
	friend SortExprFuncs::DistanceBetweenJoinedIndexesSameNs;
	friend class ReindexerImpl;
	friend class RxSelector;
	friend QueryResults;
	friend class ItemsLoader;
	friend class IndexInserters;

	class NSUpdateSortedContext final : public UpdateSortedContext {
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

	private:
		const NamespaceImpl &ns_;
		const int sorted_indexes_{0};
		const IdType curSortId_{-1};
		std::vector<SortType> ids2Sorts_;
		int64_t ids2SortsMemSize_{0};
	};

	class IndexesStorage final : public std::vector<std::unique_ptr<Index>> {
	public:
		using Base = std::vector<std::unique_ptr<Index>>;

		explicit IndexesStorage(const NamespaceImpl &ns);

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

	class Items final : public std::vector<PayloadValue> {
	public:
		bool exists(IdType id) const { return id < IdType(size()) && !at(id).IsFree(); }
	};

public:
	enum OptimizationState : int { NotOptimized, OptimizedPartially, OptimizationCompleted };
	enum class FieldChangeType { Add = 1, Delete = -1 };
	using Ptr = intrusive_ptr<NamespaceImpl>;
	using Mutex = MarkedMutex<shared_timed_mutex, MutexMark::Namespace>;

	NamespaceImpl(const std::string &_name, UpdatesObservers &observers);
	NamespaceImpl &operator=(const NamespaceImpl &) = delete;
	~NamespaceImpl() override;

	std::string GetName(const RdxContext &ctx) const {
		auto rlck = rLock(ctx);
		return name_;
	}
	bool IsSystem(const RdxContext &ctx) const {
		auto rlck = rLock(ctx);
		return isSystem();
	}
	bool IsTemporary(const RdxContext &ctx) const { return GetReplState(ctx).temporary; }

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
	void Upsert(Item &item, const RdxContext &);
	void Delete(Item &item, const RdxContext &);
	void ModifyItem(Item &item, ItemModifyMode mode, const RdxContext &ctx);
	void Truncate(const RdxContext &);
	void Refill(std::vector<Item> &, const RdxContext &);

	template <typename JoinPreResultCtx>
	void Select(QueryResults &result, SelectCtxWithJoinPreSelect<JoinPreResultCtx> &params, const RdxContext &);
	NamespaceDef GetDefinition(const RdxContext &ctx);
	NamespaceMemStat GetMemStat(const RdxContext &);
	NamespacePerfStat GetPerfStat(const RdxContext &);
	void ResetPerfStat(const RdxContext &);
	std::vector<std::string> EnumMeta(const RdxContext &ctx);

	void BackgroundRoutine(RdxActivityContext *);
	void StorageFlushingRoutine();
	void CloseStorage(const RdxContext &);

	Transaction NewTransaction(const RdxContext &ctx);
	void CommitTransaction(Transaction &tx, QueryResults &result, NsContext ctx,
						   QueryStatCalculator<Transaction, long_actions::Logger> &queryStatCalculator);

	Item NewItem(const RdxContext &ctx);
	void ToPool(ItemImpl *item);
	// Get metadata from storage by key
	std::string GetMeta(const std::string &key, const RdxContext &ctx);
	// Put metadata to storage by key
	void PutMeta(const std::string &key, std::string_view data, const RdxContext &ctx);
	// Delete metadata from storage by key
	void DeleteMeta(const std::string &key, const RdxContext &ctx);
	int64_t GetSerial(const std::string &field);

	int getIndexByName(std::string_view index) const;
	int getIndexByNameOrJsonPath(std::string_view name) const;
	int getScalarIndexByName(std::string_view name) const;
	bool tryGetIndexByName(std::string_view name, int &index) const;
	bool getIndexByNameOrJsonPath(std::string_view name, int &index) const;
	bool getScalarIndexByName(std::string_view name, int &index) const;
	bool getSparseIndexByJsonPath(std::string_view jsonPath, int &index) const;

	void FillResult(QueryResults &result, const IdSet &ids) const;

	void EnablePerfCounters(bool enable = true) { enablePerfCounters_ = enable; }

	// Replication slave mode functions
	ReplicationState GetReplState(const RdxContext &) const;
	void SetReplLSNs(LSNPair LSNs, const RdxContext &ctx);

	void SetSlaveReplStatus(ReplicationState::Status, const Error &, const RdxContext &);
	void SetSlaveReplMasterState(MasterState state, const RdxContext &);

	Error ReplaceTagsMatcher(const TagsMatcher &tm, const RdxContext &);

	void OnConfigUpdated(DBConfigProvider &configProvider, const RdxContext &ctx);
	StorageOpts GetStorageOpts(const RdxContext &);
	std::shared_ptr<const Schema> GetSchemaPtr(const RdxContext &ctx) const;
	int getNsNumber() const noexcept { return schema_ ? schema_->GetProtobufNsNumber() : 0; }
	IndexesCacheCleaner GetIndexesCacheCleaner() { return IndexesCacheCleaner{*this}; }
	// Separate method for the v3/v4 replication compatibility.
	// It should not be used outside of this scenario
	void MergeTagsMatcher(const TagsMatcher &tm, const RdxContext &ctx);
	void SetDestroyFlag() noexcept { dbDestroyed_ = true; }
	Error FlushStorage(const RdxContext &ctx) {
		const auto flushOpts = StorageFlushOpts().WithImmediateReopen();
		auto lck = rLock(ctx);
		storage_.Flush(flushOpts);
		return storage_.GetStatusCached().err;
	}

private:
	struct SysRecordsVersions {
		uint64_t idxVersion{0};
		uint64_t tagsVersion{0};
		uint64_t replVersion{0};
		uint64_t schemaVersion{0};
	};

	class Locker {
	public:
		typedef contexted_shared_lock<Mutex, const RdxContext> RLockT;
		typedef contexted_unique_lock<Mutex, const RdxContext> WLockT;

		RLockT RLock(const RdxContext &ctx) const { return RLockT(mtx_, ctx); }
		WLockT WLock(const RdxContext &ctx) const {
			using namespace std::string_view_literals;
			WLockT lck(mtx_, ctx);
			if (readonly_.load(std::memory_order_acquire)) {
				throw Error(errNamespaceInvalidated, "NS invalidated"sv);
			}
			return lck;
		}
		bool IsNotLocked(const RdxContext &ctx) const { return WLockT(mtx_, std::try_to_lock_t{}, ctx).owns_lock(); }
		void MarkReadOnly() { readonly_.store(true, std::memory_order_release); }
		std::atomic_bool &IsReadOnly() { return readonly_; }

	private:
		mutable Mutex mtx_;
		std::atomic_bool readonly_{false};
	};

	struct PKModifyRevertData {
		PKModifyRevertData(PayloadValue &p, lsn_t l) : pv(p), lsn(l) {}
		PayloadValue &pv;
		lsn_t lsn;
	};

	Error rebuildIndexesTagsPaths(const TagsMatcher &newTm);
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
	void loadMetaFromStorage();

	void initWAL(int64_t minLSN, int64_t maxLSN);

	void markUpdated(IndexOptimization requestedOptimization);
	void scheduleIndexOptimization(IndexOptimization requestedOptimization);
	void doUpdate(const Query &query, QueryResults &result, const NsContext &);
	void doDelete(const Query &query, QueryResults &result, const NsContext &);
	void doTruncate(const NsContext &ctx);
	void doUpsert(ItemImpl *ritem, IdType id, bool doUpdate);
	void modifyItem(Item &item, ItemModifyMode mode, const NsContext &);
	void doModifyItem(Item &item, ItemModifyMode mode, const NsContext &ctx);
	void deleteItem(Item &item, const NsContext &ctx);
	void updateTagsMatcherFromItem(ItemImpl *ritem);
	template <NeedRollBack needRollBack, FieldChangeType fieldChangeType>
	[[nodiscard]] RollBack_updateItems<needRollBack> updateItems(const PayloadType &oldPlType, int changedField);
	void fillSparseIndex(Index &, std::string_view jsonPath);
	void doDelete(IdType id);
	void optimizeIndexes(const NsContext &);
	[[nodiscard]] RollBack_insertIndex insertIndex(std::unique_ptr<Index> newIndex, int idxNo, const std::string &realName);
	bool addIndex(const IndexDef &indexDef);
	void addCompositeIndex(const IndexDef &indexDef);
	template <typename PathsT, typename JsonPathsContainerT>
	void createCompositeFieldsSet(const std::string &idxName, const PathsT &paths, FieldsSet &fields);
	void verifyCompositeIndex(const IndexDef &indexDef) const;
	template <typename GetNameF>
	void verifyAddIndex(const IndexDef &indexDef, GetNameF &&) const;
	void verifyUpdateIndex(const IndexDef &indexDef) const;
	void verifyUpdateCompositeIndex(const IndexDef &indexDef) const;
	bool updateIndex(const IndexDef &indexDef);
	void dropIndex(const IndexDef &index);
	void addToWAL(const IndexDef &indexDef, WALRecType type, const RdxContext &ctx);
	void addToWAL(std::string_view json, WALRecType type, const RdxContext &ctx);
	void replicateItem(IdType itemId, const NsContext &ctx, bool statementReplication, uint64_t oldPlHash, size_t oldItemCapacity,
					   std::optional<PKModifyRevertData> &&modifyData);
	void removeExpiredItems(RdxActivityContext *);
	void removeExpiredStrings(RdxActivityContext *);
	void mergeTagsMatcher(const TagsMatcher &tm, const NsContext &ctx);
	Item newItem();

	template <NeedRollBack needRollBack>
	[[nodiscard]] RollBack_recreateCompositeIndexes<needRollBack> recreateCompositeIndexes(size_t startIdx, size_t endIdx);
	NamespaceDef getDefinition() const;
	IndexDef getIndexDefinition(const std::string &indexName) const;
	IndexDef getIndexDefinition(size_t) const;

	std::string getMeta(const std::string &key) const;
	void putMeta(const std::string &key, std::string_view data, const RdxContext &ctx);
	void deleteMeta(const std::string &key, const RdxContext &ctx);

	std::pair<IdType, bool> findByPK(ItemImpl *ritem, bool inTransaction, const RdxContext &);

	RX_ALWAYS_INLINE SelectKeyResult getPkDocs(const ConstPayload &cpl, bool inTransaction, const RdxContext &ctx);
	RX_ALWAYS_INLINE VariantArray getPkKeys(const ConstPayload &cpl, Index *pkIndex, int fieldNum);
	void checkUniquePK(const ConstPayload &cpl, bool inTransaction, const RdxContext &ctx);

	int getSortedIdxCount() const noexcept;
	void updateSortedIdxCount();
	void setFieldsBasedOnPrecepts(ItemImpl *ritem);

	void putToJoinCache(JoinCacheRes &res, std::shared_ptr<const JoinPreResult> preResult) const;
	void putToJoinCache(JoinCacheRes &res, JoinCacheVal &&val) const;
	void getFromJoinCache(const Query &, const JoinedQuery &, JoinCacheRes &out) const;
	void getFromJoinCache(const Query &, JoinCacheRes &out) const;
	void getFromJoinCacheImpl(JoinCacheRes &out) const;
	void getInsideFromJoinCache(JoinCacheRes &ctx) const;
	int64_t lastUpdateTimeNano() const noexcept { return repl_.updatedUnixNano; }

	const FieldsSet &pkFields();

	std::vector<std::string> enumMeta() const;

	void warmupFtIndexes();
	void updateSelectTime() noexcept {
		using namespace std::chrono;
		lastSelectTime_ = duration_cast<seconds>(system_clock_w::now().time_since_epoch()).count();
	}
	bool hadSelects() const noexcept { return lastSelectTime_.load(std::memory_order_relaxed) != 0; }
	void markReadOnly() { locker_.MarkReadOnly(); }
	Locker::WLockT wLock(const RdxContext &ctx) const { return locker_.WLock(ctx); }
	bool isNotLocked(const RdxContext &ctx) const { return locker_.IsNotLocked(ctx); }
	Locker::RLockT rLock(const RdxContext &ctx) const { return locker_.RLock(ctx); }

	bool SortOrdersBuilt() const noexcept { return optimizationState_.load(std::memory_order_acquire) == OptimizationCompleted; }

	int64_t correctMaxIterationsIdSetPreResult(int64_t maxIterationsIdSetPreResult) const;

	IndexesStorage indexes_;
	fast_hash_map<std::string, int, nocase_hash_str, nocase_equal_str, nocase_less_str> indexesNames_;
	fast_hash_map<int, std::vector<int>> indexesToComposites_;	// Maps index fields to corresponding composite indexes
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
	std::atomic<unsigned> replStateUpdates_{0};

	std::unordered_map<std::string, std::string> meta_;

	int sparseIndexesCount_{0};
	VariantArray krefs, skrefs;

	SysRecordsVersions sysRecordsVersions_;

	Locker locker_;
	std::shared_ptr<Schema> schema_;

	StringsHolderPtr strHolder() const noexcept { return strHolder_; }
	size_t itemsCount() const noexcept { return items_.size() - free_.size(); }
	const NamespaceConfigData &config() const noexcept { return config_; }

	void DumpIndex(std::ostream &os, std::string_view index, const RdxContext &ctx) const;

	NamespaceImpl(const NamespaceImpl &src, AsyncStorage::FullLockT &storageLock);

	bool isSystem() const { return isSystemNamespaceNameFast(name_); }
	IdType createItem(size_t realSize);
	void checkApplySlaveUpdate(bool v);

	void processWalRecord(const WALRecord &wrec, const RdxContext &ctx, lsn_t itemLsn = lsn_t(), Item *item = nullptr);

	void setReplLSNs(LSNPair LSNs);
	void setTemporary() { repl_.temporary = true; }
	void setSlaveMode(const RdxContext &ctx);

	void removeIndex(std::unique_ptr<Index> &);
	void dumpIndex(std::ostream &os, std::string_view index) const;
	void tryForceFlush(Locker::WLockT &&wlck) {
		if (wlck.owns_lock()) {
			wlck.unlock();
			storage_.TryForceFlush();
		}
	}
	size_t getWalSize(const NamespaceConfigData &cfg) const noexcept { return isSystem() ? int64_t(1) : std::max(cfg.walSize, int64_t(1)); }
	void clearNamespaceCaches();
	std::vector<TagsPath> pickJsonPath(const PayloadFieldType &fld);

	PerfStatCounterMT updatePerfCounter_, selectPerfCounter_;
	std::atomic_bool enablePerfCounters_{false};

	NamespaceConfigData config_;
	std::unique_ptr<QueryCountCache> queryCountCache_;
	std::unique_ptr<JoinCache> joinCache_;
	// Replication variables
	WALTracker wal_;
	ReplicationState repl_;
	UpdatesObservers *observers_;

	StorageOpts storageOpts_;
	std::atomic_int64_t lastSelectTime_{0};

	sync_pool<ItemImpl, 1024> pool_;
	std::atomic_int32_t cancelCommitCnt_{0};
	std::atomic_int64_t lastUpdateTime_{0};

	std::atomic_uint32_t itemsCount_{0};
	std::atomic_uint32_t itemsCapacity_{0};
	bool nsIsLoading_{false};

	int serverId_{0};
	std::atomic_bool serverIdChanged_{false};
	size_t itemsDataSize_{0};

	std::atomic_int optimizationState_{OptimizationState::NotOptimized};
	StringsHolderPtr strHolder_;
	std::deque<StringsHolderPtr> strHoldersWaitingToBeDeleted_;
	std::chrono::seconds lastExpirationCheckTs_{0};
	mutable std::atomic_int64_t nsUpdateSortedContextMemory_{0};
	std::atomic_bool dbDestroyed_{false};
};
}  // namespace reindexer
