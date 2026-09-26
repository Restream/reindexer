#pragma once

#include <deque>
#include <memory>
#include <optional>
#include <vector>
#include "ann_storage_cache_helper.h"
#include "asyncstorage.h"
#include "cluster/idatareplicator.h"
#include "core/cjson/tagsmatcher.h"
#include "core/dbconfig.h"
#include "core/definitions/namespacedef.h"
#include "core/item.h"
#include "core/nsselecter/joins/cache.h"
#include "core/payload/payloadiface.h"
#include "core/perfstatcounter.h"
#include "core/querycache.h"
#include "core/queryresults/fields_filter.h"
#include "core/schema.h"
#include "core/selectkeyresult.h"
#include "core/storage/storagetype.h"
#include "core/transaction/localtransaction.h"
#include "estl/contexted_locks.h"
#include "estl/fast_hash_map.h"
#include "estl/shared_mutex.h"
#include "estl/syncpool.h"
#include "events/observer.h"
#include "float_vectors_indexes.h"
#include "index_optimizer.h"
#include "indexes/registry.h"
#include "namespacename.h"
#include "stringsholder.h"
#include "wal/waltracker.h"

namespace reindexer {

class ConstQueryImpl;

using reindexer::datastorage::StorageType;

class Index;
class EmbeddersCache;
class DBConfigProvider;
class QueryPreprocessor;
class RdxContext;
class RdxActivityContext;
class SortExpression;
class FloatVectorIndex;
class TransactionContext;
class ProxiedSortExpression;
class LocalQueryResults;
class Snapshot;
class SnapshotChunk;
struct SnapshotOpts;
struct ExprField;
class ExpressionEvaluator;
class BuiltinFunction;

namespace joins {
class ItemsProcessor;
struct PreSelect;

template <typename T>
class SelectStrategy;
}  // namespace joins

namespace long_actions {
template <typename T>
struct Logger;
}  // namespace long_actions

template <typename T, template <typename> class>
class QueryStatCalculator;

namespace SortExprFuncs {
struct DistanceBetweenJoinedIndexesSameNs;
}  // namespace SortExprFuncs

namespace migrations {
class PKMigrationService;
}

namespace functions {
class PrecomputedValues;
}

namespace ns_indexes {
class TransactionDDL;
}

class [[nodiscard]] NsContext {
public:
	explicit NsContext(const RdxContext& rdxCtx) noexcept : rdxContext(rdxCtx) {}
	NsContext& InTransaction(lsn_t stepLsn, TransactionContext* _txCtx) noexcept {
		inTransaction_ = true;
		originLsn_ = stepLsn;
		txCtx = _txCtx;
		if (isInitialLeaderSync) {
			assertrx_dbg(!originLsn_.isEmpty());
		} else {
			assertrx_dbg(originLsn_.isEmpty() == rdxContext.GetOriginLSN().isEmpty());
		}
		return *this;
	}
	NsContext& DeferMarkUpdated(std::optional<IndexOptimization>& slot) noexcept {
		deferredMarkUpdated_ = &slot;
		return *this;
	}
	NsContext& InSnapshot(lsn_t stepLsn, bool wal, bool requireResync, bool initialLeaderSync) noexcept {
		inSnapshot_ = true;
		isWal_ = wal;
		originLsn_ = stepLsn;
		isRequireResync = requireResync;
		isInitialLeaderSync = initialLeaderSync;
		return *this;
	}
	lsn_t GetOriginLSN() const noexcept { return (inTransaction_ || inSnapshot_) ? originLsn_ : rdxContext.GetOriginLSN(); }
	bool HasEmitterServer() const noexcept { return rdxContext.HasEmitterServer(); }
	int EmitterServerId() const noexcept { return rdxContext.EmitterServerId(); }
	bool IsForceSyncItem() const noexcept { return inSnapshot_ && !isWal_; }
	bool IsWalSyncItem() const noexcept { return inSnapshot_ && isWal_; }
	bool IsInSnapshot() const noexcept { return inSnapshot_; }
	bool IsInTransaction() const noexcept { return inTransaction_; }
	std::optional<IndexOptimization>* DeferredMarkUpdated() const noexcept { return deferredMarkUpdated_; }

	const RdxContext& rdxContext;
	bool isCopiedNsRequest{false};
	bool isRequireResync{false};
	bool isInitialLeaderSync{false};
	TransactionContext* txCtx{nullptr};

private:
	bool inTransaction_{false};
	bool inSnapshot_{false};
	bool isWal_{false};
	lsn_t originLsn_;
	// Points at CommitTransaction's stack slot; valid only for that call.
	std::optional<IndexOptimization>* deferredMarkUpdated_{nullptr};
};

namespace composite_substitution_helpers {
class CompositeSearcher;
}

enum class [[nodiscard]] StoredValuesOptimizationStatus : int8_t { DisabledByCompositeIndex, DisabledByJoinedFieldSort, Enabled };

class [[nodiscard]] NamespaceImpl final : public intrusive_atomic_rc_base {	 // NOLINT(*performance.Padding) Padding does not
	// matter for this class
	class [[nodiscard]] IndexesCacheCleaner {
	public:
		explicit IndexesCacheCleaner(const NamespaceImpl& ns) noexcept : ns_{ns} {}
		IndexesCacheCleaner(const IndexesCacheCleaner&) = delete;
		IndexesCacheCleaner(IndexesCacheCleaner&&) = delete;
		IndexesCacheCleaner& operator=(const IndexesCacheCleaner&) = delete;
		IndexesCacheCleaner& operator=(IndexesCacheCleaner&&) = delete;
		void Add(Index& idx) noexcept;
		~IndexesCacheCleaner();

	private:
		const NamespaceImpl& ns_;
		bool requiresCleanup_{false};
	};

	template <typename T>
	friend class joins::SelectStrategy;
	friend class NsSelecter;
	friend class joins::ItemsProcessor;
	friend class WALSelecter;
	friend class NsFtFuncInterface;
	friend class QueryPreprocessor;
	friend class composite_substitution_helpers::CompositeSearcher;
	friend class SelectIteratorContainer;
	friend class ItemComparator;
	friend class ItemModifier;
	friend class Namespace;
	friend class SortExpression;
	friend class ProxiedSortExpression;
	friend struct SortExprFuncs::DistanceBetweenJoinedIndexesSameNs;
	friend class ReindexerImpl;
	friend class RxSelector;
	friend class LocalQueryResults;
	friend class SnapshotHandler;
	friend class FieldComparator;
	friend class QueryResults;
	friend class ItemsLoader;
	friend class IndexInserters;
	friend class TransactionContext;
	friend class TransactionConcurrentInserter;
	friend class ann_storage_cache::Writer;
	friend class FloatVectorsHolderMap;
	friend class FieldsFilter;
	friend struct ExprField;
	friend class ExpressionEvaluator;
	friend class BuiltinFunction;
	friend class FloatVectorsGetter;
	friend class functions::Serial;
	friend class migrations::PKMigrationService;
	friend class ns_indexes::TransactionDDL;

	using IndexesStorage = ns_indexes::IndexesStorage;

	class [[nodiscard]] Items final : public std::vector<PayloadValue> {
		using Base = std::vector<PayloadValue>;

	public:
		bool exists(IdType id) const noexcept { return id.ToNumber() < ptrdiff_t(size()) && !(*this)[id].IsFree(); }
		PayloadValue& operator[](IdType id) & noexcept { return Base::operator[](id.ToNumber()); }
		const PayloadValue& operator[](IdType id) const& noexcept { return Base::operator[](id.ToNumber()); }
		auto operator[](IdType id) const&& = delete;
	};

public:
	enum class [[nodiscard]] InvalidationType : int { Valid, Readonly, OverwrittenByUser, OverwrittenByReplicator };

	using Ptr = intrusive_ptr<NamespaceImpl>;
	using Mutex = MarkedMutex<shared_timed_mutex, MutexMark::Namespace>;

	class [[nodiscard]] Locker {
	public:
		class [[nodiscard]] NsWLock {
		public:
			using MutexType = Mutex;

			NsWLock() = default;
			NsWLock(MutexType& mtx, std::try_to_lock_t t, const RdxContext& ctx, bool isCL) : impl_(mtx, t, ctx), isClusterLck_(isCL) {}
			NsWLock(MutexType& mtx, const RdxContext& ctx, bool isCL) : impl_(mtx, ctx), isClusterLck_(isCL) {}
			NsWLock(const NsWLock&) = delete;
			NsWLock(NsWLock&&) = default;
			NsWLock& operator=(const NsWLock&) = delete;
			NsWLock& operator=(NsWLock&&) = default;
			~NsWLock() = default;
			void lock() { impl_.lock(); }
			void unlock() { impl_.unlock(); }
			bool owns_lock() const { return impl_.owns_lock(); }
			bool isClusterLck() const noexcept { return isClusterLck_; }

		private:
			contexted_unique_lock<MutexType, const RdxContext> impl_;
			bool isClusterLck_ = false;
		};
		typedef contexted_shared_lock<Mutex, const RdxContext> RLockT;
		typedef NsWLock WLockT;

		Locker(const cluster::IDataSyncer& syncer, const NamespaceImpl& owner) noexcept : syncer_(syncer), owner_(owner) {}

		RLockT RLock(const RdxContext& ctx) const {
			assertrx_dbg(ctx.GetOriginLSN().isEmpty() || ctx.IsCancelable());
			return RLockT(mtx_, ctx);
		}
		RLockT TryRLock(const RdxContext& ctx) const {
			assertrx_dbg(ctx.GetOriginLSN().isEmpty() || ctx.IsCancelable());
			return RLockT(mtx_, std::try_to_lock_t{}, ctx);
		}
		WLockT DataWLock(const RdxContext& ctx, bool skipClusterStatusCheck) const {
			assertrx_dbg(ctx.GetOriginLSN().isEmpty() || ctx.IsCancelable());

			WLockT lck(mtx_, ctx, true);
			checkInvalidation();
			const bool requireSync = !ctx.NoWaitSync() && ctx.GetOriginLSN().isEmpty() && !owner_.isSystem();
			const bool isFollowerNS = owner_.repl_.clusterStatus.role == ClusterOperationStatus::Role::SimpleReplica ||
									  owner_.repl_.clusterStatus.role == ClusterOperationStatus::Role::ClusterReplica;

			if (!ctx.GetOriginLSN().isEmpty() && !owner_.repl_.token.empty() && !owner_.isTemporary() &&
				owner_.repl_.token != ctx.LeaderReplicationToken()) {
				throw Error(errReplParams,
							"Different replication tokens on leader and follower for namespace '{}'. Expected '{}', but got '{}'",
							owner_.name_, owner_.repl_.token, ctx.LeaderReplicationToken());
			}

			bool synchronized = isFollowerNS || !requireSync || syncer_.IsInitialSyncDone(owner_.name_);
			while (!synchronized) {
				// This is required in case of rename during sync wait
				auto name = owner_.name_;

				lck.unlock();
				syncer_.AwaitInitialSync(name, ctx);
				lck.lock();
				checkInvalidation();
				synchronized = syncer_.IsInitialSyncDone(owner_.name_);
			}

			if (!skipClusterStatusCheck) {
				owner_.checkClusterStatus(ctx);	 // throw exception if false
			}

			return lck;
		}
		WLockT SimpleWLock(const RdxContext& ctx) const {
			assertrx_dbg(ctx.GetOriginLSN().isEmpty() || ctx.IsCancelable());

			WLockT lck(mtx_, ctx, false);
			checkInvalidation();
			return lck;
		}
		bool IsNotLocked(const RdxContext& ctx) const { return WLockT(mtx_, std::try_to_lock_t{}, ctx, false).owns_lock(); }
		bool IsMutexCorrect(const Mutex* mtx) const noexcept { return mtx == &mtx_; }
		void MarkReadOnly() noexcept { invalidation_.store(int(InvalidationType::Readonly), std::memory_order_release); }
		void MarkOverwrittenByUser() noexcept { invalidation_.store(int(InvalidationType::OverwrittenByUser), std::memory_order_release); }
		void MarkOverwrittenByForceSync() noexcept {
			invalidation_.store(int(InvalidationType::OverwrittenByReplicator), std::memory_order_release);
		}
		const std::atomic<int>& InvalidationType() const noexcept { return invalidation_; }
		bool IsValid() const noexcept {
			return NamespaceImpl::InvalidationType(invalidation_.load(std::memory_order_acquire)) == InvalidationType::Valid;
		}
		const cluster::IDataSyncer& Syncer() const noexcept { return syncer_; }
		bool IsInvalidated() const noexcept {
			return NamespaceImpl::InvalidationType(invalidation_.load(std::memory_order_acquire)) != InvalidationType::Valid;
		}

	private:
		void checkInvalidation() const {
			using namespace std::string_view_literals;
			switch (NamespaceImpl::InvalidationType(invalidation_.load(std::memory_order_acquire))) {
				case InvalidationType::Readonly:
					throw Error(errNamespaceInvalidated, "NS invalidated"sv);
				case InvalidationType::OverwrittenByUser:
					throw Error(errNamespaceOverwritten, "NS was overwritten via rename"sv);
				case InvalidationType::OverwrittenByReplicator:
					throw Error(errWrongReplicationData, "NS was overwritten via rename (force sync)"sv);
				case InvalidationType::Valid:
				default:
					break;
			}
		}

		mutable Mutex mtx_;
		std::atomic<int> invalidation_ = {int(InvalidationType::Valid)};
		const cluster::IDataSyncer& syncer_;
		const NamespaceImpl& owner_;
	};

	NamespaceImpl(const std::string& name, std::optional<int32_t> stateToken, const cluster::IDataSyncer& syncer,
				  UpdatesObservers& observers, const std::shared_ptr<EmbeddersCache>& embeddersCache);
	NamespaceImpl& operator=(const NamespaceImpl&) = delete;
	~NamespaceImpl() override;

	NamespaceName GetName(const RdxContext& ctx) const {
		auto rlck = rLock(ctx);
		return name_;
	}
	bool IsSystem(const RdxContext& ctx) const {
		auto rlck = rLock(ctx);
		return isSystem();
	}
	bool IsTemporary(const RdxContext& ctx) const { return isTmpNamespaceName(GetName(ctx)); }
	void SetNsVersion(lsn_t version, const RdxContext& ctx);

	void EnableStorage(const std::string& path, StorageOpts opts, StorageType storageType, const RdxContext& ctx);
	void LoadFromStorage(unsigned threadsCount, const RdxContext& ctx);
	void DeleteStorage(const RdxContext&);

	uint32_t GetItemsCount() const { return itemsCount_.load(std::memory_order_relaxed); }
	uint32_t GetItemsCapacity() const { return itemsCapacity_.load(std::memory_order_relaxed); }
	void AddIndex(const IndexDef& indexDef, const RdxContext& ctx);
	void UpdateIndex(const IndexDef& indexDef, const RdxContext& ctx);
	void DropIndex(const IndexDef& indexDef, const RdxContext& ctx);
	void SetSchema(std::string_view schema, const RdxContext& ctx);
	std::string GetSchema(int format, const RdxContext& ctx);

	void Insert(Item& item, const RdxContext&);
	void Update(Item& item, const RdxContext&);
	void Upsert(Item& item, const RdxContext&);
	void Delete(Item& item, const RdxContext&);
	void ModifyItem(Item& item, ItemModifyMode mode, const RdxContext&);
	void Truncate(const RdxContext&);
	void Refill(std::vector<Item>&, const RdxContext&);

	template <typename SelectCtxT>
	void Select(LocalQueryResults& result, SelectCtxT& params, const RdxContext&);
	NamespaceDef GetDefinition(const RdxContext& ctx);
	NamespaceMemStat GetMemStat(const RdxContext&);
	NamespacePerfStat GetPerfStat(const RdxContext&);
	void ResetPerfStat(const RdxContext&);
	std::vector<std::string> EnumMeta(const RdxContext& ctx);

	void BackgroundRoutine(RdxActivityContext*);
	void StorageFlushingRoutine();
	void ANNCachingRoutine();
	void UpdateANNStorageCache(bool skipTimeCheck, const RdxContext& ctx);
	void DropANNStorageCache(std::string_view index, const RdxContext& ctx);
	void RebuildIVFIndex(std::string_view index, float dataPart, const RdxContext& ctx);
	void CloseStorage(const RdxContext&);

	LocalTransaction NewTransaction(const RdxContext& ctx);
	void CommitTransaction(LocalTransaction& tx, LocalQueryResults& result, const NsContext& ctx,
						   QueryStatCalculator<LocalTransaction, long_actions::Logger>& queryStatCalculator);

	Item NewItem(const RdxContext& ctx);
	void ToPool(ItemImpl* item);
	std::string GetMeta(const std::string& key, const RdxContext& ctx);
	void PutMeta(const std::string& key, std::string_view data, const RdxContext& ctx);
	void DeleteMeta(const std::string& key, const RdxContext& ctx);
	int64_t GetSerial(std::string_view field, UpdatesContainer& replUpdates, const NsContext& ctx);

	PayloadType GetPayloadType(const RdxContext& ctx) const;

	void FillResult(LocalQueryResults& result, const IdSetPlain& ids) const;

	ReplicationState GetReplState(const RdxContext&) const;
	ReplicationStateV2 GetReplStateV2(const RdxContext&) const;

	void OnConfigUpdated(const DBConfigProvider& configProvider, const RdxContext& ctx);
	std::shared_ptr<const Schema> GetSchemaPtr(const RdxContext& ctx) const;
	IndexesCacheCleaner GetIndexesCacheCleaner() { return IndexesCacheCleaner{*this}; }
	void SetClusterOperationStatus(ClusterOperationStatus&& status, const RdxContext& ctx);
	void ApplySnapshotChunk(const SnapshotChunk& ch, bool isInitialLeaderSync, const RdxContext& ctx);
	void GetSnapshot(Snapshot& snapshot, const SnapshotOpts& opts, const RdxContext& ctx);
	void SetTagsMatcher(TagsMatcher&& tm, const RdxContext& ctx);
	void SetDestroyFlag() noexcept { dbDestroyed_ = true; }
	Error FlushStorage(const RdxContext& ctx) {
		const auto flushOpts = StorageFlushOpts().WithImmediateReopen();
		auto lck = rLock(ctx);
		storage_.Flush(flushOpts);
		return storage_.GetStatusCached().err;
	}
	void RebuildFreeItemsStorage(const RdxContext& ctx);
	std::shared_ptr<const reindexer::QueryEmbedder> QueryEmbedder(std::string_view fieldName, const RdxContext& ctx) const;

private:
	void loadHashMapStats() noexcept;

	struct [[nodiscard]] SysRecordsVersions {
		uint64_t idxVersion{0};
		uint64_t tagsVersion{0};
		uint64_t replVersion{0};
		uint64_t schemaVersion{0};
	};

	struct [[nodiscard]] PKModifyRevertData {
		PKModifyRevertData(PayloadValue& p, lsn_t l) : pv(p), lsn(l) {}
		PayloadValue& pv;
		lsn_t lsn;
	};

	friend struct IndexFastUpdate;

	int getIndexByName(std::string_view index) const;
	bool tryGetIndexByName(std::string_view name, int& index) const noexcept;
	bool tryGetScalarIndexByName(std::string_view name, int& index) const noexcept;
	// Those functions do not return indexes with multiple jsonpaths, when searching by jsonpath
	bool tryGetIndexByNameOrJsonPath(std::string_view name, int& index, EnableMultiJsonPath multi = EnableMultiJsonPath_False) const;
	bool tryGetIndexByJsonPath(std::string_view jsonPath, int& index, EnableMultiJsonPath multi = EnableMultiJsonPath_False) const noexcept;
	FloatVectorsIndexes getVectorIndexes() const;
	FloatVectorsIndexes getVectorIndexes(const PayloadType& pt) const;
	bool haveFloatVectorsIndexes() const noexcept { return indexRegistry_.HasFloatVectorIndexes(); }
	ReplicationState getReplState() const;
	std::string sysRecordName(std::string_view sysTag, uint64_t version);
	void writeSysRecToStorage(std::string_view data, std::string_view sysTag, uint64_t& version, bool direct);
	void saveIndexesToStorage();
	void saveSchemaToStorage();
	Error loadLatestSysRecord(std::string_view baseSysTag, uint64_t& version, std::string& content);
	bool loadIndexesFromStorage();
	void saveReplStateToStorage(bool direct = true);
	void saveTagsMatcherToStorage(bool clearUpdate);
	void loadReplStateFromStorage();
	void loadMetaFromStorage();

	void initWAL(int64_t minLSN, int64_t maxLSN);

	void markUpdated(IndexOptimization requestedOptimization);
	// Defers the call, if the context asks for it (tx commit collapses all of its calls into a single one)
	void markUpdated(IndexOptimization requestedOptimization, const NsContext& ctx);
	Item newItem();
	void doUpdate(LocalQueryResults& result, UpdatesContainer& pendedRepl, ConstQueryImpl query, const NsContext& ctx,
				  const functions::PrecomputedValues&);
	void doUpdateTr(LocalQueryResults& result, UpdatesContainer& pendedRepl, ConstQueryImpl query, const NsContext& ctx,
					const functions::PrecomputedValues&);
	void doDelete(LocalQueryResults& result, UpdatesContainer& pendedRepl, ConstQueryImpl query, const NsContext& ctx,
				  const functions::PrecomputedValues&);
	void doDeleteTr(LocalQueryResults& result, UpdatesContainer& pendedRepl, ConstQueryImpl query, const NsContext& ctx,
					const functions::PrecomputedValues&);
	void doUpsert(ItemImpl& item, IdType id, bool doUpdate, TransactionContext* txCtx);
	void modifyItem(Item& item, ItemModifyMode mode, UpdatesContainer& pendedRepl, const NsContext& ctx);
	void deleteItem(Item& item, UpdatesContainer& pendedRepl, const NsContext& ctx);
	void doModifyItem(Item& item, ItemModifyMode mode, UpdatesContainer& pendedRepl, const NsContext& ctx,
					  IdType suggestedId = IdType::NotSet());
	void updateTagsMatcherFromItem(ItemImpl* ritem, const NsContext& ctx);
	Error tryWriteItemIntoStorage(const FieldsSet& pkFields, ItemImpl& item, IdType rowId, WrSerializer& pk, WrSerializer& data) noexcept;
	void doDelete(IdType id, const NsContext& ctx);
	void doTruncate(UpdatesContainer& pendedRepl, const NsContext& ctx);
	void optimizeIndexes(const NsContext&);
	void tryCleanFulltextIndexes(bool skipTimeCheck, const index::ICancelable& cancelable);
	void addIndex(const IndexDef& indexDef, bool disableTmVersionInc, bool skipEqualityCheck = false);
	void doAddIndex(const IndexDef& indexDef, bool skipEqualityCheck, UpdatesContainer& pendedRepl, const NsContext& ctx);
	bool checkIfSameIndexExists(const IndexDef& indexDef, bool* requireTtlUpdate) const;
	void verifyCompositeIndex(const IndexDef& indexDef) const;
	void verifyEmbeddingFields(const h_vector<std::string, 1>& fields, std::string_view fieldName, std::string_view action) const;
	void verifyUpsertEmbedder(std::string_view action, const IndexDef& indexDef) const;
	void verifyUpsertIndex(std::string_view action, const IndexDef& indexDef) const;
	// tm takes the JSON-paths, which the verification has to register. It is never the namespace tags matcher itself:
	// registering a tag is irreversible, so a failed verification must not leave its tags behind
	void verifyUpdateIndex(const IndexDef& indexDef, TagsMatcher& tm) const;
	void verifyUpdateQuantizationConfigHNSWIndex(const Index* curIndex, const IndexDef& newIndexDef) const;
	void verifyUpsertQuantizationConfigHNSWIndex(std::string_view action, const IndexDef& indexDef) const;
	// Returns the position of the index being dropped
	int verifyDropIndex(const IndexDef&) const;
	bool isPkAffectingIndexChange(const IndexDef& indexDef) const noexcept;
	// Must run first, before any other validation or side effect, for every PK-affecting Add/Update/Drop - throws if a
	// previous PK migration is still incomplete (see migrations::PKMigrationService::HasIncompleteMigration()), since
	// the storage may still hold items under a stale PK-derived key, and stacking another PK change on top isn't safe
	void verifyPkMigrationNotPending(const IndexDef& indexDef);
	bool doUpdateIndex(const IndexDef& indexDef, UpdatesContainer& pendedRepl, const NsContext& ctx);
	void doDropIndex(const IndexDef& index, UpdatesContainer& pendedRepl, const NsContext& ctx);
	void addToWAL(const IndexDef& indexDef, WALRecType type, const NsContext& ctx);
	void addToWAL(std::string_view json, WALRecType type, const NsContext& ctx);
	void removeExpiredItems(RdxActivityContext*);
	void removeExpiredStrings(RdxActivityContext*);
	void optimizeFloatVectorKeeper(RdxActivityContext*);
	void backgroundHNSWIndexesQuantization(RdxActivityContext*);
	void setSchema(std::string_view schema, UpdatesContainer& pendedRepl, const NsContext& ctx);
	void setTagsMatcher(TagsMatcher&& tm, UpdatesContainer& pendedRepl, const NsContext& ctx);
	void replicateItem(IdType itemId, const NsContext& ctx, bool statementReplication, uint64_t oldPlHash, size_t oldItemCapacity,
					   int oldTmVersion, std::optional<PKModifyRevertData>&& modifyData, UpdatesContainer& pendedRepl,
					   const FieldsFilter& pkFilter);

	NamespaceDef getDefinition() const;
	IndexDef getIndexDefinition(const std::string& indexName) const;
	IndexDef getIndexDefinition(size_t i) const;

	std::string getMeta(const std::string& key) const;
	void putMeta(const std::string& key, std::string_view data, UpdatesContainer& pendedRepl, const NsContext& ctx);
	void deleteMeta(const std::string& key, UpdatesContainer& pendedRepl, const NsContext& ctx);

	std::pair<IdType, bool> findByPK(ItemImpl* ritem, bool inTransaction, const RdxContext& ctx);

	std::pair<Index*, int> getPkIdx() const noexcept;
	RX_ALWAYS_INLINE SelectKeyResult getPkDocs(const ConstPayload& cpl, bool inTransaction, const RdxContext& ctx);
	RX_ALWAYS_INLINE VariantArray getPkKeys(const ConstPayload& cpl, Index* pkIndex, int fieldNum);
	[[noreturn]] void throwDuplicatePK(const ConstPayload& cpl, IdType itemId, IdType conflictingItemId);
	[[noreturn]] void throwCannotModifyNsWithoutPK() const;

	void setFieldsBasedOnPrecepts(ItemImpl* ritem, UpdatesContainer& replUpdates, const NsContext& ctx);

	void putToJoinCache(joins::CacheRes& res, std::shared_ptr<const joins::PreSelect> preSelect) const;
	void putToJoinCache(joins::CacheRes& res, joins::CacheVal&& val) const;
	void getFromJoinCache(ConstQueryImpl, const JoinedQuery&, joins::CacheRes& out) const;
	void getFromJoinCache(ConstQueryImpl, joins::CacheRes& out) const;
	void getFromJoinCacheImpl(joins::CacheRes& out) const;
	void getInsideFromJoinCache(joins::CacheRes& ctx) const;
	int64_t lastUpdateTimeNano() const noexcept { return repl_.updatedUnixNano; }
	void getFloatVectorView(VariantArray& result, const Index& index, IdType rowId, size_t elementsCount, bool isMultithreadTxInsertion,
							const TransactionContext* txCtx, int field);

	const FieldsSet* pkFields() const noexcept;

	std::vector<std::string> enumMeta() const;

	void warmupFtIndexes();
	void updateSelectTime() noexcept {
		using namespace std::chrono;
		lastSelectTime_ = duration_cast<seconds>(system_clock_w::now().time_since_epoch()).count();
	}
	bool hadSelects() const noexcept { return lastSelectTime_.load(std::memory_order_relaxed) != 0; }
	void markReadOnly() noexcept { locker_.MarkReadOnly(); }
	void markOverwrittenByUser() noexcept { locker_.MarkOverwrittenByUser(); }
	void markOverwrittenByForceSync() noexcept { locker_.MarkOverwrittenByForceSync(); }
	Locker::WLockT simpleWLock(const RdxContext& ctx) const { return locker_.SimpleWLock(ctx); }
	Locker::WLockT dataWLock(const RdxContext& ctx, bool skipClusterStatusCheck = false) const {
		return locker_.DataWLock(ctx, skipClusterStatusCheck);
	}
	bool isNotLocked(const RdxContext& ctx) const { return locker_.IsNotLocked(ctx); }
	Locker::RLockT rLock(const RdxContext& ctx) const { return locker_.RLock(ctx); }
	Locker::RLockT tryRLock(const RdxContext& ctx) const { return locker_.TryRLock(ctx); }
	void checkClusterRole(const RdxContext& ctx) const { checkClusterRole(ctx.GetOriginLSN()); }
	void checkClusterRole(lsn_t originLsn) const;
	void checkClusterStatus(const RdxContext& ctx) const { checkClusterStatus(ctx.GetOriginLSN()); }
	void checkClusterStatus(lsn_t lsn) const;
	void checkSnapshotLSN(lsn_t lsn);
	void replicateTmUpdateIfRequired(UpdatesContainer& pendedRepl, int oldTmVersion, const NsContext& ctx) noexcept;

	bool SortOrdersBuilt() const noexcept { return indexOptimizer_.IsOptimizationCompleted(); }

	uint64_t calculateItemChecksum(IdType rowId, int removedIdxId = -1) const noexcept;

	// All items with data
	Items items_;
	std::vector<IdType> free_;
	NamespaceName name_;

	// Owns the indexes, their names, the payload type, the tags matcher and the derived layout data. All of them
	// describe the same fields layout, so only ns_indexes::Transaction may change it
	ns_indexes::Registry indexRegistry_;

	AsyncStorage storage_;
	std::atomic<unsigned> replStateUpdates_{0};

	std::unordered_map<std::string, std::string> meta_;

	VariantArray krefs, skrefs;

	SysRecordsVersions sysRecordsVersions_;

	Locker locker_;
	std::shared_ptr<Schema> schema_;

	const IndexesStorage& indexes() const& noexcept { return indexRegistry_.Indexes(); }
	const ns_indexes::Registry::CompositesMap& indexesToComposites() const& noexcept { return indexRegistry_.Composites(); }
	const PayloadType& payloadType() const& noexcept { return indexRegistry_.GetPayloadType(); }
	const TagsMatcher& tagsMatcher() const& noexcept { return indexRegistry_.GetTagsMatcher(); }
	auto indexes() const&& = delete;
	auto indexesToComposites() const&& = delete;
	auto payloadType() const&& = delete;
	auto tagsMatcher() const&& = delete;

	StringsHolderPtr strHolder() const noexcept { return strHolder_; }
	size_t itemsCount() const noexcept { return items_.size() - free_.size(); }
	const NamespaceConfigData& config() const noexcept { return config_; }

	void DumpIndex(std::ostream& os, std::string_view index, const RdxContext& ctx) const;

	bool IsFulltextOrVector(std::string_view indexName, const RdxContext&) const;

	NamespaceImpl(const NamespaceImpl& src, size_t newCapacity, AsyncStorage::FullLock& storageLock);

	bool isSystem() const noexcept { return isSystemNamespaceNameFast(name_); }
	bool isTemporary() const noexcept { return isTmpNamespaceName(name_); }
	IdType createItem(size_t realSize, IdType suggestedId, const NsContext& ctx);

	void processWalRecord(WALRecord&& wrec, const NsContext& ctx, lsn_t itemLsn = lsn_t(), Item* item = nullptr);
	void replicateAsync(updates::UpdateRecord&& rec, const RdxContext& ctx);
	void replicateAsync(UpdatesContainer&& recs, const RdxContext& ctx);
	template <typename QueryStatsCalculatorT>
	void replicate(UpdatesContainer&& recs, NamespaceImpl::Locker::WLockT&& wlck, bool tryForceFlush,
				   QueryStatsCalculatorT&& statCalculator, const NsContext& ctx) {
		if (!isTemporary()) {
			assertrx(!ctx.isCopiedNsRequest);
			auto err = observers_.SendUpdates(
				std::move(recs),
				[&wlck]() {
					assertrx(wlck.isClusterLck());
					wlck.unlock();
				},
				ctx.rdxContext);
			// Unlock before flushing, but after sending updates
			if (wlck.owns_lock()) {
				wlck.unlock();
			}
			if constexpr (std::is_same_v<QueryStatsCalculatorT, std::nullptr_t>) {
				storage_.TryForceFlush();
			} else {
				statCalculator.LogFlushDuration(storage_, &AsyncStorage::TryForceFlush);
			}
			if (!err.ok()) {
				throw Error(errUpdateReplication, err.what());
			}
		} else if (tryForceFlush && wlck.owns_lock()) {
			wlck.unlock();
			if constexpr (std::is_same_v<QueryStatsCalculatorT, std::nullptr_t>) {
				storage_.TryForceFlush();
			} else {
				statCalculator.LogFlushDuration(storage_, &AsyncStorage::TryForceFlush);
			}
		}
	}

	void removeIndex(std::unique_ptr<Index>&&);
	void dumpIndex(std::ostream& os, std::string_view index) const;
	void tryForceFlush(Locker::WLockT&& wlck) {
		if (wlck.owns_lock()) {
			wlck.unlock();
			storage_.TryForceFlush();
		}
	}
	int64_t getWalSize(const NamespaceConfigData& cfg) const noexcept {
		return isSystem() ? int64_t(1) : std::max(cfg.walSize, int64_t(1));
	}
	void clearNamespaceCaches();
	[[noreturn]] void throwIndexUpsertErrorWithPKInfo(const ConstPayload&, const std::exception&);

	Variant getFloatVector(FloatVectorId, const FloatVectorIndex&, const PayloadType* = nullptr, const TagsMatcher* = nullptr,
						   const FieldsSet* = nullptr) const;
	h_vector<Variant, 1> getFloatVectorArray(IdType rowId, size_t arrSize, const FloatVectorIndex&, const PayloadType* = nullptr,
											 const TagsMatcher* = nullptr, const FieldsSet* = nullptr) const;
	FloatVectorsGetter floatVectorsGetterFn(IdType rowId, const FieldsSet* = nullptr) const noexcept;

	void quantize(std::string_view index, const RdxContext& ctx);
	void reloadNonQuantizedIndex(int idx);

	PerfStatCounterMT updatePerfCounter_, selectPerfCounter_;
	std::atomic_bool enablePerfCounters_{false};

	NamespaceConfigData config_;
	QueryCountCache queryCountCache_;
	joins::Cache joinCache_;
	// Replication variables
	WALTracker wal_;
	ReplicationState repl_;

	StorageOpts storageOpts_;
	std::atomic_int64_t lastSelectTime_{0};

	sync_pool<ItemImpl, 1024> pool_;
	std::atomic_int32_t cancelCommitCnt_{0};
	std::atomic_int64_t lastUpdateTime_{0};
	ann_storage_cache::UpdateInfo annStorageCacheState_;

	std::atomic_uint32_t itemsCount_{0};
	std::atomic_uint32_t itemsCapacity_{0};
	bool nsIsLoading_{false};

	size_t itemsDataSize_{0};

	IndexOptimizer indexOptimizer_;
	StringsHolderPtr strHolder_;
	std::deque<StringsHolderPtr> strHoldersWaitingToBeDeleted_;
	std::chrono::seconds lastExpirationCheckTs_{0};
	std::atomic<bool> dbDestroyed_{false};
	lsn_t incarnationTag_;	// Determines unique namespace incarnation for the correct go cache invalidation
	UpdatesObservers& observers_;

	const std::shared_ptr<EmbeddersCache> embeddersCache_;
};

}  // namespace reindexer
