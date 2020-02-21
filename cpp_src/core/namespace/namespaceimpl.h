#pragma once

#include <atomic>
#include <memory>
#include <thread>
#include <vector>
#include "core/cjson/tagsmatcher.h"
#include "core/dbconfig.h"
#include "core/index/keyentry.h"
#include "core/item.h"
#include "core/joincache.h"
#include "core/namespacedef.h"
#include "core/payload/payloadiface.h"
#include "core/perfstatcounter.h"
#include "core/querycache.h"
#include "core/storage/idatastorage.h"
#include "core/storage/storagetype.h"
#include "core/transactionimpl.h"
#include "estl/contexted_locks.h"
#include "estl/fast_hash_map.h"
#include "estl/shared_mutex.h"
#include "estl/smart_lock.h"
#include "estl/syncpool.h"
#include "replicator/updatesobserver.h"
#include "replicator/waltracker.h"

namespace reindexer {

using std::pair;
using std::shared_ptr;
using std::string;
using std::unique_lock;
using std::unique_ptr;
using std::vector;

using reindexer::datastorage::StorageType;

class Index;
struct SelectCtx;
struct JoinPreResult;
class QueryResults;
class DBConfigProvider;
class SelectLockUpgrader;
class QueryPreprocessor;
class SelectIteratorContainer;
class RdxContext;
class RdxActivityContext;
class ItemComparator;
struct SortExpressionJoinedIndex;

struct NsContext {
	NsContext(const RdxContext &rdxCtx) : rdxContext(rdxCtx) {}
	NsContext &NoLock() {
		noLock = true;
		return *this;
	}
	NsContext &Lsn(int64_t lsn_) {
		lsn = lsn_;
		return *this;
	}
	NsContext &InTransaction() {
		inTransaction = true;
		return *this;
	}

	const RdxContext &rdxContext;
	bool noLock = false;
	int64_t lsn = -1;
	bool inTransaction = false;
};

class NamespaceImpl {
protected:
	friend class NsSelecter;
	friend class JoinedSelector;
	friend class WALSelecter;
	friend class NsSelectFuncInterface;
	friend class QueryPreprocessor;
	friend class SelectIteratorContainer;
	friend class ItemComparator;
	friend class Namespace;
	friend struct SortExpressionJoinedIndex;
	friend class ReindexerImpl;

	class NSUpdateSortedContext : public UpdateSortedContext {
	public:
		NSUpdateSortedContext(const NamespaceImpl &ns, SortType curSortId)
			: ns_(ns), sorted_indexes_(ns_.getSortedIdxCount()), curSortId_(curSortId) {
			ids2Sorts_.reserve(ns.items_.size());
			for (IdType i = 0; i < IdType(ns_.items_.size()); i++)
				ids2Sorts_.push_back(ns_.items_[i].IsFree() ? SortIdUnexists : SortIdUnfilled);
		}
		int getSortedIdxCount() const override { return sorted_indexes_; }
		SortType getCurSortId() const override { return curSortId_; }
		const vector<SortType> &ids2Sorts() const override { return ids2Sorts_; }
		vector<SortType> &ids2Sorts() override { return ids2Sorts_; }

	protected:
		const NamespaceImpl &ns_;
		const int sorted_indexes_;
		const IdType curSortId_;
		vector<SortType> ids2Sorts_;
	};

	class IndexesStorage : public vector<unique_ptr<Index>> {
	public:
		using Base = vector<unique_ptr<Index>>;

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

	class Items : public vector<PayloadValue> {
	public:
		bool exists(IdType id) const { return id < IdType(size()) && !at(id).IsFree(); }
	};

public:
	typedef shared_ptr<NamespaceImpl> Ptr;
	using Mutex = MarkedMutex<shared_timed_mutex, MutexMark::Namespace>;

	NamespaceImpl(const string &_name, UpdatesObservers &observers);
	NamespaceImpl &operator=(const NamespaceImpl &) = delete;
	~NamespaceImpl();

	const string &GetName() const { return name_; }
	bool IsSystem(const RdxContext &ctx) const {
		auto rlck = rLock(ctx);
		return isSystem();
	}
	bool IsTemporary(const RdxContext &ctx) const { return GetReplState(ctx).temporary; }

	void EnableStorage(const string &path, StorageOpts opts, StorageType storageType, const RdxContext &ctx);
	void LoadFromStorage(const RdxContext &ctx);
	void DeleteStorage(const RdxContext &);

	uint32_t GetItemsCount() const { return itemsCount_.load(std::memory_order_relaxed); }
	uint32_t GetItemsCapacity() const { return itemsCapacity_.load(std::memory_order_relaxed); }
	void AddIndex(const IndexDef &indexDef, const RdxContext &ctx);
	void UpdateIndex(const IndexDef &indexDef, const RdxContext &ctx);
	void DropIndex(const IndexDef &indexDef, const RdxContext &ctx);

	void Insert(Item &item, const RdxContext &ctx);
	void Update(Item &item, const RdxContext &ctx);
	void Update(const Query &query, QueryResults &result, const NsContext &);
	void Upsert(Item &item, const NsContext &);

	void Delete(Item &item, const NsContext &);
	void Delete(const Query &query, QueryResults &result, const NsContext &);
	void Truncate(const NsContext &);
	void Refill(vector<Item> &, const NsContext &);

	void Select(QueryResults &result, SelectCtx &params, const RdxContext &);
	NamespaceDef GetDefinition(const RdxContext &ctx);
	NamespaceMemStat GetMemStat(const RdxContext &);
	NamespacePerfStat GetPerfStat(const RdxContext &);
	void ResetPerfStat(const RdxContext &);
	vector<string> EnumMeta(const RdxContext &ctx);

	void BackgroundRoutine(RdxActivityContext *);
	void CloseStorage(const RdxContext &);

	Transaction NewTransaction(const RdxContext &ctx);
	void CommitTransaction(Transaction &tx, QueryResults &result, const NsContext &ctx);

	Item NewItem(const RdxContext &ctx);
	void ToPool(ItemImpl *item);
	// Get meta data from storage by key
	string GetMeta(const string &key, const RdxContext &ctx);
	// Put meta data to storage by key
	void PutMeta(const string &key, const string_view &data, const NsContext &);
	int64_t GetSerial(const string &field);

	int getIndexByName(const string &index) const;
	bool getIndexByName(const string &name, int &index) const;

	void FillResult(QueryResults &result, IdSet::Ptr ids) const;

	void EnablePerfCounters(bool enable = true) { enablePerfCounters_ = enable; }

	// Replication slave mode functions
	ReplicationState GetReplState(const RdxContext &) const;
	void SetSlaveLSN(int64_t slaveLSN, const RdxContext &);
	void SetSlaveReplStatus(ReplicationState::Status, const Error &, const RdxContext &);
	void SetSlaveReplMasterState(MasterState state, const RdxContext &);

	void ReplaceTagsMatcher(const TagsMatcher &tm, const RdxContext &);

	void OnConfigUpdated(DBConfigProvider &configProvider, const RdxContext &ctx);
	void SetStorageOpts(StorageOpts opts, const RdxContext &ctx);
	StorageOpts GetStorageOpts(const RdxContext &);

protected:
	struct SysRecordsVersions {
		uint64_t idxVersion{0};
		uint64_t tagsVersion{0};
		uint64_t replVersion{0};
	};

	class Locker {
	public:
		typedef contexted_shared_lock<Mutex, const RdxContext> RLockT;
		typedef contexted_unique_lock<Mutex, const RdxContext> WLockT;

		RLockT RLock(const RdxContext &ctx) const { return RLockT(mtx_, &ctx); }
		WLockT WLock(const RdxContext &ctx) const {
			WLockT lck(mtx_, &ctx);
			if (readonly_.load(std::memory_order_acquire)) {
				throw Error(errNamespaceInvalidated, "NS invalidated"_sv);
			}
			return lck;
		}
		unique_lock<std::mutex> StorageLock() const {
			unique_lock<std::mutex> lck(storage_mtx_);
			if (readonly_.load(std::memory_order_acquire)) {
				throw Error(errNamespaceInvalidated, "NS invalidated"_sv);
			}
			return lck;
		}
		void MarkReadOnly() { readonly_.store(true, std::memory_order_release); }

	private:
		mutable Mutex mtx_;
		mutable std::mutex storage_mtx_;
		std::atomic<bool> readonly_ = {false};
	};

	void copyContentsFrom(const NamespaceImpl &);
	ReplicationState getReplState() const;
	bool tryToReload(const RdxContext &);
	void reloadStorage();
	std::string sysRecordName(string_view sysTag, uint64_t version);
	void writeSysRecToStorage(string_view data, string_view sysTag, uint64_t &version, bool direct);
	void saveIndexesToStorage();
	Error loadLatestSysRecord(string_view baseSysTag, uint64_t &version, string &content);
	bool loadIndexesFromStorage();
	void saveReplStateToStorage();
	void loadReplStateFromStorage();
	bool isEmptyAfterStorageReload() const;

	void initWAL(int64_t maxLSN);

	void markUpdated();
	void doUpsert(ItemImpl *ritem, IdType id, bool doUpdate);
	void modifyItem(Item &item, const NsContext &, int mode = ModeUpsert);
	void updateFieldsFromQuery(IdType itemId, const Query &q, bool rowBasedReplication, const NsContext &);
	void updateTagsMatcherFromItem(ItemImpl *ritem);
	void updateItems(PayloadType oldPlType, const FieldsSet &changedFields, int deltaFields);
	void doDelete(IdType id);
	void optimizeIndexes(const RdxContext &);
	void insertIndex(Index *newIndex, int idxNo, const string &realName);
	void addIndex(const IndexDef &indexDef);
	void addCompositeIndex(const IndexDef &indexDef);
	void verifyUpdateIndex(const IndexDef &indexDef) const;
	void verifyUpdateCompositeIndex(const IndexDef &indexDef) const;
	void updateIndex(const IndexDef &indexDef);
	void dropIndex(const IndexDef &index);
	void addToWAL(const IndexDef &indexDef, WALRecType type);
	VariantArray preprocessUpdateFieldValues(const UpdateEntry &updateEntry, IdType itemId);
	void removeExpiredItems(RdxActivityContext *);

	void recreateCompositeIndexes(int startIdx, int endIdx);
	NamespaceDef getDefinition() const;
	IndexDef getIndexDefinition(const string &indexName) const;
	IndexDef getIndexDefinition(size_t) const;

	string getMeta(const string &key);
	void flushStorage(const RdxContext &);
	void putMeta(const string &key, const string_view &data);

	pair<IdType, bool> findByPK(ItemImpl *ritem, const RdxContext &);
	int getSortedIdxCount() const;
	void updateSortedIdxCount();
	void setFieldsBasedOnPrecepts(ItemImpl *ritem);

	void putToJoinCache(JoinCacheRes &res, std::shared_ptr<JoinPreResult> preResult) const;
	void putToJoinCache(JoinCacheRes &res, JoinCacheVal &val) const;
	void getFromJoinCache(JoinCacheRes &ctx) const;
	void getIndsideFromJoinCache(JoinCacheRes &ctx) const;

	const FieldsSet &pkFields();
	void writeToStorage(const string_view &key, const string_view &data);
	void doFlushStorage();

	bool needToLoadData(const RdxContext &) const;

	void updateSelectTime();
	int64_t getLastSelectTime() const;
	void markReadOnly() { locker_.MarkReadOnly(); }
	Locker::WLockT wLock(const RdxContext &ctx) const { return locker_.WLock(ctx); }
	Locker::RLockT rLock(const RdxContext &ctx) const { return locker_.RLock(ctx); }

	IndexesStorage indexes_;
	fast_hash_map<string, int, nocase_hash_str, nocase_equal_str> indexesNames_;
	// All items with data
	Items items_;
	vector<IdType> free_;
	// NamespaceImpl name
	string name_;
	// Payload types
	PayloadType payloadType_;

	// Tags matcher
	TagsMatcher tagsMatcher_;

	shared_ptr<datastorage::IDataStorage> storage_;
	datastorage::UpdatesCollection::Ptr updates_;
	std::atomic<int> unflushedCount_;

	// Commit phases state
	std::atomic<bool> sortOrdersBuilt_;

	std::unordered_map<string, string> meta_;

	string dbpath_;

	shared_ptr<QueryCache> queryCache_;

	int sparseIndexesCount_ = 0;
	VariantArray krefs, skrefs;

	SysRecordsVersions sysRecordsVersions_;

	Locker locker_;

private:
	NamespaceImpl(const NamespaceImpl &src);

	bool isSystem() const { return !name_.empty() && name_[0] == '#'; }
	IdType createItem(size_t realSize);
	void deleteStorage();
	void checkApplySlaveUpdate(int64_t lsn);

	JoinCache::Ptr joinCache_;

	PerfStatCounterMT updatePerfCounter_, selectPerfCounter_;
	std::atomic<bool> enablePerfCounters_;

	NamespaceConfigData config_;
	// Replication variables
	WALTracker wal_;
	ReplicationState repl_;
	UpdatesObservers *observers_;

	StorageOpts storageOpts_;
	std::atomic<bool> storageLoaded_;
	std::atomic<int64_t> lastSelectTime_;

	sync_pool<ItemImpl, 1024> pool_;
	std::atomic<bool> cancelCommit_;
	std::atomic<int64_t> lastUpdateTime_;

	std::atomic<uint32_t> itemsCount_ = {0};
	std::atomic<uint32_t> itemsCapacity_ = {0};
};

}  // namespace reindexer
