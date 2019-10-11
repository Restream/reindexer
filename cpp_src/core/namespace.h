#pragma once

#include <atomic>
#include <memory>
#include <vector>
#include "core/cjson/tagsmatcher.h"
#include "core/dbconfig.h"
#include "core/item.h"
#include "estl/contexted_locks.h"
#include "estl/fast_hash_map.h"
#include "estl/shared_mutex.h"
#include "estl/syncpool.h"
#include "index/keyentry.h"
#include "joincache.h"
#include "namespacedef.h"
#include "payload/payloadiface.h"
#include "perfstatcounter.h"
#include "querycache.h"
#include "replicator/updatesobserver.h"
#include "replicator/waltracker.h"
#include "storage/idatastorage.h"
#include "storage/storagetype.h"
#include "transactionimpl.h"

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

class Namespace {
	using Mutex = MarkedMutex<shared_timed_mutex, MutexMark::Namespace>;

protected:
	friend class NsSelecter;
	friend class JoinedSelector;
	friend class WALSelecter;
	friend class NsSelectFuncInterface;
	friend class ReindexerImpl;
	friend QueryPreprocessor;
	friend SelectIteratorContainer;

	class NSUpdateSortedContext : public UpdateSortedContext {
	public:
		NSUpdateSortedContext(const Namespace &ns, SortType curSortId)
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
		const Namespace &ns_;
		const int sorted_indexes_;
		const IdType curSortId_;
		vector<SortType> ids2Sorts_;
	};

	class IndexesStorage : public vector<unique_ptr<Index>> {
	public:
		using Base = vector<unique_ptr<Index>>;

		IndexesStorage(const Namespace &ns);

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
		const Namespace &ns_;
	};

	class Items : public vector<PayloadValue> {
	public:
		bool exists(IdType id) const { return id < IdType(size()) && !at(id).IsFree(); }
	};

public:
	typedef shared_ptr<Namespace> Ptr;

	Namespace(const string &_name, UpdatesObservers &observers);
	Namespace &operator=(const Namespace &) = delete;
	void CopyContentsFrom(const Namespace &);
	~Namespace();

	const string &GetName() const { return name_; }
	bool IsSystem(const RdxContext &ctx) const {
		RLock lck(mtx_, &ctx);
		return isSystem();
	}
	bool IsTemporary(const RdxContext &ctx) const { return GetReplState(ctx).temporary; }

	void EnableStorage(const string &path, StorageOpts opts, StorageType storageType, const RdxContext &ctx);
	void LoadFromStorage(const RdxContext &ctx);
	void DeleteStorage(const RdxContext &);

	uint32_t GetItemsCount();
	void AddIndex(const IndexDef &indexDef, const RdxContext &ctx);
	void UpdateIndex(const IndexDef &indexDef, const RdxContext &ctx);
	void DropIndex(const IndexDef &indexDef, const RdxContext &ctx);

	void Insert(Item &item, const RdxContext &ctx, bool store = true);
	void Update(Item &item, const RdxContext &ctx, bool store = true);
	void Update(const Query &query, QueryResults &result, const RdxContext &ctx, int64_t lsn = -1, bool noLock = false);
	void Upsert(Item &item, const RdxContext &ctx, bool store = true, bool noLock = false);

	void Delete(Item &item, const RdxContext &ctx, bool noLock = false);
	void Delete(const Query &query, QueryResults &result, const RdxContext &ctx, int64_t lsn = -1, bool noLock = false);
	void Truncate(const RdxContext &ctx, int64_t lsn = -1, bool noLock = false);

	void Select(QueryResults &result, SelectCtx &params, const RdxContext &);
	NamespaceDef GetDefinition(const RdxContext &ctx);
	NamespaceMemStat GetMemStat(const RdxContext &);
	NamespacePerfStat GetPerfStat(const RdxContext &);
	void ResetPerfStat(const RdxContext &);
	vector<string> EnumMeta(const RdxContext &ctx);

	void BackgroundRoutine(RdxActivityContext *);
	void CloseStorage(const RdxContext &);

	Transaction NewTransaction(const RdxContext &ctx);
	void CommitTransaction(Transaction &tx, const RdxContext &ctx);

	Item NewItem(const RdxContext &ctx);
	void ToPool(ItemImpl *item);
	// Get meta data from storage by key
	string GetMeta(const string &key, const RdxContext &ctx);
	// Put meta data to storage by key
	void PutMeta(const string &key, const string_view &data, const RdxContext &ctx, int64_t lsn = -1);
	int64_t GetSerial(const string &field);

	int getIndexByName(const string &index) const;
	bool getIndexByName(const string &name, int &index) const;

	void FillResult(QueryResults &result, IdSet::Ptr ids) const;

	void EnablePerfCounters(bool enable = true) { enablePerfCounters_ = enable; }

	// Replication slave mode functions
	ReplicationState GetReplState(const RdxContext &) const;
	ReplicationState getReplState() const;
	void SetSlaveLSN(int64_t slaveLSN, const RdxContext &);
	void SetSlaveReplError(const Error &err, const RdxContext &);

	void ReplaceTagsMatcher(const TagsMatcher &tm, const RdxContext &);

	void Rename(Namespace::Ptr dst, const std::string &storagePath, const RdxContext &ctx);
	void Rename(const std::string &newName, const std::string &storagePath, const RdxContext &ctx);

protected:
	struct SysRecordsVersions {
		uint64_t idxVersion{0};
		uint64_t tagsVersion{0};
		uint64_t replVersion{0};
	};

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
	void modifyItem(Item &item, const RdxContext &ctx, bool store = true, int mode = ModeUpsert, bool noLock = false);
	void updateFieldsFromQuery(IdType itemId, const Query &q, bool store = true);
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
	void onConfigUpdated(DBConfigProvider &configProvider, const RdxContext &ctx);
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

	void PutToJoinCache(JoinCacheRes &res, std::shared_ptr<JoinPreResult> preResult) const;
	void PutToJoinCache(JoinCacheRes &res, JoinCacheVal &val) const;
	void GetFromJoinCache(JoinCacheRes &ctx) const;
	void GetIndsideFromJoinCache(JoinCacheRes &ctx) const;

	const FieldsSet &pkFields();
	void writeToStorage(const string_view &key, const string_view &data) {
		std::unique_lock<std::mutex> lck(storage_mtx_);
		updates_->Put(key, data);
		unflushedCount_.fetch_add(1, std::memory_order_release);
	}

	bool needToLoadData(const RdxContext &) const;
	StorageOpts getStorageOpts(const RdxContext &);
	void SetStorageOpts(StorageOpts opts, const RdxContext &ctx);

	void updateSelectTime();
	int64_t getLastSelectTime() const;

	IndexesStorage indexes_;
	fast_hash_map<string, int, nocase_hash_str, nocase_equal_str> indexesNames_;
	// All items with data
	Items items_;
	vector<IdType> free_;
	// Namespace name
	string name_;
	// Payload types
	PayloadType payloadType_;

	// Tags matcher
	TagsMatcher tagsMatcher_;

	shared_ptr<datastorage::IDataStorage> storage_;
	datastorage::UpdatesCollection::Ptr updates_;
	std::atomic<int> unflushedCount_;

	mutable Mutex mtx_;
	std::mutex storage_mtx_;

	// Commit phases state
	std::atomic<bool> sortOrdersBuilt_;

	unordered_map<string, string> meta_;

	string dbpath_;

	shared_ptr<QueryCache> queryCache_;

	int sparseIndexesCount_ = 0;
	VariantArray krefs, skrefs;

	SysRecordsVersions sysRecordsVersions_;

private:
	Namespace(const Namespace &src);

	typedef contexted_shared_lock<Mutex, const RdxContext> RLock;
	typedef contexted_unique_lock<Mutex, const RdxContext> WLock;

	bool isSystem() const { return !name_.empty() && name_[0] == '#'; }
	IdType createItem(size_t realSize);
	void deleteStorage();
	void doRename(Namespace::Ptr dst, const std::string &newName, const std::string &storagePath, const RdxContext &ctx);
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

	std::atomic<uint32_t> itemsCount_;

};  // namespace reindexer

}  // namespace reindexer
