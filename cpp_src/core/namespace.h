#pragma once

#include <memory>
#include <mutex>
#include <vector>
#include "core/cjson/tagsmatcher.h"
#include "core/dbconfig.h"
#include "core/item.h"
#include "core/selectfunc/selectfunc.h"
#include "estl/fast_hash_map.h"
#include "estl/fast_hash_set.h"
#include "estl/shared_mutex.h"
#include "index/keyentry.h"
#include "joincache.h"
#include "namespacedef.h"
#include "payload/payloadiface.h"
#include "perfstatcounter.h"
#include "query/querycache.h"
#include "replicator/waltracker.h"
#include "storage/idatastorage.h"
#include "transactionimpl.h"

namespace reindexer {

using std::pair;
using std::shared_ptr;
using std::string;
using std::unique_lock;
using std::unique_ptr;
using std::vector;

class Index;
struct SelectCtx;
class QueryResults;
class DBConfigProvider;
class SelectLockUpgrader;
class UpdatesObservers;

class Namespace {
protected:
	friend class NsSelecter;
	friend class WALSelecter;
	friend class NsSelectFuncInterface;
	friend class ReindexerImpl;

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

		int denseIndexesSize() const { return ns_.payloadType_.NumFields(); }
		int sparseIndexesSize() const { return ns_.sparseIndexesCount_; }
		int compositeIndexesSize() const { return totalSize() - denseIndexesSize() - sparseIndexesSize(); }

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
	~Namespace();

	const string &GetName() { return name_; }
	bool isSystem() const { return !name_.empty() && name_[0] == '#'; }

	void EnableStorage(const string &path, StorageOpts opts);
	void LoadFromStorage();
	void DeleteStorage();

	void AddIndex(const IndexDef &indexDef);
	void UpdateIndex(const IndexDef &indexDef);
	void DropIndex(const IndexDef &indexDef);

	void Insert(Item &item, bool store = true);
	void Update(Item &item, bool store = true);
	void Update(const Query &query, QueryResults &result, int64_t lsn = -1);
	void Upsert(Item &item, bool store = true);

	void Delete(Item &item, bool noLock = false);
	void Select(QueryResults &result, SelectCtx &params);
	NamespaceDef GetDefinition();
	NamespaceMemStat GetMemStat();
	NamespacePerfStat GetPerfStat();
	vector<string> EnumMeta();
	void Delete(const Query &query, QueryResults &result, int64_t lsn = -1);
	void BackgroundRoutine();
	void CloseStorage();

	void ApplyTransactionStep(TransactionStep &step);

	void StartTransaction();
	void EndTransaction();

	Item NewItem();
	void ToPool(ItemImpl *item);
	// Get meta data from storage by key
	string GetMeta(const string &key);
	// Put meta data to storage by key
	void PutMeta(const string &key, const string_view &data, int64_t lsn = -1);

	int getIndexByName(const string &index) const;
	bool getIndexByName(const string &name, int &index) const;

	static Namespace *Clone(Namespace::Ptr);

	void FillResult(QueryResults &result, IdSet::Ptr ids, const h_vector<std::string, 1> &selectFilter);

	void EnablePerfCounters(bool enable = true) { enablePerfCounters_ = enable; }

	// Replication slave mode functions
	ReplicationState GetReplState();
	void SetSlaveLSN(int64_t slaveLSN);

protected:
	bool tryToReload();
	void reloadStorage();
	void saveIndexesToStorage();
	bool loadIndexesFromStorage();
	void saveReplStateToStorage();
	void loadReplStateFromStorage();
	bool isEmptyAfterStorageReload() const;

	void initWAL(int64_t maxLSN);

	void markUpdated();
	void doUpsert(ItemImpl *ritem, IdType id, bool doUpdate);
	void modifyItem(Item &item, bool store = true, int mode = ModeUpsert, bool noLock = false);
	void updateFieldsFromQuery(IdType itemId, const Query &q, bool store = true);
	void updateTagsMatcherFromItem(ItemImpl *ritem, string &jsonSliceBuf);
	void updateItems(PayloadType oldPlType, const FieldsSet &changedFields, int deltaFields);
	void doDelete(IdType id);
	void commitIndexes();
	void insertIndex(Index *newIndex, int idxNo, const string &realName);
	void addIndex(const IndexDef &indexDef);
	void addCompositeIndex(const IndexDef &indexDef);
	void updateIndex(const IndexDef &indexDef);
	void dropIndex(const IndexDef &index);
	void addToWAL(const IndexDef &indexDef, WALRecType type);

	void recreateCompositeIndexes(int startIdx, int endIdx);
	void onConfigUpdated(DBConfigProvider &configProvider);
	NamespaceDef getDefinition();
	IndexDef getIndexDefinition(const string &indexName);

	string getMeta(const string &key);
	void flushStorage();
	void putMeta(const string &key, const string_view &data);

	pair<IdType, bool> findByPK(ItemImpl *ritem);
	int getSortedIdxCount() const;
	void setFieldsBasedOnPrecepts(ItemImpl *ritem);
	int64_t funcGetSerial(SelectFuncStruct sqlFuncStruct);

	void PutToJoinCache(JoinCacheRes &res, SelectCtx::PreResult::Ptr preResult);
	void PutToJoinCache(JoinCacheRes &res, JoinCacheVal &val);
	void GetFromJoinCache(JoinCacheRes &ctx);
	void GetIndsideFromJoinCache(JoinCacheRes &ctx);

	const FieldsSet &pkFields();
	void writeToStorage(const string_view &key, const string_view &data) {
		std::unique_lock<std::mutex> lck(storage_mtx_);
		updates_->Put(key, data);
	}

	bool needToLoadData() const;
	StorageOpts getStorageOpts();
	void SetStorageOpts(StorageOpts opts);

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
	int unflushedCount_;

	shared_timed_mutex mtx_;
	std::mutex storage_mtx_;

	// Commit phases state
	std::atomic<bool> sortOrdersBuilt_;

	unordered_map<string, string> meta_;

	string dbpath_;

	shared_ptr<QueryCache> queryCache_;

	int sparseIndexesCount_ = 0;
	VariantArray krefs, skrefs;

private:
	Namespace(const Namespace &src);

private:
	typedef shared_lock<shared_timed_mutex> RLock;
	typedef unique_lock<shared_timed_mutex> WLock;

	IdType createItem(size_t realSize);

	JoinCache::Ptr joinCache_;

	PerfStatCounterMT updatePerfCounter_, selectPerfCounter_;
	std::atomic<bool> enablePerfCounters_;

	NamespaceConfigData config_;
	// Replication variables
	WALTracker wal_;
	ReplicationState repl_;
	UpdatesObservers &observers_;

	StorageOpts storageOpts_;
	std::atomic<bool> storageLoaded_;
	std::atomic<int64_t> lastSelectTime_;

	vector<std::unique_ptr<ItemImpl>> pool_;
	std::atomic<bool> cancelCommit_;
	std::atomic<int64_t> lastUpdateTime_;

	friend class Query;
};

}  // namespace reindexer
