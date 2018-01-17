#pragma once

#include <leveldb/db.h>
#include <leveldb/write_batch.h>
#include <memory>
#include <mutex>
#include <vector>
#include "core/index/index.h"
#include "core/item.h"
#include "core/nsdescriber/nsdescriber.h"
#include "core/nsselecter/nsselecter.h"
#include "core/sqlfunc/sqlfunc.h"
#include "estl/fast_hash_map.h"
#include "estl/fast_hash_set.h"
#include "estl/shared_mutex.h"
#include "namespacedef.h"
#include "payload/payloadiface.h"
#include "query/querycache.h"
#include "tools/logger.h"

namespace reindexer {

using std::mutex;
using std::pair;
using std::shared_ptr;
using std::string;
using std::unique_lock;
using std::unique_ptr;
using std::vector;

class Namespace {
protected:
	friend class NsSelecter;
	friend class NsDescriber;
	friend class Reindexer;
	class NSCommitContext : public CommitContext {
	public:
		NSCommitContext(const Namespace &ns, int phases, const FieldsSet *indexes = nullptr)
			: ns_(ns), sorted_indexes_(ns_.getSortedIdxCount()), phases_(phases), indexes_(indexes) {}
		int getSortedIdxCount() const override { return sorted_indexes_; }
		int phases() const override { return phases_; }
		const FieldsSet *indexes() const { return indexes_; }

	protected:
		const Namespace &ns_;
		int sorted_indexes_;
		int phases_;
		const FieldsSet *indexes_;
	};

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
		const vector<SortType> &ids2Sorts() const override { return ids2Sorts_; };
		vector<SortType> &ids2Sorts() override { return ids2Sorts_; };

	protected:
		const Namespace &ns_;
		int sorted_indexes_;
		IdType curSortId_;
		vector<SortType> ids2Sorts_;
	};

	class Items : public vector<PayloadValue> {
	public:
		bool exists(IdType id) const { return id < IdType(size()) && !at(id).IsFree(); }
	};

public:
	typedef shared_ptr<Namespace> Ptr;

	Namespace(const string &_name);

	Namespace &operator=(const Namespace &) = delete;

	~Namespace();

	void EnableStorage(const string &path, StorageOpts opts);
	void LoadFromStorage();
	void DeleteStorage();

	bool AddIndex(const string &index, const string &jsonPath, IndexType type, IndexOpts opts);
	bool AddCompositeIndex(const string &index, IndexType type, IndexOpts opts);
	void ConfigureIndex(const string &index, const string &config);

	void Insert(Item *item, bool store = true);
	void Update(Item *item, bool store = true);

	void Upsert(Item *item, bool store = true);

	void Delete(Item *item);
	void Select(QueryResults &result, const SelectCtx &params);
	void Describe(QueryResults &result);
	void Delete(const Query &query, QueryResults &result);
	void FlushStorage();

	void NewItem(Item **item);

	// Get meta data from storage by key
	string GetMeta(const string &key);
	// Put meta data to storage by key
	void PutMeta(const string &key, const Slice &data);
	// Lock reads to snapshot
	void LockSnapshot();

	int getIndexByName(const string &index);

	static Namespace *Clone(Namespace::Ptr);

protected:
	void saveIndexesToStorage();
	bool loadIndexesFromStorage();
	void markUpdated(IdType id);
	void upsert(ItemImpl *ritem, IdType id, bool doUpdate);
	void upsertInternal(Item *ritem, bool store = true, uint8_t mode = (INSERT_MODE | UPDATE_MODE));
	void updateTagsMatcherFromItem(ItemImpl *ritem, string &jsonSliceBuf);

	void _delete(IdType id);
	void commit(const NSCommitContext &ctx, std::function<void()> lockUpgrader);
	void insertIndex(Index *newIndex, int idxNo, const string &realName);
	bool addIndex(const string &index, const string &jsonPath, IndexType type, IndexOpts opts);

	pair<IdType, bool> findByPK(ItemImpl *ritem);

	int getSortedIdxCount() const;

	void setFieldsBasedOnPrecepts(ItemImpl *ritem);

	int64_t funcGetSerial(SqlFuncStruct sqlFuncStruct);

	vector<unique_ptr<Index>> indexes_;
	fast_hash_map<string, int> indexesNames_;
	// All items with data
	Items items_;
	fast_hash_set<IdType> free_;
	// Namespace name
	string name;
	// Payload types
	PayloadType::Ptr payloadType_;

	// Tags matcher
	TagsMatcher tagsMatcher_;

	shared_ptr<leveldb::DB> db_;
	const leveldb::Snapshot *snapshot_;
	leveldb::WriteBatch batch_;
	int unflushedCount_;

	shared_timed_mutex mtx_;
	// Commit phases state
	bool sortOrdersBuilt_;
	FieldsSet preparedIndexes_, commitedIndexes_;
	FieldsSet pkFields_;

	unordered_map<string, string> meta_;

	string dbpath_;

	shared_ptr<QueryCache> queryCache;

private:
	Namespace(const Namespace &src);

private:
	typedef shared_lock<shared_timed_mutex> RLock;
	typedef unique_lock<shared_timed_mutex> WLock;

	enum { INSERT_MODE = 0x01, UPDATE_MODE = 0x02 };
	IdType createItem(size_t realSize);

	void invalidateQueryCache();
};

}  // namespace reindexer
