#pragma once

#include <leveldb/db.h>
#include <leveldb/write_batch.h>
#include <algorithm>
#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include "core/basepayload.h"
#include "core/index.h"
#include "core/item.h"
#include "core/queryiterator.h"
#include "query/query.h"
#include "query/queryresults.h"
#include "tools/logger.h"

using std::string;
using std::vector;
using std::unordered_map;
using std::unordered_set;
using std::function;
using std::unique_ptr;
using std::shared_ptr;
using std::mutex;

namespace reindexer {

struct JoinedSelector {
	JoinType type;
	function<bool(IdType, bool)> func;
};

struct StorageOpts {
	// create new storage for namespace, if not exists
	bool createIfMissing = true;
	// drop storage if it is corrupted
	bool dropIfCorrupted = true;
	// drop storage if field structure is different
	bool dropIfFieldsMismatch = true;
};

typedef vector<JoinedSelector> JoinedSelectors;

class Namespace : public PayloadProvider {
protected:
	struct RawQueryResult : public vector<QueryIterator> {};

	class NSCommitContext : public CommitContext {
	public:
		NSCommitContext(const Namespace &ns, int phases, const FieldsSet *indexes = nullptr)
			: ns_(ns), phases_(phases), prepareIndexes_(indexes) {}
		bool exists(IdType id) const override { return ns_.items_.exists(id); }
		bool updated(IdType id) const override { return ns_.items_.exists(id) && ns_.items_[id].IsUpdated(); }
		int getUpdatedCount() const override { return ns_.updatedCount_; }
		int phases() const override { return phases_; }
		const FieldsSet *prepareIndexes() const { return prepareIndexes_; }

	protected:
		const Namespace &ns_;
		int phases_;
		const FieldsSet *prepareIndexes_;
	};

	class NSUpdateSortedContext : public UpdateSortedContext {
	public:
		NSUpdateSortedContext(const Namespace &ns, SortType curSortId)
			: ns_(ns), sorted_indexes_(ns_.getSortedIdxCount()), curSortId_(curSortId) {
			ids2Sorts_.reserve(ns.items_.size());
			for (IdType i = 0; i < (IdType)ns_.items_.size(); i++)
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

	class TagsMatcher : public ItemTagsMatcher {
	public:
		static const int tagBits = 12;

		TagsMatcher(PayloadType &payloadType) : payloadType_(payloadType), updated_(false) {}

		int name2tag(const string &name, const string &path) override {
			auto res = names2tags_.emplace(name, tags2names_.size());
			if (res.second) tags2names_.push_back(name);
			updated_ |= res.second;
			int tag = res.first->second | ((payloadType_.FieldByJsonPath(path) + 1) << tagBits);
			return tag + 1;
		}
		const char *tag2name(int tag) const override {
			tag &= (1 << tagBits) - 1;
			if (tag == 0 || tag - 1 >= (int)tags2names_.size()) return nullptr;
			return tags2names_[tag - 1].c_str();
		}
		int tag2field(int tag) const override { return (tag >> tagBits) - 1; }

		void serialize(WrSerializer &ser) {
			ser.PutVarUint(tags2names_.size());
			for (size_t tag = 0; tag < tags2names_.size(); ++tag) ser.PutString(tags2names_[tag]);
			updated_ = false;
		}

		void deserialize(Serializer &ser) {
			size_t cnt = ser.GetVarUint();
			tags2names_.resize(cnt);
			for (size_t tag = 0; tag < tags2names_.size(); ++tag) {
				string name = ser.GetString();
				names2tags_.emplace(name, tag);
				tags2names_[tag] = name;
			}
			updated_ = false;
		}
		bool isUpdated() { return updated_; }
		void clear() {
			names2tags_.clear();
			tags2names_.clear();
			updated_ = false;
		}

	protected:
		unordered_map<string, int> names2tags_;
		vector<string> tags2names_;
		PayloadType &payloadType_;
		bool updated_;
	};
	class Items : public vector<PayloadData> {
	public:
		bool exists(IdType id) const { return id < (IdType)size() && !at(id).IsFree(); }
	};

public:
	Namespace(const string &_name);
	Namespace(const Namespace &src);

	Namespace &operator=(const Namespace &);

	~Namespace();

	void EnableStorage(const string &path, StorageOpts opts = StorageOpts());

	void AddIndex(const string &index, const string &jsonPath, IndexType type, const IndexOpts *opts);
	void AddCompositeIndex(const string &index, IndexType type, const IndexOpts *opts);

	void Upsert(Item *item, bool store = true);

	void Delete(Item *item);
	void Select(const Query &query, const IdSet *preResult, QueryResults &result,
				const JoinedSelectors &joinedSelectors = JoinedSelectors());
	void Delete(const Query &query, QueryResults &result);
	void Commit();

	void GetItem(IdType id, Item **item);
	void NewItem(Item **item);

	// Get meta data from storage by key
	string GetMeta(const string &key);
	// Put meta data to storage by key
	void PutMeta(const string &key, const Slice &data);
	// Lock reads to snapshot
	void LockSnapshot();

	ConstPayload GetPayload(IdType id) const override;
	Payload GetPayload(IdType id) override;

	int getIndexByName(const string &index);

protected:
	void loadFromStorage();
	void flushToStorage();
	void saveIndexesToStorage();
	bool loadIndexesFromStorage();

	void upsert(ItemImpl *ritem, IdType id, bool doUpdate);
	void _delete(IdType id);
	void commit(const NSCommitContext &ctx);

	pair<IdType, bool> findByPK(ItemImpl *ritem);

	int getCompositeIndex(const FieldsSet &fieldsmask);
	int getSortedIdxCount() const;
	void selectWhere(const QueryEntries &entries, RawQueryResult &result, SortType sortId, bool is_ft);
	void substituteCompositeIndexes(QueryEntries &entries);
	QueryEntries lookupQueryIndexes(const QueryEntries &entries);
	bool mergeQueryEntries(QueryEntry *lhs, QueryEntry *rhs);
	const string &getOptimalSortOrder(const QueryEntries &qe);

	template <bool reverse, bool haveComparators, bool haveDistinct>
	void selectLoop(const Query &q, RawQueryResult &qres, QueryResults &result, Index *sortIndex, const JoinedSelectors &joinedSelectors);

	bool isContainsFullText(const QueryEntries &entries);

	// Indexes
	vector<unique_ptr<Index>> indexes_;
	unordered_map<string, int> indexesNames_;
	// All items with data
	Items items_;
	vector<IdType> deleted_;
	int updatedCount_;
	// Namespace name
	string name;
	// Payload types
	shared_ptr<PayloadType> payloadType_;

	// Tags matcher
	shared_ptr<TagsMatcher> tagsMatcher_;

	shared_ptr<leveldb::DB> db_;
	const leveldb::Snapshot *snapshot_;
	leveldb::WriteBatch batch_;
	int unflushedCount_;

	mutex commitLock_;
	// Commit phases state
	int commitPhasesState_ = 0;
	FieldsSet preparedIndexes_;
};

}  // namespace reindexer
