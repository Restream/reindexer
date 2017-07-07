#pragma once

#include <map>
#include <unordered_map>
#include <vector>
#include "core/basepayload.h"
#include "core/idset.h"
#include "core/keyvalue.h"
#include "core/selectkeyresult.h"

using std::unordered_map;
using std::map;
using std::vector;
using std::shared_ptr;

class UpdateSortedContext {
public:
	virtual ~UpdateSortedContext(){};
	virtual int getSortedIdxCount() const = 0;
	virtual SortType getCurSortId() const = 0;
	virtual const vector<SortType>& ids2Sorts() const = 0;
	virtual vector<SortType>& ids2Sorts() = 0;
};

namespace reindexer {

class Index {
public:
	class KeyEntry {
	public:
		KeyEntry() {}
		KeyEntry(const KeyEntry& src) : ids_(src.ids_) {}

		KeyEntry& operator=(const KeyEntry& src) {
			if (this == &src) return *this;
			ids_ = src.ids_;
			return *this;
		}

		IdSet& Unsorted() { return ids_; }
		IdSetRef Sorted(unsigned sortId) const {
			assert(ids_.capacity() >= (sortId + 1) * ids_.size());
			return IdSetRef(ids_.data() + sortId * ids_.size(), ids_.size());
		}
		void UpdateSortedIds(const UpdateSortedContext& ctx);

		IdSet ids_;
	};
	enum ResultType {
		Optimal = 0,
		ForceIdset = 1,
		ForceComparator = 2,
	};

	Index(IndexType _type, const string& _name, const IndexOpts& opts, const FieldsSet& fields);
	virtual ~Index();
	KeyValueType KeyType();
	virtual KeyRef Upsert(const KeyRef& key, IdType id) = 0;
	virtual void Delete(const KeyRef& key, IdType id) = 0;
	virtual void DumpKeys() = 0;
	virtual KeyEntry& Find(const KeyRef& key) = 0;

	virtual SelectKeyResults SelectKey(const KeyValues& keys, CondType condition, SortType stype, ResultType res_type) = 0;
	virtual void Commit(const CommitContext& ctx) = 0;
	virtual void MakeSortOrders(UpdateSortedContext&){};
	virtual void UpdateSortedIds(const UpdateSortedContext& ctx) = 0;
	virtual bool IsSorted() const { return false; }
	virtual size_t Size() const { return 0; }
	virtual Index* Clone() = 0;

	static Index* New(IndexType type, const string& _name, const IndexOpts& opts);
	static Index* NewComposite(IndexType type, const string& _name, const IndexOpts& opts, const PayloadType& payloadType,
							   const FieldsSet& fields_);

public:
	// Index type. Can be one of enum IndexType
	IndexType type;
	// Name of index (usualy name of field).
	string name;
	// Vector or ids, sorted by this index. Available only for ordered indexes
	vector<IdType> sort_orders;

	SortType sort_id = 0;
	// Index options
	IndexOpts opts_;
	// Fields in index. Valid only for composite indexes
	FieldsSet fields_;
};

struct comparator_sptr {
	bool operator()(const key_string& lhs, const key_string& rhs) const { return *lhs < *rhs; }
};

struct equal_sptr {
	bool operator()(const key_string& lhs, const key_string& rhs) const { return *lhs == *rhs; }
};
struct hash_sptr {
	size_t operator()(const key_string& s) const { return std::hash<std::string>()(*s); }
};

template <class T1>
struct unordered_str_map : public unordered_map<key_string, T1, hash_sptr, equal_sptr> {
	unordered_str_map() : unordered_map<key_string, T1, hash_sptr, equal_sptr>() {}
};
template <class T1>
struct str_map : public map<key_string, T1, comparator_sptr> {};

}  // namespace reindexer
