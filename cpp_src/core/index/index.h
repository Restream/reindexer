#pragma once

#include <vector>
#include "core/idset.h"
#include "core/index/keyentry.h"
#include "core/keyvalue/keyvalue.h"
#include "core/payload/payloadiface.h"
#include "core/selectkeyresult.h"

namespace reindexer {

using std::string;
using std::vector;

class Index {
public:
	enum ResultType {
		Optimal = 0,
		ForceIdset = 1,
		ForceComparator = 2,
	};
	using KeyEntry = reindexer::KeyEntry<IdSet>;
	using KeyEntryPlain = reindexer::KeyEntry<IdSetPlain>;

	Index(IndexType _type, const string& _name, const IndexOpts& opts = IndexOpts(), const PayloadType::Ptr payloadType = nullptr,
		  const FieldsSet& fields = FieldsSet());
	Index& operator=(const Index&) = delete;
	virtual ~Index();
	KeyValueType KeyType();
	virtual KeyRef Upsert(const KeyRef& key, IdType id) = 0;
	virtual void Delete(const KeyRef& key, IdType id) = 0;
	virtual void DumpKeys() = 0;
	virtual IdSetRef Find(const KeyRef& key) = 0;

	virtual SelectKeyResults SelectKey(const KeyValues& keys, CondType condition, SortType stype, ResultType res_type) = 0;
	virtual void Commit(const CommitContext& ctx) = 0;
	virtual void MakeSortOrders(UpdateSortedContext&) {}

	virtual void UpdateSortedIds(const UpdateSortedContext& ctx) = 0;
	virtual size_t Size() const { return 0; }
	virtual Index* Clone() = 0;
	virtual void Configure(const string&) {}
	virtual bool IsOrdered() const { return false; }
	void UpdatePayloadType(const PayloadType::Ptr payloadType) { payloadType_ = payloadType; }

	static Index* New(IndexType type, const string& _name, const IndexOpts& opts);
	static Index* NewComposite(IndexType type, const string& _name, const IndexOpts& opts, const PayloadType::Ptr payloadType,
							   const FieldsSet& fields_);

	string TypeName();
	vector<string> Conds();

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
	// Payload type of items
	mutable PayloadType::Ptr payloadType_;
	// Fields in index. Valid only for composite indexes
	FieldsSet fields_;
};
}  // namespace reindexer
