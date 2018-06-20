#pragma once

#include <vector>
#include "core/idset.h"
#include "core/index/keyentry.h"
#include "core/indexopts.h"
#include "core/keyvalue/keyvalue.h"
#include "core/namespacestat.h"
#include "core/payload/payloadiface.h"
#include "core/selectfunc/ctx/basefunctionctx.h"
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

	Index(IndexType type, const string& name, const IndexOpts& opts = IndexOpts(), const PayloadType payloadType = PayloadType(),
		  const FieldsSet& fields = FieldsSet());
	Index& operator=(const Index&) = delete;
	virtual ~Index();
	virtual KeyRef Upsert(const KeyRef& key, IdType id) = 0;
	virtual void Delete(const KeyRef& key, IdType id) = 0;
	virtual void DumpKeys() = 0;
	virtual IdSetRef Find(const KeyRef& key) = 0;

	virtual SelectKeyResults SelectKey(const KeyValues& keys, CondType condition, SortType stype, ResultType res_type,
									   BaseFunctionCtx::Ptr ctx) = 0;
	virtual void Commit(const CommitContext& ctx) = 0;
	virtual void MakeSortOrders(UpdateSortedContext&) {}

	virtual void UpdateSortedIds(const UpdateSortedContext& ctx) = 0;
	virtual size_t Size() const { return 0; }
	virtual Index* Clone() = 0;
	virtual void Configure(const string&) {}
	virtual bool IsOrdered() const { return false; }
	virtual IndexMemStat GetMemStat() = 0;
	void UpdatePayloadType(const PayloadType payloadType) { payloadType_ = payloadType; }

	static Index* New(IndexType type, const string& name, const IndexOpts& opts, const PayloadType payloadType, const FieldsSet& fields_);

	virtual KeyValueType KeyType() = 0;
	const FieldsSet& Fields() const { return fields_; }
	const string& Name() const { return name_; }
	IndexType Type() const { return type_; }
	const vector<IdType>& SortOrders() const { return sortOrders_; }
	const IndexOpts& Opts() const { return opts_; }
	void SetOpts(const IndexOpts& opts) { opts_ = opts; }
	SortType SortId() const { return sortId_; }

protected:
	// Index type. Can be one of enum IndexType
	IndexType type_;
	// Name of index (usualy name of field).
	string name_;
	// Vector or ids, sorted by this index. Available only for ordered indexes
	vector<IdType> sortOrders_;

	SortType sortId_ = 0;
	// Index options
	IndexOpts opts_;
	// Payload type of items
	mutable PayloadType payloadType_;
	// Fields in index. Valid only for composite indexes
	FieldsSet fields_;
};

}  // namespace reindexer
