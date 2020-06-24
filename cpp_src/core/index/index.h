#pragma once

#include <vector>
#include "core/idset.h"
#include "core/index/keyentry.h"
#include "core/indexdef.h"
#include "core/indexopts.h"
#include "core/keyvalue/variant.h"
#include "core/namespace/namespacestat.h"
#include "core/payload/payloadiface.h"
#include "core/perfstatcounter.h"
#include "core/selectfunc/ctx/basefunctionctx.h"
#include "core/selectkeyresult.h"
#include "indexiterator.h"

namespace reindexer {

using std::string;
using std::vector;

class RdxContext;

class Index {
public:
	struct SelectOpts {
		SelectOpts() : itemsCountInNamespace(0), distinct(0), disableIdSetCache(0), forceComparator(0), unbuiltSortOrders(0) {}
		unsigned itemsCountInNamespace;
		unsigned distinct : 1;
		unsigned disableIdSetCache : 1;
		unsigned forceComparator : 1;
		unsigned unbuiltSortOrders : 1;
	};
	using KeyEntry = reindexer::KeyEntry<IdSet>;
	using KeyEntryPlain = reindexer::KeyEntry<IdSetPlain>;

	Index(const IndexDef& idef, const PayloadType payloadType, const FieldsSet& fields);
	Index(const Index&);
	Index& operator=(const Index&) = delete;
	virtual ~Index();
	virtual Variant Upsert(const Variant& key, IdType id) = 0;
	virtual void Delete(const Variant& key, IdType id) = 0;

	virtual SelectKeyResults SelectKey(const VariantArray& keys, CondType condition, SortType stype, SelectOpts opts,
									   BaseFunctionCtx::Ptr ctx, const RdxContext&) = 0;
	virtual void Commit() = 0;
	virtual void MakeSortOrders(UpdateSortedContext&) {}

	virtual void UpdateSortedIds(const UpdateSortedContext& ctx) = 0;
	virtual size_t Size() const { return 0; }
	virtual Index* Clone() = 0;
	virtual bool IsOrdered() const { return false; }
	virtual IndexMemStat GetMemStat() = 0;
	virtual int64_t GetTTLValue() const { return 0; }
	virtual IndexIterator::Ptr CreateIterator() const { return nullptr; }

	const PayloadType& GetPayloadType() const { return payloadType_; }
	void UpdatePayloadType(const PayloadType payloadType) { payloadType_ = payloadType; }

	static Index* New(const IndexDef& idef, const PayloadType payloadType, const FieldsSet& fields_);

	KeyValueType KeyType() const { return keyType_; }
	KeyValueType SelectKeyType() const { return selectKeyType_; }
	const FieldsSet& Fields() const { return fields_; }
	const string& Name() const { return name_; }
	IndexType Type() const { return type_; }
	const vector<IdType>& SortOrders() const { return sortOrders_; }
	const IndexOpts& Opts() const { return opts_; }
	virtual void SetOpts(const IndexOpts& opts) { opts_ = opts; }
	void SetFields(const FieldsSet& fields) { fields_ = fields; }
	SortType SortId() const { return sortId_; }
	virtual void SetSortedIdxCount(int sortedIdxCount) { sortedIdxCount_ = sortedIdxCount; }

	PerfStatCounterMT& GetSelectPerfCounter() { return selectPerfCounter_; }
	PerfStatCounterMT& GetCommitPerfCounter() { return commitPerfCounter_; }

	IndexPerfStat GetIndexPerfStat() {
		return IndexPerfStat(name_, selectPerfCounter_.Get<PerfStat>(), commitPerfCounter_.Get<PerfStat>());
	}
	void ResetIndexPerfStat() {
		selectPerfCounter_.Reset();
		commitPerfCounter_.Reset();
	}

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
	// Perfstat counter
	PerfStatCounterMT commitPerfCounter_;
	PerfStatCounterMT selectPerfCounter_;
	KeyValueType keyType_ = KeyValueUndefined;
	KeyValueType selectKeyType_ = KeyValueUndefined;
	// Count of sorted indexes in namespace to resereve additional space in idsets
	int sortedIdxCount_ = 0;
};

}  // namespace reindexer
