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
#include "core/perfstatcounter.h"

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
	Index(const Index&);
	Index& operator=(const Index&) = delete;
	virtual ~Index();
	virtual KeyRef Upsert(const KeyRef& key, IdType id) = 0;
	virtual void Delete(const KeyRef& key, IdType id) = 0;
	virtual void DumpKeys() = 0;
	virtual IdSetRef Find(const KeyRef& key) = 0;

	virtual SelectKeyResults SelectKey(const KeyValues& keys, CondType condition, SortType stype, ResultType res_type,
									   BaseFunctionCtx::Ptr ctx) = 0;
	virtual bool Commit(const CommitContext& ctx) = 0;
	virtual void MakeSortOrders(UpdateSortedContext&) {}

	virtual void UpdateSortedIds(const UpdateSortedContext& ctx) = 0;
	virtual size_t Size() const { return 0; }
	virtual Index* Clone() = 0;
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
	virtual void SetOpts(const IndexOpts& opts) { opts_ = opts; }
	void SetFields(const FieldsSet& fields) { fields_ = fields; }
	SortType SortId() const { return sortId_; }

	PerfStatCounterST& GetSelectPerfCounter() { return selectPerfCounter_; }
	PerfStatCounterST& GetCommitPerfCounter() { return commitPerfCounter_; }

	IndexPerfStat GetIndexPerfStat() {
		return IndexPerfStat(name_, selectPerfCounter_.Get<PerfStat>(), commitPerfCounter_.Get<PerfStat>());
	}

protected:
	// Resets count of selects before commit
	void resetQueriesCountTillCommit();

	// Indicates is it possible perform commit
	bool allowedToCommit(int phases) const;

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
	// Amount of raw selects to index without any update
	std::atomic<int> rawQueriesCount_;
	// Perfstat counter
	PerfStatCounterST commitPerfCounter_;
	PerfStatCounterST selectPerfCounter_;
};

}  // namespace reindexer
