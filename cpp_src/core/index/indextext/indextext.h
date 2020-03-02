#pragma once

#include <mutex>
#include "core/ft/config/baseftconfig.h"
#include "core/ft/filters/itokenfilter.h"
#include "core/ft/ft_fast/dataholder.h"
#include "core/ft/ftdsl.h"
#include "core/ft/ftsetcashe.h"
#include "core/index/indexunordered.h"
#include "core/selectfunc/ctx/ftctx.h"
#include "estl/fast_hash_map.h"
#include "estl/shared_mutex.h"
#include "fieldsgetter.h"

namespace reindexer {
using std::unique_ptr;

template <typename T>
class IndexText : public IndexUnordered<T> {
public:
	IndexText(const IndexText<T>& other);
	IndexText(const IndexDef& idef, const PayloadType payloadType, const FieldsSet& fields)
		: IndexUnordered<T>(idef, payloadType, fields), cache_ft_(new FtIdSetCache), isBuilt_(false) {
		this->selectKeyType_ = KeyValueString;
		initSearchers();
	}

	SelectKeyResults SelectKey(const VariantArray& keys, CondType condition, SortType stype, Index::SelectOpts opts,
							   BaseFunctionCtx::Ptr ctx, const RdxContext&) override final;
	void UpdateSortedIds(const UpdateSortedContext&) override {}
	virtual IdSet::Ptr Select(FtCtx::Ptr fctx, FtDSLQuery& dsl) = 0;
	void SetOpts(const IndexOpts& opts) override;
	void Commit() override final;
	virtual void commitFulltext() = 0;
	void SetSortedIdxCount(int) override final{};

protected:
	using Mutex = MarkedMutex<shared_timed_mutex, MutexMark::IndexText>;

	void initSearchers();
	FieldsGetter Getter();

	shared_ptr<FtIdSetCache> cache_ft_;
	fast_hash_map<string, int> ftFields_;
	unique_ptr<BaseFTConfig> cfg_;
	DataHolder holder_;
	Mutex mtx_;
	bool isBuilt_;
};

}  // namespace reindexer
