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

template <typename T>
class IndexText : public IndexUnordered<T> {
	using Base = IndexUnordered<T>;

public:
	IndexText(const IndexText<T>& other);
	IndexText(const IndexDef& idef, PayloadType payloadType, const FieldsSet& fields)
		: IndexUnordered<T>(idef, std::move(payloadType), fields), cache_ft_(new FtIdSetCache) {
		this->selectKeyType_ = KeyValueString;
		initSearchers();
	}

	SelectKeyResults SelectKey(const VariantArray& keys, CondType condition, SortType stype, Index::SelectOpts opts,
							   BaseFunctionCtx::Ptr ctx, const RdxContext&) override final;
	void UpdateSortedIds(const UpdateSortedContext&) override {}
	virtual IdSet::Ptr Select(FtCtx::Ptr fctx, FtDSLQuery& dsl) = 0;
	void SetOpts(const IndexOpts& opts) override;
	void Commit() override final;
	void CommitFulltext() override final {
		commitFulltextImpl();
		this->isBuilt_ = true;
	}
	void SetSortedIdxCount(int) override final {}
	bool RequireWarmupOnNsCopy() const noexcept override final { return cfg_ && cfg_->enableWarmupOnNsCopy; }
	void ClearCache() override {
		Base::ClearCache();
		cache_ft_.reset();
	}
	void MarkBuilt() noexcept override { assert(0); }
	bool IsFulltext() const noexcept override { return true; }

protected:
	using Mutex = MarkedMutex<shared_timed_mutex, MutexMark::IndexText>;

	virtual void commitFulltextImpl() = 0;

	void initSearchers();
	FieldsGetter Getter();

	shared_ptr<FtIdSetCache> cache_ft_;
	fast_hash_map<string, int> ftFields_;
	std::unique_ptr<BaseFTConfig> cfg_;
	DataHolder holder_;
	Mutex mtx_;
};

}  // namespace reindexer
