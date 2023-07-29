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
		: IndexUnordered<T>(idef, std::move(payloadType), fields), cache_ft_(std::make_shared<FtIdSetCache>()) {
		this->selectKeyType_ = KeyValueType::String{};
		initSearchers();
	}

	SelectKeyResults SelectKey(const VariantArray& keys, CondType, SortType, Index::SelectOpts, const BaseFunctionCtx::Ptr&,
							   const RdxContext&) override final;
	SelectKeyResults SelectKey(const VariantArray& keys, CondType, Index::SelectOpts, const BaseFunctionCtx::Ptr&, FtPreselectT&&,
							   const RdxContext&) override;
	void UpdateSortedIds(const UpdateSortedContext&) override {}
	virtual IdSet::Ptr Select(FtCtx::Ptr fctx, FtDSLQuery&& dsl, bool inTransaction, FtMergeStatuses&&, FtUseExternStatuses,
							  const RdxContext&) = 0;
	void SetOpts(const IndexOpts& opts) override;
	void Commit() override final {
		// Do nothing
		// Rebuild will be done on first select
	}
	void CommitFulltext() override final {
		cache_ft_ = std::make_shared<FtIdSetCache>();
		commitFulltextImpl();
		this->isBuilt_ = true;
	}
	void SetSortedIdxCount(int) override final {}
	bool RequireWarmupOnNsCopy() const noexcept override final { return cfg_ && cfg_->enableWarmupOnNsCopy; }
	void ClearCache() override {
		Base::ClearCache();
		cache_ft_.reset();
	}
	void ClearCache(const std::bitset<kMaxIndexes>& s) override { Base::ClearCache(s); }
	void MarkBuilt() noexcept override { assertrx(0); }
	bool IsFulltext() const noexcept override { return true; }

protected:
	using Mutex = MarkedMutex<shared_timed_mutex, MutexMark::IndexText>;

	virtual void commitFulltextImpl() = 0;
	FtCtx::Ptr prepareFtCtx(const BaseFunctionCtx::Ptr&);
	SelectKeyResults doSelectKey(const VariantArray& keys, const std::optional<IdSetCacheKey>&, FtMergeStatuses&&,
								 FtUseExternStatuses useExternSt, bool inTransaction, FtCtx::Ptr, const RdxContext&);
	SelectKeyResults resultFromCache(const VariantArray& keys, FtIdSetCache::Iterator&&, FtCtx::Ptr&);
	void build(const RdxContext& rdxCtx);

	void initSearchers();
	FieldsGetter Getter();

	std::shared_ptr<FtIdSetCache> cache_ft_;

	RHashMap<std::string, int> ftFields_;
	std::unique_ptr<BaseFTConfig> cfg_;
	Mutex mtx_;
};

}  // namespace reindexer
