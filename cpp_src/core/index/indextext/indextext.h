#pragma once

#include "core/ft/config/baseftconfig.h"
#include "core/ft/filters/itokenfilter.h"
#include "core/ft/ft_fast/dataholder.h"
#include "core/ft/ftdsl.h"
#include "core/ft/ftsetcashe.h"
#include "core/index/indexunordered.h"
#include "core/selectfunc/ctx/ftctx.h"
#include "estl/shared_mutex.h"
#include "fieldsgetter.h"

namespace reindexer {

template <typename T>
class IndexText : public IndexUnordered<T> {
	using Base = IndexUnordered<T>;

public:
	IndexText(const IndexText<T>& other);
	IndexText(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields, const NamespaceCacheConfigData& cacheCfg)
		: IndexUnordered<T>(idef, std::move(payloadType), std::move(fields), cacheCfg),
		  cache_ft_(cacheCfg.ftIdxCacheSize, cacheCfg.ftIdxHitsToCache),
		  cacheMaxSize_(cacheCfg.ftIdxCacheSize),
		  hitsToCache_(cacheCfg.ftIdxHitsToCache) {
		this->selectKeyType_ = KeyValueType::String{};
		initSearchers();
	}

	SelectKeyResults SelectKey(const VariantArray& keys, CondType, SortType, Index::SelectOpts, const BaseFunctionCtx::Ptr&,
							   const RdxContext&) override final;
	SelectKeyResults SelectKey(const VariantArray& keys, CondType, Index::SelectOpts, const BaseFunctionCtx::Ptr&, FtPreselectT&&,
							   const RdxContext&) override;
	void UpdateSortedIds(const UpdateSortedContext&) override {}
	virtual IdSet::Ptr Select(FtCtx::Ptr ctx, FtDSLQuery&& dsl, bool inTransaction, FtSortType ftSortType, FtMergeStatuses&&,
							  FtUseExternStatuses, const RdxContext&) = 0;

	void SetOpts(const IndexOpts& opts) override;
	void Commit() override final {
		// Do nothing
		// Rebuild will be done on first select
	}
	void CommitFulltext() override final {
		cache_ft_.Reinitialize(cacheMaxSize_, hitsToCache_);
		commitFulltextImpl();
		this->isBuilt_ = true;
	}
	void SetSortedIdxCount(int) override final {}
	void DestroyCache() override {
		Base::DestroyCache();
		cache_ft_.ResetImpl();
	}
	void ClearCache() override {
		Base::ClearCache();
		cache_ft_.Clear();
	}
	void ClearCache(const std::bitset<kMaxIndexes>& s) override { Base::ClearCache(s); }
	void MarkBuilt() noexcept override { assertrx(0); }
	bool IsFulltext() const noexcept override final { return true; }
	void ReconfigureCache(const NamespaceCacheConfigData& cacheCfg) override final;
	IndexPerfStat GetIndexPerfStat() override final;
	void ResetIndexPerfStat() override final;

protected:
	using Mutex = MarkedMutex<shared_timed_mutex, MutexMark::IndexText>;

	virtual void commitFulltextImpl() = 0;
	SelectKeyResults doSelectKey(const VariantArray& keys, const std::optional<IdSetCacheKey>&, FtMergeStatuses&&,
								 FtUseExternStatuses useExternSt, bool inTransaction, FtSortType ftSortType,
								 const BaseFunctionCtx::Ptr& ctx, const RdxContext&);

	SelectKeyResults resultFromCache(const VariantArray& keys, FtIdSetCache::Iterator&&, const BaseFunctionCtx::Ptr&);
	void build(const RdxContext& rdxCtx);

	void initSearchers();
	FieldsGetter Getter();

	FtIdSetCache cache_ft_;
	size_t cacheMaxSize_;
	uint32_t hitsToCache_;

	RHashMap<std::string, int> ftFields_;
	std::unique_ptr<BaseFTConfig> cfg_;
	Mutex mtx_;
};

}  // namespace reindexer
