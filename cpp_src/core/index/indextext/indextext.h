#pragma once

#include "core/enums.h"
#include "core/ft/config/baseftconfig.h"
#include "core/ft/filters/itokenfilter.h"
#include "core/ft/ft_fast/dataholder.h"
#include "core/ft/ftctx.h"
#include "core/ft/ftdsl.h"
#include "core/ft/ftsetcashe.h"
#include "core/index/indexunordered.h"
#include "estl/marked_mutex.h"
#include "estl/shared_mutex.h"
#include "fieldsgetter.h"

namespace reindexer {

template <typename Map>
class [[nodiscard]] IndexText : public IndexUnordered<Map> {
	using Base = IndexUnordered<Map>;

public:
	IndexText(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields, const NamespaceCacheConfigData& cacheCfg);

	SelectKeyResults SelectKey(const VariantArray& keys, CondType, SortType, const Index::SelectContext&, const RdxContext&) override final;
	SelectKeyResults SelectKey(const VariantArray& keys, CondType, const Index::SelectContext&, FtPreselectT&&, const RdxContext&) override;
	void UpdateSortedIds(const IUpdateSortedContext&) override { assertrx_dbg(!IsSupportSortedIdsBuild()); }
	bool IsSupportSortedIdsBuild() const noexcept override { return false; }
	virtual IdSet::Ptr Select(FtCtx&, FtDSLQuery&& dsl, bool inTransaction, RankSortType, FtMergeStatuses&&, FtUseExternStatuses,
							  const RdxContext&) = 0;
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
	void MarkBuilt() noexcept override { assertrx(0); }
	bool IsFulltext() const noexcept override final { return true; }
	void ReconfigureCache(const NamespaceCacheConfigData& cacheCfg) override final;
	IndexPerfStat GetIndexPerfStat() override final;
	void ResetIndexPerfStat() override final;
	QueryRankType RankedType() const noexcept override final { return QueryRankType::FullText; }

protected:
	using Mutex = MarkedMutex<shared_timed_mutex, MutexMark::IndexText>;

	IndexText(const IndexText<Map>& other);
	virtual void commitFulltextImpl() = 0;
	SelectKeyResults doSelectKey(std::string_view key, FtDSLQuery&&, std::optional<IdSetCacheKey>&&, FtMergeStatuses&&,
								 FtUseExternStatuses useExternSt, bool inTransaction, RankSortType, FtCtx&, const RdxContext&);

	SelectKeyResults resultFromCache(std::string_view key, FtIdSetCache::Iterator&&, FtCtx&, RanksHolder::Ptr&);
	void build(const RdxContext& rdxCtx) RX_REQUIRES(!mtx_);

	void initSearchers();
	FieldsGetter Getter();

	FtIdSetCache cache_ft_;
	size_t cacheMaxSize_;
	uint32_t hitsToCache_;

	RHashMap<std::string, FtIndexFieldPros> ftFields_;
	std::unique_ptr<BaseFTConfig> cfg_;
	Mutex mtx_;
};

}  // namespace reindexer
