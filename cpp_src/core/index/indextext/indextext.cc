#include "indextext.h"
#include <memory>
#include "core/rdxcontext.h"
#include "estl/smart_lock.h"
#include "tools/errors.h"
#include "tools/logger.h"

namespace reindexer {

template <typename T>
IndexText<T>::IndexText(const IndexText<T>& other)
	: IndexUnordered<T>(other),
	  cache_ft_(other.cacheMaxSize_, other.hitsToCache_),
	  cacheMaxSize_(other.cacheMaxSize_),
	  hitsToCache_(other.hitsToCache_) {
	cache_ft_.CopyInternalPerfStatsFrom(other.cache_ft_);
	initSearchers();
}

// Generic implementation for string index
template <typename T>
void IndexText<T>::initSearchers() {
	size_t jsonPathIdx = 0;

	if (this->payloadType_) {
		const auto& fields = this->Fields();
		for (unsigned i = 0, s = fields.size(); i < s; i++) {
			auto fieldIdx = fields[i];
			if (fieldIdx == IndexValueType::SetByJsonPath) {
				assertrx(jsonPathIdx < fields.getJsonPathsLength());
				ftFields_.emplace(fields.getJsonPath(jsonPathIdx++), i);
			} else {
				ftFields_.emplace(this->payloadType_->Field(fieldIdx).Name(), i);
			}
		}
		if rx_unlikely (ftFields_.size() != fields.size()) {
			throw Error(errParams, "Composite fulltext index '%s' contains duplicated fields", this->name_);
		}
		if rx_unlikely (ftFields_.size() > kMaxFtCompositeFields) {
			throw Error(errParams, "Unable to create composite fulltext '%s' index with %d fields. Fields count limit is %d", this->name_,
						ftFields_.size(), kMaxFtCompositeFields);
		}
	}
}

template <typename T>
void IndexText<T>::SetOpts(const IndexOpts& opts) {
	std::string oldCfg = this->opts_.config;

	this->opts_ = opts;

	if (oldCfg != opts.config) {
		try {
			cfg_->parse(this->opts_.config, ftFields_);
		} catch (...) {
			this->opts_.config = std::move(oldCfg);
			cfg_->parse(this->opts_.config, ftFields_);
			throw;
		}
	}
}

template <typename T>
void IndexText<T>::ReconfigureCache(const NamespaceCacheConfigData& cacheCfg) {
	if (cacheMaxSize_ != cacheCfg.ftIdxCacheSize || hitsToCache_ != cacheCfg.ftIdxHitsToCache) {
		cacheMaxSize_ = cacheCfg.ftIdxCacheSize;
		hitsToCache_ = cacheCfg.ftIdxHitsToCache;
		if (cache_ft_.IsActive()) {
			cache_ft_.Reinitialize(cacheMaxSize_, hitsToCache_);
		}
	}
	Base::ReconfigureCache(cacheCfg);
}

template <typename T>
IndexPerfStat IndexText<T>::GetIndexPerfStat() {
	auto stats = Base::GetIndexPerfStat();
	stats.cache = cache_ft_.GetPerfStat();
	return stats;
}

template <typename T>
void IndexText<T>::ResetIndexPerfStat() {
	Base::ResetIndexPerfStat();
	cache_ft_.ResetPerfStat();
}

template <typename T>
void IndexText<T>::build(const RdxContext& rdxCtx) {
	smart_lock<Mutex> lck(mtx_, rdxCtx);
	if (!this->isBuilt_) {
		// non atomic upgrade mutex to unique
		lck.unlock();
		lck = smart_lock<Mutex>(mtx_, rdxCtx, true);
		if (!this->isBuilt_) {
			CommitFulltext();
		}
	}
}

// Generic implementation for string index
template <typename T>
SelectKeyResults IndexText<T>::SelectKey(const VariantArray& keys, CondType condition, SortType, Index::SelectOpts opts,
										 const BaseFunctionCtx::Ptr& ctx, const RdxContext& rdxCtx) {
	const auto indexWard(rdxCtx.BeforeIndexWork());
	if rx_unlikely (keys.size() < 1 || (condition != CondEq && condition != CondSet)) {
		throw Error(errParams, "Full text index (%s) support only EQ or SET condition with 1 or 2 parameter", Index::Name());
	}

	auto mergeStatuses = this->GetFtMergeStatuses(rdxCtx);
	bool needPutCache = false;
	IdSetCacheKey ckey{keys, condition, 0};
	auto cache_ft = cache_ft_.Get(ckey);
	if (cache_ft.valid) {
		if (!cache_ft.val.IsInitialized()) {
			needPutCache = true;
		} else if (ctx->type == BaseFunctionCtx::CtxType::kFtArea &&
				   (!cache_ft.val.ctx || !(cache_ft.val.ctx->type == BaseFunctionCtx::CtxType::kFtArea))) {
			needPutCache = true;
		} else {
			return resultFromCache(keys, std::move(cache_ft), ctx);
		}
	}

	return doSelectKey(keys, needPutCache ? std::optional{std::move(ckey)} : std::nullopt, std::move(mergeStatuses),
					   FtUseExternStatuses::No, opts.inTransaction, FtSortType(opts.ftSortType), ctx, rdxCtx);
}

template <typename T>
SelectKeyResults IndexText<T>::resultFromCache(const VariantArray& keys, FtIdSetCache::Iterator&& it, const BaseFunctionCtx::Ptr& ctx) {
	if rx_unlikely (cfg_->logLevel >= LogInfo) {
		logPrintf(LogInfo, "Get search results for '%s' in '%s' from cache", keys[0].As<std::string>(),
				  this->payloadType_ ? this->payloadType_->Name() : "");
	}
	assertrx(it.val.ctx);
	FtCtx::Ptr ftctx = static_ctx_pointer_cast<FtCtx>(ctx);
	ftctx->SetData(std::move(it.val.ctx));
	return SelectKeyResult{{SingleSelectKeyResult{std::move(it.val.ids)}}};
}

template <typename T>
SelectKeyResults IndexText<T>::doSelectKey(const VariantArray& keys, const std::optional<IdSetCacheKey>& ckey,
										   FtMergeStatuses&& mergeStatuses, FtUseExternStatuses useExternSt, bool inTransaction,
										   FtSortType ftSortType, const BaseFunctionCtx::Ptr& bctx, const RdxContext& rdxCtx) {
	if rx_unlikely (cfg_->logLevel >= LogInfo) {
		logPrintf(LogInfo, "Searching for '%s' in '%s' %s", keys[0].As<std::string>(), this->payloadType_ ? this->payloadType_->Name() : "",
				  ckey ? "(will cache)" : "");
	}

	// STEP 1: Parse search query dsl
	FtDSLQuery dsl(this->ftFields_, this->cfg_->stopWords, this->cfg_->extraWordSymbols);
	dsl.parse(keys[0].As<p_string>());

	auto ftCtx = static_ctx_pointer_cast<FtCtx>(bctx);
	IdSet::Ptr mergedIds = Select(ftCtx, std::move(dsl), inTransaction, ftSortType, std::move(mergeStatuses), useExternSt, rdxCtx);
	SelectKeyResult res;
	if (mergedIds) {
		auto ftCtxDataBase = ftCtx->GetData();
		bool need_put = (useExternSt == FtUseExternStatuses::No) && ckey.has_value();
		// count the number of Areas and determine whether the request should be cached
		if (bctx->type == BaseFunctionCtx::CtxType::kFtArea && need_put && mergedIds->size()) {
			auto config = dynamic_cast<FtFastConfig*>(cfg_.get());
			auto ftCtxDataArea = static_ctx_pointer_cast<FtCtxAreaData<Area>>(ftCtxDataBase);

			if (config && config->maxTotalAreasToCache >= 0) {
				size_t totalAreas = 0;
				assertrx_throw(ftCtxDataArea->holders.has_value());
				// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
				for (auto& area : ftCtxDataArea->holders.value()) {
					totalAreas += ftCtxDataArea->area[area.second].GetAreasCount();
				}

				if (totalAreas > unsigned(config->maxTotalAreasToCache)) {
					need_put = false;
				}
			}
			if (need_put && ftCtxDataArea->holders.has_value()) {
				// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
				for (auto& area : ftCtxDataArea->holders.value()) {
					if (auto& aData = ftCtxDataArea->area[area.second]; !aData.IsCommited()) {
						aData.Commit();
					}
				}
			}
		}
		if (need_put && mergedIds->size()) {
			cache_ft_.Put(*ckey, FtIdSetCacheVal{IdSet::Ptr(mergedIds), std::move(ftCtxDataBase)});
		}

		res.emplace_back(std::move(mergedIds));
	}
	return SelectKeyResults(std::move(res));
}

template <typename T>
SelectKeyResults IndexText<T>::SelectKey(const VariantArray& keys, CondType condition, Index::SelectOpts opts,
										 const BaseFunctionCtx::Ptr& ctx, FtPreselectT&& preselect, const RdxContext& rdxCtx) {
	const auto indexWard(rdxCtx.BeforeIndexWork());
	if rx_unlikely (keys.size() < 1 || (condition != CondEq && condition != CondSet)) {
		throw Error(errParams, "Full text index (%s) support only EQ or SET condition with 1 or 2 parameter", Index::Name());
	}
	return doSelectKey(keys, std::nullopt, std::move(preselect), FtUseExternStatuses::Yes, opts.inTransaction, FtSortType(opts.ftSortType),
					   ctx, rdxCtx);
}

template <typename T>
FieldsGetter IndexText<T>::Getter() {
	return FieldsGetter(this->Fields(), this->payloadType_, this->KeyType());
}

template class IndexText<unordered_str_map<FtKeyEntry>>;
template class IndexText<unordered_payload_map<FtKeyEntry, true>>;

}  // namespace reindexer
