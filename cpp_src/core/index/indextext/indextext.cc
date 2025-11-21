#include "indextext.h"
#include <memory>
#include "core/dbconfig.h"
#include "core/ft/functions/ft_function.h"
#include "core/rdxcontext.h"
#include "estl/smart_lock.h"
#include "tools/errors.h"
#include "tools/logger.h"

namespace reindexer {

static FtCtx::Ptr createFtCtx(const Index::SelectContext& selectCtx) {
	assertrx_throw(selectCtx.selectFuncCtx);
	assertrx_dbg(!selectCtx.selectFuncCtx->ranks);
	selectCtx.selectFuncCtx->ranks = make_intrusive<RanksHolder>();
	return selectCtx.selectFuncCtx->selectFunc.CreateCtx(selectCtx.selectFuncCtx->indexNo, selectCtx.selectFuncCtx->ranks);
}

template <typename T>
IndexText<T>::IndexText(const IndexText<T>& other)
	: IndexUnordered<T>(other),
	  cache_ft_(other.cacheMaxSize_, other.hitsToCache_),
	  cacheMaxSize_(other.cacheMaxSize_),
	  hitsToCache_(other.hitsToCache_) {
	cache_ft_.CopyInternalPerfStatsFrom(other.cache_ft_);
	initSearchers();
}

template <typename T>
IndexText<T>::IndexText(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields, const NamespaceCacheConfigData& cacheCfg)
	: IndexUnordered<T>(idef, std::move(payloadType), std::move(fields), cacheCfg),
	  cache_ft_(cacheCfg.ftIdxCacheSize, cacheCfg.ftIdxHitsToCache),
	  cacheMaxSize_(cacheCfg.ftIdxCacheSize),
	  hitsToCache_(cacheCfg.ftIdxHitsToCache) {
	this->selectKeyType_ = KeyValueType::String{};
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
				ftFields_.emplace(fields.getJsonPath(jsonPathIdx++), FtIndexFieldPros{.isIndexed = false, .fieldNumber = i});
			} else {
				ftFields_.emplace(this->payloadType_->Field(fieldIdx).Name(), FtIndexFieldPros{.isIndexed = true, .fieldNumber = i});
			}
		}
		if (ftFields_.size() != fields.size()) [[unlikely]] {
			throw Error(errParams, "Composite fulltext index '{}' contains duplicated fields", this->name_);
		}
		if (ftFields_.size() > kMaxFtCompositeFields) [[unlikely]] {
			throw Error(errParams, "Unable to create composite fulltext '{}' index with {} fields. Fields count limit is {}", this->name_,
						ftFields_.size(), kMaxFtCompositeFields);
		}
	}
}

template <typename T>
void IndexText<T>::SetOpts(const IndexOpts& opts) {
	std::string oldCfg = this->opts_.Config();

	this->opts_ = opts;

	if (oldCfg != opts.Config()) {
		try {
			cfg_->parse(this->opts_.Config(), ftFields_);
		} catch (...) {
			this->opts_.SetConfig(this->Type(), std::move(oldCfg));
			cfg_->parse(this->opts_.Config(), ftFields_);
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
	smart_lock lckNonUnique(mtx_, rdxCtx, NonUnique);
	if (!this->isBuilt_) {
		// non atomic upgrade mutex to unique
		lckNonUnique.unlock();
		smart_lock lckUnique(mtx_, rdxCtx, Unique);
		if (!this->isBuilt_) {
			CommitFulltext();
		}
	}
}

// Generic implementation for string index
template <typename T>
SelectKeyResults IndexText<T>::SelectKey(const VariantArray& keys, CondType condition, SortType sortId,
										 const Index::SelectContext& selectCtx, const RdxContext& rdxCtx) {
	const auto indexWard(rdxCtx.BeforeIndexWork());

	if (condition == CondAny && selectCtx.opts.distinct) {
		if (IsComposite(this->Type())) [[unlikely]] {
			throw Error(errParams, "Composite full text index ({}) does not support DISTINCT", Index::Name());
		}
		return Base::SelectKey(keys, condition, sortId, selectCtx, rdxCtx);
	}
	if (selectCtx.opts.distinct) [[unlikely]] {
		throw Error(errParams, "Unexpected condition '{}' with DISTINCT in full text index ({})", CondTypeToStr(condition), Index::Name());
	}
	if (keys.size() < 1 || (condition != CondEq && condition != CondSet)) [[unlikely]] {
		throw Error(errParams, "Full text index ({}) support only EQ or SET condition with 1 or 2 parameter", Index::Name());
	}

	// Parse search query DSL here to perform strict mode validation before cache check
	FtDSLQuery dsl(this->ftFields_, cfg_->stopWords, cfg_->splitOptions, selectCtx.opts.strictMode);
	const std::string_view key = keys[0].As<p_string>();
	dsl.Parse(keys[0].As<p_string>());

	auto mergeStatuses = this->GetFtMergeStatuses(rdxCtx);
	bool needPutCache = false;
	const auto rankSortType = RankSortType(selectCtx.opts.rankSortType);
	IdSetCacheKey ckey{keys, condition, rankSortType};
	auto cache_ft = cache_ft_.Get(ckey);
	FtCtx::Ptr ftCtx = createFtCtx(selectCtx);
	if (cache_ft.valid) {
		if (!cache_ft.val.IsInitialized()) {
			needPutCache = true;
		} else if (ftCtx->Type() == FtCtxType::kFtArea && (!cache_ft.val.ctx || cache_ft.val.ctx->type != FtCtxType::kFtArea)) {
			needPutCache = true;
		} else {
			// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
			return resultFromCache(key, std::move(cache_ft), *ftCtx, selectCtx.selectFuncCtx->ranks);
		}
	}

	return doSelectKey(key, std::move(dsl), needPutCache ? std::optional{std::move(ckey)} : std::nullopt, std::move(mergeStatuses),
					   FtUseExternStatuses::No, selectCtx.opts.inTransaction, rankSortType, *ftCtx, rdxCtx);
}

template <typename T>
SelectKeyResults IndexText<T>::resultFromCache(std::string_view key, FtIdSetCache::Iterator&& it, FtCtx& ftCtx, RanksHolder::Ptr& ranks) {
	if (cfg_->logLevel >= LogInfo) [[unlikely]] {
		logFmt(LogInfo, "Get search results for '{}' in '{}' from cache", key, this->payloadType_ ? this->payloadType_->Name() : "");
	}
	assertrx(it.val.ctx);
	ftCtx.SetData(std::move(it.val.ctx));
	ranks = ftCtx.RanksPtr();
	return SelectKeyResult{{SingleSelectKeyResult{std::move(it.val.ids)}}};
}

template <typename T>
SelectKeyResults IndexText<T>::doSelectKey(std::string_view key, FtDSLQuery&& dsl, std::optional<IdSetCacheKey>&& ckey,
										   FtMergeStatuses&& mergeStatuses, FtUseExternStatuses useExternSt, bool inTransaction,
										   RankSortType rankSortType, FtCtx& ftCtx, const RdxContext& rdxCtx) {
	if (cfg_->logLevel >= LogInfo) [[unlikely]] {
		logFmt(LogInfo, "Searching for '{}' in '{}' {}", key, this->payloadType_ ? this->payloadType_->Name() : "",
			   ckey ? "(will cache)" : "");
	}

	IdSet::Ptr mergedIds = Select(ftCtx, std::move(dsl), inTransaction, rankSortType, std::move(mergeStatuses), useExternSt, rdxCtx);
	SelectKeyResult res;
	if (mergedIds) {
		auto ftCtxDataBase = ftCtx.GetData();
		bool need_put = (useExternSt == FtUseExternStatuses::No) && ckey.has_value();
		// count the number of Areas and determine whether the request should be cached
		if (ftCtx.Type() == FtCtxType::kFtArea && need_put && mergedIds->size()) {
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
					if (auto& aData = ftCtxDataArea->area[area.second]; !aData.IsCommitted()) {
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
SelectKeyResults IndexText<T>::SelectKey(const VariantArray& keys, CondType condition, const Index::SelectContext& selectCtx,
										 FtPreselectT&& preselect, const RdxContext& rdxCtx) {
	const auto indexWard(rdxCtx.BeforeIndexWork());
	if (keys.size() < 1 || (condition != CondEq && condition != CondSet)) [[unlikely]] {
		throw Error(errParams, "Full text index ({}) support only EQ or SET condition with 1 or 2 parameter", Index::Name());
	}
	// Parse search query DSL here to perform strict mode validation before cache check
	FtDSLQuery dsl(this->ftFields_, cfg_->stopWords, cfg_->splitOptions, selectCtx.opts.strictMode);
	const std::string_view key = keys[0].As<p_string>();
	dsl.Parse(keys[0].As<p_string>());

	FtCtx::Ptr ftCtx = createFtCtx(selectCtx);
	return doSelectKey(key, std::move(dsl), std::nullopt, std::move(preselect), FtUseExternStatuses::Yes, selectCtx.opts.inTransaction,
					   RankSortType(selectCtx.opts.rankSortType), *ftCtx, rdxCtx);
}

template <typename T>
FieldsGetter IndexText<T>::Getter() {
	return FieldsGetter(this->Fields(), this->payloadType_, this->KeyType());
}

template class IndexText<unordered_str_map<FtKeyEntry>>;
template class IndexText<unordered_payload_map<FtKeyEntry, true>>;

}  // namespace reindexer
