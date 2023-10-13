#include "indextext.h"
#include <memory>
#include "core/rdxcontext.h"
#include "estl/overloaded.h"
#include "estl/smart_lock.h"
#include "tools/errors.h"
#include "tools/logger.h"

namespace reindexer {

template <typename T>
IndexText<T>::IndexText(const IndexText<T> &other) : IndexUnordered<T>(other), cache_ft_(std::make_shared<FtIdSetCache>()) {
	initSearchers();
}
// Generic implemetation for string index

template <typename T>
void IndexText<T>::initSearchers() {
	size_t jsonPathIdx = 0;

	if (this->payloadType_) {
		for (unsigned i = 0; i < this->fields_.size(); i++) {
			auto fieldIdx = this->fields_[i];
			if (fieldIdx == IndexValueType::SetByJsonPath) {
				assertrx(jsonPathIdx < this->fields_.getJsonPathsLength());
				ftFields_.emplace(this->fields_.getJsonPath(jsonPathIdx++), i);
			} else {
				ftFields_.emplace(this->payloadType_->Field(fieldIdx).Name(), i);
			}
		}
		if rx_unlikely (ftFields_.size() != this->fields_.size()) {
			throw Error(errParams, "Composite fulltext index '%s' contains duplicated fields", this->name_);
		}
		if rx_unlikely (ftFields_.size() > kMaxFtCompositeFields) {
			throw Error(errParams, "Unable to create composite fulltext '%s' index with %d fields. Fileds count limit is %d", this->name_,
						ftFields_.size(), kMaxFtCompositeFields);
		}
	}
}

template <typename T>
void IndexText<T>::SetOpts(const IndexOpts &opts) {
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
FtCtx::Ptr IndexText<T>::prepareFtCtx(const BaseFunctionCtx::Ptr &ctx) {
	FtCtx::Ptr ftctx = reindexer::reinterpret_pointer_cast<FtCtx>(ctx);
	if rx_unlikely (!ftctx) {
		throw Error(errParams, "Full text index (%s) may not be used without context", Index::Name());
	}
	ftctx->PrepareAreas(ftFields_, this->name_);
	return ftctx;
}

template <typename T>
void IndexText<T>::build(const RdxContext &rdxCtx) {
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

// Generic implemetation for string index
template <typename T>
SelectKeyResults IndexText<T>::SelectKey(const VariantArray &keys, CondType condition, SortType, Index::SelectOpts opts,
										 const BaseFunctionCtx::Ptr &ctx, const RdxContext &rdxCtx) {
	const auto indexWard(rdxCtx.BeforeIndexWork());
	if rx_unlikely (keys.size() < 1 || (condition != CondEq && condition != CondSet)) {
		throw Error(errParams, "Full text index (%s) support only EQ or SET condition with 1 or 2 parameter", Index::Name());
	}

	FtCtx::Ptr ftctx = prepareFtCtx(ctx);
	auto mergeStatuses = this->GetFtMergeStatuses(rdxCtx);
	bool needPutCache = false;
	IdSetCacheKey ckey{keys, condition, 0};
	auto cache_ft = cache_ft_->Get(ckey);
	if (cache_ft.valid) {
		if (!cache_ft.val.ids) {
			needPutCache = true;
		} else if (ftctx->NeedArea() && (!cache_ft.val.ctx || !cache_ft.val.ctx->need_area_)) {
			needPutCache = true;
		} else {
			return resultFromCache(keys, std::move(cache_ft), ftctx);
		}
	}
	return doSelectKey(keys, needPutCache ? std::optional{std::move(ckey)} : std::nullopt, std::move(mergeStatuses),
					   FtUseExternStatuses::No, opts.inTransaction, std::move(ftctx), rdxCtx);
}

template <typename T>
SelectKeyResults IndexText<T>::resultFromCache(const VariantArray &keys, FtIdSetCache::Iterator &&it, FtCtx::Ptr &ftctx) {
	if rx_unlikely (cfg_->logLevel >= LogInfo) {
		logPrintf(LogInfo, "Get search results for '%s' in '%s' from cache", keys[0].As<std::string>(),
				  this->payloadType_ ? this->payloadType_->Name() : "");
	}
	SelectKeyResults r;
	auto &res = r.emplace_back();
	res.emplace_back(std::move(it.val.ids));

	assertrx(it.val.ctx);
	ftctx->SetData(std::move(it.val.ctx));
	return r;
}

template <typename T>
SelectKeyResults IndexText<T>::doSelectKey(const VariantArray &keys, const std::optional<IdSetCacheKey> &ckey,
										   FtMergeStatuses &&mergeStatuses, FtUseExternStatuses useExternSt, bool inTransaction,
										   FtCtx::Ptr ftctx, const RdxContext &rdxCtx) {
	if rx_unlikely (cfg_->logLevel >= LogInfo) {
		logPrintf(LogInfo, "Searching for '%s' in '%s' %s", keys[0].As<std::string>(), this->payloadType_ ? this->payloadType_->Name() : "",
				  ckey ? "(will cache)" : "");
	}

	// STEP 1: Parse search query dsl
	FtDSLQuery dsl(this->ftFields_, this->cfg_->stopWords, this->cfg_->extraWordSymbols);
	dsl.parse(keys[0].As<std::string>());

	IdSet::Ptr mergedIds = Select(ftctx, std::move(dsl), inTransaction, std::move(mergeStatuses), useExternSt, rdxCtx);
	SelectKeyResult res;
	if (mergedIds) {
		bool need_put = (useExternSt == FtUseExternStatuses::No) && ckey.has_value();
		if (ftctx->NeedArea() && need_put && mergedIds->size()) {
			auto config = dynamic_cast<FtFastConfig *>(cfg_.get());
			if (config && config->maxTotalAreasToCache >= 0) {
				auto d = ftctx->GetData();
				size_t totalAreas = 0;
				for (auto &area : d->holders_) {
					totalAreas += d->area_[area.second].GetAreasCount();
				}
				if (totalAreas > unsigned(config->maxTotalAreasToCache)) {
					need_put = false;
				}
			}
		}
		if (need_put && mergedIds->size()) {
			// This areas will be shared via cache, so lazy commit may race
			auto d = ftctx->GetData();
			for (auto &area : d->holders_) {
				if (!d->area_[area.second].IsCommited()) {
					d->area_[area.second].Commit();
				}
			}
			cache_ft_->Put(*ckey, FtIdSetCacheVal{IdSet::Ptr(mergedIds), std::move(d)});
		}

		res.emplace_back(std::move(mergedIds));
	}
	return SelectKeyResults(std::move(res));
}

template <typename T>
SelectKeyResults IndexText<T>::SelectKey(const VariantArray &keys, CondType condition, Index::SelectOpts opts,
										 const BaseFunctionCtx::Ptr &ctx, FtPreselectT &&preselect, const RdxContext &rdxCtx) {
	const auto indexWard(rdxCtx.BeforeIndexWork());
	if rx_unlikely (keys.size() < 1 || (condition != CondEq && condition != CondSet)) {
		throw Error(errParams, "Full text index (%s) support only EQ or SET condition with 1 or 2 parameter", Index::Name());
	}
	return doSelectKey(keys, std::nullopt, std::move(preselect), FtUseExternStatuses::Yes, opts.inTransaction, prepareFtCtx(ctx), rdxCtx);
}

template <typename T>
FieldsGetter IndexText<T>::Getter() {
	return FieldsGetter(this->fields_, this->payloadType_, this->KeyType());
}

template class IndexText<unordered_str_map<FtKeyEntry>>;
template class IndexText<unordered_payload_map<FtKeyEntry, true>>;

}  // namespace reindexer
