
#include "indextext.h"
#include <memory>
#include "core/rdxcontext.h"
#include "estl/smart_lock.h"
#include "tools/errors.h"
#include "tools/logger.h"

namespace reindexer {

template <typename T>
IndexText<T>::IndexText(const IndexText<T> &other) : IndexUnordered<T>(other), cache_ft_(new FtIdSetCache) {
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
				ftFields_.insert({this->fields_.getJsonPath(jsonPathIdx++), i});
			} else {
				ftFields_.insert({this->payloadType_->Field(fieldIdx).Name(), i});
			}
		}
	}
}

template <typename T>
void IndexText<T>::SetOpts(const IndexOpts &opts) {
	string oldCfg = this->opts_.config;

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

// Generic implemetation for string index
template <typename T>
SelectKeyResults IndexText<T>::SelectKey(const VariantArray &keys, CondType condition, SortType /*stype*/, Index::SelectOpts opts,
										 BaseFunctionCtx::Ptr ctx, const RdxContext &rdxCtx) {
	const auto indexWard(rdxCtx.BeforeIndexWork());
	if (keys.size() < 1 || (condition != CondEq && condition != CondSet)) {
		throw Error(errParams, "Full text index (%s) support only EQ or SET condition with 1 or 2 parameter", Index::Name());
	}

	FtCtx::Ptr ftctx = reindexer::reinterpret_pointer_cast<FtCtx>(ctx);
	if (!ftctx) {
		throw Error(errParams, "Full text index (%s) may not be used without context", Index::Name());
	}
	ftctx->PrepareAreas(ftFields_, this->name_);

	smart_lock<Mutex> lck(mtx_, rdxCtx);
	if (!this->isBuilt_) {
		// non atomic upgrade mutex to unique
		lck.unlock();
		lck = smart_lock<Mutex>(mtx_, rdxCtx, true);
		if (!this->isBuilt_) {
			CommitFulltext();
		}
	}

	bool need_put = false;
	IdSetCacheKey ckey{keys, condition, 0};
	SelectKeyResult res;
	auto cache_ft = cache_ft_->Get(ckey);
	if (cache_ft.valid) {
		if (!cache_ft.val.ids->size() || (ftctx->NeedArea() && !cache_ft.val.ctx->need_area_)) {
			need_put = true;
		} else {
			if (cfg_->logLevel >= LogInfo) {
				logPrintf(LogInfo, "Get search results for '%s' in '%s' from cache", keys[0].As<string>(),
						  this->payloadType_ ? this->payloadType_->Name() : "");
			}
			res.push_back(SingleSelectKeyResult(cache_ft.val.ids));
			SelectKeyResults r(std::move(res));
			assertrx(cache_ft.val.ctx);
			ftctx->SetData(cache_ft.val.ctx);
			return r;
		}
	}

	if (cfg_->logLevel >= LogInfo) {
		logPrintf(LogInfo, "Searching for '%s' in '%s' %s", keys[0].As<string>(), this->payloadType_ ? this->payloadType_->Name() : "",
				  need_put ? "(will cache)" : "");
	}

	// STEP 1: Parse search query dsl
	FtDSLQuery dsl(this->ftFields_, this->cfg_->stopWords, this->cfg_->extraWordSymbols);
	dsl.parse(keys[0].As<string>());

	auto mergedIds = Select(ftctx, dsl, opts.inTransaction, rdxCtx);
	if (mergedIds) {
		if (ftctx->NeedArea() && need_put && mergedIds->size()) {
			auto config = dynamic_cast<FtFastConfig *>(cfg_.get());
			if (config && config->maxTotalAreasToCache >= 0) {
				auto d = ftctx->GetData();
				size_t totalAreas = 0;
				for (auto &area : d->holders_) {
					totalAreas += area.second->GetAreasCount();
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
				if (!area.second->IsCommited()) {
					area.second->Commit();
				}
			}
			cache_ft_->Put(ckey, FtIdSetCacheVal{mergedIds, std::move(d)});
		}

		res.push_back(SingleSelectKeyResult(mergedIds));
	}
	return SelectKeyResults(std::move(res));
}

template <typename T>
FieldsGetter IndexText<T>::Getter() {
	return FieldsGetter(this->fields_, this->payloadType_, this->KeyType());
}

template class IndexText<unordered_str_map<FtKeyEntry>>;
template class IndexText<unordered_payload_map<FtKeyEntry, true>>;

}  // namespace reindexer
