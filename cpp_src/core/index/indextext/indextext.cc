
#include "indextext.h"
#include <memory>
#include "core/ft/filters/kblayout.h"
#include "core/ft/filters/synonyms.h"
#include "core/ft/filters/translit.h"
#include "core/rdxcontext.h"
#include "estl/smart_lock.h"
#include "tools/errors.h"
#include "tools/logger.h"

namespace reindexer {

// Available stemmers for languages
const char *stemLangs[] = {"en", "ru", "nl", "fin", "de", "da", "fr", "it", "hu", "no", "pt", "ro", "es", "sv", "tr", nullptr};

template <typename T>
IndexText<T>::IndexText(const IndexText<T> &other) : IndexUnordered<T>(other), cache_ft_(new FtIdSetCache), isBuilt_(false) {
	initSearchers();
}
// Generic implemetation for string index

template <typename T>
void IndexText<T>::initSearchers() {
	holder_.stemmers_.clear();
	holder_.translit_.reset(new Translit);
	holder_.kbLayout_.reset(new KbLayout);
	holder_.synonyms_.reset(new Synonyms);
	for (const char **lang = stemLangs; *lang; ++lang) {
		holder_.stemmers_.emplace(*lang, *lang);
	}

	size_t jsonPathIdx = 0;

	if (this->payloadType_) {
		for (unsigned i = 0; i < this->fields_.size(); i++) {
			auto fieldIdx = this->fields_[i];
			if (fieldIdx == IndexValueType::SetByJsonPath) {
				assert(jsonPathIdx < this->fields_.getJsonPathsLength());
				ftFields_.insert({this->fields_.getJsonPath(jsonPathIdx++), i});
			} else {
				ftFields_.insert({this->payloadType_->Field(fieldIdx).Name(), i});
			}
		}
	}
}

template <typename T>
void IndexText<T>::Commit() {
	// Do nothing
	// Rebuild will be done on first select
}

template <typename T>
void IndexText<T>::SetOpts(const IndexOpts &opts) {
	string oldCfg = this->opts_.config;

	this->opts_ = opts;

	if (oldCfg != opts.config) {
		try {
			cfg_->parse(this->opts_.config);
		} catch (...) {
			this->opts_.config = oldCfg;
			cfg_->parse(this->opts_.config);
			throw;
		}
	}
}

// Generic implemetation for string index
template <typename T>
SelectKeyResults IndexText<T>::SelectKey(const VariantArray &keys, CondType condition, SortType /*stype*/, Index::SelectOpts /*opts*/,
										 BaseFunctionCtx::Ptr ctx, const RdxContext &rdxCtx) {
	const auto indexWard(rdxCtx.BeforeIndexWork());
	if (keys.size() < 1 || (condition != CondEq && condition != CondSet)) {
		throw Error(errParams, "Full text index support only EQ or SET condition with 1 or 2 parameter");
	}

	FtCtx::Ptr ftctx = reindexer::reinterpret_pointer_cast<FtCtx>(ctx);
	assert(ftctx);
	ftctx->PrepareAreas(ftFields_, this->name_);

	bool need_put = false;
	IdSetCacheKey ckey{keys, condition, 0};
	auto cache_ft = cache_ft_->Get(ckey);
	SelectKeyResult res;
	if (cache_ft.valid) {
		if (!cache_ft.val.ids->size() || (ftctx->NeedArea() && !cache_ft.val.ctx->need_area_)) {
			need_put = true;
		} else {
			logPrintf(LogInfo, "Get search results for '%s' in '%s' from cache", keys[0].As<string>(),
					  this->payloadType_ ? this->payloadType_->Name() : "");
			res.push_back(SingleSelectKeyResult(cache_ft.val.ids));
			SelectKeyResults r(std::move(res));
			assert(cache_ft.val.ctx);
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

	smart_lock<Mutex> lck(mtx_, rdxCtx);
	if (!isBuilt_) {
		// non atomic upgrade mutex to unique
		lck.unlock();
		lck = smart_lock<Mutex>(mtx_, rdxCtx, true);
		if (!isBuilt_) {
			commitFulltext();
			need_put = false;
			isBuilt_ = true;
		}
	}

	auto mergedIds = Select(ftctx, dsl);
	if (mergedIds) {
		if (need_put && mergedIds->size()) cache_ft_->Put(ckey, FtIdSetCacheVal{mergedIds, ftctx->GetData()});

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
