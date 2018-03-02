#pragma once

#include <chrono>
#include <thread>
#include "core/ft/bm25.h"
#include "core/ft/ft_fuzzy/searchers/kblayout.h"
#include "core/ft/ft_fuzzy/searchers/translit.h"
#include "core/ft/typos.h"
#include "indextext.h"
#include "tools/errors.h"
#include "tools/logger.h"
#include "tools/stringstools.h"

namespace reindexer {

// Available stemmers for languages
const char *stemLangs[] = {"en", "ru", "nl", "fin", "de", "da", "fr", "it", "hu", "no", "pt", "ro", "es", "sv", "tr", nullptr};

using std::thread;
using std::chrono::duration_cast;
using std::chrono::milliseconds;
using std::chrono::high_resolution_clock;

template <typename T>
IndexText<T>::IndexText(IndexType _type, const string &_name) : IndexUnordered<T>(_type, _name, IndexOpts()) {
	initSearchers();
}

template <typename T>
IndexText<T>::IndexText(const IndexText<T> &other) : IndexUnordered<T>(other), cache_ft_(other.cache_ft_) {
	initSearchers();
}

template <typename T>
void IndexText<T>::initSearchers() {
	searchers_.clear();
	stemmers_.clear();
	searchers_.push_back(search_engine::ISeacher::Ptr(new search_engine::Translit));
	searchers_.push_back(search_engine::ISeacher::Ptr(new search_engine::KbLayout));
	for (const char **lang = stemLangs; *lang; ++lang) {
		stemmers_.emplace(*lang, *lang);
	}

	if (this->payloadType_) {
		for (unsigned i = 0; i < this->fields_.size(); i++) {
			ftFields_.insert({this->payloadType_->Field(this->fields_[i]).Name(), i});
		}
	}
}

template <typename T>
void IndexText<T>::Commit(const CommitContext &ctx) {
	cache_ft_.reset(new FtIdSetCache());

	IndexUnordered<T>::Commit(ctx);

	if (!(ctx.phases() & CommitContext::PrepareForSelect)) return;

	vdocs_.clear();
	Commit();
}

// Generic implemetation for string index
template <typename T>
h_vector<pair<const string *, int>, 8> IndexText<T>::getDocFields(const typename T::key_type &doc, vector<unique_ptr<string>> &) {
	return {{doc.get(), 0}};
}

// Specific implemetation for composite index
template <>
h_vector<pair<const string *, int>, 8> IndexText<unordered_payload_map<Index::KeyEntryPlain>>::getDocFields(
	const typename unordered_payload_map<Index::KeyEntryPlain>::key_type &doc, vector<unique_ptr<string>> &strsBuf) {
	ConstPayload pl(this->payloadType_, doc);

	h_vector<pair<const string *, int>, 8> ret;
	int fieldPos = 0;
	for (auto field : fields_) {
		KeyRefs krefs;
		pl.Get(field, krefs);
		for (auto kref : krefs) {
			if (kref.Type() != KeyValueString) {
				strsBuf.emplace_back(unique_ptr<string>(new string(kref.As<string>())));
				ret.push_back({strsBuf.back().get(), fieldPos});

			} else {
				ret.push_back({(p_string(kref)).getCxxstr(), fieldPos});
			}
		}
		fieldPos++;
	}
	return ret;
}

template <typename T>
SelectKeyResults IndexText<T>::SelectKey(const KeyValues &keys, CondType condition, SortType /*stype*/, Index::ResultType /*res_type*/) {
	if (keys.size() < 1 || (condition != CondEq && condition != CondSet)) {
		throw Error(errParams, "Full text index support only EQ or SET condition with 1 or 2 parameter");
	}

	bool need_put = false;
	auto cache_ft = cache_ft_->Get(IdSetCacheKey{keys, condition, 0});
	SelectKeyResult res;
	if (cache_ft.key) {
		if (!cache_ft.val.ids->size()) {
			need_put = true;
			//;
		} else {
			res.push_back(SingleSelectKeyResult(cache_ft.val.ids));
			SelectKeyResults r(res);
			r.ctx = cache_ft.val.ctx;
			return r;
		}
	}

	if (cfg_->logLevel >= LogInfo) {
		logPrintf(LogInfo, "Searching for '%s' in '%s'", keys[0].As<string>().c_str(),
				  this->payloadType_ ? this->payloadType_->Name().c_str() : "");
	}

	FullTextCtx::Ptr ftctx = std::make_shared<FullTextCtx>();
	// STEP 1: Parse search query dsl
	FtDSLQuery dsl(this->ftFields_, this->cfg_->stopWords);
	dsl.parse(keys[0].As<string>());
	auto mergedIds = Select(ftctx, dsl);

	if (need_put && mergedIds->size()) cache_ft_->Put(*cache_ft.key, FtIdSetCacheVal{mergedIds, ftctx});

	res.push_back(SingleSelectKeyResult(mergedIds));
	SelectKeyResults r(res);
	r.ctx = ftctx;
	return r;
}

template <typename T>
void IndexText<T>::Configure(const string &config) {
	string config_nc = config;
	cfg_->parse(&config_nc[0]);
};

}  // namespace reindexer
