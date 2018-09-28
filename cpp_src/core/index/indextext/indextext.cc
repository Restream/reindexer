
#include "indextext.h"
#include <chrono>
#include <memory>
#include <thread>
#include "core/ft/bm25.h"
#include "core/ft/ft_fuzzy/searchers/kblayout.h"
#include "core/ft/ft_fuzzy/searchers/translit.h"
#include "tools/errors.h"
#include "tools/logger.h"
#include "tools/stringstools.h"
#include "utf8cpp/utf8.h"
namespace reindexer {

// Available stemmers for languages
const char *stemLangs[] = {"en", "ru", "nl", "fin", "de", "da", "fr", "it", "hu", "no", "pt", "ro", "es", "sv", "tr", nullptr};

using std::chrono::duration_cast;
using std::chrono::high_resolution_clock;
using std::chrono::milliseconds;
using std::thread;
template <typename T>
IndexText<T>::IndexText(IndexType _type, const string &_name, const IndexOpts &opts) : IndexUnordered<T>(_type, _name, opts) {
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
bool IndexText<T>::Commit(const CommitContext &ctx) {
	cache_ft_.reset(new FtIdSetCache());
	IndexUnordered<T>::Commit(ctx);
	if ((ctx.phases() & CommitContext::PrepareForSelect)) {
		vdocs_.clear();
		Commit();
	}
	return true;
}

template <typename T>
void IndexText<T>::SetOpts(const IndexOpts &opts) {
	string oldCfg = this->opts_.config;

	this->opts_ = opts;

	if (oldCfg != opts.config) {
		auto newCfg = this->opts_.config;
		cfg_->parse(&newCfg[0]);
	}
}

// Generic implemetation for string index
template <typename T>
h_vector<pair<string_view, int>, 8> IndexText<T>::getDocFields(const typename T::key_type &doc, vector<unique_ptr<string>> &) {
	if (!utf8::is_valid(doc->cbegin(), doc->cend())) throw Error(errParams, "Invalid UTF8 string in FullText index");

	return {{string_view(*doc.get()), 0}};
}

// Specific implemetation for composite index
template <>
h_vector<pair<string_view, int>, 8> IndexText<unordered_payload_map<Index::KeyEntryPlain>>::getDocFields(
	const typename unordered_payload_map<Index::KeyEntryPlain>::key_type &doc, vector<unique_ptr<string>> &strsBuf) {
	ConstPayload pl(this->payloadType_, doc);

	int fieldPos = 0;
	size_t tagsPathIdx = 0;
	h_vector<pair<string_view, int>, 8> ret;

	for (auto field : fields_) {
		KeyRefs krefs;
		bool fieldFromCjson = (field == IndexValueType::SetByJsonPath);
		if (fieldFromCjson) {
			assert(tagsPathIdx < fields_.getTagsPathsLength());
			pl.GetByJsonPath(fields_.getTagsPath(tagsPathIdx++), krefs, this->KeyType());
		} else {
			pl.Get(field, krefs);
		}
		for (auto kref : krefs) {
			if (kref.Type() != KeyValueString) {
				strsBuf.emplace_back(unique_ptr<string>(new string(kref.As<string>())));
				ret.push_back({*strsBuf.back().get(), fieldPos});
			} else {
				p_string pstr(kref);
				const string_view stringRef(pstr.data(), pstr.length());
				if (!utf8::is_valid(stringRef.data(), stringRef.data() + stringRef.size()))
					throw Error(errParams, "Invalid UTF8 string in FullTextindex");
				ret.push_back({stringRef, fieldPos});
			}
		}
		fieldPos++;
	}
	return ret;
}

template <typename T>
SelectKeyResults IndexText<T>::SelectKey(const KeyValues &keys, CondType condition, SortType /*stype*/, Index::ResultType /*res_type*/,
										 BaseFunctionCtx::Ptr ctx) {
	if (keys.size() < 1 || (condition != CondEq && condition != CondSet)) {
		throw Error(errParams, "Full text index support only EQ or SET condition with 1 or 2 parameter");
	}

	++this->rawQueriesCount_;

	FtCtx::Ptr ftctx = reindexer::reinterpret_pointer_cast<FtCtx>(ctx);
	assert(ftctx);
	ftctx->PrepareAreas(ftFields_, this->name_);

	bool need_put = false;
	auto cache_ft = cache_ft_->Get(IdSetCacheKey{keys, condition, 0});
	SelectKeyResult res;
	if (cache_ft.key) {
		if (!cache_ft.val.ids->size() || (ftctx->NeedArea() && !cache_ft.val.ctx->need_area_)) {
			need_put = true;
		} else {
			logPrintf(LogInfo, "Get search results for '%s' in '%s' from cache", keys[0].As<string>().c_str(),
					  this->payloadType_ ? this->payloadType_->Name().c_str() : "");
			res.push_back(SingleSelectKeyResult(cache_ft.val.ids));
			SelectKeyResults r(res);
			assert(cache_ft.val.ctx);
			ftctx->SetData(cache_ft.val.ctx);
			return r;
		}
	}

	if (cfg_->logLevel >= LogInfo) {
		logPrintf(LogInfo, "Searching for '%s' in '%s' %s", keys[0].As<string>().c_str(),
				  this->payloadType_ ? this->payloadType_->Name().c_str() : "", need_put ? "(will cache)" : "");
	}

	// STEP 1: Parse search query dsl
	FtDSLQuery dsl(this->ftFields_, this->cfg_->stopWords, this->cfg_->extraWordSymbols);
	dsl.parse(keys[0].As<string>());
	auto mergedIds = Select(ftctx, dsl);

	if (need_put && mergedIds->size()) cache_ft_->Put(*cache_ft.key, FtIdSetCacheVal{mergedIds, ftctx->GetData()});

	res.push_back(SingleSelectKeyResult(mergedIds));
	SelectKeyResults r(res);
	return r;
}

template class IndexText<unordered_str_map<Index::KeyEntryPlain>>;
template class IndexText<unordered_payload_map<Index::KeyEntryPlain>>;
}  // namespace reindexer
