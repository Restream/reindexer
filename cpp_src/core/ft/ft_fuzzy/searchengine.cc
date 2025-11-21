#include "searchengine.h"
#include <tools/stringstools.h>
#include <string_view>
#include "core/ft/filters/kblayout.h"
#include "core/ft/filters/translit.h"

using namespace reindexer;

namespace search_engine {

SearchEngine::SearchEngine() {
	searcher_.AddSearcher(ITokenFilter::Ptr(new Translit));
	searcher_.AddSearcher(ITokenFilter::Ptr(new KbLayout));
	holder_ = std::make_shared<BaseHolder>();
}
void SearchEngine::SetConfig(const std::unique_ptr<FtFuzzyConfig>& cfg) { holder_->SetConfig(cfg); }

void SearchEngine::AddData(std::string_view src_data, const IdType id, unsigned field, unsigned arrayIdx,
						   const reindexer::SplitOptions& splitOptions) {
	if (committed_) {
		committed_ = false;
		holder_->Clear();
	}
	searcher_.AddIndex(holder_, src_data, id, field, arrayIdx, splitOptions);
}
void SearchEngine::Commit() {
	committed_ = true;
	searcher_.Commit(holder_);
}

SearchResult SearchEngine::Search(const FtDSLQuery& dsl, bool inTransaction, const RdxContext& rdxCtx) {
	return searcher_.Compare(holder_, dsl, inTransaction, rdxCtx);
}

}  // namespace search_engine
