#include "searchengine.h"
#include <tools/stringstools.h>
#include <string>
#include <string_view>
#include "core/ft/filters/kblayout.h"
#include "core/ft/filters/translit.h"

using namespace reindexer;

namespace search_engine {

SearchEngine::SearchEngine() {
	seacher_.AddSeacher(ITokenFilter::Ptr(new Translit));
	seacher_.AddSeacher(ITokenFilter::Ptr(new KbLayout));
	last_max_id_ = 0;
	holder_ = std::make_shared<BaseHolder>();
	commited_ = false;
}
void SearchEngine::SetConfig(const std::unique_ptr<FtFuzzyConfig>& cfg) { holder_->SetConfig(cfg); }

void SearchEngine::Rebuild() { holder_.reset(new BaseHolder); }
void SearchEngine::AddData(std::string_view src_data, const IdType id, int field, const std::string& extraWordSymbols) {
	if (commited_) {
		commited_ = false;
		holder_->Clear();
	}
	seacher_.AddIndex(holder_, src_data, id, field, extraWordSymbols);
}
void SearchEngine::Commit() {
	commited_ = true;
	seacher_.Commit(holder_);
}

SearchResult SearchEngine::Search(const FtDSLQuery& dsl, bool inTransaction, const RdxContext& rdxCtx) {
	return seacher_.Compare(holder_, dsl, inTransaction, rdxCtx);
}

}  // namespace search_engine
