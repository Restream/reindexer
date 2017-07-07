#include "searchengine.h"
#include <tools/strings.h>
#include <locale>
#include <string>
#include "searchers/isearcher.h"
#include "searchers/kblayout.h"
#include "searchers/translit.h"

namespace search_engine {
using reindexer::utf8_to_utf16;

SearchEngine::SearchEngine() {
	holder_ = make_shared<BaseHolder>();
	seacher_.AddSeacher(ISeacher::Ptr(new Translit));
	seacher_.AddSeacher(ISeacher::Ptr(new KbLayout));
}

SearchEngine::SearchEngine(const SearchEngine& rhs) {
	holder_ = make_shared<BaseHolder>(*rhs.holder_.get());
	seacher_.AddSeacher(ISeacher::Ptr(new Translit));
	seacher_.AddSeacher(ISeacher::Ptr(new KbLayout));
}

SearchEngine& SearchEngine::operator=(const SearchEngine& rhs) {
	if (this == &rhs) return *this;

	holder_ = make_shared<BaseHolder>(*rhs.holder_.get());
	seacher_.AddSeacher(ISeacher::Ptr(new Translit));
	seacher_.AddSeacher(ISeacher::Ptr(new KbLayout));

	return *this;
}

void SearchEngine::Rebuild(const map<IdType, string>& data, bool only_add) {
	holder_->AddReserve(data.size());
	if (only_add) {
		for (auto& raw : data) {
			if (!raw.second.empty()) {
				seacher_.AddIndex(holder_, utf8_to_utf16(raw.second), raw.first);
			}
		}
		return;
	}
	BaseHolder::Ptr holder = make_shared<BaseHolder>();
	holder->Reinit(holder_, data);
	for (auto& raw : data) {
		if (!raw.second.empty()) {
			seacher_.AddIndex(holder, utf8_to_utf16(raw.second), raw.first);
		}
	}
	holder_ = holder;
}

BaseHolder::SearchTypePtr SearchEngine::Search(const string& data) {
	BaseHolder::Ptr search_holder = make_shared<BaseHolder>();
	return seacher_.Compare(holder_, utf8_to_utf16(data));
}

}  // namespace search_engine
