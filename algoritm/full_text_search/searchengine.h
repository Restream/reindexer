#pragma once
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include "core/idset.h"
#include "dataholder/basebuildedholder.h"
#include "searchers/base_searcher/baseseacher.h"

namespace search_engine {

using std::shared_ptr;
using std::unordered_map;
using std::vector;

class SearchEngine {
public:
	typedef shared_ptr<SearchEngine> Ptr;

	SearchEngine();
	SearchEngine(const SearchEngine &rhs);

	SearchEngine &operator=(const SearchEngine &);

	BaseHolder::SearchTypePtr Search(const string &data);
	void Rebuild(const map<IdType, string> &data, bool only_add);

private:
	BaseHolder::Ptr holder_;
	BaseSearcher seacher_;
};
}  // namespace search_engine
