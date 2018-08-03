#pragma once
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include "core/ft/config/ftfuzzyconfig.h"
#include "core/ft/ftdsl.h"
#include "core/idset.h"
#include "dataholder/basebuildedholder.h"
#include "searchers/base_searcher/baseseacher.h"

namespace reindexer {
class string_view;
}

namespace search_engine {

using std::shared_ptr;
using std::unordered_map;
using std::vector;

class SearchEngine {
public:
	typedef shared_ptr<SearchEngine> Ptr;

	SearchEngine();
	void SetConfig(const unique_ptr<FtFuzzyConfig> &cfg);
	SearchEngine(const SearchEngine &rhs) = delete;

	SearchEngine &operator=(const SearchEngine &) = delete;

	SearchResult Search(const FtDSLQuery &dsl);
	void Rebuild();
	void AddData(const reindexer::string_view &src_data, const IdType id, int field, const string &extraWordSymbols);
	void Commit();

private:
	BaseHolder::Ptr holder_;
	BaseSearcher seacher_;
	size_t last_max_id_;
	bool commited_;
};
}  // namespace search_engine
