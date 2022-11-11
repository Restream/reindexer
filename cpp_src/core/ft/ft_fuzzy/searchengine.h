#pragma once
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include "baseseacher.h"
#include "core/ft/config/ftfuzzyconfig.h"
#include "core/ft/ftdsl.h"
#include "core/idset.h"
#include "dataholder/basebuildedholder.h"

namespace reindexer {
class RdxContext;
}  // namespace reindexer

namespace search_engine {

class SearchEngine {
public:
	typedef std::shared_ptr<SearchEngine> Ptr;

	SearchEngine();
	void SetConfig(const std::unique_ptr<FtFuzzyConfig> &cfg);
	SearchEngine(const SearchEngine &rhs) = delete;

	SearchEngine &operator=(const SearchEngine &) = delete;

	SearchResult Search(const FtDSLQuery &dsl, bool inTransaction, const reindexer::RdxContext &);
	void Rebuild();
	void AddData(std::string_view src_data, const IdType id, int field, const std::string &extraWordSymbols);
	void Commit();

private:
	BaseHolder::Ptr holder_;
	BaseSearcher seacher_;
	size_t last_max_id_;
	bool commited_;
};

}  // namespace search_engine
