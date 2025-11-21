#pragma once
#include <memory>
#include "basesearcher.h"
#include "core/ft/config/ftfuzzyconfig.h"
#include "core/ft/ftdsl.h"
#include "dataholder/basebuildedholder.h"

namespace reindexer {
class RdxContext;
}  // namespace reindexer

namespace search_engine {

class [[nodiscard]] SearchEngine {
public:
	typedef std::shared_ptr<SearchEngine> Ptr;

	SearchEngine();
	void SetConfig(const std::unique_ptr<reindexer::FtFuzzyConfig>& cfg);
	SearchEngine(const SearchEngine& rhs) = delete;

	SearchEngine& operator=(const SearchEngine&) = delete;

	SearchResult Search(const reindexer::FtDSLQuery& dsl, bool inTransaction, const reindexer::RdxContext&);

	void AddData(std::string_view src_data, const IdType id, unsigned field, unsigned arrayIdx,
				 const reindexer::SplitOptions& splitOptions);
	void Commit();

private:
	BaseHolder::Ptr holder_;
	BaseSearcher searcher_;
	bool committed_ = false;
};

}  // namespace search_engine
