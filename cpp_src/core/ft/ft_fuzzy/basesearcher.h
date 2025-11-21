#pragma once

#include <string>
#include <vector>

#include "core/ft/filters/itokenfilter.h"
#include "core/ft/ft_fuzzy/dataholder/basebuildedholder.h"
#include "core/ft/ft_fuzzy/merger/basemerger.h"
#include "core/ft/ftdsl.h"

namespace reindexer {
class RdxContext;
}  // namespace reindexer

namespace search_engine {

class [[nodiscard]] BaseSearcher {
public:
	void AddSearcher(reindexer::ITokenFilter::Ptr&& searcher);
	void AddIndex(BaseHolder::Ptr& holder, std::string_view src_data, const IdType id, unsigned field, unsigned arrayIdx,
				  const reindexer::SplitOptions& splitOptions);
	SearchResult Compare(const BaseHolder::Ptr& holder, const reindexer::FtDSLQuery& dsl, bool inTransaction, const reindexer::RdxContext&);

	void Commit(BaseHolder::Ptr& holder);

private:
#ifdef FULL_LOG_FT
	std::vector<std::pair<size_t, std::string>> words;
#endif

	std::pair<bool, size_t> GetData(const BaseHolder::Ptr& holder, unsigned int i, wchar_t* buf, const wchar_t* src_data, size_t data_size);

	size_t ParseData(const BaseHolder::Ptr& holder, const std::wstring& src_data, int& max_id, int& min_id,
					 std::vector<FirstResult>& results, const reindexer::FtDslOpts& opts, double proc);

	void AddIdToInfo(Info* info, const IdType id, std::pair<PosType, ProcType> pos, uint32_t total_size);
	uint32_t FindHash(const std::wstring& data);
	std::vector<std::unique_ptr<reindexer::ITokenFilter>> searchers_;
};
}  // namespace search_engine
