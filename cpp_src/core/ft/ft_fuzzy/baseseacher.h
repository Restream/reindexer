#pragma once
#include "core/ft/config/ftfastconfig.h"
#include "core/ft/filters/itokenfilter.h"
#include "core/ft/ft_fuzzy/dataholder/basebuildedholder.h"
#include "core/ft/ft_fuzzy/merger/basemerger.h"
#include "core/ft/ftdsl.h"

#include <string>
#include <vector>

namespace reindexer {
class string_view;
}

namespace search_engine {
using std::vector;
using std::wstring;
using std::pair;
using reindexer::ITokenFilter;

class BaseSearcher {
public:
	void AddSeacher(ITokenFilter::Ptr &&seacher);
	void AddIndex(BaseHolder::Ptr holder, const reindexer::string_view &src_data, const IdType id, int field,
				  const string &extraWordSymbols);
	SearchResult Compare(BaseHolder::Ptr holder, const reindexer::FtDSLQuery &dsl);

	void Commit(BaseHolder::Ptr holder);

private:
#ifdef FULL_LOG_FT
	std::vector<std::pair<size_t, std::string>> words;
#endif

	pair<bool, size_t> GetData(BaseHolder::Ptr holder, unsigned int i, wchar_t *buf, const wchar_t *src_data, size_t data_size);

	size_t ParseData(BaseHolder::Ptr holder, const wstring &src_data, int &max_id, int &min_id, std::vector<FirstResult> &rusults,
					 const FtDslOpts &opts, double proc);

	void AddIdToInfo(Info *info, const IdType id, pair<PosType, ProcType> pos, uint32_t total_size);
	uint32_t FindHash(const wstring &data);
	vector<std::unique_ptr<ITokenFilter>> searchers_;
};
}  // namespace search_engine
