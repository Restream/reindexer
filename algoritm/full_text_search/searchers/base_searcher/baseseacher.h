#pragma once
#include "algoritm/full_text_search/dataholder/basebuildedholder.h"
#include "algoritm/full_text_search/searchers/isearcher.h"

#include <string>
#include <vector>

namespace search_engine {
using std::vector;
using std::wstring;
using std::pair;

class BaseSearcher {
public:
	void AddSeacher(ISeacher::Ptr seacher);
	void AddIndex(BaseHolder::Ptr holder, const wstring &src_data, const IdType ids = {});
	BaseHolder::SearchTypePtr Compare(BaseHolder::Ptr holder, const wstring &src_data);

private:
	const ProcType BaseCompareProcent = 100;
	const ProcType max_proc = 100;

	void BuildThread(size_t pos, const wstring &data);
	const wchar_t *GetData(size_t i, wchar_t *buf, const wstring &src_data, size_t data_size);
	void AddIdToInfo(Info *info, const IdType id, pair<PosType, ProcType> pos, uint32_t total_size);
	uint32_t FindHash(const wstring &data);
	vector<ISeacher::Ptr> searchers_;
};
}  // namespace search_engine
