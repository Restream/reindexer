#pragma once

#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include "algoritm/full_text_search/dataholder/datastruct.h"
#include "core/index.h"
using std::vector;
using std::string;
using std::set;
using std::shared_ptr;
using std::wstring;

namespace search_engine {

using std::pair;

class ISeacher {
public:
	typedef shared_ptr<ISeacher> Ptr;

	virtual void Build(const wchar_t* data, size_t len, vector<pair<HashType, ProcType>>& result) = 0;
	virtual ~ISeacher() {}
};

}  // namespace search_engine
