#pragma once

#include <map>
#include <memory>
#include "algoritm/full_text_search/dataholder/datastruct.h"
#include "algoritm/full_text_search/searchers/isearcher.h"
#include "core/index.h"
using std::map;
using std::string;

namespace search_engine {

class KbLayout : public ISeacher {
public:
	typedef shared_ptr<KbLayout> Ptr;

	KbLayout();
	virtual void Build(const wchar_t* data, size_t len, vector<pair<HashType, ProcType>>& result) override final;

private:
	void PrepareRuLayout();
	void PrepareEnLayout();

	void setEnLayout(wchar_t sym, wchar_t data);

	static const int ruLettersStartUTF16 = 1072;
	static const int allSymbolStartUTF16 = 39;

	static const int ruAlfavitSize = 32;
	static const int engAndAllSymbols = 87;

	char ru_layout_[ruAlfavitSize];
	char all_symbol_[engAndAllSymbols];
};

}  // namespace search_engine
