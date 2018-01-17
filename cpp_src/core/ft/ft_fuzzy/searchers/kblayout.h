#pragma once

#include <map>
#include <memory>
#include "isearcher.h"
using std::map;
using std::string;

namespace search_engine {

class KbLayout : public ISeacher {
public:
	typedef shared_ptr<KbLayout> Ptr;

	KbLayout();
	virtual void Build(const wchar_t* data, size_t len, vector<pair<std::wstring, ProcType>>& result) override final;
	void BuildThreeGramm(wchar_t* dst, const wchar_t* src);

private:
	void PrepareRuLayout();
	void PrepareEnLayout();

	void setEnLayout(wchar_t sym, wchar_t data);

	static const int ruLettersStartUTF16 = 1072;
	static const int allSymbolStartUTF16 = 39;

	static const int ruAlfavitSize = 32;
	static const int engAndAllSymbols = 87;

	wchar_t ru_layout_[ruAlfavitSize];
	wchar_t all_symbol_[engAndAllSymbols];
};

}  // namespace search_engine
