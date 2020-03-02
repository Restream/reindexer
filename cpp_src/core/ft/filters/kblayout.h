#pragma once

#include "itokenfilter.h"

namespace reindexer {

class KbLayout : public ITokenFilter {
public:
	KbLayout();
	virtual void GetVariants(const std::wstring& data, std::vector<std::pair<std::wstring, int>>& result) override final;

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

}  // namespace reindexer
