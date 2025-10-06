#pragma once

#include "itokenfilter.h"

namespace reindexer {

class [[nodiscard]] KbLayout final : public ITokenFilter {
public:
	KbLayout();
	void GetVariants(const std::wstring& data, ITokenFilter::ResultsStorage& result, int proc,
					 fast_hash_map<std::wstring, size_t>& patternsUsed) override final;

private:
	void PrepareRuLayout();
	void PrepareEnLayout();

	void setEnLayout(wchar_t sym, wchar_t data);

	static const int ruAlphabetSize = 32;
	static const int engAndAllSymbols = 87;

	wchar_t ru_layout_[ruAlphabetSize];
	wchar_t all_symbol_[engAndAllSymbols];
};

}  // namespace reindexer
