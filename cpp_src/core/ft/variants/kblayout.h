#pragma once

#include <string>

namespace reindexer {

class [[nodiscard]] KbLayout {
public:
	KbLayout();
	void Transform(const std::wstring& data, std::wstring& res) const;

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
