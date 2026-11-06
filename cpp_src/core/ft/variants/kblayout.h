#pragma once

#include <string>

namespace reindexer {

class [[nodiscard]] KbLayout {
public:
	KbLayout();
	void Transform(const std::u16string& data, std::u16string& res) const;

private:
	void PrepareRuLayout();
	void PrepareEnLayout();

	void setEnLayout(char16_t sym, char16_t data);

	static const int ruAlphabetSize = 32;
	static const int engAndAllSymbols = 87;

	char16_t ru_layout_[ruAlphabetSize];
	char16_t all_symbol_[engAndAllSymbols];
};

}  // namespace reindexer
