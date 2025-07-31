#include "kblayout.h"

namespace reindexer {

static const int ruLettersStartUTF16 = 1072;
static const int allSymbolStartUTF16 = 39;

void KbLayout::GetVariants(const std::wstring& data, ITokenFilter::ResultsStorage& result, int proc,
						   fast_hash_map<std::wstring, size_t>& patternsUsed) {
	std::wstring result_string;
	result_string.reserve(data.length());

	for (auto sym : data) {
		if (sym >= ruLettersStartUTF16 && sym <= ruLettersStartUTF16 + ruAlphabetSize - 1) {  // russian layout
			assertrx(sym >= ruLettersStartUTF16 && sym - ruLettersStartUTF16 < ruAlphabetSize);
			result_string.push_back(ru_layout_[sym - ruLettersStartUTF16]);

		} else if (sym >= allSymbolStartUTF16 && sym < allSymbolStartUTF16 + engAndAllSymbols) {  // en symbol
			assertrx(sym >= allSymbolStartUTF16 && sym - allSymbolStartUTF16 < engAndAllSymbols);
			result_string.push_back(all_symbol_[sym - allSymbolStartUTF16]);

		} else {
			result_string.push_back(sym);
		}
	}

	AddOrUpdateVariant(result, patternsUsed, {std::move(result_string), proc, PrefAndStemmersForbidden_False});
}

void KbLayout::setEnLayout(wchar_t sym, wchar_t data) {
	assertrx(((sym >= allSymbolStartUTF16) && (sym - allSymbolStartUTF16 < engAndAllSymbols)));
	all_symbol_[sym - allSymbolStartUTF16] = data;	// '
}

void KbLayout::PrepareEnLayout() {
	for (int i = 0; i < engAndAllSymbols; ++i) {
		all_symbol_[i] = i + allSymbolStartUTF16;
	}

	for (int i = 0; i < ruAlphabetSize; ++i) {
		setEnLayout(ru_layout_[i], i + ruLettersStartUTF16);
	}
}

void KbLayout::PrepareRuLayout() {
	ru_layout_[0] = L'f';	 // а
	ru_layout_[1] = L',';	 // б
	ru_layout_[2] = L'd';	 // в
	ru_layout_[3] = L'u';	 // г
	ru_layout_[4] = L'l';	 // д
	ru_layout_[5] = L't';	 // е
	ru_layout_[6] = L';';	 // ж
	ru_layout_[7] = L'p';	 // з
	ru_layout_[8] = L'b';	 // и
	ru_layout_[9] = L'q';	 // й
	ru_layout_[10] = L'r';	 // к
	ru_layout_[11] = L'k';	 // л
	ru_layout_[12] = L'v';	 // м
	ru_layout_[13] = L'y';	 // н
	ru_layout_[14] = L'j';	 // о
	ru_layout_[15] = L'g';	 // п
	ru_layout_[16] = L'h';	 // р
	ru_layout_[17] = L'c';	 // с
	ru_layout_[18] = L'n';	 // т
	ru_layout_[19] = L'e';	 // у
	ru_layout_[20] = L'a';	 // ф
	ru_layout_[21] = L'[';	 // х
	ru_layout_[22] = L'w';	 // ц
	ru_layout_[23] = L'x';	 // ч
	ru_layout_[24] = L'i';	 // ш
	ru_layout_[25] = L'o';	 // щ
	ru_layout_[26] = L']';	 // ъ
	ru_layout_[27] = L's';	 // ы
	ru_layout_[28] = L'm';	 // ь
	ru_layout_[29] = L'\'';	 // э
	ru_layout_[30] = L'.';	 // ю
	ru_layout_[31] = L'z';	 // я
}
KbLayout::KbLayout() {
	PrepareRuLayout();
	PrepareEnLayout();
}
}  // namespace reindexer
