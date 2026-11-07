#include "kblayout.h"
#include "tools/assertrx.h"

namespace reindexer {

static const int ruLettersStartUTF16 = 1072;
static const int allSymbolStartUTF16 = 39;

void KbLayout::Transform(const std::u16string& data, std::u16string& res) const {
	res.resize(0);
	res.reserve(data.length());

	for (auto sym : data) {
		if (sym >= ruLettersStartUTF16 && sym <= ruLettersStartUTF16 + ruAlphabetSize - 1) {  // russian layout
			assertrx_throw(sym >= ruLettersStartUTF16 && sym - ruLettersStartUTF16 < ruAlphabetSize);
			res.push_back(ru_layout_[sym - ruLettersStartUTF16]);

		} else if (sym >= allSymbolStartUTF16 && sym < allSymbolStartUTF16 + engAndAllSymbols) {  // en symbol
			assertrx_throw(sym >= allSymbolStartUTF16 && sym - allSymbolStartUTF16 < engAndAllSymbols);
			res.push_back(all_symbol_[sym - allSymbolStartUTF16]);

		} else {
			res.push_back(sym);
		}
	}
}

void KbLayout::setEnLayout(char16_t sym, char16_t data) {
	assertrx_throw(((sym >= allSymbolStartUTF16) && (sym - allSymbolStartUTF16 < engAndAllSymbols)));
	all_symbol_[sym - allSymbolStartUTF16] = data;	// '
}

void KbLayout::PrepareEnLayout() {
	for (int i = 0; i < engAndAllSymbols; ++i) {
		all_symbol_[i] = i + allSymbolStartUTF16;
	}

	for (int i = 0; i < ruAlphabetSize; ++i) {
		setEnLayout(ru_layout_[i], i + ruLettersStartUTF16);
	}
	setEnLayout(u'{', u'\u0445');  // х
	setEnLayout(u'}', u'\u044A');  // ъ
	setEnLayout(u':', u'\u0436');  // ж
	setEnLayout(u'<', u'\u0431');  // б
	setEnLayout(u'>', u'\u044E');  // ю
}

void KbLayout::PrepareRuLayout() {
	ru_layout_[0] = u'f';	 // а
	ru_layout_[1] = u',';	 // б
	ru_layout_[2] = u'd';	 // в
	ru_layout_[3] = u'u';	 // г
	ru_layout_[4] = u'l';	 // д
	ru_layout_[5] = u't';	 // е
	ru_layout_[6] = u';';	 // ж
	ru_layout_[7] = u'p';	 // з
	ru_layout_[8] = u'b';	 // и
	ru_layout_[9] = u'q';	 // й
	ru_layout_[10] = u'r';	 // к
	ru_layout_[11] = u'k';	 // л
	ru_layout_[12] = u'v';	 // м
	ru_layout_[13] = u'y';	 // н
	ru_layout_[14] = u'j';	 // о
	ru_layout_[15] = u'g';	 // п
	ru_layout_[16] = u'h';	 // р
	ru_layout_[17] = u'c';	 // с
	ru_layout_[18] = u'n';	 // т
	ru_layout_[19] = u'e';	 // у
	ru_layout_[20] = u'a';	 // ф
	ru_layout_[21] = u'[';	 // х
	ru_layout_[22] = u'w';	 // ц
	ru_layout_[23] = u'x';	 // ч
	ru_layout_[24] = u'i';	 // ш
	ru_layout_[25] = u'o';	 // щ
	ru_layout_[26] = u']';	 // ъ
	ru_layout_[27] = u's';	 // ы
	ru_layout_[28] = u'm';	 // ь
	ru_layout_[29] = u'\'';	 // э
	ru_layout_[30] = u'.';	 // ю
	ru_layout_[31] = u'z';	 // я
}
KbLayout::KbLayout() {
	PrepareRuLayout();
	PrepareEnLayout();
}
}  // namespace reindexer
