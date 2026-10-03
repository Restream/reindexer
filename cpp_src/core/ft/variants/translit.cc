#include "translit.h"
#include <string.h>
#include "tools/assertrx.h"
#include "tools/scope_guard.h"

namespace reindexer {

Translit::Translit() {
	PrepareRussian();
	PrepareEnglish();
}

void Translit::Transliterate(const std::u16string& data, h_vector<std::u16string, 5>& res) {
	res.resize(0);
	thread_local std::u16string strings[maxTranslitVariants];
	auto cleanup = MakeScopeGuard([] {
		for (auto& s : strings) {
			if (s.size() < 255) [[likely]] {
				s.resize(0);
			} else {
				s = std::u16string();
			}
		}
	});

	Context ctx;
	if (data.length()) {
		for (int j = 0; j < maxTranslitVariants; ++j) {
			strings[j].reserve(data.length());
		}
	}

	for (size_t i = 0; i < data.length(); ++i) {
		char16_t symbol = data[i];
		if (symbol >= ruLettersStartUTF16 && symbol <= ruLettersStartUTF16 + ruAlphabetSize - 1) {	// russian symbol
			for (int j = 0; j < maxTranslitVariants; ++j) {
				assertrx_throw(symbol >= ruLettersStartUTF16 && symbol - ruLettersStartUTF16 < ruAlphabetSize);
				strings[j] += ru_buf_[symbol - ruLettersStartUTF16][j];
			}

			ctx.Clear();

		} else if (symbol >= enLettersStartUTF16 && symbol < enLettersStartUTF16 + engAlphabetSize) {  // en symbol
			for (int j = 0; j < maxTranslitVariants; ++j) {
				auto sym = GetEnglish(symbol, j, ctx);
				if (sym.second) {
					auto& str = strings[j];
					if (sym.first) {
						str.erase(str.end() - sym.first, str.end());
					}
					str += sym.second;
				}
			}

		} else {
			for (int j = 0; j < maxTranslitVariants; ++j) {
				strings[j] += symbol;
			}

			ctx.Clear();
		}
	}

	for (int i = 0; i < maxTranslitVariants; ++i) {
		auto& current = strings[i];
		for (int j = i + 1; j < maxTranslitVariants; ++j) {
			if (current == strings[j]) {
				current.resize(0);
				break;
			}
		}

		if (!current.empty()) {
			res.emplace_back(std::move(current));
		}
	}
}

std::pair<uint8_t, char16_t> Translit::GetEnglish(char16_t symbol, size_t variant, Context& ctx) {
	assertrx_throw(symbol != 0 && symbol >= enLettersStartUTF16 && symbol - enLettersStartUTF16 < engAlphabetSize);

	if (variant == 1 && ctx.GetCount() > 0) {
		auto sym = en_d_buf_[ctx.GetLast()][symbol - enLettersStartUTF16];
		if (sym) {
			return {1, sym};
		}
	} else if (variant == 2 && ctx.GetCount() > 1) {
		auto sym = en_t_buf_[ctx.GetPrevious()][ctx.GetLast()][symbol - enLettersStartUTF16];
		ctx.Set(symbol - enLettersStartUTF16);
		if (sym) {
			return {2, sym};
		}
	}

	if (variant == 2) {
		ctx.Set(symbol - enLettersStartUTF16);
	}
	return {0, en_buf_[symbol - enLettersStartUTF16]};
}

void Translit::Context::Set(unsigned short num) {
	if (total_count_ > 0) {
		num_[1] = num_[0];
		num_[0] = num;
		total_count_ = 2;

	} else {
		num_[0] = num;
		++total_count_;
	}
}

unsigned short Translit::Context::GetLast() const { return num_[0]; }
unsigned short Translit::Context::GetPrevious() const { return num_[1]; }
unsigned short Translit::Context::GetCount() const { return total_count_; }

void Translit::Context::Clear() { total_count_ = 0; }

void Translit::PrepareRussian() {
	for (int i = 0; i < ruAlphabetSize; ++i) {
		for (int j = 0; j < maxTranslitVariants; ++j) {
			ru_buf_[i][j] = u"";
		}
	}

	ru_buf_[0][0] = u"a";	  // а
	ru_buf_[1][0] = u"b";	  // б
	ru_buf_[2][0] = u"v";	  // в
	ru_buf_[3][0] = u"g";	  // г
	ru_buf_[4][0] = u"d";	  // д
	ru_buf_[5][0] = u"e";	  // е
	ru_buf_[6][0] = u"zh";	  // ж
	ru_buf_[7][0] = u"z";	  // з
	ru_buf_[8][0] = u"i";	  // и
	ru_buf_[9][0] = u"y";	  // й
	ru_buf_[9][1] = u"j";	  // й
	ru_buf_[10][0] = u"k";	  // к
	ru_buf_[11][0] = u"l";	  // л
	ru_buf_[12][0] = u"m";	  // м
	ru_buf_[13][0] = u"n";	  // н
	ru_buf_[14][0] = u"o";	  // о
	ru_buf_[15][0] = u"p";	  // п
	ru_buf_[16][0] = u"r";	  // р
	ru_buf_[17][0] = u"s";	  // с
	ru_buf_[18][0] = u"t";	  // т
	ru_buf_[19][0] = u"u";	  // у
	ru_buf_[20][0] = u"f";	  // ф
	ru_buf_[21][0] = u"kh";	  // х
	ru_buf_[21][1] = u"h";	  // х
	ru_buf_[21][2] = u"x";	  // х
	ru_buf_[22][0] = u"c";	  // ц
	ru_buf_[23][0] = u"ch";	  // ч
	ru_buf_[24][0] = u"sh";	  // ш
	ru_buf_[25][0] = u"shh";  // щ
	ru_buf_[25][1] = u"w";	  // щ
	ru_buf_[26][0] = u"jhh";  // ъ
							  //	ru_buf_[26][1] = u"";	 //ъ
	ru_buf_[27][0] = u"ih";	  // ы
	ru_buf_[28][0] = u"jh";	  // ь
	ru_buf_[28][1] = u"'";	  // ь
	ru_buf_[29][0] = u"eh";	  // э
	ru_buf_[29][1] = u"je";	  // э
	ru_buf_[30][0] = u"ju";	  // ю
	ru_buf_[30][1] = u"yu";	  // ю
	ru_buf_[31][0] = u"ja";	  // я
	ru_buf_[31][1] = u"ya";	  // я
	ru_buf_[31][2] = u"q";	  // я

	for (int i = 0; i < ruAlphabetSize; ++i) {
		for (int j = 0; j < maxTranslitVariants; ++j) {
			if (ru_buf_[i][j].empty()) {
				ru_buf_[i][j] = ru_buf_[i][0];
			}
		}
	}
}

bool Translit::CheckIsEn(char16_t symbol) {
	return (symbol != 0 && symbol >= enLettersStartUTF16 && symbol - enLettersStartUTF16 < engAlphabetSize);
}

void Translit::PrepareEnglish() {
	memset(en_buf_, 0, sizeof(en_buf_));
	memset(en_d_buf_, 0, sizeof(en_d_buf_));
	memset(en_t_buf_, 0, sizeof(en_t_buf_));

	for (int i = 0; i < ruAlphabetSize; ++i) {
		for (int j = 0; j < maxTranslitVariants; ++j) {
			size_t length = ru_buf_[i][j].size();

			if (length == 1) {
				char16_t sym = ru_buf_[i][j][0];

				if (CheckIsEn(sym)) {
					assertrx(sym != 0 && sym >= enLettersStartUTF16 && sym - enLettersStartUTF16 < engAlphabetSize);
					en_buf_[ru_buf_[i][j][0] - enLettersStartUTF16] = char16_t(i + ruLettersStartUTF16);
				}

			} else if (length == 2 && CheckIsEn(ru_buf_[i][j][0]) && CheckIsEn(ru_buf_[i][j][1])) {
				char16_t symFirst = ru_buf_[i][j][0];
				char16_t symSecond = ru_buf_[i][j][1];

				if (CheckIsEn(symFirst) && CheckIsEn(symSecond)) {
					assertrx(symFirst != 0 && symFirst >= enLettersStartUTF16 && symFirst - enLettersStartUTF16 < engAlphabetSize);
					assertrx(symSecond != 0 && symSecond >= enLettersStartUTF16 && symSecond - enLettersStartUTF16 < engAlphabetSize);

					en_d_buf_[ru_buf_[i][j][0] - enLettersStartUTF16][ru_buf_[i][j][1] - enLettersStartUTF16] =
						char16_t(i + ruLettersStartUTF16);
				}

			} else if (length == 3 && CheckIsEn(ru_buf_[i][j][0]) && CheckIsEn(ru_buf_[i][j][1]) && CheckIsEn(ru_buf_[i][j][2])) {
				char16_t symFirst = ru_buf_[i][j][0];
				char16_t symSecond = ru_buf_[i][j][1];
				char16_t symThird = ru_buf_[i][j][2];

				if (CheckIsEn(symFirst) && CheckIsEn(symSecond) && CheckIsEn(symThird)) {
					assertrx(symFirst != 0 && symFirst >= enLettersStartUTF16 && symFirst - enLettersStartUTF16 < engAlphabetSize);
					assertrx(symSecond != 0 && symSecond >= enLettersStartUTF16 && symSecond - enLettersStartUTF16 < engAlphabetSize);
					assertrx(symThird != 0 && symThird >= enLettersStartUTF16 && symThird - enLettersStartUTF16 < engAlphabetSize);
					en_t_buf_[ru_buf_[i][j][0] - enLettersStartUTF16][ru_buf_[i][j][1] - enLettersStartUTF16]
							 [ru_buf_[i][j][2] - enLettersStartUTF16] = char16_t(i + ruLettersStartUTF16);
				}
			}
		}
	}
}
}  // namespace reindexer
