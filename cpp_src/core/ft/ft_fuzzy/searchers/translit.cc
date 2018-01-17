#include "translit.h"
#include <cstring>
#include <memory>
#include <sstream>

#include <memory>
#include "tools/customhash.h"
namespace search_engine {

using std::make_shared;
using std::wstringstream;
using std::make_pair;
Translit::Translit() {
	PrepareRussian();
	PrepareEnglish();
}

void Translit::Build(const wchar_t *data, size_t len, vector<pair<std::wstring, ProcType>> &result) {
	std::wstring strings[maxTraslitVariants];
	Context ctx;

	for (size_t i = 0; i < len; ++i) {
		wchar_t symbol = data[i];
		if (symbol >= ruLettersStartUTF16 && symbol <= ruLettersStartUTF16 + ruAlfavitSize - 1) {  // russian symbol
			for (int j = 0; j < maxTraslitVariants; ++j) {
				assert(symbol >= ruLettersStartUTF16 && symbol - ruLettersStartUTF16 < ruAlfavitSize);
				strings[j] += ru_buf_[symbol - ruLettersStartUTF16][j];
			}

			ctx.Clear();

		} else if (symbol >= enLettersStartUTF16 && symbol < enLettersStartUTF16 + enAlfavitSize) {  // en symbol
			for (int j = 0; j < maxTraslitVariants; ++j) {
				auto sym = GetEnglish(symbol, j, ctx);
				if (sym.second) {
					if (sym.first) {
						strings[j].erase(strings[j].end() - sym.first, strings[j].end());
					}
					strings[j] += sym.second;
				}
			}

		} else {
			for (int j = 0; j < maxTraslitVariants; ++j) {
				strings[j] += symbol;
			}

			ctx.Clear();
		}
	}

	wstring result_string;

	for (int i = 0; i < maxTraslitVariants; ++i) {
		wstring &curent = strings[i];
		bool skip = false;
		for (int j = i + 1; j < maxTraslitVariants; ++j) {
			if (curent == strings[j]) skip = true;
		}
		if (!skip && curent != result_string && curent.length()) {
			result_string = curent;
			result.push_back({std::move(curent), 90});
		}
	}
}

pair<uint8_t, wchar_t> Translit::GetEnglish(wchar_t symbol, size_t variant, Context &ctx) {
	assert(symbol != 0 && symbol >= enLettersStartUTF16 && symbol - enLettersStartUTF16 < enAlfavitSize);

	if (variant == 1 && ctx.GetCount() > 0) {
		auto sym = en_d_buf_[ctx.GetLast()][symbol - enLettersStartUTF16];
		if (sym) return make_pair(1, sym);
	} else if (variant == 2 && ctx.GetCount() > 1) {
		auto sym = en_t_buf_[ctx.GetPrevios()][ctx.GetLast()][symbol - enLettersStartUTF16];
		ctx.Set(symbol - enLettersStartUTF16);
		if (sym) return make_pair(2, sym);
	}

	if (variant == 2) {
		ctx.Set(symbol - enLettersStartUTF16);
	}
	return make_pair(0, en_buf_[symbol - enLettersStartUTF16]);
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
unsigned short Translit::Context::GetPrevios() const { return num_[1]; }
unsigned short Translit::Context::GetCount() const { return total_count_; }

void Translit::Context::Clear() { total_count_ = 0; }

void Translit::PrepareRussian() {
	for (int i = 0; i < ruAlfavitSize; ++i) {
		for (int j = 0; j < maxTraslitVariants; ++j) {
			ru_buf_[i][j] = L"";
		}
	}

	ru_buf_[0][0] = L"a";	 //а
	ru_buf_[1][0] = L"b";	 //б
	ru_buf_[2][0] = L"v";	 //в
	ru_buf_[3][0] = L"g";	 //г
	ru_buf_[4][0] = L"d";	 //д
	ru_buf_[5][0] = L"e";	 //е
	ru_buf_[6][0] = L"zh";	//ж
	ru_buf_[7][0] = L"z";	 //з
	ru_buf_[8][0] = L"i";	 //и
	ru_buf_[9][0] = L"y";	 //й
	ru_buf_[9][1] = L"j";	 //й
	ru_buf_[10][0] = L"k";	//к
	ru_buf_[11][0] = L"l";	//л
	ru_buf_[12][0] = L"m";	//м
	ru_buf_[13][0] = L"n";	//н
	ru_buf_[14][0] = L"o";	//о
	ru_buf_[15][0] = L"p";	//п
	ru_buf_[16][0] = L"r";	//р
	ru_buf_[17][0] = L"s";	//с
	ru_buf_[18][0] = L"t";	//т
	ru_buf_[19][0] = L"u";	//у
	ru_buf_[20][0] = L"f";	//ф
	ru_buf_[21][0] = L"kh";   //х
	ru_buf_[21][1] = L"h";	//х
	ru_buf_[21][2] = L"x";	//х
	ru_buf_[22][0] = L"c";	//ц
	ru_buf_[23][0] = L"ch";   //ч
	ru_buf_[24][0] = L"sh";   //ш
	ru_buf_[25][0] = L"shh";  //щ
	ru_buf_[25][1] = L"w";	//щ
	ru_buf_[26][0] = L"jhh";  //ъ
							  //	ru_buf_[26][1] = L"";	 //ъ
	ru_buf_[27][0] = L"ih";   //ы
	ru_buf_[28][0] = L"jh";   //ь
	ru_buf_[28][1] = L"'";	//ь
	ru_buf_[29][0] = L"eh";   //э
	ru_buf_[29][1] = L"je";   //э
	ru_buf_[30][0] = L"ju";   //ю
	ru_buf_[30][1] = L"yu";   //ю
	ru_buf_[31][0] = L"ja";   //я
	ru_buf_[31][1] = L"ya";   //я
	ru_buf_[31][2] = L"q";	//я

	for (int i = 0; i < ruAlfavitSize; ++i) {
		for (int j = 0; j < maxTraslitVariants; ++j) {
			if (ru_buf_[i][j].empty()) {
				ru_buf_[i][j] = ru_buf_[i][0];
			}
		}
	}
}

bool Translit::CheckIsEn(wchar_t symbol) {
	return (symbol != 0 && symbol >= enLettersStartUTF16 && symbol - enLettersStartUTF16 < enAlfavitSize);
}

void Translit::PrepareEnglish() {
	memset(en_buf_, 0, sizeof(en_buf_));
	memset(en_d_buf_, 0, sizeof(en_d_buf_));
	memset(en_t_buf_, 0, sizeof(en_t_buf_));

	for (int i = 0; i < ruAlfavitSize; ++i) {
		for (int j = 0; j < maxTraslitVariants; ++j) {
			size_t length = ru_buf_[i][j].size();

			if (length == 1) {
				wchar_t sym = ru_buf_[i][j][0];

				if (CheckIsEn(sym)) {
					assert(sym != 0 && sym >= enLettersStartUTF16 && sym - enLettersStartUTF16 < enAlfavitSize);
					en_buf_[ru_buf_[i][j][0] - enLettersStartUTF16] = wchar_t(i + ruLettersStartUTF16);
				}

			} else if (length == 2 && CheckIsEn(ru_buf_[i][j][0]) && CheckIsEn(ru_buf_[i][j][1])) {
				wchar_t symFirst = ru_buf_[i][j][0];
				wchar_t symSecond = ru_buf_[i][j][1];

				if (CheckIsEn(symFirst) && CheckIsEn(symSecond)) {
					assert(symFirst != 0 && symFirst >= enLettersStartUTF16 && symFirst - enLettersStartUTF16 < enAlfavitSize);
					assert(symSecond != 0 && symSecond >= enLettersStartUTF16 && symSecond - enLettersStartUTF16 < enAlfavitSize);

					en_d_buf_[ru_buf_[i][j][0] - enLettersStartUTF16][ru_buf_[i][j][1] - enLettersStartUTF16] =
						wchar_t(i + ruLettersStartUTF16);
				}

			} else if (length == 3 && CheckIsEn(ru_buf_[i][j][0]) && CheckIsEn(ru_buf_[i][j][1]) && CheckIsEn(ru_buf_[i][j][2])) {
				wchar_t symFirst = ru_buf_[i][j][0];
				wchar_t symSecond = ru_buf_[i][j][1];
				wchar_t symThird = ru_buf_[i][j][1];

				if (CheckIsEn(symFirst) && CheckIsEn(symSecond) && CheckIsEn(symThird)) {
					assert(symFirst != 0 && symFirst >= enLettersStartUTF16 && symFirst - enLettersStartUTF16 < enAlfavitSize);
					assert(symSecond != 0 && symSecond >= enLettersStartUTF16 && symSecond - enLettersStartUTF16 < enAlfavitSize);
					assert(symThird != 0 && symThird >= enLettersStartUTF16 && symThird - enLettersStartUTF16 < enAlfavitSize);
					en_t_buf_[ru_buf_[i][j][0] - enLettersStartUTF16][ru_buf_[i][j][1] - enLettersStartUTF16]
							 [ru_buf_[i][j][2] - enLettersStartUTF16] = wchar_t(i + ruLettersStartUTF16);
				}
			}
		}
	}
}
}  // namespace search_engine
