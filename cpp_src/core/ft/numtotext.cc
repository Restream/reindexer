
#include "numtotext.h"
#include "tools/errors.h"

namespace reindexer {

constexpr std::string_view units[] = {"", "один", "два", "три", "четыре", "пять", "шесть", "семь", "восемь", "девять"};
constexpr std::string_view unitsNominat[] = {"", "одна", "две", "три", "четыре", "пять", "шесть", "семь", "восемь", "девять"};
constexpr std::string_view tens[] = {"",		   "одиннадцать", "двенадцать", "тринадцать",	"четырнадцать",
									 "пятнадцать", "шестнадцать", "семнадцать", "восемнадцать", "девятнадцать"};
constexpr std::string_view decades[] = {"",			 "десять",	   "двадцать",	"тридцать",	   "сорок",
										"пятьдесят", "шестьдесят", "семьдесят", "восемьдесят", "девяносто"};
constexpr std::string_view hundreads[] = {"",		 "сто",		 "двести",	"триста",	 "четыреста",
										  "пятьсот", "шестьсот", "семьсот", "восемьсот", "девятьсот"};

// clang-format off
constexpr static std::string_view kNumOrders[][10] = {
//        0                 1               2                3                4                5                 6                 7                 8                 9
        {"тысяч",          "тысяча",       "тысячи",        "тысячи",        "тысячи",        "тысяч",          "тысяч",          "тысяч",          "тысяч",          "тысяч"},
        {"миллионов",      "миллион",      "миллиона",      "миллиона",      "миллиона",      "миллионов",      "миллионов",      "миллионов",      "миллионов",      "миллионов"},
        {"миллиардов",     "миллиард",     "миллиарда",     "миллиарда",     "миллиарда",     "миллиардов",     "миллиардов",     "миллиардов",     "миллиардов",     "миллиардов"},
        {"триллионов",     "триллион",     "триллиона",     "триллиона",     "триллиона",     "триллионов",     "триллионов",     "триллионов",     "триллионов",     "триллионов"},
        {"квадриллионов",  "квадриллион",  "квадриллиона",  "квадриллиона",  "квадриллиона",  "квадриллионов",  "квадриллионов",  "квадриллионов",  "квадриллионов",  "квадриллионов"},
        {"квинтиллионов",  "квинтиллион",  "квинтиллиона",  "квинтиллиона",  "квинтиллиона",  "квинтиллионов",  "квинтиллионов",  "квинтиллионов",  "квинтиллионов",  "квинтиллионов"},
        {"секстиллионов",  "секстиллион",  "секстиллиона",  "секстиллиона",  "секстиллиона",  "секстиллионов",  "секстиллионов",  "секстиллионов",  "секстиллионов",  "секстиллионов"},
        {"септиллионов",   "септиллион",   "септиллиона",   "септиллиона",   "септиллиона",   "септиллионов",   "септиллионов",   "септиллионов",   "септиллионов",   "септиллионов"}};
// clang-format on
RX_ALWAYS_INLINE static int ansiCharacterToDigit(char ch) noexcept { return static_cast<int>(ch - 48); }

static std::vector<std::string_view>& formTextString(std::string_view str, std::vector<std::string_view>& words) {
	if (str.empty()) {
		return words;
	}
	unsigned int ordersMax = (str.length() - 1) / 3 + 1;
	unsigned int orderDigitCount = str.length() - (ordersMax - 1) * 3;
	unsigned int baseOffset = 0;
	for (int k = ordersMax; k > 0; k--) {
		unsigned int hundreadsIndx = 0;
		unsigned int tenIndex = 0;
		unsigned int numIndex = 0;
		switch (orderDigitCount) {
			case 1:
				numIndex = ansiCharacterToDigit(str[baseOffset]);
				break;
			case 2:
				tenIndex = ansiCharacterToDigit(str[baseOffset]);
				numIndex = ansiCharacterToDigit(str[baseOffset + 1]);
				break;
			case 3:
				hundreadsIndx = ansiCharacterToDigit(str[baseOffset]);
				tenIndex = ansiCharacterToDigit(str[baseOffset + 1]);
				numIndex = ansiCharacterToDigit(str[baseOffset + 2]);
				break;
			default:
				throw Error(errLogic, "Incorrect orderDigitCount {}", orderDigitCount);
		}
		if (hundreadsIndx != 0) {
			words.emplace_back(hundreads[hundreadsIndx]);
		}

		if (tenIndex == 1 && numIndex != 0) {
			words.emplace_back(tens[numIndex]);
		} else if (tenIndex != 0) {
			words.emplace_back(decades[tenIndex]);
		}

		if (numIndex != 0 && tenIndex != 1) {
			if (k == 2) {  // thousands
				words.emplace_back(unitsNominat[numIndex]);
			} else {
				words.emplace_back(units[numIndex]);
			}
		}
		bool isAllNull = hundreadsIndx == 0 && tenIndex == 0 && numIndex == 0;
		if (k > 1 && !isAllNull) {
			words.emplace_back(kNumOrders[k - 2][numIndex]);
		}
		baseOffset += orderDigitCount;
		orderDigitCount = 3;
	}
	return words;
}

std::vector<std::string_view>& NumToText::convert(std::string_view str, std::vector<std::string_view>& output) {
	output.resize(0);
	unsigned int k = 0;
	for (; k < str.length() && str[k] == '0'; ++k) {
		output.emplace_back("ноль");
	}
	str = str.substr(k);
	// unreasonably big
	if (str.length() > 27) {
		output.resize(0);
		return output;
	}

	return formTextString(str, output);
}

}  // namespace reindexer
