
#include "numtotext.h"

#include <algorithm>
#include <cmath>
#include <cstdlib>
#include "tools/errors.h"

namespace reindexer {

constexpr std::string_view units[] = {"", "один", "два", "три", "четыре", "пять", "шесть", "семь", "восемь", "девять"};
constexpr std::string_view unitsNominat[] = {"", "одна", "две"};
constexpr std::string_view tens[] = {"",		   "одиннадцать", "двенадцать", "тринадцать",	"четырнадцать",
									 "пятнадцать", "шестнадцать", "семнадцать", "восемнадцать", "девятнадцать"};
constexpr std::string_view decades[] = {"",			 "десять",	   "двадцать",	"тридцать",	   "сорок",
										"пятьдесят", "шестьдесят", "семьдесят", "восемьдесят", "девяносто"};
constexpr std::string_view hundreads[] = {"",		 "сто",		 "двести",	"триста",	 "четыреста",
										  "пятьсот", "шестьсот", "семьсот", "восемьсот", "девятьсот"};
constexpr std::string_view thousands[] = {"тысяча", "тысячи", "тысяч"};
constexpr std::string_view millions[] = {"миллион", "миллиона", "миллионов"};
constexpr std::string_view billions[] = {"миллиард", "миллиарда", "миллиардов"};
constexpr std::string_view trillions[] = {"триллион", "триллиона", "триллионов"};
constexpr std::string_view quadrillion[] = {"квадриллион", "квадриллиона", "квадриллионов"};
constexpr std::string_view quintillion[] = {"квинтиллион", "квинтиллиона", "квинтиллионов"};
constexpr std::string_view sextillion[] = {"секстиллион", "секстиллиона", "секстиллионов"};
constexpr std::string_view septillion[] = {"септиллион", "септиллиона", "септиллионов"};

enum Numorders : int { Thousands, Millions, Billions, Trillions, Quadrillion, Quintillion, Sextillion, Septillion };

static std::string_view getNumorder(int numorder, int i) {
	switch (numorder) {
		case Thousands:
			return thousands[i];
		case Millions:
			return millions[i];
		case Billions:
			return billions[i];
		case Trillions:
			return trillions[i];
		case Quadrillion:
			return quadrillion[i];
		case Quintillion:
			return quintillion[i];
		case Sextillion:
			return sextillion[i];
		case Septillion:
			return septillion[i];
		default:
			throw Error(errParams, "Incorrect order [%s]: too big", numorder);
	}
}

RX_ALWAYS_INLINE int ansiCharacterToDigit(char ch) noexcept { return static_cast<int>(ch - 48); }

static std::vector<std::string> getOrders(std::string_view str) {
	std::string numStr(str);
	std::reverse(numStr.begin(), numStr.end());
	int numChars = numStr.length();
	std::vector<std::string> orders;
	orders.reserve(numChars / 3);
	for (int i = 0; i < numChars; i += 3) {
		std::string tempString;
		if (i <= numChars - 3) {
			tempString += numStr[i + 2];
			tempString += numStr[i + 1];
			tempString += numStr[i];
		} else {
			int lostChars = numChars - i;
			switch (lostChars) {
				case 1:
					tempString = numStr[i];
					break;
				case 2:
					tempString += numStr[i + 1];
					tempString += numStr[i];
					break;
				default:
					throw Error(errLogic, "Unexpected lost characters number: %d", lostChars);
			}
		}
		orders.emplace_back(std::move(tempString));
	}
	return orders;
}

static std::vector<std::string> getDecimal(const std::string& str, int i) {
	std::vector<std::string> words;
	int v = std::stoi(str);
	if (v < 10) {
		words.emplace_back(units[v]);
	} else if (v % 10 == 0) {
		words.emplace_back(decades[v / 10]);
	} else if (v < 20) {
		words.emplace_back(tens[v % 10]);
	} else if (v % 10 < 3 && i == 1) {
		words.emplace_back(decades[ansiCharacterToDigit(str[0])]);
		words.emplace_back(unitsNominat[ansiCharacterToDigit(str[1])]);
	} else {
		words.emplace_back(decades[ansiCharacterToDigit(str[0])]);
		words.emplace_back(units[ansiCharacterToDigit(str[1])]);
	}
	return words;
}

static std::string getNumOrders(int i, int num) {
	std::string orders;
	if (i > 0) {
		if (num % 10 > 4 || (num % 100 > 10 && num % 100 < 20) || num % 10 == 0) {
			orders = getNumorder(i - 1, 2);
		} else if (num % 10 > 1 && num % 10 < 5) {
			orders = getNumorder(i - 1, 1);
		} else {
			orders = getNumorder(i - 1, 0);
		}
	}
	return orders;
}

static std::vector<std::string> formTextString(const std::string& str, int i) {
	std::vector<std::string> words;
	int strlen = str.length();
	if (strlen == 3) {
		words.emplace_back(hundreads[ansiCharacterToDigit(str[0])]);
		std::string decimal;
		decimal += str[1];
		decimal += str[2];
		std::vector<std::string> decimalWords(getDecimal(decimal, i));
		words.insert(words.end(), make_move_iterator(decimalWords.begin()), make_move_iterator(decimalWords.end()));
	} else if (strlen == 2) {
		words = getDecimal(str, i);
	} else {
		if ((i == 1) && std::stoi(str) < 3) {
			words.emplace_back(unitsNominat[std::stoi(str)]);
		} else {
			words.emplace_back(units[std::stoi(str)]);
		}
	}
	if (i > 0) {
		words.emplace_back(getNumOrders(i, std::stoi(str)));
	}
	return words;
}

std::vector<std::string>& NumToText::convert(std::string_view str, std::vector<std::string>& output) {
	output.resize(0);
	if ((str.length() == 1) && (str[0] == '0')) {
		output = {"ноль"};
		return output;
	}
	// unreasonably big
	if (str.length() > 27) {
		return output;
	}
	std::vector<std::string> orders(getOrders(str));
	for (size_t i = 0; i < orders.size(); ++i) {
		size_t oppositeSideIndex = orders.size() - 1 - i;
		std::vector<std::string> digits(formTextString(orders[oppositeSideIndex], oppositeSideIndex));
		output.insert(output.end(), make_move_iterator(digits.begin()), make_move_iterator(digits.end()));
	}
	return output;
}

}  // namespace reindexer
