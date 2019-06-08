
#include "numtotext.h"

#include <math.h>
#include <stdlib.h>
#include <algorithm>
#include <iostream>
#include <vector>

namespace reindexer {

using std::string;
using std::vector;
using std::pair;

const string units[] = {"", "один", "два", "три", "четыре", "пять", "шесть", "семь", "восемь", "девять"};
const string unitsNominat[] = {"", "одна", "две"};
const string tens[] = {"",			 "одиннадцать", "двенадцать", "тринадцать",   "четырнадцать",
					   "пятнадцать", "шестнадцать", "семнадцать", "восемнадцать", "девятнадцать"};
const string decades[] = {"",		   "десять",	 "двадцать",  "тридцать",	"сорок",
						  "пятьдесят", "шестьдесят", "семьдесят", "восемьдесят", "девяносто"};
const string hundreads[] = {"", "сто", "двести", "триста", "четыреста", "пятьсот", "шестьсот", "семьсот", "восемьсот", "девятьсот"};
const string thousands[] = {"тысяча", "тысячи", "тысяч"};
const string millions[] = {"миллион", "миллиона", "миллионов"};
const string billions[] = {"миллиард", "миллиарда", "миллиардов"};

enum Numorders : int { Thousands, Millions, Billions };

const string& getNumorder(int numorder, int i) {
	switch (numorder) {
		case Thousands:
			return thousands[i];
		case Millions:
			return millions[i];
		case Billions:
			return billions[i];
	}
	std::abort();
}

int ansiCharacterToDigit(char ch) { return static_cast<int>(ch - 48); }

vector<string> getOrders(string_view str) {
	string numStr(str);
	std::reverse(numStr.begin(), numStr.end());
	int numChars = numStr.length();
	vector<string> orders;
	for (int i = 0; i < numChars; i += 3) {
		string tempString;
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
			}
		}
		orders.push_back(tempString);
	}
	return orders;
}

vector<string> getDecimal(const string& str, int i) {
	vector<string> words;
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

string getNumOrders(int i, int num) {
	string orders;
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

vector<string> formTextString(const string& str, int i) {
	vector<string> words;
	int strlen = str.length();
	if (strlen == 3) {
		words.emplace_back(hundreads[ansiCharacterToDigit(str[0])]);
		string decimal;
		decimal += str[1];
		decimal += str[2];
		vector<string> decimalWords(getDecimal(decimal, i));
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

vector<string>& NumToText::convert(string_view str, std::vector<std::string>& output) {
	output.clear();
	if ((str.length() == 1) && (str[0] == '0')) {
		output = {"ноль"};
		return output;
	}
	vector<string> orders(getOrders(str));
	for (size_t i = 0; i < orders.size(); ++i) {
		size_t oppositeSideIndex = orders.size() - 1 - i;
		vector<string> digits(formTextString(orders[oppositeSideIndex], oppositeSideIndex));
		output.insert(output.end(), make_move_iterator(digits.begin()), make_move_iterator(digits.end()));
	}
	return output;
}
}  // namespace reindexer
