#include <assert.h>
#include <memory.h>
#include <algorithm>
#include <cctype>
#include <locale>
#include <string>
#include <unordered_map>
#include <vector>

#include "atoi/atoi.h"
#include "itoa/itoa.h"
#include "tools/customlocal.h"
#include "tools/stringstools.h"
#include "utf8cpp/utf8.h"

using std::min;
using std::transform;
using std::distance;
using std::make_pair;

namespace reindexer {

string toLower(string_view src) {
	string ret;
	ret.reserve(src.size());
	for (char ch : src) ret.push_back(tolower(ch));
	return ret;
}

string escapeString(string_view str) {
	string dst = "";
	dst.reserve(str.length());
	for (auto it = str.begin(); it != str.end(); it++) {
		if (*it < 0x20 || unsigned(*it) >= 0x80 || *it == '\\') {
			char tmpbuf[16];
			snprintf(tmpbuf, sizeof(tmpbuf), "\\%02X", unsigned(*it) & 0xFF);
			dst += tmpbuf;
		} else
			dst.push_back(*it);
	}
	return dst;
}

string unescapeString(string_view str) {
	string dst = "";
	dst.reserve(str.length());
	for (auto it = str.begin(); it != str.end(); it++) {
		if (*it == '\\' && ++it != str.end() && it + 1 != str.end()) {
			char tmpbuf[16], *endp;
			tmpbuf[0] = *it++;
			tmpbuf[1] = *it;
			tmpbuf[2] = 0;
			dst += char(strtol(tmpbuf, &endp, 16));
		} else
			dst.push_back(*it);
	}
	return dst;
}

wstring &utf8_to_utf16(string_view src, wstring &dst) {
	dst.resize(src.length());
	auto end = utf8::unchecked::utf8to32(src.begin(), src.end(), dst.begin());
	dst.resize(std::distance(dst.begin(), end));
	return dst;
}

string &utf16_to_utf8(const wstring &src, string &dst) {
	dst.resize(src.length() * 4);
	auto end = utf8::unchecked::utf32to8(src.begin(), src.end(), dst.begin());
	dst.resize(std::distance(dst.begin(), end));
	return dst;
}

wstring utf8_to_utf16(string_view src) {
	wstring dst;
	return utf8_to_utf16(src, dst);
}
string utf16_to_utf8(const wstring &src) {
	string dst;
	return utf16_to_utf8(src, dst);
}

// This functions calc how many bytes takes limit symbols in UTF8 forward
size_t calcUTf8Size(const char *str, size_t size, size_t limit) {
	const char *ptr;
	for (ptr = str; limit && ptr < str + size; limit--) utf8::unchecked::next(ptr);
	return ptr - str;
}

// This functions calc how many bytes takes limit symbols in UTF8 backward
size_t calcUTf8SizeEnd(const char *end, int pos, size_t limit) {
	const char *ptr;
	for (ptr = end; limit && ptr > end - pos; limit--) utf8::unchecked::prior(ptr);
	return end - ptr;
}

void check_for_replacement(wchar_t &ch) {
	if (ch == 0x451) {	// 'ё'
		ch = 0x435;		// 'е'
	}
}

void check_for_replacement(uint32_t &ch) {
	if (ch == 0x451) {	// 'ё'
		ch = 0x435;		// 'е'
	}
}

bool is_number(string_view str) {
	uint16_t i = 0;
	while ((i < str.length() && IsDigit(str[i]))) i++;
	return (i && i == str.length());
}

void split(string_view str, string &buf, vector<const char *> &words, const string &extraWordSymbols) {
	buf.resize(str.length());
	words.resize(0);
	auto bufIt = buf.begin();

	for (auto it = str.begin(); it != str.end();) {
		auto ch = utf8::unchecked::next(it);

		while (it != str.end() && extraWordSymbols.find(ch) == string::npos && !IsAlpha(ch) && !IsDigit(ch)) {
			ch = utf8::unchecked::next(it);
		}

		auto begIt = bufIt;
		while (it != str.end() && (IsAlpha(ch) || IsDigit(ch) || extraWordSymbols.find(ch) != string::npos)) {
			ch = ToLower(ch);
			check_for_replacement(ch);
			bufIt = utf8::unchecked::append(ch, bufIt);
			ch = utf8::unchecked::next(it);
		}
		if ((IsAlpha(ch) || IsDigit(ch) || extraWordSymbols.find(ch) != string::npos)) {
			ch = ToLower(ch);
			check_for_replacement(ch);

			bufIt = utf8::unchecked::append(ch, bufIt);
		}

		if (begIt != bufIt) {
			if (bufIt != buf.end()) *bufIt++ = 0;
			words.push_back(&*begIt);
		}
	}
}

std::pair<int, int> word2Pos(string_view str, int wordPos, int endPos, const string &extraWordSymbols) {
	auto wordStartIt = str.begin();
	auto wordEndIt = str.begin();
	auto it = str.begin();
	assert(endPos > wordPos);
	int numWords = endPos - (wordPos + 1);
	for (; it != str.end();) {
		auto ch = utf8::unchecked::next(it);

		while (it != str.end() && extraWordSymbols.find(ch) == string::npos && !IsAlpha(ch) && !IsDigit(ch)) {
			wordStartIt = it;
			ch = utf8::unchecked::next(it);
		}

		while (IsAlpha(ch) || IsDigit(ch) || extraWordSymbols.find(ch) != string::npos) {
			wordEndIt = it;
			if (it == str.end()) break;
			ch = utf8::unchecked::next(it);
		}

		if (wordStartIt != it) {
			if (!wordPos)
				break;
			else {
				wordPos--;
				wordStartIt = it;
			}
		}
	}

	for (; numWords != 0 && it != str.end(); numWords--) {
		auto ch = utf8::unchecked::next(it);

		while (it != str.end() && !IsAlpha(ch) && !IsDigit(ch)) {
			ch = utf8::unchecked::next(it);
		}

		while (IsAlpha(ch) || IsDigit(ch) || extraWordSymbols.find(ch) != string::npos) {
			wordEndIt = it;
			if (it == str.end()) break;
			ch = utf8::unchecked::next(it);
		}
	}

	return {int(std::distance(str.begin(), wordStartIt)), int(std::distance(str.begin(), wordEndIt))};
}

Word2PosHelper::Word2PosHelper(string_view data, const string &extraWordSymbols)
	: data_(data), lastWordPos_(0), lastOffset_(0), extraWordSymbols_(extraWordSymbols) {}

std::pair<int, int> Word2PosHelper::convert(int wordPos, int endPos) {
	if (wordPos < lastWordPos_) {
		lastWordPos_ = 0;
		lastOffset_ = 0;
	}

	auto ret = word2Pos(data_.substr(lastOffset_), wordPos - lastWordPos_, endPos - lastWordPos_, extraWordSymbols_);
	ret.first += lastOffset_;
	ret.second += lastOffset_;
	lastOffset_ = ret.first;
	lastWordPos_ = wordPos;
	return ret;
}

void split(string_view utf8Str, wstring &utf16str, vector<std::wstring> &words, const string &extraWordSymbols) {
	utf8_to_utf16(utf8Str, utf16str);
	words.resize(0);
	size_t outSz = 0;
	for (auto it = utf16str.begin(); it != utf16str.end();) {
		while (it != utf16str.end() && !IsAlpha(*it) && !IsDigit(*it)) it++;

		auto begIt = it;
		while (it != utf16str.end() && (IsAlpha(*it) || IsDigit(*it) || extraWordSymbols.find(*it) != string::npos)) {
			*it = ToLower(*it);
			it++;
		}
		size_t sz = it - begIt;
		if (sz) {
			words.push_back({&*begIt, &*(begIt + sz)});
		}
		outSz += sz + 1;
	}
}

string lower(string s) {
	transform(s.begin(), s.end(), s.begin(), [](char c) { return 'A' <= c && c <= 'Z' ? c ^ 32 : c; });
	return s;
}

bool iequals(string_view lhs, string_view rhs) {
	if (lhs.size() != rhs.size()) return false;
	for (auto itl = lhs.begin(), itr = rhs.begin(); itl != lhs.end() && itr != rhs.end();) {
		if (tolower(*itl++) != tolower(*itr++)) return false;
	}
	return true;
}

bool checkIfStartsWith(string_view src, string_view pattern, bool casesensitive) {
	if (src.empty() || pattern.empty()) return false;
	if (src.length() > pattern.length()) return false;
	if (casesensitive) {
		for (size_t i = 0; i < src.length(); ++i) {
			if (src[i] != pattern[i]) return false;
		}
	} else {
		for (size_t i = 0; i < src.length(); ++i) {
			if (tolower(src[i]) != tolower(pattern[i])) return false;
		}
	}
	return true;
}

int collateCompare(string_view lhs, string_view rhs, const CollateOpts &collateOpts) {
	if (collateOpts.mode == CollateASCII) {
		auto itl = lhs.begin();
		auto itr = rhs.begin();

		for (; itl != lhs.end() && itr != rhs.end();) {
			auto chl = tolower(*itl++);
			auto chr = tolower(*itr++);

			if (chl > chr) return 1;
			if (chl < chr) return -1;
		}

		if (lhs.size() > rhs.size()) {
			return 1;
		} else if (lhs.size() < rhs.size()) {
			return -1;
		}

		return 0;
	} else if (collateOpts.mode == CollateUTF8) {
		auto itl = lhs.data();
		auto itr = rhs.data();

		for (; itl != lhs.data() + lhs.size() && itr != rhs.size() + rhs.data();) {
			auto chl = ToLower(utf8::unchecked::next(itl));
			auto chr = ToLower(utf8::unchecked::next(itr));

			if (chl > chr) return 1;
			if (chl < chr) return -1;
		}

		if (lhs.size() > rhs.size()) {
			return 1;
		} else if (lhs.size() < rhs.size()) {
			return -1;
		}
		return 0;
	} else if (collateOpts.mode == CollateNumeric) {
		char *posl = nullptr;
		char *posr = nullptr;

		int numl = strtol(lhs.data(), &posl, 10);
		int numr = strtol(rhs.data(), &posr, 10);

		if (numl == numr) {
			auto minlen = min(lhs.size() - (posl - lhs.data()), rhs.size() - (posr - rhs.data()));
			auto res = strncmp(posl, posr, minlen);

			if (res != 0) return res;

			return lhs.size() > rhs.size() ? 1 : (lhs.size() < rhs.size() ? -1 : 0);
		}

		return numl > numr ? 1 : (numl < numr ? -1 : 0);
	} else if (collateOpts.mode == CollateCustom) {
		auto itl = lhs.data();
		auto itr = rhs.data();

		for (; itl != lhs.data() + lhs.size() && itr != rhs.size() + rhs.data();) {
			auto chl = utf8::unchecked::next(itl);
			auto chr = utf8::unchecked::next(itr);

			int chlPriority = collateOpts.sortOrderTable.GetPriority(chl);
			int chrPriority = collateOpts.sortOrderTable.GetPriority(chr);

			if (chlPriority > chrPriority) return 1;
			if (chlPriority < chrPriority) return -1;
		}

		if (lhs.size() > rhs.size()) {
			return 1;
		} else if (lhs.size() < rhs.size()) {
			return -1;
		}
	}

	size_t l1 = lhs.size();
	size_t l2 = rhs.size();
	int res = memcmp(lhs.data(), rhs.data(), std::min(l1, l2));

	return res ? res : ((l1 < l2) ? -1 : (l1 > l2) ? 1 : 0);
}

static string_view urldecode2(char *buf, string_view str) {
	char a, b;
	const char *src = str.data();
	char *dst = buf;

	for (size_t l = 0; l < str.length(); l++) {
		if (l + 2 < str.length() && (*src == '%') && ((a = src[1]) && (b = src[2])) && (isxdigit(a) && isxdigit(b))) {
			if (a >= 'a') a -= 'a' - 'A';
			if (a >= 'A')
				a -= ('A' - 10);
			else
				a -= '0';
			if (b >= 'a') b -= 'a' - 'A';
			if (b >= 'A')
				b -= ('A' - 10);
			else
				b -= '0';
			*dst++ = 16 * a + b;
			src += 3;
			l += 2;
		} else if (*src == '+') {
			*dst++ = ' ';
			src++;
		} else {
			*dst++ = *src++;
		}
	}
	*dst = '\0';
	return string_view(buf, dst - buf);
}

string urldecode2(const string_view str) {
	string ret(str.length(), ' ');
	string_view sret = urldecode2(&ret[0], str);
	ret.resize(sret.size());
	return ret;
}

// Sat Jul 15 14 : 18 : 56 2017 GMT

static const char *daysOfWeek[] = {"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"};
static const char *months[] = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};

inline static char *strappend(char *dst, const char *src) {
	while (*src) *dst++ = *src++;
	return dst;
}

int fast_strftime(char *buf, const tm *tm) {
	char *d = buf;

	if (unsigned(tm->tm_wday) < sizeof(daysOfWeek) / sizeof daysOfWeek[0]) d = strappend(d, daysOfWeek[tm->tm_wday]);
	d = strappend(d, ", ");
	d = i32toa(tm->tm_mday, d);
	*d++ = ' ';
	if (unsigned(tm->tm_mon) < sizeof(months) / sizeof months[0]) d = strappend(d, months[tm->tm_mon]);
	*d++ = ' ';
	d = i32toa(tm->tm_year + 1900, d);
	*d++ = ' ';
	d = i32toa(tm->tm_hour, d);
	*d++ = ':';
	d = i32toa(tm->tm_min, d);
	*d++ = ':';
	d = i32toa(tm->tm_sec, d);
	d = strappend(d, " GMT");
	*d = 0;
	return d - buf;
}

bool validateObjectName(string_view name) {
	if (!name.length()) {
		return false;
	}
	for (auto c : name) {
		if (!(std::isalpha(c) || std::isdigit(c) || c == '_' || c == '-' || c == '#')) {
			return false;
		}
	}
	return true;
}

LogLevel logLevelFromString(const string &strLogLevel) {
	static std::unordered_map<string, LogLevel> levels = {
		{"none", LogNone}, {"warning", LogWarning}, {"error", LogError}, {"info", LogInfo}, {"trace", LogTrace}};

	auto configLevelIt = levels.find(strLogLevel);
	if (configLevelIt != levels.end()) {
		return configLevelIt->second;
	}
	return LogNone;
}

static std::unordered_map<string, StrictMode> strictModes = {
	{"", StrictModeNotSet}, {"none", StrictModeNone}, {"names", StrictModeNames}, {"indexes", StrictModeIndexes}};

StrictMode strictModeFromString(const string &strStrictMode) {
	auto configModeIt = strictModes.find(strStrictMode);
	if (configModeIt != strictModes.end()) {
		return configModeIt->second;
	}
	return StrictModeNotSet;
}

string_view strictModeToString(StrictMode mode) {
	for (auto &it : strictModes) {
		if (it.second == mode) {
			return it.first;
		}
	}
	static string_view empty = ""_sv;
	return empty;
}

bool isPrintable(string_view str) {
	if (str.length() > 256) {
		return false;
	}

	for (int i = 0; i < int(str.length()); i++) {
		if (unsigned(str.data()[i]) < 0x20) {
			return false;
		}
	}
	return true;
}

bool isBlank(string_view str) {
	if (str.empty()) return true;
	for (size_t i = 0; i < str.length(); ++i)
		if (!isspace(str[i])) return false;
	return true;
}

int getUTF8StringCharactersCount(string_view str) {
	int len = 0;
	try {
		for (auto it = str.begin(); it != str.end(); ++len) {
			utf8::next(it, str.end());
		}
	} catch (const std::exception &) {
		return str.length();
	}
	return len;
}

int stoi(string_view sl) {
	bool valid;
	return jsteemann::atoi<int>(sl.begin(), sl.end(), valid);
}

int64_t stoll(string_view sl) {
	bool valid;
	return jsteemann::atoi<int64_t>(sl.begin(), sl.end(), valid);
}

std::string randStringAlph(size_t len) {
	static const std::string symbols = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
	std::string result;
	result.reserve(len);
	while (result.size() < len) {
		const size_t f = rand() % symbols.size();
		result += symbols[f];
	}
	return result;
}

template <bool isUtf8>
void toNextCh(string_view::iterator &it, const string_view &str) {
	if (isUtf8) {
		utf8::next(it, str.end());
	} else {
		++it;
	}
}

template <bool isUtf8>
void toPrevCh(string_view::iterator &it, const string_view &str) {
	if (isUtf8) {
		utf8::previous(it, str.begin());
	} else {
		--it;
	}
}

template <bool isUtf8>
Error getBytePosInMultilineString(string_view str, const size_t line, const size_t charPos, size_t &bytePos) {
	auto it = str.begin();
	size_t currLine = 0, currCharPos = 0;
	for (size_t i = 0; it != str.end() && ((currLine != line) || (currCharPos != charPos)); toNextCh<isUtf8>(it, str), ++i) {
		if (*it == '\n') {
			++currLine;
		} else if (currLine == line) {
			++currCharPos;
		}
	}
	if ((currLine == line) && (charPos == currCharPos)) {
		bytePos = it - str.begin() - 1;
		return errOK;
	}
	return Error(errNotValid, "Wrong cursor position: line=%d, pos=%d", line, charPos);
}

Error cursosPosToBytePos(string_view str, size_t line, size_t charPos, size_t &bytePos) {
	try {
		return getBytePosInMultilineString<true>(str, line, charPos, bytePos);
	} catch (const utf8::exception &) {
		return getBytePosInMultilineString<false>(str, line, charPos, bytePos);
	}
}

}  // namespace reindexer
