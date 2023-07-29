#include <memory.h>
#include <algorithm>
#include <cctype>
#include <locale>
#include <string>
#include <unordered_map>
#include <vector>

#include "atoi/atoi.h"
#include "itoa/itoa.h"
#include "tools/assertrx.h"
#include "tools/customlocal.h"
#include "tools/stringstools.h"
#include "utf8cpp/utf8.h"

namespace reindexer {

std::string toLower(std::string_view src) {
	std::string ret;
	ret.reserve(src.size());
	for (char ch : src) ret.push_back(tolower(ch));
	return ret;
}

std::string escapeString(std::string_view str) {
	std::string dst;
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

std::string unescapeString(std::string_view str) {
	std::string dst;
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

wstring &utf8_to_utf16(std::string_view src, wstring &dst) {
	dst.resize(src.length());
	auto end = utf8::unchecked::utf8to32(src.begin(), src.end(), dst.begin());
	dst.resize(std::distance(dst.begin(), end));
	return dst;
}

std::string &utf16_to_utf8(const wstring &src, std::string &dst) {
	dst.resize(src.length() * 4);
	auto end = utf8::unchecked::utf32to8(src.begin(), src.end(), dst.begin());
	dst.resize(std::distance(dst.begin(), end));
	return dst;
}

wstring utf8_to_utf16(std::string_view src) {
	wstring dst;
	return utf8_to_utf16(src, dst);
}
std::string utf16_to_utf8(const wstring &src) {
	std::string dst;
	return utf16_to_utf8(src, dst);
}

// This functions calculate how many bytes takes limit symbols in UTF8 forward
size_t calcUtf8After(std::string_view str, size_t limit) noexcept {
	const char *ptr;
	const char *strEnd = str.data() + str.size();
	for (ptr = str.data(); limit && ptr < strEnd; limit--) utf8::unchecked::next(ptr);
	return ptr - str.data();
}
std::pair<size_t, size_t> calcUtf8AfterDelims(std::string_view str, size_t limit, std::string_view delims) noexcept {
	const char *ptr;
	const char *strEnd;
	const char *ptrDelims;
	const char *delimsEnd;
	size_t charCounter = 0;
	for (ptr = str.data(), strEnd = str.data() + str.size(); limit && ptr < strEnd; limit--) {
		uint32_t c = utf8::unchecked::next(ptr);
		charCounter++;
		for (ptrDelims = delims.data(), delimsEnd = delims.data() + delims.size(); ptrDelims < delimsEnd;) {
			uint32_t d = utf8::unchecked::next(ptrDelims);
			if (c == d) {
				utf8::unchecked::prior(ptr);
				return std::make_pair(ptr - str.data(), charCounter - 1);
			}
		}
	}
	return std::make_pair(ptr - str.data(), charCounter);
}

// This functions calculate how many bytes takes limit symbols in UTF8 backward
size_t calcUtf8Before(const char *str, int pos, size_t limit) noexcept {
	const char *ptr = str + pos;
	for (; limit && ptr > str; limit--) utf8::unchecked::prior(ptr);
	return str + pos - ptr;
}

std::pair<size_t, size_t> calcUtf8BeforeDelims(const char *str, int pos, size_t limit, std::string_view delims) noexcept {
	const char *ptr = str + pos;
	const char *ptrDelim;
	const char *delimsEnd;
	int charCounter = 0;
	for (; limit && ptr > str; limit--) {
		uint32_t c = utf8::unchecked::prior(ptr);
		charCounter++;
		for (ptrDelim = delims.data(), delimsEnd = delims.data() + delims.size(); ptrDelim < delimsEnd;) {
			uint32_t d = utf8::unchecked::next(ptrDelim);
			if (c == d) {
				utf8::unchecked::next(ptr);
				return std::make_pair(str + pos - ptr, charCounter - 1);
			}
		}
	}
	return std::make_pair(str + pos - ptr, charCounter);
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

bool is_number(std::string_view str) {
	uint16_t i = 0;
	while ((i < str.length() && IsDigit(str[i]))) i++;
	return (i && i == str.length());
}

void split(std::string_view str, std::string &buf, std::vector<const char *> &words, const std::string &extraWordSymbols) {
	buf.resize(str.length());
	words.resize(0);
	auto bufIt = buf.begin();

	for (auto it = str.begin(); it != str.end();) {
		auto ch = utf8::unchecked::next(it);

		while (it != str.end() && extraWordSymbols.find(ch) == std::string::npos && !IsAlpha(ch) && !IsDigit(ch)) {
			ch = utf8::unchecked::next(it);
		}

		auto begIt = bufIt;
		while (it != str.end() && (IsAlpha(ch) || IsDigit(ch) || extraWordSymbols.find(ch) != std::string::npos)) {
			ch = ToLower(ch);
			check_for_replacement(ch);
			bufIt = utf8::unchecked::append(ch, bufIt);
			ch = utf8::unchecked::next(it);
		}
		if ((IsAlpha(ch) || IsDigit(ch) || extraWordSymbols.find(ch) != std::string::npos)) {
			ch = ToLower(ch);
			check_for_replacement(ch);

			bufIt = utf8::unchecked::append(ch, bufIt);
		}

		if (begIt != bufIt) {
			if (bufIt != buf.end()) *bufIt++ = 0;
			words.emplace_back(&*begIt);
		}
	}
}
template <typename Pos>
Pos wordToByteAndCharPos(std::string_view str, int wordPosition, const std::string &extraWordSymbols) {
	auto wordStartIt = str.begin();
	auto wordEndIt = str.begin();
	auto it = str.begin();
	Pos wp;
	const bool constexpr neadChar = std::is_same_v<Pos, WordPositionEx>;
	if constexpr (neadChar) {
		wp.start.ch = -1;
	}
	for (; it != str.end();) {
		auto ch = utf8::unchecked::next(it);
		if constexpr (neadChar) {
			wp.start.ch++;
		}
		// skip not word symbols
		while (it != str.end() && extraWordSymbols.find(ch) == std::string::npos && !IsAlpha(ch) && !IsDigit(ch)) {
			wordStartIt = it;
			ch = utf8::unchecked::next(it);
			if constexpr (neadChar) {
				wp.start.ch++;
			}
		}
		if constexpr (neadChar) {
			wp.end.ch = wp.start.ch;
		}
		while (IsAlpha(ch) || IsDigit(ch) || extraWordSymbols.find(ch) != std::string::npos) {
			wordEndIt = it;
			if constexpr (neadChar) {
				wp.end.ch++;
			}
			if (it == str.end()) {
				break;
			}
			ch = utf8::unchecked::next(it);
		}

		if (wordStartIt != it) {
			if (!wordPosition) {
				break;
			} else {
				wordPosition--;
				wordStartIt = it;
			}
		}
		if constexpr (neadChar) {
			wp.start.ch = wp.end.ch;
		}
	}
	if (wordPosition != 0) {
		throw Error(errParams, "wordToByteAndCharPos: incorrect input string=%s wordPosition=%d", str, wordPosition);
	}
	wp.SetBytePosition(wordStartIt - str.begin(), wordEndIt - str.begin());
	return wp;
}
template WordPositionEx wordToByteAndCharPos<WordPositionEx>(std::string_view str, int wordPosition, const std::string &extraWordSymbols);
template WordPosition wordToByteAndCharPos<WordPosition>(std::string_view str, int wordPosition, const std::string &extraWordSymbols);

static std::pair<int, int> word2Pos(std::string_view str, int wordPos, int endPos, const std::string &extraWordSymbols) {
	auto wordStartIt = str.begin();
	auto wordEndIt = str.begin();
	auto it = str.begin();
	assertrx(endPos > wordPos);
	int numWords = endPos - (wordPos + 1);
	for (; it != str.end();) {
		auto ch = utf8::unchecked::next(it);

		while (it != str.end() && extraWordSymbols.find(ch) == std::string::npos && !IsAlpha(ch) && !IsDigit(ch)) {
			wordStartIt = it;
			ch = utf8::unchecked::next(it);
		}

		while (IsAlpha(ch) || IsDigit(ch) || extraWordSymbols.find(ch) != std::string::npos) {
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

		while (IsAlpha(ch) || IsDigit(ch) || extraWordSymbols.find(ch) != std::string::npos) {
			wordEndIt = it;
			if (it == str.end()) break;
			ch = utf8::unchecked::next(it);
		}
	}

	return {int(std::distance(str.begin(), wordStartIt)), int(std::distance(str.begin(), wordEndIt))};
}

Word2PosHelper::Word2PosHelper(std::string_view data, const std::string &extraWordSymbols)
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

void split(std::string_view utf8Str, wstring &utf16str, std::vector<std::wstring> &words, const std::string &extraWordSymbols) {
	utf8_to_utf16(utf8Str, utf16str);
	words.resize(0);
	for (auto it = utf16str.begin(); it != utf16str.end();) {
		while (it != utf16str.end() && !IsAlpha(*it) && !IsDigit(*it)) it++;

		auto begIt = it;
		while (it != utf16str.end() && (IsAlpha(*it) || IsDigit(*it) || extraWordSymbols.find(*it) != std::string::npos)) {
			*it = ToLower(*it);
			it++;
		}
		size_t sz = it - begIt;
		if (sz) {
			words.emplace_back(&*begIt, &*(begIt + sz));
		}
	}
}

bool iequals(std::string_view lhs, std::string_view rhs) noexcept {
	if (lhs.size() != rhs.size()) return false;
	for (auto itl = lhs.begin(), itr = rhs.begin(); itl != lhs.end() && itr != rhs.end();) {
		if (tolower(*itl++) != tolower(*itr++)) return false;
	}
	return true;
}

bool iless(std::string_view lhs, std::string_view rhs) noexcept {
	const auto len = std::min(lhs.size(), rhs.size());
	for (size_t i = 0; i < len; ++i) {
		if (const auto l = tolower(lhs[i]), r = tolower(rhs[i]); l != r) {
			return l < r;
		}
	}
	return lhs.size() < rhs.size();
}

template <CaseSensitive sensitivity>
bool checkIfStartsWith(std::string_view pattern, std::string_view str) noexcept {
	if (pattern.empty() || str.empty()) return false;
	if (pattern.length() > str.length()) return false;
	if constexpr (sensitivity == CaseSensitive::Yes) {
		for (size_t i = 0; i < pattern.length(); ++i) {
			if (pattern[i] != str[i]) return false;
		}
	} else {
		for (size_t i = 0; i < pattern.length(); ++i) {
			if (tolower(pattern[i]) != tolower(str[i])) return false;
		}
	}
	return true;
}
template bool checkIfStartsWith<CaseSensitive::Yes>(std::string_view pattern, std::string_view src) noexcept;
template bool checkIfStartsWith<CaseSensitive::No>(std::string_view pattern, std::string_view src) noexcept;

template <CaseSensitive sensitivity>
bool checkIfEndsWith(std::string_view pattern, std::string_view src) noexcept {
	if (pattern.length() > src.length()) return false;
	if (pattern.length() == 0) return true;
	const auto offset = src.length() - pattern.length();
	if constexpr (sensitivity == CaseSensitive::Yes) {
		for (size_t i = 0; i < pattern.length(); ++i) {
			if (src[offset + i] != pattern[i]) return false;
		}
	} else {
		for (size_t i = 0; i < pattern.length(); ++i) {
			if (tolower(src[offset + i]) != tolower(pattern[i])) return false;
		}
	}
	return true;
}
template bool checkIfEndsWith<CaseSensitive::Yes>(std::string_view pattern, std::string_view src) noexcept;
template bool checkIfEndsWith<CaseSensitive::No>(std::string_view pattern, std::string_view src) noexcept;

template <>
int collateCompare<CollateASCII>(std::string_view lhs, std::string_view rhs, const SortingPrioritiesTable &) {
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
}

template <>
int collateCompare<CollateUTF8>(std::string_view lhs, std::string_view rhs, const SortingPrioritiesTable &) {
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
}

template <>
int collateCompare<CollateNumeric>(std::string_view lhs, std::string_view rhs, const SortingPrioritiesTable &) {
	char *posl = nullptr;
	char *posr = nullptr;

	int numl = strtol(lhs.data(), &posl, 10);
	int numr = strtol(rhs.data(), &posr, 10);

	if (numl == numr) {
		auto minlen = std::min(lhs.size() - (posl - lhs.data()), rhs.size() - (posr - rhs.data()));
		auto res = strncmp(posl, posr, minlen);

		if (res != 0) return res;

		return lhs.size() > rhs.size() ? 1 : (lhs.size() < rhs.size() ? -1 : 0);
	}

	return numl > numr ? 1 : (numl < numr ? -1 : 0);
}

template <>
int collateCompare<CollateCustom>(std::string_view lhs, std::string_view rhs, const SortingPrioritiesTable &sortOrderTable) {
	auto itl = lhs.data();
	auto itr = rhs.data();

	for (; itl != lhs.data() + lhs.size() && itr != rhs.size() + rhs.data();) {
		auto chl = utf8::unchecked::next(itl);
		auto chr = utf8::unchecked::next(itr);

		int chlPriority = sortOrderTable.GetPriority(chl);
		int chrPriority = sortOrderTable.GetPriority(chr);

		if (chlPriority > chrPriority) return 1;
		if (chlPriority < chrPriority) return -1;
	}

	if (lhs.size() > rhs.size()) {
		return 1;
	} else if (lhs.size() < rhs.size()) {
		return -1;
	}
	return 0;
}

template <>
int collateCompare<CollateNone>(std::string_view lhs, std::string_view rhs, const SortingPrioritiesTable &) {
	size_t l1 = lhs.size();
	size_t l2 = rhs.size();
	int res = memcmp(lhs.data(), rhs.data(), std::min(l1, l2));

	return res ? res : ((l1 < l2) ? -1 : (l1 > l2) ? 1 : 0);
}

static std::string_view urldecode2(char *buf, std::string_view str) {
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
	return std::string_view(buf, dst - buf);
}

std::string urldecode2(std::string_view str) {
	std::string ret(str.length(), ' ');
	std::string_view sret = urldecode2(&ret[0], str);
	ret.resize(sret.size());
	return ret;
}

// Sat Jul 15 14 : 18 : 56 2017 GMT

static const char *daysOfWeek[] = {"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"};
static const char *months[] = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};

inline static char *strappend(char *dst, const char *src) noexcept {
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

bool validateObjectName(std::string_view name, bool allowSpecialChars) noexcept {
	if (!name.length()) {
		return false;
	}
	for (auto c : name) {
		if (!(std::isalnum(c) || c == '_' || c == '-' || c == '#' || (c == '@' && allowSpecialChars))) {
			return false;
		}
	}
	return true;
}

bool validateUserNsName(std::string_view name) noexcept {
	if (!name.length()) {
		return false;
	}
	for (auto c : name) {
		if (!(std::isalnum(c) || c == '_' || c == '-')) {
			return false;
		}
	}
	return true;
}

LogLevel logLevelFromString(const std::string &strLogLevel) {
	static std::unordered_map<std::string, LogLevel> levels = {
		{"none", LogNone}, {"warning", LogWarning}, {"error", LogError}, {"info", LogInfo}, {"trace", LogTrace}};

	auto configLevelIt = levels.find(strLogLevel);
	if (configLevelIt != levels.end()) {
		return configLevelIt->second;
	}
	return LogNone;
}

static std::unordered_map<std::string, StrictMode> strictModes = {
	{"", StrictModeNotSet}, {"none", StrictModeNone}, {"names", StrictModeNames}, {"indexes", StrictModeIndexes}};

StrictMode strictModeFromString(const std::string &strStrictMode) {
	auto configModeIt = strictModes.find(strStrictMode);
	if (configModeIt != strictModes.end()) {
		return configModeIt->second;
	}
	return StrictModeNotSet;
}

std::string_view strictModeToString(StrictMode mode) {
	for (auto &it : strictModes) {
		if (it.second == mode) {
			return it.first;
		}
	}
	static std::string_view empty{""};
	return empty;
}

bool isPrintable(std::string_view str) noexcept {
	if (str.length() > 256) {
		return false;
	}
	for (auto c : str) {
		if (c < 0x20) return false;
	}
	return true;
}

bool isBlank(std::string_view str) noexcept {
	if (str.empty()) return true;
	for (auto c : str) {
		if (!isspace(c)) return false;
	}
	return true;
}

int getUTF8StringCharactersCount(std::string_view str) noexcept {
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

int stoi(std::string_view sl) {
	bool valid;
	return jsteemann::atoi<int>(sl.data(), sl.data() + sl.size(), valid);
}

int64_t stoll(std::string_view sl) {
	bool valid;
	auto ret = jsteemann::atoi<int64_t>(sl.data(), sl.data() + sl.size(), valid);
	if (!valid) {
		throw Error(errParams, "Can't convert %s to number", sl);
	}
	return ret;
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
void toNextCh(std::string_view::iterator &it, std::string_view str) {
	if (isUtf8) {
		utf8::next(it, str.end());
	} else {
		++it;
	}
}

template <bool isUtf8>
void toPrevCh(std::string_view::iterator &it, std::string_view str) {
	if (isUtf8) {
		utf8::previous(it, str.begin());
	} else {
		--it;
	}
}

template <bool isUtf8>
Error getBytePosInMultilineString(std::string_view str, const size_t line, const size_t charPos, size_t &bytePos) {
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

Error cursosPosToBytePos(std::string_view str, size_t line, size_t charPos, size_t &bytePos) {
	try {
		return getBytePosInMultilineString<true>(str, line, charPos, bytePos);
	} catch (const utf8::exception &) {
		return getBytePosInMultilineString<false>(str, line, charPos, bytePos);
	}
}

}  // namespace reindexer
