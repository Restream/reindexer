#include <memory.h>
#include <algorithm>
#include <cctype>
#include <locale>
#include <string>
#include <vector>

#include "atoi/atoi.h"
#include "fmt/compile.h"
#include "frozen_str_tools.h"
#include "itoa/itoa.h"
#include "tools/assertrx.h"
#include "tools/randomgenerator.h"
#include "tools/stringstools.h"
#include "utf8cpp/utf8.h"
#include "vendor/double-conversion/double-conversion.h"
#include "vendor/frozen/unordered_map.h"

namespace reindexer {

namespace stringtools_impl {

static std::string_view urldecode2(char* buf, std::string_view str) {
	char a, b;
	const char* src = str.data();
	char* dst = buf;

	for (size_t l = 0; l < str.length(); l++) {
		if (l + 2 < str.length() && (*src == '%') && ((a = src[1]) && (b = src[2])) && (isxdigit(a) && isxdigit(b))) {
			if (a >= 'a') {
				a -= 'a' - 'A';
			}
			if (a >= 'A') {
				a -= ('A' - 10);
			} else {
				a -= '0';
			}
			if (b >= 'a') {
				b -= 'a' - 'A';
			}
			if (b >= 'A') {
				b -= ('A' - 10);
			} else {
				b -= '0';
			}
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

// Sat Jul 15 14 : 18 : 56 2017 GMT

static const char* kDaysOfWeek[] = {"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"};
static const char* kMonths[] = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};

inline static char* strappend(char* dst, const char* src) noexcept {
	while (*src) {
		*dst++ = *src++;
	}
	return dst;
}

constexpr static auto kStrictModes = frozen::make_unordered_map<std::string_view, StrictMode>(
	{{"", StrictModeNotSet}, {"none", StrictModeNone}, {"names", StrictModeNames}, {"indexes", StrictModeIndexes}},
	frozen::nocase_hash_str{}, frozen::nocase_equal_str{});

}  // namespace stringtools_impl

std::string toLower(std::string_view src) {
	std::string ret;
	ret.reserve(src.size());
	for (char ch : src) {
		ret.push_back(tolower(ch));
	}
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
		} else {
			dst.push_back(*it);
		}
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
		} else {
			dst.push_back(*it);
		}
	}
	return dst;
}

std::wstring& utf8_to_utf16(std::string_view src, std::wstring& dst) {
	dst.resize(src.length());
	auto end = utf8::unchecked::utf8to32(src.begin(), src.end(), dst.begin());
	dst.resize(std::distance(dst.begin(), end));
	return dst;
}

std::string& utf16_to_utf8(const std::wstring& src, std::string& dst) {
	dst.resize(src.length() * 4);
	auto end = utf8::unchecked::utf32to8(src.begin(), src.end(), dst.begin());
	dst.resize(std::distance(dst.begin(), end));
	return dst;
}

std::wstring utf8_to_utf16(std::string_view src) {
	std::wstring dst;
	return utf8_to_utf16(src, dst);
}
std::string utf16_to_utf8(const std::wstring& src) {
	std::string dst;
	return utf16_to_utf8(src, dst);
}
size_t utf16_to_utf8_size(const std::wstring& src) { return utf8::unchecked::utf32to8_size(src.begin(), src.end()); }

// This functions calculate how many bytes takes limit symbols in UTF8 forward
size_t calcUtf8After(std::string_view str, size_t limit) noexcept {
	const char* ptr;
	const char* strEnd = str.data() + str.size();
	for (ptr = str.data(); limit && ptr < strEnd; limit--) {
		utf8::unchecked::next(ptr);
	}
	return ptr - str.data();
}
std::pair<size_t, size_t> calcUtf8AfterDelims(std::string_view str, size_t limit, std::string_view delims) noexcept {
	const char* ptr;
	const char* strEnd;
	const char* ptrDelims;
	const char* delimsEnd;
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
size_t calcUtf8Before(const char* str, int pos, size_t limit) noexcept {
	const char* ptr = str + pos;
	for (; limit && ptr > str; limit--) {
		utf8::unchecked::prior(ptr);
	}
	return str + pos - ptr;
}

std::pair<size_t, size_t> calcUtf8BeforeDelims(const char* str, int pos, size_t limit, std::string_view delims) noexcept {
	const char* ptr = str + pos;
	const char* ptrDelim;
	const char* delimsEnd;
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

void split(std::string_view str, std::string& buf, std::vector<std::string_view>& words, std::string_view extraWordSymbols) {
	// assuming that the 'ToLower' function and the 'check for replacement' function should not change the character size in bytes
	buf.resize(str.length());
	words.resize(0);
	auto bufIt = buf.begin();

	for (auto it = str.begin(), endIt = str.end(); it != endIt;) {
		auto ch = utf8::unchecked::next(it);

		while (!IsAlpha(ch) && !IsDigit(ch) && extraWordSymbols.find(ch) == std::string::npos && it != endIt) {
			ch = utf8::unchecked::next(it);
		}

		const auto begIt = bufIt;
		while (IsAlpha(ch) || IsDigit(ch) || extraWordSymbols.find(ch) != std::string::npos) {
			ch = ToLower(ch);
			check_for_replacement(ch);
			bufIt = utf8::unchecked::append(ch, bufIt);
			if (it != endIt) {
				ch = utf8::unchecked::next(it);
			} else {
				break;
			}
		}

		if (begIt != bufIt) {
			words.emplace_back(&(*begIt), bufIt - begIt);
		}
	}
}

void split(std::string_view utf8Str, std::wstring& utf16str, std::vector<std::wstring>& words, std::string_view extraWordSymbols) {
	utf8_to_utf16(utf8Str, utf16str);
	words.resize(0);
	for (auto it = utf16str.begin(); it != utf16str.end();) {
		while (it != utf16str.end() && !IsAlpha(*it) && !IsDigit(*it)) {
			it++;
		}

		auto begIt = it;
		while (it != utf16str.end() && (IsAlpha(*it) || IsDigit(*it) || extraWordSymbols.find(*it) != std::string::npos)) {
			*it = ToLower(*it);
			++it;
		}
		size_t sz = it - begIt;
		if (sz) {
			words.emplace_back(&*begIt, &*(begIt + sz));
		}
	}
}

template <CaseSensitive sensitivity>
bool checkIfStartsWith(std::string_view pattern, std::string_view str) noexcept {
	if (pattern.empty() || str.empty()) {
		return false;
	}
	if (pattern.length() > str.length()) {
		return false;
	}
	if constexpr (sensitivity == CaseSensitive::Yes) {
		for (size_t i = 0; i < pattern.length(); ++i) {
			if (pattern[i] != str[i]) {
				return false;
			}
		}
	} else {
		for (size_t i = 0; i < pattern.length(); ++i) {
			if (tolower(pattern[i]) != tolower(str[i])) {
				return false;
			}
		}
	}
	return true;
}
template bool checkIfStartsWith<CaseSensitive::Yes>(std::string_view pattern, std::string_view src) noexcept;
template bool checkIfStartsWith<CaseSensitive::No>(std::string_view pattern, std::string_view src) noexcept;

template <CaseSensitive sensitivity>
bool checkIfEndsWith(std::string_view pattern, std::string_view src) noexcept {
	if (pattern.length() > src.length()) {
		return false;
	}
	if (pattern.length() == 0) {
		return true;
	}
	const auto offset = src.length() - pattern.length();
	if constexpr (sensitivity == CaseSensitive::Yes) {
		for (size_t i = 0; i < pattern.length(); ++i) {
			if (src[offset + i] != pattern[i]) {
				return false;
			}
		}
	} else {
		for (size_t i = 0; i < pattern.length(); ++i) {
			if (tolower(src[offset + i]) != tolower(pattern[i])) {
				return false;
			}
		}
	}
	return true;
}
template bool checkIfEndsWith<CaseSensitive::Yes>(std::string_view pattern, std::string_view src) noexcept;
template bool checkIfEndsWith<CaseSensitive::No>(std::string_view pattern, std::string_view src) noexcept;

template <>
ComparationResult collateCompare<CollateASCII>(std::string_view lhs, std::string_view rhs, const SortingPrioritiesTable&) noexcept {
	auto itl = lhs.begin();
	auto itr = rhs.begin();

	for (; itl != lhs.end() && itr != rhs.end();) {
		auto chl = tolower(*itl++);
		auto chr = tolower(*itr++);

		if (chl > chr) {
			return ComparationResult::Gt;
		}
		if (chl < chr) {
			return ComparationResult::Lt;
		}
	}

	if (lhs.size() > rhs.size()) {
		return ComparationResult::Gt;
	} else if (lhs.size() < rhs.size()) {
		return ComparationResult::Lt;
	}

	return ComparationResult::Eq;
}

template <>
ComparationResult collateCompare<CollateUTF8>(std::string_view lhs, std::string_view rhs, const SortingPrioritiesTable&) noexcept {
	auto itl = lhs.data();
	auto itr = rhs.data();

	for (auto lhsEnd = lhs.data() + lhs.size(), rhsEnd = rhs.size() + rhs.data(); itl != lhsEnd && itr != rhsEnd;) {
		auto chl = ToLower(utf8::unchecked::next(itl));
		auto chr = ToLower(utf8::unchecked::next(itr));

		if (chl > chr) {
			return ComparationResult::Gt;
		}
		if (chl < chr) {
			return ComparationResult::Lt;
		}
	}

	if (lhs.size() > rhs.size()) {
		return ComparationResult::Gt;
	} else if (lhs.size() < rhs.size()) {
		return ComparationResult::Lt;
	}
	return ComparationResult::Eq;
}

template <>
ComparationResult collateCompare<CollateNumeric>(std::string_view lhs, std::string_view rhs, const SortingPrioritiesTable&) noexcept {
	char* posl = nullptr;
	char* posr = nullptr;

	int numl = strtol(lhs.data(), &posl, 10);
	int numr = strtol(rhs.data(), &posr, 10);

	if (numl == numr) {
		auto minlen = std::min(lhs.size() - (posl - lhs.data()), rhs.size() - (posr - rhs.data()));
		auto res = strncmp(posl, posr, minlen);

		if (res != 0) {
			return res < 0 ? ComparationResult::Lt : ComparationResult::Gt;
		}

		return lhs.size() > rhs.size() ? ComparationResult::Gt : (lhs.size() < rhs.size() ? ComparationResult::Lt : ComparationResult::Eq);
	}

	return numl > numr ? ComparationResult::Gt : (numl < numr ? ComparationResult::Lt : ComparationResult::Eq);
}

template <>
ComparationResult collateCompare<CollateCustom>(std::string_view lhs, std::string_view rhs,
												const SortingPrioritiesTable& sortOrderTable) noexcept {
	auto itl = lhs.data();
	auto itr = rhs.data();

	for (; itl != lhs.data() + lhs.size() && itr != rhs.size() + rhs.data();) {
		auto chl = utf8::unchecked::next(itl);
		auto chr = utf8::unchecked::next(itr);

		int chlPriority = sortOrderTable.GetPriority(chl);
		int chrPriority = sortOrderTable.GetPriority(chr);

		if (chlPriority > chrPriority) {
			return ComparationResult::Gt;
		}
		if (chlPriority < chrPriority) {
			return ComparationResult::Lt;
		}
	}

	if (lhs.size() > rhs.size()) {
		return ComparationResult::Gt;
	} else if (lhs.size() < rhs.size()) {
		return ComparationResult::Lt;
	}
	return ComparationResult::Eq;
}

template <>
ComparationResult collateCompare<CollateNone>(std::string_view lhs, std::string_view rhs, const SortingPrioritiesTable&) noexcept {
	size_t l1 = lhs.size();
	size_t l2 = rhs.size();
	int res = memcmp(lhs.data(), rhs.data(), std::min(l1, l2));

	return res ? (res < 0 ? ComparationResult::Lt : ComparationResult::Gt)
			   : ((l1 < l2)	  ? ComparationResult::Lt
				  : (l1 > l2) ? ComparationResult::Gt
							  : ComparationResult::Eq);
}

std::string urldecode2(std::string_view str) {
	std::string ret(str.length(), ' ');
	std::string_view sret = stringtools_impl::urldecode2(&ret[0], str);
	ret.resize(sret.size());
	return ret;
}

int fast_strftime(char* buf, const tm* tm) {
	char* d = buf;

	if (unsigned(tm->tm_wday) < sizeof(stringtools_impl::kDaysOfWeek) / sizeof stringtools_impl::kDaysOfWeek[0]) {
		d = stringtools_impl::strappend(d, stringtools_impl::kDaysOfWeek[tm->tm_wday]);
	}
	d = stringtools_impl::strappend(d, ", ");
	d = i32toa(tm->tm_mday, d);
	*d++ = ' ';
	if (unsigned(tm->tm_mon) < sizeof(stringtools_impl::kMonths) / sizeof stringtools_impl::kMonths[0]) {
		d = stringtools_impl::strappend(d, stringtools_impl::kMonths[tm->tm_mon]);
	}
	*d++ = ' ';
	d = i32toa(tm->tm_year + 1900, d);
	*d++ = ' ';
	d = i32toa(tm->tm_hour, d);
	*d++ = ':';
	d = i32toa(tm->tm_min, d);
	*d++ = ':';
	d = i32toa(tm->tm_sec, d);
	d = stringtools_impl::strappend(d, " GMT");
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

LogLevel logLevelFromString(std::string_view strLogLevel) {
	constexpr static auto levels = frozen::make_unordered_map<std::string_view, LogLevel>(
		{{"none", LogNone}, {"warning", LogWarning}, {"error", LogError}, {"info", LogInfo}, {"trace", LogTrace}},
		frozen::nocase_hash_str{}, frozen::nocase_equal_str{});

	auto configLevelIt = levels.find(strLogLevel);
	if (configLevelIt != levels.end()) {
		return configLevelIt->second;
	}
	return LogNone;
}

StrictMode strictModeFromString(std::string_view strStrictMode) {
	auto configModeIt = stringtools_impl::kStrictModes.find(strStrictMode);
	if (configModeIt != stringtools_impl::kStrictModes.end()) {
		return configModeIt->second;
	}
	return StrictModeNotSet;
}

std::string_view strictModeToString(StrictMode mode) {
	for (auto& it : stringtools_impl::kStrictModes) {
		if (it.second == mode) {
			return it.first;
		}
	}
	return std::string_view();
}

bool isPrintable(std::string_view str) noexcept {
	if (str.length() > 256) {
		return false;
	}
	for (auto c : str) {
		if (c < 0x20) {
			return false;
		}
	}
	return true;
}

bool isBlank(std::string_view str) noexcept {
	if (str.empty()) {
		return true;
	}
	for (auto c : str) {
		if (!isspace(c)) {
			return false;
		}
	}
	return true;
}

int getUTF8StringCharactersCount(std::string_view str) noexcept {
	int len = 0;
	try {
		for (auto it = str.begin(); it != str.end(); ++len) {
			utf8::next(it, str.end());
		}
	} catch (const std::exception&) {
		return str.length();
	}
	return len;
}

int stoi(std::string_view sl) {
	bool valid;
	const int res = jsteemann::atoi<int>(sl.data(), sl.data() + sl.size(), valid);
	if (!valid) {
		throw Error(errParams, "Can't convert '%s' to number", sl);
	}
	return res;
}

std::optional<int> try_stoi(std::string_view sl) {
	bool valid;
	const int res = jsteemann::atoi<int>(sl.data(), sl.data() + sl.size(), valid);
	if (!valid) {
		return std::nullopt;
	}
	return res;
}

int64_t stoll(std::string_view sl) {
	bool valid;
	auto ret = jsteemann::atoi<int64_t>(sl.data(), sl.data() + sl.size(), valid);
	if (!valid) {
		throw Error(errParams, "Can't convert '%s' to number", sl);
	}
	return ret;
}

int double_to_str(double v, char* buf, int capacity) {
	(void)capacity;
	auto end = fmt::format_to(buf, FMT_COMPILE("{}"), v);
	auto p = buf;
	do {
		if (*p == '.' || *p == 'e') {
			break;
		}
	} while (++p != end);

	if (p == end) {
		*end++ = '.';
		*end++ = '0';
	}

	assertrx_dbg(end - buf < capacity);
	return end - buf;
}

int double_to_str_no_trailing(double v, char* buf, int capacity) {
	(void)capacity;
	auto end = fmt::format_to(buf, FMT_COMPILE("{}"), v);
	assertrx_dbg(end - buf < capacity);
	return end - buf;
}

std::string double_to_str(double v) {
	std::string res;
	res.resize(32);
	auto len = double_to_str(v, res.data(), res.size());
	res.resize(len);
	return res;
}

std::string randStringAlph(size_t len) {
	constexpr std::string_view symbols = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
	std::string result;
	result.reserve(len);
	while (result.size() < len) {
		const size_t f = tools::RandomGenerator::getu32(0, symbols.size() - 1);
		result += symbols[f];
	}
	return result;
}

template <bool isUtf8>
void toNextCh(std::string_view::iterator& it, std::string_view str) {
	if (isUtf8) {
		utf8::next(it, str.end());
	} else {
		++it;
	}
}

template <bool isUtf8>
void toPrevCh(std::string_view::iterator& it, std::string_view str) {
	if (isUtf8) {
		utf8::previous(it, str.begin());
	} else {
		--it;
	}
}

template <bool isUtf8>
Error getBytePosInMultilineString(std::string_view str, const size_t line, const size_t charPos, size_t& bytePos) {
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
		return Error();
	}
	return Error(errNotValid, "Wrong cursor position: line=%d, pos=%d", line, charPos);
}

Error cursosPosToBytePos(std::string_view str, size_t line, size_t charPos, size_t& bytePos) {
	try {
		return getBytePosInMultilineString<true>(str, line, charPos, bytePos);
	} catch (const utf8::exception&) {
		return getBytePosInMultilineString<false>(str, line, charPos, bytePos);
	}
}

}  // namespace reindexer
