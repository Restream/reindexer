#include <assert.h>
#include <memory.h>
#include <algorithm>
#include <cctype>
#include <locale>
#include <string>
#include <unordered_map>
#include <vector>

#include "itoa/itoa.h"
#include "tools/customlocal.h"
#include "tools/stringstools.h"
#include "utf8cpp/utf8.h"

using std::min;
using std::stoi;
using std::transform;
using std::distance;
using std::make_pair;

namespace reindexer {

wstring &utf8_to_utf16(const string &src, wstring &dst) {
	dst.resize(src.length());
	auto end = utf8::unchecked::utf8to32(src.begin(), src.end(), dst.begin());
	dst.resize(std::distance(dst.begin(), end));
	return dst;
}

wstring &utf8_to_utf16(const char *src, wstring &dst) {
	size_t len = strlen(src);
	dst.resize(len);
	auto end = utf8::unchecked::utf8to32(src, src + len, dst.begin());
	dst.resize(std::distance(dst.begin(), end));
	return dst;
}

string &utf16_to_utf8(const wstring &src, string &dst) {
	dst.resize(src.length() * 4);
	auto end = utf8::unchecked::utf32to8(src.begin(), src.end(), dst.begin());
	dst.resize(std::distance(dst.begin(), end));
	return dst;
}

wstring utf8_to_utf16(const string &src) {
	wstring dst;
	return utf8_to_utf16(src, dst);
}
string utf16_to_utf8(const wstring &src) {
	string dst;
	return utf16_to_utf8(src, dst);
}

vector<string> &split(const string &str, const string &delimiters, bool trimEmpty, vector<string> &tokens) {
	tokens.resize(0);

	for (size_t pos, lastPos = 0;; lastPos = pos + 1) {
		pos = str.find_first_of(delimiters, lastPos);
		if (pos == string::npos) {
			pos = str.length();
			if (pos != lastPos || !trimEmpty) tokens.push_back(string(str.data() + lastPos, (pos - lastPos)));
			break;
		} else if (pos != lastPos || !trimEmpty)
			tokens.push_back(string(str.data() + lastPos, (pos - lastPos)));
	}
	return tokens;
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
	for (ptr = end; limit && ptr >= end - pos; limit--) utf8::unchecked::prior(ptr);
	return end - ptr;
}

void check_for_replacement(wchar_t &ch) {
	if (ch == 0x451) {  // 'ё'
		ch = 0x435;		// 'е'
	}
}

void check_for_replacement(uint32_t &ch) {
	if (ch == 0x451) {  // 'ё'
		ch = 0x435;		// 'е'
	}
}

bool is_number(const string &str) {
	uint16_t i = 0;
	while ((i < str.length() && IsDigit(str[i]))) i++;
	return (i == str.length());
}

void splitWithPos(const string &str, string &buf, vector<pair<const char *, int>> &words) {
	buf.resize(str.length());
	words.resize(0);
	auto bufIt = buf.begin();

	for (auto it = str.begin(); it != str.end();) {
		auto wordStartIt = it;
		auto ch = utf8::unchecked::next(it);

		while (it != str.end() && !IsAlpha(ch) && !IsDigit(ch)) {
			wordStartIt = it;
			ch = utf8::unchecked::next(it);
		}

		auto begIt = bufIt;
		while (it != str.end() && (IsAlpha(ch) || IsDigit(ch) || ch == '+' || ch == '-' || ch == '/')) {
			ch = ToLower(ch);
			check_for_replacement(ch);
			bufIt = utf8::unchecked::append(ch, bufIt);
			ch = utf8::unchecked::next(it);
		}
		if ((IsAlpha(ch) || IsDigit(ch) || ch == '+' || ch == '-' || ch == '/')) {
			ch = ToLower(ch);
			check_for_replacement(ch);

			bufIt = utf8::unchecked::append(ch, bufIt);
		}

		if (begIt != bufIt) {
			*bufIt++ = 0;
			words.push_back({&*begIt, std::distance(str.begin(), wordStartIt)});
		}
	}
}

void split(const string &utf8Str, wstring &utf16str, vector<std::wstring> &words) {
	utf8_to_utf16(utf8Str, utf16str);
	words.resize(0);
	size_t outSz = 0;
	for (auto it = utf16str.begin(); it != utf16str.end();) {
		while (it != utf16str.end() && !IsAlpha(*it) && !IsDigit(*it)) it++;

		auto begIt = it;
		while (it != utf16str.end() && (IsAlpha(*it) || IsDigit(*it) || *it == '+' || *it == '-' || *it == '/')) {
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

int collateCompare(const string_view &lhs, const string_view &rhs, const CollateOpts &collateOpts) {
	if (collateOpts.mode == CollateASCII) {
		auto itl = lhs.data();
		auto itr = rhs.data();

		for (; itl != lhs.data() + lhs.size() && itr != rhs.size() + rhs.data();) {
			auto chl = ToLower(*itl++);
			auto chr = ToLower(*itr++);

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

			return lhs.size() > rhs.size() ? 1 : (lhs.size() > rhs.size() ? -1 : 0);
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

string_view urldecode2(char *buf, const string_view &str) {
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

string urldecode2(const string_view &str) {
	string ret(str.length(), ' ');
	string_view sret = urldecode2(&ret[0], str);
	ret.resize(sret.size());
	return ret;
}

// Sat Jul 15 14 : 18 : 56 2017 GMT

static const char *daysOfWeek[] = {"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"};
static const char *months[] = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};

int fast_strftime(char *buf, const tm *tm) {
	char *d = buf;

	if (unsigned(tm->tm_wday) < sizeof(daysOfWeek) / sizeof daysOfWeek[0]) d = strappend(d, daysOfWeek[tm->tm_wday]);
	*d++ = ' ';
	if (unsigned(tm->tm_mon) < sizeof(months) / sizeof months[0]) d = strappend(d, months[tm->tm_mon]);
	*d++ = ' ';
	d = i32toa(tm->tm_mday, d);
	*d++ = ' ';
	d = i32toa(tm->tm_hour, d);
	*d++ = ':';
	d = i32toa(tm->tm_min, d);
	*d++ = ':';
	d = i32toa(tm->tm_sec, d);
	*d++ = ' ';
	d = i32toa(tm->tm_year + 1900, d);
	d = strappend(d, " GMT");
	*d = 0;
	return d - buf;
}

bool validateObjectName(const char *name) {
	if (!*name) {
		return false;
	}
	for (const char *p = name; *p; p++) {
		if (!(std::isalpha(*name) || std::isdigit(*name) || *name == '_' || *name == '-' || *name == '#')) {
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

}  // namespace reindexer
