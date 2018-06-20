#pragma once

#include <time.h>
#include <string>
#include <vector>
#include "core/indexopts.h"
#include "core/type_consts.h"
#include "estl/string_view.h"

using std::string;
using std::vector;
using std::wstring;
using std::pair;

namespace reindexer {

vector<string>& split(const string& str, const string& delimiters, bool trimEmpty, vector<string>&);
void split(const string& utf8Str, wstring& utf16str, vector<std::wstring>& words);
void splitWithPos(const string& str, string& buf, vector<pair<const char*, int>>& words);
size_t calcUTf8Size(const char* s, size_t size, size_t limit);
size_t calcUTf8SizeEnd(const char* end, int pos, size_t limit);

string lower(string s);
int collateCompare(const string_view& lhs, const string_view& rhs, const CollateOpts& collateOpts);

wstring utf8_to_utf16(const string& src);
string utf16_to_utf8(const wstring& src);
wstring& utf8_to_utf16(const string& src, wstring& dst);
string& utf16_to_utf8(const wstring& src, string& dst);
wstring& utf8_to_utf16(const char* src, wstring& dst);

size_t utf16_to_utf8(const wchar_t* src, size_t len, char* dst, size_t dstLen);
void check_for_replacement(wchar_t& ch);
void check_for_replacement(uint32_t& ch);
bool is_number(const string& str);

int fast_strftime(char* buf, const tm* tm);
string_view urldecode2(char* buf, const string_view& str);
string urldecode2(const string_view& str);

inline static char* strappend(char* dst, const char* src) {
	while (*src) *dst++ = *src++;
	return dst;
}

inline static char* strappend(char* dst, const string_view& src) {
	const char* p = src.data();
	size_t l = src.length();
	while (l) {
		*dst++ = *p++;
		l--;
	}
	return dst;
}

inline static int stoi(const string_view& sl) { return atoi(sl.data()); }

bool validateObjectName(const char* name);
LogLevel logLevelFromString(const string& strLogLevel);

static bool inline iequals(const string_view& lhs, const string_view& rhs) {
	return lhs.size() == rhs.size() && collateCompare(lhs, rhs, CollateOpts(CollateASCII)) == 0;
}

}  // namespace reindexer
