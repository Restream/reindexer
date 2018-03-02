#pragma once

#include <time.h>
#include <string>
#include <vector>
#include "core/type_consts.h"
#include "tools/slice.h"

using std::string;
using std::vector;
using std::wstring;

namespace reindexer {

vector<string>& split(const string& str, const string& delimiters, bool trimEmpty, vector<string>&);
void split(const string& utf8Str, wstring& utf16str, string& buf, vector<const char*>& words);
void split(const string& utf8Str, wstring& utf16str, vector<std::wstring>& words);
string lower(string s);
int collateCompare(const Slice& lhs, const Slice& rhs, int mode);

wstring utf8_to_utf16(const string& src);
string utf16_to_utf8(const wstring& src);
wstring& utf8_to_utf16(const string& src, wstring& dst);
string& utf16_to_utf8(const wstring& src, string& dst);
wstring& utf8_to_utf16(const char* src, wstring& dst);

size_t utf16_to_utf8(const wchar_t* src, size_t len, char* dst, size_t dstLen);

int fast_strftime(char* buf, const tm* tm);
void urldecode2(char* dst, const char* src);

inline static char* strappend(char* dst, const char* src) {
	while (*src) *dst++ = *src++;
	return dst;
}

bool validateObjectName(const char* name);

}  // namespace reindexer
