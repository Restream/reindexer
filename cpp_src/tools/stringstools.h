#pragma once

#include <string.h>
#include <time.h>
#include <string>
#include <vector>
#include "core/indexopts.h"
#include "core/type_consts.h"
#include "estl/string_view.h"
#include "tools/customhash.h"

using std::string;
using std::vector;
using std::wstring;
using std::pair;

namespace reindexer {

vector<string>& split(const string_view& str, const string& delimiters, bool trimEmpty, vector<string>&);
void split(const string& utf8Str, wstring& utf16str, vector<std::wstring>& words);
void split(const string_view& utf8Str, wstring& utf16str, vector<std::wstring>& words, const string& extraWordSymbols);
void split(const string_view& str, string& buf, vector<const char*>& words, const string& extraWordSymbols);
size_t calcUTf8Size(const char* s, size_t size, size_t limit);
size_t calcUTf8SizeEnd(const char* end, int pos, size_t limit);

class Word2PosHelper {
public:
	Word2PosHelper(string_view data, const string& extraWordSymbols);
	std::pair<int, int> convert(int wordPos, int endPos);

protected:
	string_view data_;
	int lastWordPos_, lastOffset_;
	const string& extraWordSymbols_;
};

string lower(string s);
int collateCompare(const string_view& lhs, const string_view& rhs, const CollateOpts& collateOpts);

wstring utf8_to_utf16(const string& src);
string utf16_to_utf8(const wstring& src);
wstring& utf8_to_utf16(const string_view& src, wstring& dst);
string& utf16_to_utf8(const wstring& src, string& dst);

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
	for (auto p = src.begin(); p != src.end(); p++) *dst++ = *p;
	return dst;
}

inline static int stoi(const string_view& sl) { return atoi(sl.data()); }

bool validateObjectName(const string_view& name);
LogLevel logLevelFromString(const string& strLogLevel);

static bool inline iequals(const string_view& lhs, const string_view& rhs) {
	return lhs.size() == rhs.size() && collateCompare(lhs, rhs, CollateOpts(CollateASCII)) == 0;
}

struct nocase_equal_str {
	using is_transparent = void;

	bool operator()(const string_view& lhs, const string& rhs) const { return iequals(lhs, rhs); }
	bool operator()(const string& lhs, const string_view& rhs) const { return iequals(lhs, rhs); }
	bool operator()(const string& lhs, const string& rhs) const { return iequals(lhs, rhs); }
};

struct nocase_hash_str {
	using is_transparent = void;
	size_t operator()(const string_view& hs) const { return collateHash(hs, CollateASCII); }
	size_t operator()(const string& hs) const { return collateHash(hs, CollateASCII); }
};

}  // namespace reindexer
