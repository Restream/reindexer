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

static inline bool isalpha(char c) { return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z'); }
static inline bool isdigit(char c) { return (c >= '0' && c <= '9'); }
static inline char tolower(char c) { return (c >= 'A' && c <= 'Z') ? c + 'a' - 'A' : c; }

template <typename Container>
Container& split(const string& str, const string& delimiters, bool trimEmpty, Container& tokens) {
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

bool iequals(const string_view& lhs, const string_view& rhs);

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
