#pragma once

#include <string.h>
#include <time.h>
#include <string>
#include <vector>
#include "core/indexopts.h"
#include "core/type_consts.h"
#include "estl/string_view.h"
#include "tools/customhash.h"
#include "tools/errors.h"

using std::string;
using std::vector;
using std::wstring;
using std::pair;

namespace reindexer {

string escapeString(string_view str);
string unescapeString(string_view str);

static inline bool isalpha(char c) { return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z'); }
static inline bool isdigit(char c) { return (c >= '0' && c <= '9'); }
static inline char tolower(char c) { return (c >= 'A' && c <= 'Z') ? c + 'a' - 'A' : c; }
string toLower(string_view src);

template <typename Container>
Container& split(const typename Container::value_type& str, const string& delimiters, bool trimEmpty, Container& tokens) {
	tokens.resize(0);

	for (size_t pos, lastPos = 0;; lastPos = pos + 1) {
		pos = str.find_first_of(delimiters, lastPos);
		if (pos == string::npos) {
			pos = str.length();
			if (pos != lastPos || !trimEmpty) tokens.push_back(str.substr(lastPos, pos - lastPos));
			break;
		} else if (pos != lastPos || !trimEmpty)
			tokens.push_back(str.substr(lastPos, pos - lastPos));
	}
	return tokens;
}

void split(const string& utf8Str, wstring& utf16str, vector<std::wstring>& words);
void split(string_view utf8Str, wstring& utf16str, vector<std::wstring>& words, const string& extraWordSymbols);
void split(string_view str, string& buf, vector<const char*>& words, const string& extraWordSymbols);
size_t calcUTf8Size(const char* s, size_t size, size_t limit);
size_t calcUTf8SizeEnd(const char* end, int pos, size_t limit);

int getUTF8StringCharactersCount(string_view str);

class Word2PosHelper {
public:
	Word2PosHelper(string_view data, const string& extraWordSymbols);
	std::pair<int, int> convert(int wordPos, int endPos);

protected:
	string_view data_;
	int lastWordPos_, lastOffset_;
	const string& extraWordSymbols_;
};

int collateCompare(string_view lhs, string_view rhs, const CollateOpts& collateOpts);

wstring utf8_to_utf16(string_view src);
string utf16_to_utf8(const wstring& src);
wstring& utf8_to_utf16(string_view src, wstring& dst);
string& utf16_to_utf8(const wstring& src, string& dst);

void check_for_replacement(wchar_t& ch);
void check_for_replacement(uint32_t& ch);
bool is_number(string_view str);

int fast_strftime(char* buf, const tm* tm);
string urldecode2(string_view str);

int stoi(string_view sl);
int64_t stoll(string_view sl);

bool validateObjectName(string_view name);
LogLevel logLevelFromString(const string& strLogLevel);
StrictMode strictModeFromString(const std::string& strStrictMode);
string_view strictModeToString(StrictMode mode);

bool iequals(string_view lhs, string_view rhs);
bool checkIfStartsWith(string_view src, string_view pattern, bool casesensitive = false);
bool isPrintable(string_view str);
bool isBlank(string_view token);

Error cursosPosToBytePos(string_view str, size_t line, size_t charPos, size_t& bytePos);

string randStringAlph(size_t len);

struct nocase_equal_str {
	using is_transparent = void;

	bool operator()(string_view lhs, const string& rhs) const { return iequals(lhs, rhs); }
	bool operator()(const string& lhs, string_view rhs) const { return iequals(lhs, rhs); }
	bool operator()(const string& lhs, const string& rhs) const { return iequals(lhs, rhs); }
};

struct nocase_hash_str {
	using is_transparent = void;
	size_t operator()(string_view hs) const { return collateHash(hs, CollateASCII); }
	size_t operator()(const string& hs) const { return collateHash(hs, CollateASCII); }
};

struct equal_str {
	using is_transparent = void;

	bool operator()(string_view lhs, const string& rhs) const { return lhs == rhs; }
	bool operator()(const string& lhs, string_view rhs) const { return rhs == lhs; }
	bool operator()(const string& lhs, const string& rhs) const { return lhs == rhs; }
};

struct hash_str {
	using is_transparent = void;
	size_t operator()(string_view hs) const { return collateHash(hs, CollateNone); }
	size_t operator()(const string& hs) const { return collateHash(hs, CollateNone); }
};

}  // namespace reindexer
