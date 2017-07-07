#pragma once

#include <string>
#include <vector>

using namespace std;

namespace reindexer {

vector<string> mktypos(const string &word, vector<string> &typos);
vector<string> &split(const string &str, const string &delimiters, bool trimEmpty, vector<string> &);

wstring utf8_to_utf16(const string &src);
string utf16_to_utf8(const wstring &src);
wstring &utf8_to_utf16(const string &src, wstring &dst);
string &utf16_to_utf8(const wstring &src, string &dst);

}  // namespace reindexer
