#include <assert.h>
#include <string>
#include <vector>

#if 0
#include <codecvt>
#include <locale>
#else
#include "convertutf/convertutf.h"
#endif

using namespace std;

namespace reindexer {
#if 0
// buggy. gcc 5.3 has broken implementation
wstring utf8_to_utf16 (const string &src) {
	wstring_convert<std::codecvt_utf8_utf16<wchar_t> > convert ("?");
	wstring res;
	try {
		res = convert.from_bytes( src );
	}
	catch (const std::range_error &err) {

	}
	return res;
}

string utf16_to_utf8 (const wstring &src) {
	wstring_convert<std::codecvt_utf8_utf16<wchar_t> > convert ("?");
	string res;
	try {
		res = convert.to_bytes( src );
	}
	catch (const std::range_error &err) {

	}
	return res;
}
#else
wstring &utf8_to_utf16(const string &src, wstring &dst) {
	assert(sizeof(UTF32) == sizeof(wchar_t));
	const UTF8 *srcStart = (const UTF8 *)src.data();
	size_t len = src.length();
	UTF32 *tgt = (UTF32 *)alloca((len + 1) * sizeof(UTF32));
	UTF32 *tgtStart = tgt;
	ConvertUTF8toUTF32(&srcStart, srcStart + len, &tgtStart, tgt + len, lenientConversion);
	*tgtStart = 0;
	dst.assign((const wchar_t *)tgt);
	return dst;
}
string &utf16_to_utf8(const wstring &src, string &dst) {
	assert(sizeof(UTF32) == sizeof(wchar_t));
	const UTF32 *srcStart = (const UTF32 *)src.data();
	size_t len = src.length();
	UTF8 *tgt = (UTF8 *)alloca((len + 1) * sizeof(UTF8) * 4);
	UTF8 *tgtStart = tgt;
	ConvertUTF32toUTF8(&srcStart, srcStart + len, &tgtStart, tgt + len * 4, lenientConversion);
	*tgtStart = 0;
	dst.assign((const char *)tgt);
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

#endif

vector<string> mktypos(const string &word, vector<string> &typos) {
	typos.resize(0);
	wstring utf16Word = utf8_to_utf16(word);
	if (utf16Word.length() < 2) return typos;

	wstring utf16Typo = utf16Word.substr(1);
	typos.push_back(utf16_to_utf8(utf16Typo));
	for (size_t i = 0; i < utf16Typo.length(); ++i) {
		utf16Typo[i] = utf16Word[i];
		typos.push_back(utf16_to_utf8(utf16Typo));
	}
	return typos;
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
}