#include <assert.h>
#include <memory.h>
#include <algorithm>
#include <locale>
#include <string>
#include <vector>

#include "convertutf/convertutf.h"
#include "tools/customlocal.h"
#include "tools/stringstools.h"

using std::min;
using std::stoi;
using std::transform;

namespace reindexer {

wstring &utf8_to_utf16(const string &src, wstring &dst) {
	assert(sizeof(UTF32) == sizeof(wchar_t));
	const UTF8 *srcStart = reinterpret_cast<const UTF8 *>(src.data());
	size_t len = src.length();
	dst.resize(len + 1);
	UTF32 *tgt = reinterpret_cast<UTF32 *>(&dst[0]);
	UTF32 *tgtStart = tgt;
	ConvertUTF8toUTF32(&srcStart, srcStart + len, &tgtStart, tgt + len, lenientConversion);
	dst.resize(tgtStart - tgt);
	return dst;
}

wstring &utf8_to_utf16(const char *src, wstring &dst) {
	assert(sizeof(UTF32) == sizeof(wchar_t));
	const UTF8 *srcStart = reinterpret_cast<const UTF8 *>(src);
	size_t len = strlen(src);
	dst.resize(len + 1);
	UTF32 *tgt = reinterpret_cast<UTF32 *>(&dst[0]);
	UTF32 *tgtStart = tgt;
	ConvertUTF8toUTF32(&srcStart, srcStart + len, &tgtStart, tgt + len, lenientConversion);
	dst.resize(tgtStart - tgt);
	return dst;
}

string &utf16_to_utf8(const wstring &src, string &dst) {
	assert(sizeof(UTF32) == sizeof(wchar_t));
	const UTF32 *srcStart = reinterpret_cast<const UTF32 *>(src.data());
	size_t len = src.length();
	dst.resize(len * 4 + 1);
	UTF8 *tgt = reinterpret_cast<UTF8 *>(&dst[0]);
	UTF8 *tgtStart = tgt;
	ConvertUTF32toUTF8(&srcStart, srcStart + len, &tgtStart, tgt + len * 4, lenientConversion);
	dst.resize(tgtStart - tgt);
	return dst;
}

size_t utf16_to_utf8(const wchar_t *src, size_t len, char *dst, size_t dstLen) {
	assert(sizeof(UTF32) == sizeof(wchar_t));
	const UTF32 *srcStart = reinterpret_cast<const UTF32 *>(src);
	UTF8 *tgt = reinterpret_cast<UTF8 *>(dst);
	UTF8 *tgtStart = tgt;
	ConvertUTF32toUTF8(&srcStart, srcStart + len, &tgtStart, tgt + dstLen, lenientConversion);
	*tgtStart = 0;
	return tgtStart - tgt;
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

void split(const string &utf8Str, wstring &utf16str, string &buf, vector<const char *> &words) {
	utf8_to_utf16(utf8Str, utf16str);
	buf.resize(utf8Str.length());
	words.resize(0);
	size_t outSz = 0;
	for (auto it = utf16str.begin(); it != utf16str.end();) {
		while (it != utf16str.end() && !IsAlpha(*it) && !std::isdigit(*it)) it++;

		auto begIt = it;
		while (it != utf16str.end() && (IsAlpha(*it) || std::isdigit(*it) || *it == '+' || *it == '-' || *it == '/')) {
			*it = ToLower(*it);
			it++;
		}
		size_t sz = it - begIt;
		if (sz) {
			sz = utf16_to_utf8(&*begIt, sz, &buf[outSz], buf.size() - outSz);
			words.push_back(&buf[outSz]);
		}
		outSz += sz + 1;
	}
}
void split(const string &utf8Str, wstring &utf16str, vector<std::wstring> &words) {
	utf8_to_utf16(utf8Str, utf16str);
	words.resize(0);
	size_t outSz = 0;
	for (auto it = utf16str.begin(); it != utf16str.end();) {
		while (it != utf16str.end() && !IsAlpha(*it) && !std::isdigit(*it)) it++;

		auto begIt = it;
		while (it != utf16str.end() && (IsAlpha(*it) || std::isdigit(*it) || *it == '+' || *it == '-' || *it == '/')) {
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

int collateCompare(const Slice &lhs, const Slice &rhs, int collateMode) {
	if (collateMode == CollateASCII) {
		size_t itl = 0;
		size_t itr = 0;

		for (; itl < lhs.size() && itr < rhs.size(); itl++, itr++) {
			if (tolower(lhs.data()[itl]) > tolower(rhs.data()[itr])) return 1;
			if (tolower(lhs.data()[itl]) < tolower(rhs.data()[itr])) return -1;
		}

		if (lhs.size() > rhs.size()) {
			return 1;
		} else if (lhs.size() < rhs.size()) {
			return -1;
		}

		return 0;
	} else if (collateMode == CollateUTF8) {
		wstring lu16str;
		wstring ru16str;

		utf8_to_utf16(lhs.data(), lu16str);
		utf8_to_utf16(rhs.data(), ru16str);

		ToLower(lu16str);
		ToLower(ru16str);

		return lu16str.compare(ru16str);
	} else if (collateMode == CollateNumeric) {
		size_t posl = string::npos;
		size_t posr = string::npos;

		int numl = stoi(lhs.data(), &posl);
		int numr = stoi(rhs.data(), &posr);

		if (numl == numr) {
			auto minlen = min(lhs.size() - posl, rhs.size() - posr);
			auto res = strncmp(lhs.data() + posl, rhs.data() + posr, minlen);

			if (res != 0) {
				return res;
			}

			return lhs.size() > rhs.size() ? 1 : (lhs.size() > rhs.size() ? -1 : 0);
		}

		return numl > numr ? 1 : (numl < numr ? -1 : 0);
	}

	size_t l1 = lhs.size();
	size_t l2 = rhs.size();
	int res = memcmp(lhs.data(), rhs.data(), std::min(l1, l2));

	return res ? res : ((l1 < l2) ? -1 : (l1 > l2) ? 1 : 0);
}

}  // namespace reindexer
