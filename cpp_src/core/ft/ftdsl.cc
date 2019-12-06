
#include "core/ft/ftdsl.h"
#include <algorithm>
#include <locale>
#include "tools/customlocal.h"
#include "tools/errors.h"
#include "tools/stringstools.h"

namespace reindexer {

using std::string;

// Format: see fulltext.md

bool is_dslbegin(int ch) {
	return IsAlpha(ch) || IsDigit(ch) || ch == '+' || ch == '-' || ch == '*' || ch == '\'' || ch == '\"' || ch == '@' || ch == '=' ||
		   ch == '\\';
}
bool is_term(int ch, const string &extraWordSymbols) { return IsAlpha(ch) || IsDigit(ch) || extraWordSymbols.find(ch) != string::npos; }

void FtDSLQuery::parse(const string &q) {
	wstring utf16str;
	utf8_to_utf16(q, utf16str);
	parse(utf16str);
}
void FtDSLQuery::parse(wstring &utf16str) {
	int groupcnt = 0;
	bool ingroup = false;
	int maxPatternLen = 1;
	h_vector<float, 8> fieldsBoost;
	fieldsBoost.insert(fieldsBoost.end(), std::max(int(fields_.size()), 1), 1.0);

	for (auto it = utf16str.begin(); it != utf16str.end();) {
		while (it != utf16str.end() && !is_dslbegin(*it)) it++;

		FtDSLEntry fte;
		fte.opts.fieldsBoost = fieldsBoost;

		bool isSlash = (*it == '\\');
		if (isSlash) {
			++it;
		} else {
			if (*it == '@') {
				it++;
				parseFields(utf16str, it, fieldsBoost);
				continue;
			}

			if (*it == '-') {
				fte.opts.op = OpNot;
				it++;
			} else if (*it == '+') {
				fte.opts.op = OpAnd;
				it++;
			}
			if (it != utf16str.end() && (*it == '\'' || *it == '\"')) {
				ingroup = !ingroup;
				it++;
				// closing group
				if (!ingroup) {
					int distance = 1;
					if (it != utf16str.end() && *it == '~') {
						wchar_t *end = nullptr, *start = &*++it;
						distance = wcstod(start, &end);
						it += end - start;
						if (end == start)
							throw Error(errParseDSL, "Expected digit after '~' operator in phrase, but found '%c' ", char(*start));
					}
					assertf(groupcnt <= int(size()), "groupcnt=%d,size=%d", groupcnt, size());
					if (groupcnt > 1) {
						auto fteIt = end();
						while (--groupcnt) {
							fteIt--;
							fteIt->opts.distance = distance;
							fteIt->opts.op = OpAnd;
						}
					}
				}
			}
			if (it != utf16str.end() && *it == '=') {
				fte.opts.exact = true;
				it++;
			}
			if (it != utf16str.end() && *it == '*') {
				fte.opts.suff = true;
				it++;
			}
		}
		auto begIt = it;
		while (it != utf16str.end() && (isSlash || is_term(*it, extraWordSymbols_))) {
			*it = ToLower(*it);
			check_for_replacement(*it);
			isSlash = (*(++it) == '\\');
			if (isSlash) {
				std::move(it + 1, utf16str.end(), it);
				utf16str.pop_back();
			}
		}
		auto endIt = it;
		for (; it != utf16str.end(); it++) {
			if (*it == '*') {
				fte.opts.pref = true;
			} else if (*it == '~') {
				fte.opts.typos = true;
			} else if (*it == '^') {
				wchar_t *end = nullptr, *start = &*++it;
				fte.opts.boost = wcstod(start, &end);
				it += end - start - 1;
				if (end == start)
					throw Error(errParseDSL, "Expected digit after '^' operator in search query DSL, but found '%c' ", char(*start));
			} else {
				break;
			}
		}

		if (endIt != begIt) {
			fte.pattern.assign(begIt, endIt);
			string utf8str = utf16_to_utf8(fte.pattern);
			if (is_number(utf8str)) fte.opts.number = true;
			if (stopWords_.find(utf8str) != stopWords_.end()) {
				continue;
			}

			if (int(fte.pattern.length()) > maxPatternLen) {
				maxPatternLen = fte.pattern.length();
			}
			push_back(fte);
			if (ingroup) groupcnt++;
		}
	}
	if (ingroup) {
		throw Error(errParseDSL, "No closing quote in full text search query DSL");
	}

	int cnt = 0;
	for (auto &e : *this) {
		e.opts.termLenBoost = float(e.pattern.length()) / maxPatternLen;
		e.opts.qpos = cnt++;
	}
}  // namespace reindexer

void FtDSLQuery::parseFields(wstring &utf16str, wstring::iterator &it, h_vector<float, 8> &fieldsBoost) {
	float defFieldBoost = 0.0;
	for (auto &b : fieldsBoost) b = 0.0;

	while (it != utf16str.end()) {
		while (it != utf16str.end() && !(IsAlpha(*it) || IsDigit(*it) || *it == '*' || *it == '_')) it++;

		auto begIt = it;
		while (it != utf16str.end() && (IsAlpha(*it) || IsDigit(*it) || *it == '*' || *it == '_')) it++;
		auto endIt = it;

		float boost = 1.0;
		if (it != utf16str.end() && *it == '^') {
			wchar_t *end = nullptr, *start = &*++it;
			boost = wcstod(start, &end);
			it += end - start;
			if (end == start)
				throw Error(errParseDSL, "Expected digit after '^' operator in search query DSL, but found '%c' ", char(*start));
		}

		if (*begIt == '*')
			defFieldBoost = boost;
		else {
			string fname = utf16_to_utf8(wstring(&*begIt, endIt - begIt));
			auto f = fields_.find(fname);
			if (f == fields_.end()) {
				throw Error(errLogic, "Field '%s',is not included to full text index", fname);
			}
			assertf(f->second < int(fieldsBoost.size()), "f=%d,fieldsBoost.size()=%d", f->second, fieldsBoost.size());
			fieldsBoost[f->second] = boost;
		}
		if (it == utf16str.end() || *it++ != ',') break;
	}
	for (auto &b : fieldsBoost)
		if (b == 0.0) b = defFieldBoost;
}

}  // namespace reindexer
