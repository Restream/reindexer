#include "core/ft/ftdsl.h"
#include <algorithm>
#include <locale>
#include "core/ft/config/baseftconfig.h"
#include "tools/customlocal.h"
#include "tools/errors.h"
#include "tools/stringstools.h"

namespace reindexer {

// Format: see fulltext.md
static bool is_term(int ch, const std::string &extraWordSymbols) noexcept {
	return IsAlpha(ch) || IsDigit(ch) ||
		   extraWordSymbols.find(ch) != std::string::npos
		   // wrong kb layout
		   || ch == '[' || ch == ';' || ch == ',' || ch == '.';
}

static bool is_dslbegin(int ch, const std::string &extraWordSymbols) noexcept {
	return is_term(ch, extraWordSymbols) || ch == '+' || ch == '-' || ch == '*' || ch == '\'' || ch == '\"' || ch == '@' || ch == '=' ||
		   ch == '\\';
}

void FtDSLQuery::parse(const std::string &q) {
	std::wstring utf16str;
	utf8_to_utf16(q, utf16str);
	parse(utf16str);
}
void FtDSLQuery::parse(std::wstring &utf16str) {
	int groupTermCounter = 0;
	bool inGroup = false;
	bool hasAnythingExceptNot = false;
	int groupCounter = 0;
	size_t maxPatternLen = 1;
	h_vector<FtDslFieldOpts, 8> fieldsOpts;
	std::string utf8str;
	fieldsOpts.insert(fieldsOpts.end(), std::max(int(fields_.size()), 1), {1.0, false});

	for (auto it = utf16str.begin(); it != utf16str.end();) {
		if (!is_dslbegin(*it, extraWordSymbols_)) {
			++it;
			continue;
		}

		FtDSLEntry fte;
		fte.opts.fieldsOpts = fieldsOpts;

		bool isSlash = (*it == '\\');
		if (isSlash) {
			++it;
		} else {
			if (*it == '@') {
				++it;
				parseFields(utf16str, it, fieldsOpts);
				continue;
			}

			if (*it == '-') {
				fte.opts.op = OpNot;
				++it;
			} else if (*it == '+') {
				fte.opts.op = OpAnd;
				++it;
			}
			if (it != utf16str.end() && (*it == '\'' || *it == '\"')) {
				inGroup = !inGroup;
				++it;
				// closing group
				if (!inGroup) {
					int distance = 1;
					if (it != utf16str.end() && *it == '~') {
						if (++it == utf16str.end()) {
							throw Error(errParseDSL, "Expected digit after '~' operator in phrase, but found nothing");
						}
						if (!std::isdigit(*it)) {
							throw Error(errParseDSL, "Expected digit after '~' operator in phrase, but found '%c' ", char(*it));
						}
						wchar_t *end = nullptr, *start = &*it;
						distance = wcstoul(start, &end, 10);
						if (*end != 0 && !std::isspace(*end)) {
							throw Error(errParseDSL, "Expected space after '~digit' operator in phrase, but found '%c' ", char(*it));
						}
						if (distance == 0) {
							throw Error(errParseDSL, "Expected positive integer after '~', but found '0'");
						}
						it += end - start;
					}
					assertf(groupTermCounter <= int(size()), "groupTermCounter=%d,size=%d", groupTermCounter, size());
					if (groupTermCounter > 1) {
						auto fteIt = end();
						while (--groupTermCounter >= 0) {
							fteIt--;
							if (groupTermCounter > 0) {
								fteIt->opts.distance = distance;
							}
							fteIt->opts.groupNum = groupCounter;
						}
						groupTermCounter = 0;
						++groupCounter;
					}
				}
			}
			if (it != utf16str.end() && *it == '=') {
				fte.opts.exact = true;
				++it;
			}
			if (it != utf16str.end() && *it == '*') {
				fte.opts.suff = true;
				++it;
			}
		}
		auto begIt = it;
		while (it != utf16str.end() && (isSlash || is_term(*it, extraWordSymbols_))) {
			*it = ToLower(*it);
			check_for_replacement(*it);
			isSlash = (++it != utf16str.end() && *it == '\\');
			if (isSlash) {
				std::move(it + 1, utf16str.end(), it);
				utf16str.pop_back();
			}
		}
		auto endIt = it;
		for (; it != utf16str.end(); ++it) {
			if (*it == '*') {
				fte.opts.pref = true;
			} else if (*it == '~') {
				fte.opts.typos = true;
			} else if (*it == '^') {
				if (++it == utf16str.end()) {
					throw Error(errParseDSL, "Expected digit after '^' operator in search query DSL, but found nothing");
				}
				wchar_t *end = nullptr, *start = &*it;
				fte.opts.boost = wcstod(start, &end);
				if (end == start) {
					throw Error(errParseDSL, "Expected digit after '^' operator in search query DSL, but found '%c' ", char(*start));
				}
				it += end - start - 1;
			} else {
				break;
			}
		}

		if (endIt != begIt) {
			fte.pattern.assign(begIt, endIt);
			utf16_to_utf8(fte.pattern, utf8str);
			fte.opts.number = is_number(utf8str);
			// Setting up this flag before stopWords check, to prevent error on DSL with stop word + NOT
			hasAnythingExceptNot = hasAnythingExceptNot || (fte.opts.op != OpNot && groupTermCounter == 0);
			if (auto it = stopWords_.find(utf8str); it != stopWords_.end() && it->type == StopWord::Type::Stop) {
				continue;
			}

			maxPatternLen = (fte.pattern.length() > maxPatternLen) ? fte.pattern.length() : maxPatternLen;
			emplace_back(std::move(fte));
			if (inGroup) ++groupTermCounter;
		}
	}
	if (inGroup) {
		throw Error(errParseDSL, "No closing quote in full text search query DSL");
	}
	if (!hasAnythingExceptNot && size()) {
		throw Error(errParams, "Fulltext query can not contain only 'NOT' terms (i.e. terms with minus)");
	}

	int cnt = 0;
	for (auto &e : *this) {
		e.opts.termLenBoost = float(e.pattern.length()) / maxPatternLen;
		e.opts.qpos = cnt++;
	}
}

void FtDSLQuery::parseFields(std::wstring &utf16str, std::wstring::iterator &it, h_vector<FtDslFieldOpts, 8> &fieldsOpts) {
	FtDslFieldOpts defFieldOpts{0.0, false};
	for (auto &fo : fieldsOpts) fo = defFieldOpts;

	while (it != utf16str.end()) {
		while (it != utf16str.end() && !(IsAlpha(*it) || IsDigit(*it) || *it == '*' || *it == '_' || *it == '+')) ++it;
		if (it == utf16str.end()) break;

		bool needSumRank = false;
		if (*it == '+') {
			needSumRank = true;
			if (++it == utf16str.end()) {
				throw Error(errParseDSL, "Expected field name after '+' operator in search query DSL, but found nothing");
			}
		}
		auto begIt = it;
		while (it != utf16str.end() && (IsAlpha(*it) || IsDigit(*it) || *it == '*' || *it == '_' || *it == '+' || *it == '.')) ++it;
		auto endIt = it;

		float boost = 1.0;
		if (it != utf16str.end() && *it == '^') {
			++it;
			if (it == utf16str.end()) {
				throw Error(errParseDSL, "Expected digit after '^' operator in search query DSL, but found nothing");
			}
			wchar_t *end = nullptr, *start = &*it;
			boost = wcstof(start, &end);
			if (end == start) {
				throw Error(errParseDSL, "Expected digit after '^' operator in search query DSL, but found '%c' ", char(*start));
			}
			it += end - start;
		}

		if (*begIt == '*') {
			defFieldOpts = {boost, needSumRank};
		} else {
			std::string fname = utf16_to_utf8(std::wstring(&*begIt, endIt - begIt));
			auto f = fields_.find(fname);
			if (f == fields_.end()) {
				throw Error(errLogic, "Field '%s',is not included to full text index", fname);
			}
			assertf(f->second < int(fieldsOpts.size()), "f=%d,fieldsOpts.size()=%d", f->second, fieldsOpts.size());
			fieldsOpts[f->second] = {boost, needSumRank};
		}
		if (it == utf16str.end() || *it++ != ',') break;
	}
	for (auto &fo : fieldsOpts)
		if (fo.boost == 0.0) fo = defFieldOpts;
}

}  // namespace reindexer
