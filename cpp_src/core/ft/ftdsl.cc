#include "core/ft/ftdsl.h"
#include <cstdlib>
#include "tools/float_comparison.h"

namespace reindexer {

// Format: see fulltext.md
static bool is_term(int ch, const SplitOptions& opts) noexcept {
	return opts.IsWordSymbol(ch)
		   // wrong kb layout
		   || ch == '[' || ch == ';' || ch == ',' || ch == '.';
}

static bool is_quote(char16_t ch) noexcept { return ch == u'\'' || ch == u'\"'; }

static bool needToSkip(char16_t ch, const SplitOptions& opts) noexcept {
	return !is_term(ch, opts) && ch != u'+' && ch != u'-' && ch != u'*' && ch != u'\'' && ch != u'\"' && ch != u'@' && ch != u'=' &&
		   ch != u'\\';
}

void FtDSLQuery::Parse(std::string_view q) {
	std::u16string u16str = utf8_to_utf16(q);
	parseImpl(u16str.data());
}

static bool isFloatChar(char16_t ch) noexcept { return IsDigit(ch) || ch == u'.' || ch == u'+' || ch == u'-' || ch == u'e' || ch == u'E'; }

static bool parseFloat(char16_t*& str, float& res) {
	char buf[64];
	size_t i = 0;
	char16_t* const beg = str;
	while (*str && i + 1 < sizeof(buf) && isFloatChar(*str)) {
		buf[i++] = char(*str++);
	}
	if (*str && isFloatChar(*str)) {
		throw Error(errParseDSL, "Floating point number is too long in search query DSL");
	}
	buf[i] = '\0';
	char* end = nullptr;
	res = std::strtof(buf, &end);
	if (end == buf) {
		str = beg;
		return false;
	}
	str = beg + (end - buf);
	return true;
}

static void parseBoost(char16_t*& str, float& boost) {
	assertf_dbg(*str == u'^', "Expected {} in parseBoost", "'^'");
	++str;
	boost = 1.0;
	if (!*str) {
		throw Error(errParseDSL, "Expected number after '^' operator in search query DSL, but found nothing");
	}
	if (!parseFloat(str, boost)) {
		throw Error(errParseDSL, "Expected number after '^' operator in search query DSL, but found '{}' ", char(*str));
	}
}

static void parseSuffixOpts(char16_t*& str, FtDslOpts& opts) {
	while (*str) {
		if (*str == u'^') {
			parseBoost(str, opts.boost);
		} else if (*str == u'*') {
			opts.pref = true;
			++str;
		} else if (*str == u'~') {
			opts.typos = true;
			++str;
		} else {
			break;
		}
	}
}

static unsigned long parseDistance(char16_t*& str) {
	assertf_dbg(*str == u'~', "Expected {} in parseDistance", "'~'");
	++str;
	unsigned long distance = 1;
	if (!*str) {
		throw Error(errParseDSL, "Expected number after '~' operator in phrase, but found nothing");
	}
	if (!IsDigit(*str)) {
		throw Error(errParseDSL, "Expected number after '~' operator in phrase, but found '{}' ", char(*str));
	}
	char buf[32];
	size_t i = 0;
	char16_t* const beg = str;
	while (*str && i + 1 < sizeof(buf) && IsDigit(*str)) {
		buf[i++] = char(*str++);
	}
	if (*str && IsDigit(*str)) {
		throw Error(errParseDSL, "Distance number is too long in search query DSL");
	}
	buf[i] = '\0';
	char* end = nullptr;
	distance = std::strtoul(buf, &end, 10);
	str = beg + (end - buf);
	if (*str && !std::isspace(static_cast<unsigned char>(*str))) {
		throw Error(errParseDSL, "Expected space after '~digit' operator in phrase, but found '{}' ", char(*str));
	}
	if (distance == 0) {
		throw Error(errParseDSL, "Expected positive integer after '~', but found '0'");
	}
	return distance;
}

static void eraseFirstSymbol(char16_t* str) {
	while (*str) {
		*str = *(str + 1);
		str++;
	}
}

void FtDSLQuery::closeGroup(char16_t*& str, int groupTermCounter, int groupCounter) {
	unsigned long distance = 1;
	if (*str == u'~') {
		distance = parseDistance(str);
	}
	assertrx_throw(groupTermCounter <= int(terms_.size()));
	if (groupTermCounter > 1) {
		for (auto fteIt = terms_.rbegin(); groupTermCounter > 0; ++fteIt, --groupTermCounter) {
			if (groupTermCounter > 1) {
				fteIt->opts.distance = distance;
			}
			fteIt->opts.phraseNum = groupCounter;
		}
	}
}

void FtDSLQuery::parseImpl(char16_t* str) {
	int groupTermCounter = 0;
	bool inGroup = false;
	bool hasAnythingExceptNot = false;
	int groupCounter = 0;
	size_t maxPatternLen = 1;
	char16_t groupQuote = u'\'';
	h_vector<FtDslFieldOpts, 8> fieldsOpts;
	std::string utf8str;
	std::ignore = fieldsOpts.insert(fieldsOpts.cend(), std::max(int(fields_.size()), 1), {1.0, false});

	while (*str) {
		while (*str && needToSkip(*str, splitOptions_)) {
			++str;
		}

		if (!*str) {
			break;
		}

		if (*str == u'@') {
			parseFieldsOpts(str, fieldsOpts);
			continue;
		}

		FtDSLEntry fte;
		fte.opts.fieldsOpts = fieldsOpts;
		if (*str == u'-') {
			if (inGroup) {
				throw Error(errParseDSL, "Incorrect operator '-' inside of search phrase");
			}
			fte.opts.op = OpNot;
			++str;
		} else if (*str == u'+') {
			if (!inGroup) {
				fte.opts.op = OpAnd;
			}
			++str;
		}

		if (*str && is_quote(*str)) {
			if (inGroup) {
				if (*str != groupQuote) {
					throw Error(errParseDSL, "Opening and closing quotes differs for search phrase");
				}
				++str;
				inGroup = false;
				closeGroup(str, groupTermCounter, groupCounter);
				groupTermCounter = 0;
				++groupCounter;
				continue;
			} else {
				inGroup = true;
				groupQuote = *str;
				++str;
			}
		}

		while (*str && needToSkip(*str, splitOptions_)) {
			++str;
		}

		if (*str == u'=') {
			fte.opts.exact = true;
			++str;
		}
		if (*str == u'*') {
			fte.opts.suff = true;
			++str;
		}
		if (*str == u'+' && inGroup) {
			++str;
		}
		if (*str == u'-' && inGroup) {
			throw Error(errParseDSL, "Incorrect operator '-' inside of search phrase");
		}

		char16_t* beg = str;
		for (; *str; str++) {
			if (*str == u'\\') {
				eraseFirstSymbol(str);
				if (!*str) {
					throw Error(errParseDSL, "Expected symbol after \\ , but found nothing");
				}
				*str = ToLower(*str);
				if (splitOptions_.NeedToRemoveDiacritics(*str)) {
					*str = RemoveDiacritic(*str);
				}
			} else if ((*str == u'*' || *str == u'~') && str != beg) {
				break;
			} else if (inGroup && *str == groupQuote) {
				break;
			} else if (is_term(*str, splitOptions_)) {
				*str = ToLower(*str);
				if (splitOptions_.NeedToRemoveDiacritics(*str)) {
					*str = RemoveDiacritic(*str);
				}
			} else {
				break;
			}
		}
		auto end = str;
		if (end == beg) {
			continue;
		}

		parseSuffixOpts(str, fte.opts);

		fte.pattern.resize(0);
		fte.pattern.reserve(std::distance(beg, end));
		for (auto it = beg; it != end; ++it) {
			if (*it != 0) {
				// symbol not removed by RemoveDiacritic
				fte.pattern.push_back(*it);
			}
		}

		utf16_to_utf8(fte.pattern, utf8str);
		fte.opts.number = is_number(utf8str);
		// Setting up this flag before stopWords check, to prevent error on DSL with stop word + NOT
		hasAnythingExceptNot = hasAnythingExceptNot || (fte.opts.op != OpNot && groupTermCounter == 0);
		if (auto it = stopWords_.find(utf8str); it != stopWords_.end() && it->type == StopWord::Type::Stop) {
			continue;
		}

		if (splitOptions_.ContainsDelims(utf8str)) {
			std::string patternWithoutDelims = splitOptions_.RemoveDelims(utf8str);
			if (auto it = stopWords_.find(patternWithoutDelims); it != stopWords_.end() && it->type == StopWord::Type::Stop) {
				continue;
			}
		}

		maxPatternLen = (fte.Pattern().length() > maxPatternLen) ? fte.Pattern().length() : maxPatternLen;
		terms_.emplace_back(std::move(fte));
		if (inGroup) {
			++groupTermCounter;
		}
	}

	if (inGroup) {
		throw Error(errParseDSL, "No closing quote in full text search query DSL");
	}
	if (!hasAnythingExceptNot && terms_.size()) {
		throw Error(errParams, "Fulltext query can not contain only 'NOT' terms (i.e. terms with minus)");
	}

	for (auto& t : terms_) {
		t.Opts().termLenBoost = float(t.pattern.length()) / maxPatternLen;
	}
}

void FtDSLQuery::parseFieldOpts(char16_t*& str, FtDslFieldOpts& defFieldOpts, h_vector<FtDslFieldOpts, 8>& fieldsOpts) {
	while (*str && !(IsAlpha(*str) || IsDigit(*str) || *str == u'*' || *str == u'_' || *str == u'+')) {
		++str;
	}
	if (!*str) {
		return;
	}

	bool needSumRank = false;
	if (*str == u'+') {
		needSumRank = true;
		++str;
		if (!str) {
			throw Error(errParseDSL, "Expected field name after '+' operator in search query DSL, but found nothing");
		}
	}
	auto beg = str;
	while (*str && (IsAlpha(*str) || IsDigit(*str) || *str == u'*' || *str == u'_' || *str == u'+' || *str == u'.')) {
		++str;
	}
	auto end = str;

	float boost = 1.0f;
	if (*str == u'^') {
		parseBoost(str, boost);
	}

	if (*beg == u'*') {
		defFieldOpts = {boost, needSumRank};
		return;
	}

	std::string fname = utf16_to_utf8(std::u16string_view(beg, std::distance(beg, end)));
	auto f = fields_.find(fname);
	if (f == fields_.end()) [[unlikely]] {
		throw Error(errLogic, "Field '{}' is not included into fulltext index", fname);
	}
	// No reason to handle other strcit modes here: non-existing fields are already forbidden
	if (strictMode_ == StrictModeIndexes && !f->second.isIndexed) [[unlikely]] {
		throw Error(errStrictMode,
					"Field '{}' in fulltext DSL is not indexed. With current strict mode all explicit fields in DSL must be indexed",
					fname);
	}
	assertf(f->second.fieldNumber < fieldsOpts.size(), "f={},fieldsOpts.size()={}", f->second.fieldNumber, fieldsOpts.size());
	fieldsOpts[f->second.fieldNumber] = {boost, needSumRank};
}

void FtDSLQuery::parseFieldsOpts(char16_t*& str, h_vector<FtDslFieldOpts, 8>& fieldsOpts) {
	assertf_dbg(*str == u'@', "Expected '@' in parseFieldsOpts, but was '{}'", int(*str));
	++str;

	FtDslFieldOpts defFieldOpts{0.0, false};
	for (auto& fo : fieldsOpts) {
		fo = defFieldOpts;
	}

	for (; *str != 0; str++) {
		parseFieldOpts(str, defFieldOpts, fieldsOpts);
		if (*str != u',') {
			break;
		}
	}

	for (auto& fo : fieldsOpts) {
		if (fp::IsZero(fo.boost)) {
			fo = defFieldOpts;
		}
	}
}

}  // namespace reindexer
