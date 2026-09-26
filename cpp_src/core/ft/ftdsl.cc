#include "core/ft/ftdsl.h"
#include <cstdlib>
#include <limits>
#include "estl/charset.h"
#include "tools/float_comparison.h"

namespace reindexer {

constexpr estl::Charset kWrongKeyboardLayoutSymbols{'[', ']', '{', '}', ';', ':', ',', '.', '<', '>'};
constexpr estl::Charset kDslSyntaxSymbols{'+', '-', '*', '\'', '"', '@', '=', '\\'};

static bool containsAscii(const estl::Charset& charset, char16_t ch) noexcept {
	return ch <= 0x7F && charset.test(static_cast<uint8_t>(ch));
}

// Format: see fulltext.md
static bool is_term(char16_t ch, const SplitOptions& opts) noexcept { return opts.IsWordSymbol(ch); }

static bool isWrongKbLayoutSymbol(char16_t ch) noexcept { return containsAscii(kWrongKeyboardLayoutSymbols, ch); }

static bool is_quote(char16_t ch) noexcept { return ch == u'\'' || ch == u'\"'; }

static bool needToSkip(char16_t ch, const SplitOptions& opts) noexcept {
	return !is_term(ch, opts) && !containsAscii(kDslSyntaxSymbols, ch);
}

static std::u16string normalizedPattern(char16_t* beg, char16_t* end, const SplitOptions& opts) {
	std::u16string pattern;
	pattern.reserve(std::distance(beg, end));
	for (; beg != end; ++beg) {
		char16_t ch = ToLower(*beg);
		if (opts.NeedToRemoveDiacritics(ch)) {
			ch = RemoveDiacritic(ch);
		}
		if (ch != 0) {
			pattern.push_back(ch);
		}
	}
	return pattern;
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

static unsigned parseDistance(char16_t*& str) {
	assertf_dbg(*str == u'~', "Expected {} in parseDistance", "'~'");
	++str;
	if (!*str) {
		throw Error(errParseDSL, "Expected number after '~' operator in phrase, but found nothing");
	}
	if (!IsDigit(*str)) {
		throw Error(errParseDSL, "Expected number after '~' operator in phrase, but found '{}' ", char(*str));
	}
	unsigned distance = 0;
	while (IsDigit(*str)) {
		const unsigned digit = *str - u'0';
		if (distance > (std::numeric_limits<unsigned>::max() - digit) / 10) {
			throw Error(errParseDSL, "Distance number is too large in search query DSL");
		}
		distance = distance * 10 + digit;
		++str;
	}
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

void FtDSLQuery::parseImpl(char16_t* str) {
	bool hasAnythingExceptNot = false;
	size_t maxPatternLen = 1;
	char16_t* wrongKbLayoutEnd = nullptr;
	h_vector<FtDslFieldOpts, 8> fieldsOpts;
	std::ignore = fieldsOpts.insert(fieldsOpts.cend(), std::max(int(fields_.size()), 1), {1.0, false});

	while (*str) {
		while (*str && needToSkip(*str, splitOptions_) && !isWrongKbLayoutSymbol(*str)) {
			++str;
		}

		if (!*str) {
			break;
		}

		if (*str == u'@') {
			parseFieldsOpts(str, fieldsOpts);
			continue;
		}

		if (wrongKbLayoutEnd && str >= wrongKbLayoutEnd) {
			wrongKbLayoutEnd = nullptr;
		}
		parseOperand(str, wrongKbLayoutEnd, fieldsOpts, hasAnythingExceptNot, maxPatternLen);
	}

	if (!hasAnythingExceptNot && entries_.size()) {
		throw Error(errParams, "Fulltext query can not contain only 'NOT' terms (i.e. terms with minus)");
	}

	for (auto& entry : entries_) {
		if (entry.IsTerm()) {
			auto& term = entry.Term();
			term.Opts().termLenBoost = float(term.Pattern().length()) / maxPatternLen;
			term.wrongKbLayoutTermLenBoost_ = float(term.WrongKbLayoutPattern().length()) / maxPatternLen;
		} else {
			for (size_t i = 0; i < entry.Phrase().NumTerms(); ++i) {
				auto& term = entry.Phrase().GetTerm(i);
				term.Opts().termLenBoost = float(term.Pattern().length()) / maxPatternLen;
				term.wrongKbLayoutTermLenBoost_ = float(term.WrongKbLayoutPattern().length()) / maxPatternLen;
			}
		}
	}
}

void FtDSLQuery::parseOperand(char16_t*& str, char16_t*& wrongKbLayoutEnd, h_vector<FtDslFieldOpts, 8>& fieldsOpts,
							  bool& hasAnythingExceptNot, size_t& maxPatternLen) {
	FtDslOpts opts;
	opts.fieldsOpts = fieldsOpts;
	if (*str == u'-') {
		opts.op = OpNot;
		++str;
	} else if (*str == u'+') {
		opts.op = OpAnd;
		++str;
	}

	if (*str && is_quote(*str)) {
		parsePhrase(str, std::move(opts), hasAnythingExceptNot, maxPatternLen);
		return;
	}

	std::u16string wrongKbLayoutPattern;
	if (!wrongKbLayoutEnd && *str != u'=') {
		char16_t* patternBeg = str;
		if (*patternBeg == u'*') {
			++patternBeg;
		}
		wrongKbLayoutEnd = patternBeg;
		bool hasWrongKbLayoutSymbols = false;
		while (is_term(*wrongKbLayoutEnd, splitOptions_) || isWrongKbLayoutSymbol(*wrongKbLayoutEnd)) {
			hasWrongKbLayoutSymbols = hasWrongKbLayoutSymbols || isWrongKbLayoutSymbol(*wrongKbLayoutEnd);
			++wrongKbLayoutEnd;
		}
		if (hasWrongKbLayoutSymbols) {
			wrongKbLayoutPattern = normalizedPattern(patternBeg, wrongKbLayoutEnd, splitOptions_);
		} else {
			wrongKbLayoutEnd = nullptr;
		}
	}

	if (wrongKbLayoutEnd) {
		if (*str == u'*') {
			opts.suff = true;
			++str;
		}
		while (str < wrongKbLayoutEnd && isWrongKbLayoutSymbol(*str)) {
			++str;
		}
	}

	const bool correctionOnlyTerm = wrongKbLayoutEnd && str >= wrongKbLayoutEnd;
	if (correctionOnlyTerm && wrongKbLayoutPattern.empty()) {
		return;
	}
	FtDslTerm term = correctionOnlyTerm ? FtDslTerm(std::u16string{}, opts) : parseTerm(str, std::move(opts));
	if (term.Pattern().empty() && wrongKbLayoutPattern.empty()) {
		return;
	}
	if (!wrongKbLayoutPattern.empty() && wrongKbLayoutPattern != term.Pattern()) {
		term.wrongKbLayoutPattern_ = std::move(wrongKbLayoutPattern);
		term.wrongKbLayoutSuff_ = term.Opts().suff;
		for (char16_t* suffix = wrongKbLayoutEnd; *suffix == u'*' || *suffix == u'~'; ++suffix) {
			term.wrongKbLayoutPref_ = term.wrongKbLayoutPref_ || *suffix == u'*';
			term.wrongKbLayoutTypos_ = term.wrongKbLayoutTypos_ || *suffix == u'~';
		}
	}

	// Setting up this flag before stopWords check, to prevent error on DSL with stop word + NOT
	hasAnythingExceptNot = hasAnythingExceptNot || term.Opts().op != OpNot;
	if (!term.Pattern().empty() && isStopWord(term)) {
		return;
	}

	maxPatternLen = std::max({maxPatternLen, term.Pattern().length(), term.WrongKbLayoutPattern().length()});
	entries_.emplace_back(std::move(term));
}

void FtDSLQuery::parsePhrase(char16_t*& str, FtDslOpts opts, bool& hasAnythingExceptNot, size_t& maxPatternLen) {
	assertf_dbg(is_quote(*str), "Expected quote in parsePhrase, but was '{}'", int(*str));
	const char16_t quote = *str++;
	const OpType op = opts.op;
	h_vector<FtDslTerm, 3> terms;

	while (*str) {
		while (*str && needToSkip(*str, splitOptions_)) {
			++str;
		}
		if (!*str) {
			break;
		}
		if (is_quote(*str)) {
			if (*str != quote) {
				throw Error(errParseDSL, "Opening and closing quotes differs for search phrase");
			}
			++str;
			unsigned distance = 1;
			if (*str == u'~') {
				distance = parseDistance(str);
			}
			if (!terms.empty()) {
				entries_.emplace_back(FtDslPhrase(std::move(terms), distance));
			}
			return;
		}
		if (*str == u'@') {
			throw Error(errParseDSL, "Field selector is not allowed inside of search phrase; specify it before the opening quote");
		}
		if (*str == u'-') {
			throw Error(errParseDSL, "Incorrect operator '-' inside of search phrase");
		}
		if (*str == u'+') {
			++str;
		}

		opts.op = terms.empty() ? op : OpOr;
		FtDslTerm term = parseTerm(str, opts, quote);
		if (term.Pattern().empty()) {
			continue;
		}
		if (!term.Opts().exact && isWrongKbLayoutSymbol(*str)) {
			term.wrongKbLayoutPattern_ = term.Pattern();
			while (isWrongKbLayoutSymbol(*str)) {
				term.wrongKbLayoutPattern_.push_back(*str++);
			}
			term.wrongKbLayoutPref_ = term.Opts().pref;
			term.wrongKbLayoutSuff_ = term.Opts().suff;
		}

		// Setting up this flag before stopWords check, to prevent error on DSL with stop word + NOT
		hasAnythingExceptNot = hasAnythingExceptNot || (term.Opts().op != OpNot && terms.empty());
		if (isStopWord(term)) {
			continue;
		}

		maxPatternLen = std::max(maxPatternLen, term.Pattern().length());
		terms.emplace_back(std::move(term));
	}

	throw Error(errParseDSL, "No closing quote in full text search query DSL");
}

FtDslTerm FtDSLQuery::parseTerm(char16_t*& str, FtDslOpts opts, char16_t phraseQuote) {
	while (*str && needToSkip(*str, splitOptions_)) {
		++str;
	}
	if (*str == u'=') {
		opts.exact = true;
		++str;
	}
	if (*str == u'*') {
		opts.suff = true;
		++str;
	}

	char16_t* const beg = str;
	for (; *str; ++str) {
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
		} else if (phraseQuote && is_quote(*str)) {
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
	char16_t* const end = str;
	if (end == beg) {
		return FtDslTerm(std::u16string{}, opts);
	}

	parseSuffixOpts(str, opts);
	std::u16string pattern;
	pattern.reserve(std::distance(beg, end));
	for (auto it = beg; it != end; ++it) {
		if (*it != 0) {
			// symbol not removed by RemoveDiacritic
			pattern.push_back(*it);
		}
	}

	std::string utf8str;
	utf16_to_utf8(pattern, utf8str);
	opts.number = is_number(utf8str);
	return FtDslTerm(std::move(pattern), opts);
}

bool FtDSLQuery::isStopWord(const FtDslTerm& term) const {
	std::string pattern;
	utf16_to_utf8(term.Pattern(), pattern);
	if (auto it = stopWords_.find(pattern); it != stopWords_.end() && it->type == StopWord::Type::Stop) {
		return true;
	}
	if (splitOptions_.ContainsDelims(pattern)) {
		std::string patternWithoutDelims = splitOptions_.RemoveDelims(pattern);
		if (auto it = stopWords_.find(patternWithoutDelims); it != stopWords_.end() && it->type == StopWord::Type::Stop) {
			return true;
		}
	}
	return false;
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
