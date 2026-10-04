#pragma once

#include <variant>

#include "stopwords/types.h"
#include "tools/rhashmap.h"

namespace reindexer {

struct [[nodiscard]] FtIndexFieldPros {
	uint32_t isIndexed : 1;
	uint32_t fieldNumber : 31;
};

struct [[nodiscard]] FtDslFieldOpts {
	float boost = 1.0;
	bool needSumRank = false;
};

struct [[nodiscard]] FtDslOpts {
	bool suff = false;
	bool pref = false;
	bool typos = false;
	bool exact = false;
	bool number = false;
	OpType op = OpOr;
	float boost = 1.0;
	float termLenBoost = 1.0;
	h_vector<FtDslFieldOpts, 8> fieldsOpts;

	FtDslOpts() = default;
	FtDslOpts(const FtDslOpts&) = default;
	FtDslOpts& operator=(const FtDslOpts&) = default;
	FtDslOpts& operator=(FtDslOpts&&) = default;
	FtDslOpts(FtDslOpts&&) noexcept = default;

	FtDslOpts JoinWithPrevTermOpts(const FtDslOpts& prevTermOpts) const {
		FtDslOpts res = *this;
		res.suff = prevTermOpts.suff;
		res.typos |= prevTermOpts.typos;
		return res;
	}
};

class [[nodiscard]] FtDslTerm {
public:
	FtDslTerm() = default;
	FtDslTerm(std::u16string&& p, const FtDslOpts& o) : pattern{std::move(p)}, opts{o} {}
	FtDslTerm(const std::u16string& p, const FtDslOpts& o) : pattern{p}, opts{o} {}

	bool CanBeJoinedWith(const FtDslTerm& otherTerm) const noexcept {
		if (pattern.empty() || otherTerm.Pattern().empty()) {
			return false;
		}

		if (opts.op != OpOr || otherTerm.Opts().op != OpOr) {
			return false;
		}

		if (opts.exact || otherTerm.Opts().exact) {
			return false;
		}

		return true;
	}

	FtDslTerm JoinWithPrevTerm(const FtDslTerm& prevTerm) const {
		FtDslOpts resOpts = opts.JoinWithPrevTermOpts(prevTerm.Opts());
		return FtDslTerm(prevTerm.Pattern() + pattern, resOpts);
	}

	const FtDslOpts& Opts() const noexcept { return opts; }
	FtDslOpts& Opts() noexcept { return opts; }
	const std::u16string& Pattern() const noexcept { return pattern; }
	std::u16string& Pattern() noexcept { return pattern; }
	const std::u16string& WrongKbLayoutPattern() const noexcept { return wrongKbLayoutPattern_; }
	bool WrongKbLayoutPref() const noexcept { return wrongKbLayoutPref_; }
	bool WrongKbLayoutSuff() const noexcept { return wrongKbLayoutSuff_; }
	bool WrongKbLayoutTypos() const noexcept { return wrongKbLayoutTypos_; }
	float WrongKbLayoutTermLenBoost() const noexcept { return wrongKbLayoutTermLenBoost_; }

	friend class FtDSLQuery;

private:
	std::u16string pattern;
	std::u16string wrongKbLayoutPattern_;
	bool wrongKbLayoutPref_ = false;
	bool wrongKbLayoutSuff_ = false;
	bool wrongKbLayoutTypos_ = false;
	float wrongKbLayoutTermLenBoost_ = 0.0f;
	FtDslOpts opts;
};

class [[nodiscard]] FtDslPhrase {
public:
	FtDslPhrase(h_vector<FtDslTerm, 3>&& terms, unsigned distance) : terms_{std::move(terms)}, distance_{distance} {
		assertrx_throw(!terms_.empty());
		fieldsOpts_ = terms_[0].Opts().fieldsOpts;
		op_ = terms_[0].Opts().op;
	}

	OpType Op() const noexcept { return op_; }
	const h_vector<FtDslFieldOpts, 8>& FieldsOpts() const noexcept { return fieldsOpts_; }
	unsigned Distance() const noexcept { return distance_; }
	size_t NumTerms() const noexcept { return terms_.size(); }
	const FtDslTerm& GetTerm(size_t idx) const noexcept { return terms_[idx]; }
	FtDslTerm& GetTerm(size_t idx) noexcept { return terms_[idx]; }

private:
	h_vector<FtDslTerm, 3> terms_;
	h_vector<FtDslFieldOpts, 8> fieldsOpts_;
	OpType op_ = OpOr;
	unsigned distance_ = 1;
};

class [[nodiscard]] FtDSLEntry {
public:
	explicit FtDSLEntry(FtDslTerm&& term) : value_{std::move(term)} {}
	explicit FtDSLEntry(FtDslPhrase&& phrase) : value_{std::move(phrase)} {}

	bool IsTerm() const noexcept { return value_.index() == 0; }
	bool IsPhrase() const noexcept { return value_.index() == 1; }
	const FtDslTerm& Term() const { return std::get<FtDslTerm>(value_); }
	FtDslTerm& Term() { return std::get<FtDslTerm>(value_); }
	const FtDslPhrase& Phrase() const { return std::get<FtDslPhrase>(value_); }
	FtDslPhrase& Phrase() { return std::get<FtDslPhrase>(value_); }

private:
	std::variant<FtDslTerm, FtDslPhrase> value_;
};

#if !defined(__clang__) && !defined(_MSC_VER)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif
struct [[nodiscard]] FtDSLVariant {
	FtDSLVariant() = default;
	FtDSLVariant(FtDSLVariant&&) = default;
	FtDSLVariant(std::u16string p, int pr, PrefAndStemmersForbidden psForbidden) noexcept
		: pattern{std::move(p)}, proc{pr}, prefAndStemmersForbidden(psForbidden) {}

	reindexer::FtDSLVariant& operator=(FtDSLVariant&& rhs) = default;

	std::u16string pattern;
	int proc = 0;
	PrefAndStemmersForbidden prefAndStemmersForbidden = PrefAndStemmersForbidden_False;
};
#if !defined(__clang__) && !defined(_MSC_VER)
#pragma GCC diagnostic pop
#endif

struct StopWord;

// Parsed full-text DSL grammar (lexical separators and pattern characters depend on SplitOptions):
//
// query          := { [ field-selector ] operand | separator }
// operand        := [ '+' | '-' ] (term | phrase)  // OR by default; '+' is AND, '-' is NOT
// phrase         := quote { [ '+' ] term | separator } same-quote [ '~' positive-integer ]
// term           := [ '=' ] [ '*' ] pattern { '*' | '~' | '^' float }
// field-selector := '@' field-spec { ',' field-spec }
// field-spec     := [ '+' ] (field-name | '*') [ '^' float ]
// quote          := '\'' | '"'
//
// A backslash escapes the following character. Field selectors update the options for all subsequent operands. For a phrase, a field
// selector is allowed only before the opening quote and applies to the entire phrase. Quotes always produce an FtDslPhrase when at least
// one non-stop term remains, including a phrase with a single term. A phrase's boolean operator and field options are those parsed before
// its opening quote;
// '-' and field selectors are forbidden inside a phrase, while an inner '+' is accepted but has no boolean meaning.
class [[nodiscard]] FtDSLQuery {
public:
	FtDSLQuery(const RHashMap<std::string, FtIndexFieldPros>& fields, const StopWordsSetT& stopWords, const SplitOptions& splitOptions,
			   StrictMode strictMode = StrictModeNone) noexcept
		: fields_(fields), stopWords_(stopWords), splitOptions_(splitOptions), strictMode_(strictMode) {}

	FtDSLQuery CopyCtx() const noexcept { return {fields_, stopWords_, splitOptions_, strictMode_}; }
	void Parse(std::string_view q);

	const FtDSLEntry& GetEntry(size_t idx) const noexcept { return entries_[idx]; }
	FtDSLEntry& GetEntry(size_t idx) noexcept { return entries_[idx]; }
	size_t NumEntries() const noexcept { return entries_.size(); }

	h_vector<FtDSLEntry>::const_iterator begin() const noexcept { return entries_.begin(); }
	h_vector<FtDSLEntry>::const_iterator end() const noexcept { return entries_.end(); }

private:
	void parseImpl(char16_t* str);
	void parseOperand(char16_t*& str, char16_t*& wrongKbLayoutEnd, h_vector<FtDslFieldOpts, 8>& fieldsOpts, bool& hasAnythingExceptNot,
					  size_t& maxPatternLen);
	void parsePhrase(char16_t*& str, FtDslOpts opts, bool& hasAnythingExceptNot, size_t& maxPatternLen);
	FtDslTerm parseTerm(char16_t*& str, FtDslOpts opts, char16_t phraseQuote = 0);
	bool isStopWord(const FtDslTerm& term) const;
	void parseFieldOpts(char16_t*& str, FtDslFieldOpts& defFieldOpts, h_vector<FtDslFieldOpts, 8>& fieldsOpts);
	void parseFieldsOpts(char16_t*& str, h_vector<FtDslFieldOpts, 8>& fieldsOpts);

	std::function<int(const std::string&)> resolver_;

	const RHashMap<std::string, FtIndexFieldPros>& fields_;
	const StopWordsSetT& stopWords_;
	const SplitOptions& splitOptions_;
	const StrictMode strictMode_{StrictMode::StrictModeNotSet};

	h_vector<FtDSLEntry> entries_;
};

}  // namespace reindexer
