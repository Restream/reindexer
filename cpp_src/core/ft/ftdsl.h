#pragma once

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
	int groupNum = -1;
	OpType op = OpOr;
	float boost = 1.0;
	float termLenBoost = 1.0;
	int distance = INT_MAX;
	h_vector<FtDslFieldOpts, 8> fieldsOpts;
	int qpos = 0;

	FtDslOpts GetStemOpts(bool keepSuff) const {
		FtDslOpts res = *this;
		res.pref = true;
		if (!keepSuff) {
			res.suff = false;
		}
		return res;
	}

	FtDslOpts JoinWithPrevTermOpts(const FtDslOpts& prevTermOpts) const {
		FtDslOpts res = *this;
		res.suff = prevTermOpts.suff;
		res.typos |= prevTermOpts.typos;
		return res;
	}
};

class [[nodiscard]] FtDSLEntry {
public:
	FtDSLEntry() = default;
	FtDSLEntry(std::wstring&& p, const FtDslOpts& o) : pattern{std::move(p)}, opts{o} {}
	FtDSLEntry(const std::wstring& p, const FtDslOpts& o) : pattern{p}, opts{o} {}

	bool CanBeJoinedWith(const FtDSLEntry& otherTerm) const noexcept {
		if (opts.op != OpOr || otherTerm.Opts().op != OpOr) {
			return false;
		}

		if (opts.exact || otherTerm.Opts().exact) {
			return false;
		}

		return true;
	}

	FtDSLEntry JoinWithPrevTerm(const FtDSLEntry& prevTerm) const {
		FtDslOpts resOpts = opts.JoinWithPrevTermOpts(prevTerm.Opts());
		return FtDSLEntry(prevTerm.Pattern() + pattern, resOpts);
	}

	const FtDslOpts& Opts() const noexcept { return opts; }
	FtDslOpts& Opts() noexcept { return opts; }
	const std::wstring& Pattern() const noexcept { return pattern; }

	friend class FtDSLQuery;

private:
	std::wstring pattern;
	FtDslOpts opts;
};

#if !defined(__clang__) && !defined(_MSC_VER)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif
struct [[nodiscard]] FtDSLVariant {
	FtDSLVariant() = default;
	FtDSLVariant(FtDSLVariant&&) = default;
	FtDSLVariant(std::wstring p, int pr, PrefAndStemmersForbidden psForbidden) noexcept
		: pattern{std::move(p)}, proc{pr}, prefAndStemmersForbidden(psForbidden) {}

	reindexer::FtDSLVariant& operator=(FtDSLVariant&& rhs) = default;

	std::wstring pattern;
	int proc = 0;
	PrefAndStemmersForbidden prefAndStemmersForbidden = PrefAndStemmersForbidden_False;
};
#if !defined(__clang__) && !defined(_MSC_VER)
#pragma GCC diagnostic pop
#endif

struct StopWord;

class [[nodiscard]] FtDSLQuery {
public:
	FtDSLQuery(const RHashMap<std::string, FtIndexFieldPros>& fields, const StopWordsSetT& stopWords, const SplitOptions& splitOptions,
			   StrictMode strictMode = StrictModeNone) noexcept
		: fields_(fields), stopWords_(stopWords), splitOptions_(splitOptions), strictMode_(strictMode) {}

	FtDSLQuery CopyCtx() const noexcept { return {fields_, stopWords_, splitOptions_, strictMode_}; }
	void Parse(std::string_view q);

	template <typename... Args>
	FtDSLEntry& AddTerm(Args&&... args) {
		return terms_.emplace_back(std::forward<Args>(args)...);
	}

	const FtDSLEntry& GetTerm(size_t idx) const noexcept { return terms_[idx]; }
	FtDSLEntry& GetTerm(size_t idx) noexcept { return terms_[idx]; }

	size_t NumTerms() const noexcept { return terms_.size(); }

	h_vector<FtDSLEntry>::const_iterator begin() const noexcept { return terms_.begin(); }
	h_vector<FtDSLEntry>::const_iterator end() const noexcept { return terms_.end(); }

private:
	void parseImpl(wchar_t* str);
	void closeGroup(wchar_t*& str, int groupTermCounter, int groupCounter);
	void parseFieldOpts(wchar_t*& str, FtDslFieldOpts& defFieldOpts, h_vector<FtDslFieldOpts, 8>& fieldsOpts);
	void parseFieldsOpts(wchar_t*& str, h_vector<FtDslFieldOpts, 8>& fieldsOpts);

	std::function<int(const std::string&)> resolver_;

	const RHashMap<std::string, FtIndexFieldPros>& fields_;
	const StopWordsSetT& stopWords_;
	const SplitOptions& splitOptions_;
	const StrictMode strictMode_{StrictMode::StrictModeNotSet};

	h_vector<FtDSLEntry> terms_;
};

}  // namespace reindexer
