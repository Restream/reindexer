#pragma once

#include "stopwords/types.h"
#include "tools/rhashmap.h"

namespace reindexer {

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
};

struct [[nodiscard]] FtDSLEntry {
	FtDSLEntry() = default;
	FtDSLEntry(std::wstring&& p, FtDslOpts&& o) : pattern{std::move(p)}, opts{std::move(o)} {}
	FtDSLEntry(const std::wstring& p, const FtDslOpts& o) : pattern{p}, opts{o} {}
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

class [[nodiscard]] FtDSLQuery : public h_vector<FtDSLEntry> {
public:
	FtDSLQuery(const RHashMap<std::string, int>& fields, const StopWordsSetT& stopWords, const SplitOptions& splitOptions) noexcept
		: fields_(fields), stopWords_(stopWords), splitOptions_(splitOptions) {}

	FtDSLQuery CopyCtx() const noexcept { return {fields_, stopWords_, splitOptions_}; }
	void Parse(std::string_view q);

private:
	void parseImpl(wchar_t* str);
	void closeGroup(wchar_t*& str, int groupTermCounter, int groupCounter);
	void parseFieldOpts(wchar_t*& str, FtDslFieldOpts& defFieldOpts, h_vector<FtDslFieldOpts, 8>& fieldsOpts);
	void parseFieldsOpts(wchar_t*& str, h_vector<FtDslFieldOpts, 8>& fieldsOpts);

	std::function<int(const std::string&)> resolver_;

	const RHashMap<std::string, int>& fields_;
	const StopWordsSetT& stopWords_;
	const SplitOptions& splitOptions_;
};

}  // namespace reindexer
