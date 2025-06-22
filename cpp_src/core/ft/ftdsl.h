#pragma once

#include "stopwords/types.h"
#include "tools/rhashmap.h"
#include "tools/rvector.h"

namespace reindexer {

struct FtDslFieldOpts {
	float boost = 1.0;
	bool needSumRank = false;
};

struct FtDslOpts {
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

struct FtDSLEntry {
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
struct FtDSLVariant {
	FtDSLVariant() = default;
	FtDSLVariant(std::wstring p, int pr) noexcept : pattern{std::move(p)}, proc{pr} {}

	std::wstring pattern;
	int proc = 0;
};
#if !defined(__clang__) && !defined(_MSC_VER)
#pragma GCC diagnostic pop
#endif

struct StopWord;

struct [[nodiscard]] FtDSLQueryOptions {
	const StopWordsSetT& stopWords;
	const std::string& extraWordSymbols;
	SymbolTypeMask removeDiacriticsMask = 0;
};

class FtDSLQuery : public RVector<FtDSLEntry> {
public:
	FtDSLQuery(const RHashMap<std::string, int>& fields, const StopWordsSetT& stopWords, const std::string& extraWordSymbols) noexcept
		: fields_(fields), options_{stopWords, extraWordSymbols, 0} {}

	FtDSLQuery(const RHashMap<std::string, int>& fields, const FtDSLQueryOptions options) noexcept : fields_(fields), options_(options) {}

	FtDSLQuery CopyCtx() const noexcept { return {fields_, options_}; }
	void Parse(std::string_view q);

private:
	void parseImpl(wchar_t* str);
	void closeGroup(wchar_t*& str, int groupTermCounter, int groupCounter);
	void parseFieldOpts(wchar_t*& str, FtDslFieldOpts& defFieldOpts, h_vector<FtDslFieldOpts, 8>& fieldsOpts);
	void parseFieldsOpts(wchar_t*& str, h_vector<FtDslFieldOpts, 8>& fieldsOpts);

	std::function<int(const std::string&)> resolver_;

	const RHashMap<std::string, int>& fields_;
	const FtDSLQueryOptions options_;
};

}  // namespace reindexer
