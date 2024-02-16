#pragma once

#include <climits>
#include <functional>
#include <string>
#include <vector>
#include "core/type_consts.h"
#include "estl/fast_hash_map.h"
#include "estl/fast_hash_set.h"
#include "estl/h_vector.h"
#include "tools/stringstools.h"
#include "usingcontainer.h"

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
	FtDSLEntry(std::wstring &&p, FtDslOpts &&o) : pattern{std::move(p)}, opts{std::move(o)} {}
	FtDSLEntry(const std::wstring &p, const FtDslOpts &o) : pattern{p}, opts{o} {}
	std::wstring pattern;
	FtDslOpts opts;
};

struct FtDSLVariant {
	FtDSLVariant() = default;
	FtDSLVariant(std::wstring p, int pr) noexcept : pattern{std::move(p)}, proc{pr} {}

	std::wstring pattern;
	int proc = 0;
};

struct StopWord;

class FtDSLQuery : public RVector<FtDSLEntry> {
public:
	FtDSLQuery(const RHashMap<std::string, int> &fields, const fast_hash_set<StopWord, hash_str, equal_str, less_str> &stopWords,
			   const std::string &extraWordSymbols) noexcept
		: fields_(fields), stopWords_(stopWords), extraWordSymbols_(extraWordSymbols) {}
	void parse(std::wstring &utf16str);
	void parse(const std::string &q);
	FtDSLQuery CopyCtx() const noexcept { return {fields_, stopWords_, extraWordSymbols_}; }

protected:
	void parseFields(std::wstring &utf16str, std::wstring::iterator &it, h_vector<FtDslFieldOpts, 8> &fieldsOpts);

	std::function<int(const std::string &)> resolver_;

	const RHashMap<std::string, int> &fields_;
	const fast_hash_set<StopWord, hash_str, equal_str, less_str> &stopWords_;
	const std::string &extraWordSymbols_;
};

}  // namespace reindexer
