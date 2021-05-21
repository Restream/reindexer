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
	OpType op = OpOr;
	float boost = 1.0;
	float termLenBoost = 1.0;
	int distance = INT_MAX;
	h_vector<FtDslFieldOpts, 8> fieldsOpts;
	int qpos = 0;
};

struct FtDSLEntry {
	FtDSLEntry() = default;
	FtDSLEntry(const std::wstring &p, const FtDslOpts &o) : pattern{p}, opts{o} {}
	std::wstring pattern;
	FtDslOpts opts;
};

class FtDSLQuery : public h_vector<FtDSLEntry> {
public:
	FtDSLQuery(const fast_hash_map<string, int> &fields, const fast_hash_set<string, hash_str, equal_str> &stopWords,
			   const string &extraWordSymbols) noexcept
		: fields_(fields), stopWords_(stopWords), extraWordSymbols_(extraWordSymbols) {}
	void parse(wstring &utf16str);
	void parse(const string &q);
	FtDSLQuery CopyCtx() const noexcept { return {fields_, stopWords_, extraWordSymbols_}; }

protected:
	void parseFields(wstring &utf16str, wstring::iterator &it, h_vector<FtDslFieldOpts, 8> &fieldsOpts);

	std::function<int(const string &)> resolver_;

	const fast_hash_map<string, int> &fields_;
	const fast_hash_set<string, hash_str, equal_str> &stopWords_;
	const string &extraWordSymbols_;
};

}  // namespace reindexer
