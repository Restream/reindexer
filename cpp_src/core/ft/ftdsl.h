#pragma once

#include <climits>
#include <functional>
#include <string>
#include <vector>
#include "core/type_consts.h"
#include "estl/fast_hash_map.h"
#include "estl/fast_hash_set.h"
#include "estl/h_vector.h"

namespace reindexer {

using std::string;
using std::wstring;
using std::vector;

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
	h_vector<float, 8> fieldsBoost;
	int qpos = 0;
};

struct FtDSLEntry {
	std::wstring pattern;
	FtDslOpts opts;
};

class FtDSLQuery : public h_vector<FtDSLEntry> {
public:
	FtDSLQuery(const fast_hash_map<string, int> &fields, const fast_hash_set<string> &stopWords, const string &extraWordSymbols)
		: fields_(fields), stopWords_(stopWords), extraWordSymbols_(extraWordSymbols) {}
	void parse(wstring &utf16str);
	void parse(const string &q);

protected:
	void parseFields(wstring &utf16str, wstring::iterator &it, h_vector<float, 8> &fieldsBoost);

	int numFields_;
	std::function<int(const string &)> resolver_;

	const fast_hash_map<string, int> &fields_;
	const fast_hash_set<string> &stopWords_;
	const string &extraWordSymbols_;
};

}  // namespace reindexer
