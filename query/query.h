#pragma once

#include "cbinding/reindexer_ctypes.h"
#include "query/querywhere.h"

using std::string;
using std::vector;

namespace reindexer {

class Query : public QueryWhere {
public:
	Query(const string &__namespace, int _start, int _count, bool _calcTotal);
	Query(){};
	int Parse(const string &q);
	void Dump() const;
	string DumpJoined() const;

protected:
	int Parse(tokenizer &tok);

public:
	string _namespace;
	string sortBy;
	bool sortDirDesc = false;
	bool calcTotal = false;
	int start = 0;
	int count = 50;
	int debugLevel = 0;
	JoinType joinType = LeftJoin;

	// Queries for join
	vector<Query> joinQueries_;
};

}  // namespace reindexer
