#pragma once

#include <memory>
#include <string>
#include <vector>
#include "core/idset.h"
#include "core/keyvalue.h"
#include "tools/h_vector.h"

using std::string;

namespace reindexer {

class QueryWhere;
class tokenizer;

struct QueryEntry {
	QueryEntry(OpType o, CondType cond, string idx, int idxN, bool dist = false)
		: op(o), condition(cond), index(idx), idxNo(idxN), distinct(dist) {}
	QueryEntry() = default;

	OpType op = OpAnd;
	CondType condition = CondType::CondAny;
	string index;
	int idxNo = -1;
	bool distinct = false;
	KeyValues values;
};

struct QueryJoinEntry {
	CondType condition_;
	string index_;
	string joinIndex_;
};

struct QueryEntries : public h_vector<QueryEntry, 5> {};

class QueryWhere {
public:
	QueryWhere() {}

protected:
	int ParseWhere(tokenizer &tok);
	string toString() const;

public:
	QueryEntries entries;
	// Condition for join. Filled in each subqueries, empty in  root query
	vector<QueryJoinEntry> joinEntries_;
};

}  // namespace reindexer
