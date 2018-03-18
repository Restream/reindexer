#pragma once

#include <memory>
#include <string>
#include <vector>
#include "core/keyvalue/keyvalue.h"
#include "estl/h_vector.h"

namespace reindexer {

using std::string;
using std::vector;

class QueryWhere;
class tokenizer;

struct QueryEntry {
	QueryEntry(OpType o, CondType cond, const string &idx, int idxN, bool dist = false)
		: op(o), condition(cond), index(idx), idxNo(idxN), distinct(dist) {}
	QueryEntry() = default;

	OpType op = OpAnd;
	CondType condition = CondType::CondAny;
	string index;
	int idxNo = -1;
	bool distinct = false;
	KeyValues values;

	string Dump() const;
};

struct QueryJoinEntry {
	OpType op_;
	CondType condition_;
	string index_;
	string joinIndex_;
	int idxNo = -1;
};

struct QueryEntries : public h_vector<QueryEntry, 4> {};

struct AggregateEntry {
	string index_;
	AggType type_;
};

class QueryWhere {
public:
	QueryWhere() {}

protected:
	int ParseWhere(tokenizer &tok);
	string toString() const;

public:
	QueryEntries entries;
	h_vector<AggregateEntry, 1> aggregations_;
	// Condition for join. Filled in each subqueries, empty in  root query
	vector<QueryJoinEntry> joinEntries_;
};

}  // namespace reindexer
