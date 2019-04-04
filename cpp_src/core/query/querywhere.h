#pragma once

#include <climits>
#include <memory>
#include <string>
#include <vector>
#include "core/keyvalue/variant.h"
#include "estl/h_vector.h"

namespace reindexer {

enum IndexValueType : int { NotSet = -1, SetByJsonPath = -2 };

using std::string;
using std::vector;

class QueryWhere;
class tokenizer;

struct QueryEntry {
	QueryEntry(OpType o, CondType cond, const string &idx, int idxN, bool dist = false)
		: index(idx), idxNo(idxN), op(o), condition(cond), distinct(dist) {}
	QueryEntry() = default;

	bool operator==(const QueryEntry &) const;
	bool operator!=(const QueryEntry &) const;

	string index;
	int idxNo = IndexValueType::NotSet;
	OpType op = OpAnd;
	CondType condition = CondType::CondAny;
	bool distinct = false;
	VariantArray values;

	string Dump() const;
};

struct UpdateEntry {
	UpdateEntry() {}
	UpdateEntry(const string &c, const VariantArray &v) : column(c), values(v) {}
	bool operator==(const UpdateEntry &) const;
	bool operator!=(const UpdateEntry &) const;
	string column;
	VariantArray values;
	bool isExpression = false;
};

struct QueryJoinEntry {
	bool operator==(const QueryJoinEntry &) const;
	OpType op_;
	CondType condition_;
	string index_;
	string joinIndex_;
	int idxNo = -1;
};

struct QueryEntries : public h_vector<QueryEntry, 4> {};

struct SortingEntry {
	SortingEntry() {}
	SortingEntry(const string &c, bool d) : column(c), desc(d) {}
	bool operator==(const SortingEntry &) const;
	bool operator!=(const SortingEntry &) const;
	string column;
	bool desc = false;
	int index = IndexValueType::NotSet;
};

struct SortingEntries : public h_vector<SortingEntry, 1> {};

struct EqualPosition : public h_vector<int, 2> {};

struct AggregateEntry {
	AggregateEntry() = default;
	AggregateEntry(AggType type, const h_vector<string, 1> &fields, size_t limit, size_t offset)
		: type_(type), fields_(fields), limit_(limit), offset_(offset) {}
	bool operator==(const AggregateEntry &) const;
	bool operator!=(const AggregateEntry &) const;
	AggType type_;
	h_vector<string, 1> fields_;
	SortingEntries sortingEntries_;
	size_t limit_ = UINT_MAX;
	size_t offset_ = 0;
};

class QueryWhere {
public:
	QueryWhere() {}

	bool operator==(const QueryWhere &) const;

protected:
	void dumpWhere(WrSerializer &, bool stripArgs) const;
	static CondType getCondType(string_view cond);

public:
	QueryEntries entries;
	h_vector<AggregateEntry, 1> aggregations_;
	// Condition for join. Filled in each subqueries, empty in  root query
	vector<QueryJoinEntry> joinEntries_;
};

}  // namespace reindexer
