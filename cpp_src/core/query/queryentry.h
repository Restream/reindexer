#pragma once

#include <climits>
#include <string>
#include <vector>
#include "core/expressiontree.h"
#include "core/keyvalue/variant.h"
#include "estl/h_vector.h"

namespace reindexer {

class Query;
template <typename T>
class PayloadIface;
using ConstPayload = PayloadIface<const PayloadValue>;

class TagsMatcher;
using std::string;
using std::vector;

struct QueryEntry {
	const static int kNoJoins = -1;

	QueryEntry(int joinIdx) : joinIndex(joinIdx) {}
	QueryEntry(CondType cond, const string &idx, int idxN, bool dist = false) : index(idx), idxNo(idxN), condition(cond), distinct(dist) {}
	QueryEntry() = default;

	bool operator==(const QueryEntry &) const noexcept;
	bool operator!=(const QueryEntry &other) const noexcept { return !operator==(other); }

	string index;
	int idxNo = IndexValueType::NotSet;
	CondType condition = CondType::CondAny;
	bool distinct = false;
	VariantArray values;
	int joinIndex = kNoJoins;

	string Dump() const;
};

struct EqualPosition : public h_vector<unsigned, 2> {};

class JsonBuilder;

class QueryEntries : public ExpressionTree<OpType, Bracket, 4, QueryEntry> {
	using Base = ExpressionTree<OpType, Bracket, 4, QueryEntry>;
	QueryEntries(Base &&b) : Base{std::move(b)} {}

public:
	QueryEntries() = default;
	QueryEntries(QueryEntries &&) = default;
	QueryEntries(const QueryEntries &) = default;
	QueryEntries &operator=(QueryEntries &&) = default;
	QueryEntries MakeLazyCopy() & { return {makeLazyCopy()}; }

	void ForEachEntry(const std::function<void(const QueryEntry &)> &func) const { ExecuteAppropriateForEach(func); }
	void ForEachEntry(const std::function<void(QueryEntry &)> &func) { ExecuteAppropriateForEach(func); }
	const QueryEntry &operator[](size_t i) const {
		assert(i < container_.size());
		return container_[i].Value();
	}
	QueryEntry &Entry(size_t i) {
		assert(i < container_.size());
		return container_[i].Value();
	}

	template <typename T>
	std::pair<unsigned, EqualPosition> DetermineEqualPositionIndexes(const T &fields) const;
	template <typename T>
	EqualPosition DetermineEqualPositionIndexes(unsigned start, const T &fields) const;
	void ToDsl(const Query &parentQuery, JsonBuilder &builder) const { return toDsl(cbegin(), cend(), parentQuery, builder); }
	void WriteSQLWhere(const Query &parentQuery, WrSerializer &, bool stripArgs) const;
	void Serialize(WrSerializer &ser) const { serialize(cbegin(), cend(), ser); }
	bool CheckIfSatisfyConditions(const ConstPayload &pl, TagsMatcher &tm) const;

private:
	static void toDsl(const_iterator it, const_iterator to, const Query &parentQuery, JsonBuilder &);
	static void writeSQL(const Query &parentQuery, const_iterator from, const_iterator to, WrSerializer &, bool stripArgs);
	static void serialize(const_iterator it, const_iterator to, WrSerializer &);
	static bool checkIfSatisfyConditions(const_iterator begin, const_iterator end, const ConstPayload &, TagsMatcher &);
	static bool checkIfSatisfyCondition(const QueryEntry &, const ConstPayload &, TagsMatcher &);
};

extern template EqualPosition QueryEntries::DetermineEqualPositionIndexes<vector<string>>(unsigned start,
																						  const vector<string> &fields) const;
extern template std::pair<unsigned, EqualPosition> QueryEntries::DetermineEqualPositionIndexes<vector<string>>(
	const vector<string> &fields) const;
extern template std::pair<unsigned, EqualPosition> QueryEntries::DetermineEqualPositionIndexes<h_vector<string, 4>>(
	const h_vector<string, 4> &fields) const;
extern template std::pair<unsigned, EqualPosition> QueryEntries::DetermineEqualPositionIndexes<std::initializer_list<string>>(
	const std::initializer_list<string> &fields) const;

struct UpdateEntry {
	UpdateEntry() {}
	UpdateEntry(string c, VariantArray v, FieldModifyMode m = FieldModeSet, bool e = false)
		: column(std::move(c)), values(std::move(v)), mode(m), isExpression(e) {}
	bool operator==(const UpdateEntry &) const;
	bool operator!=(const UpdateEntry &) const;
	string column;
	VariantArray values;
	FieldModifyMode mode = FieldModeSet;
	bool isExpression = false;
	bool isArray = false;
};

struct QueryJoinEntry {
	bool operator==(const QueryJoinEntry &) const;
	OpType op_ = OpAnd;
	CondType condition_ = CondEq;
	string index_;
	string joinIndex_;
	int idxNo = -1;
};

struct SortingEntry {
	SortingEntry() {}
	SortingEntry(const string &e, bool d) : expression(e), desc(d) {}
	bool operator==(const SortingEntry &) const;
	bool operator!=(const SortingEntry &) const;
	string expression;
	bool desc = false;
	int index = IndexValueType::NotSet;
};

struct SortingEntries : public h_vector<SortingEntry, 1> {};

struct AggregateEntry {
	AggregateEntry() = default;
	AggregateEntry(AggType type, const h_vector<string, 1> &fields, unsigned limit = UINT_MAX, unsigned offset = 0)
		: type_(type), fields_(fields), limit_(limit), offset_(offset) {}
	bool operator==(const AggregateEntry &) const;
	bool operator!=(const AggregateEntry &) const;
	AggType type_;
	h_vector<string, 1> fields_;
	SortingEntries sortingEntries_;
	unsigned limit_ = UINT_MAX;
	unsigned offset_ = 0;
};

}  // namespace reindexer
