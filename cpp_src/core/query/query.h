#pragma once

#include <climits>
#include <functional>
#include <initializer_list>
#include "querywhere.h"
#include "tools/errors.h"

/// @namespace reindexer
/// The base namespace
namespace reindexer {

class WrSerializer;
class Serializer;
using std::initializer_list;

/// @class Query
/// Allows to select data from DB.
/// Analog to ansi-sql select query.
class Query : public QueryWhere {
public:
	/// Creates an object for certain namespace with appropriate settings.
	/// @param nsName - name of the namespace the data to be selected from.
	/// @param start - number of the first row to get from selected set. Analog to sql LIMIT Offset.
	/// @param count - number of rows to get from result set. Analog to sql LIMIT RowsCount.
	/// @param calcTotal - calculation mode.
	explicit Query(const string &nsName, unsigned start = 0, unsigned count = UINT_MAX, CalcTotalMode calcTotal = ModeNoTotal);

	/// Creates an empty object.
	Query() {}

	/// Allows to compare 2 Query objects.
	bool operator==(const Query &) const;

	/// Parses pure sql select query and initializes Query object data members as a result.
	/// @param q - sql query.
	/// @return always returns 0.
	int FromSQL(const string_view &q);

	/// Parses JSON dsl set.
	/// @param dsl - dsl set.
	/// @return always returns errOk or throws an exception.
	Error ParseJson(const string &dsl);

	/// Logs query in 'Select field1, ... field N from namespace ...' format.
	/// @param ser - serializer to store SQL string
	/// @param stripArgs - replace condition values with '?'
	WrSerializer &GetSQL(WrSerializer &ser, bool stripArgs = false) const;

	/// Enable explain query
	/// @param on - signaling on/off
	/// @return Query object ready to be executed
	Query &Explain(bool on = true) {
		explain_ = on;
		return *this;
	}

	/// Adds a condition with a single value. Analog to sql Where clause.
	/// @param idx - index used in condition clause.
	/// @param cond - type of condition.
	/// @param val - value of index to be compared with.
	/// @return Query object ready to be executed.
	template <typename Input>
	Query &Where(const string &idx, CondType cond, Input val) {
		return Where(idx, cond, {val});
	}

	/// Adds a condition with several values. Analog to sql Where clause.
	/// @param idx - index used in condition clause.
	/// @param cond - type of condition.
	/// @param l - list of index values to be compared with.
	/// @return Query object ready to be executed.
	template <typename T>
	Query &Where(const string &idx, CondType cond, std::initializer_list<T> l) {
		entries.resize(entries.size() + 1);
		QueryEntry &qe = entries.back();
		qe.condition = cond;
		qe.index = idx;
		qe.op = nextOp_;
		for (auto it = l.begin(); it != l.end(); it++) qe.values.push_back(Variant(*it));
		nextOp_ = OpAnd;
		return *this;
	}

	/// Adds a condition with several values. Analog to sql Where clause.
	/// @param idx - index used in condition clause.
	/// @param cond - type of condition.
	/// @param l - vector of index values to be compared with.
	/// @return Query object ready to be executed.
	template <typename T>
	Query &Where(const string &idx, CondType cond, const std::vector<T> &l) {
		entries.resize(entries.size() + 1);
		QueryEntry &qe = entries.back();
		qe.condition = cond;
		qe.index = idx;
		qe.op = nextOp_;
		qe.values.reserve(l.size());
		for (auto it = l.begin(); it != l.end(); it++) qe.values.push_back(Variant(*it));
		nextOp_ = OpAnd;
		return *this;
	}

	/// Adds a condition with several values. Analog to sql Where clause.
	/// @param idx - index used in condition clause.
	/// @param cond - type of condition.
	/// @param l - vector of index values to be compared with.
	/// @return Query object ready to be executed.
	Query &Where(const string &idx, CondType cond, const VariantArray &l) {
		entries.resize(entries.size() + 1);
		QueryEntry &qe = entries.back();
		qe.condition = cond;
		qe.index = idx;
		qe.op = nextOp_;
		qe.values.reserve(l.size());
		for (auto it = l.begin(); it != l.end(); it++) qe.values.push_back(Variant(*it));
		nextOp_ = OpAnd;
		return *this;
	}

	/// Adds a condition with several values to a composite index.
	/// @param idx - index name.
	/// @param cond - type of condition.
	/// @param l - list of values to be compared according to the order
	/// of indexes in composite index name.
	/// There can be maximum 2 VariantArray objects in l: in case of CondRange condition,
	/// in all other cases amount of elements in l would be striclty equal to 1.
	/// For example, composite index name is "bookid+price", so l[0][0] (and l[1][0]
	/// in case of CondRange) belongs to "bookid" and l[0][1] (and l[1][1] in case of CondRange)
	/// belongs to "price" indexes.
	/// @return Query object ready to be executed.
	Query &WhereComposite(const string &idx, CondType cond, initializer_list<VariantArray> l) {
		entries.resize(entries.size() + 1);
		QueryEntry &qe = entries.back();
		qe.condition = cond;
		qe.index = idx;
		qe.op = nextOp_;
		qe.values.reserve(l.size());
		for (auto it = l.begin(); it != l.end(); it++) {
			qe.values.push_back(Variant(*it));
		}
		nextOp_ = OpAnd;
		return *this;
	}
	Query &WhereComposite(const string &idx, CondType cond, const vector<VariantArray> &v) {
		entries.resize(entries.size() + 1);
		QueryEntry &qe = entries.back();
		qe.condition = cond;
		qe.index = idx;
		qe.op = nextOp_;
		qe.values.reserve(v.size());
		for (auto it = v.begin(); it != v.end(); it++) {
			qe.values.push_back(Variant(*it));
		}
		nextOp_ = OpAnd;
		return *this;
	}

	/// Add sql-function to query.
	/// @param function - function declaration.
	void AddFunction(const string &function) { selectFunctions_.push_back(std::move(function)); }

	/// Adds equal position fields to arrays queries.
	/// @param equalPosition - list of fields with equal array index position.
	void AddEqualPosition(const h_vector<string> &equalPosition) {
		equalPositions_.push_back(determineEqualPositionIndexes(equalPosition));
	}
	void AddEqualPosition(const vector<string> &equalPosition) { equalPositions_.push_back(determineEqualPositionIndexes(equalPosition)); }
	void AddEqualPosition(std::initializer_list<string> l) { equalPositions_.push_back(determineEqualPositionIndexes(l)); }

	/// Joins namespace with another namespace. Analog to sql JOIN.
	/// @param joinType - type of Join (Inner, Left or OrInner).
	/// @param index - name of the field in the namespace of this Query object.
	/// @param joinIndex - name of the field in the namespace of qr Query object.
	/// @param cond - condition type (Eq, Leq, Geq, etc).
	/// @param op - operation type (and, or, not).
	/// @param qr - query of the namespace that is going to be joined with this one.
	/// @return Query object ready to be executed.
	Query &Join(JoinType joinType, const string &index, const string &joinIndex, CondType cond, OpType op, Query &qr) {
		QueryJoinEntry joinEntry;
		joinEntry.op_ = op;
		joinEntry.condition_ = cond;
		joinEntry.index_ = index;
		joinEntry.joinIndex_ = joinIndex;
		qr.joinType = joinType;
		qr.joinEntries_.push_back(joinEntry);
		joinQueries_.push_back(qr);
		return *this;
	}

	/// @public
	/// Inner Join of this namespace with another one.
	/// @param index - name of the field in the namespace of this Query object.
	/// @param joinIndex - name of the field in the namespace of qr Query object.
	/// @param cond - condition type (Eq, Leq, Geq, etc).
	/// @param qr - query of the namespace that is going to be joined with this one.
	/// @return Query object ready to be executed.
	Query &InnerJoin(const string &index, const string &joinIndex, CondType cond, Query &qr) {
		return Join(JoinType::InnerJoin, index, joinIndex, cond, OpAnd, qr);
	}

	/// Left Join of this namespace with another one.
	/// @param index - name of the field in the namespace of this Query object.
	/// @param joinIndex - name of the field in the namespace of qr Query object.
	/// @param cond - condition type (Eq, Leq, Geq, etc).
	/// @param qr - query of the namespace that is going to be joined with this one.
	/// @return Query object ready to be executed.
	Query &LeftJoin(const string &index, const string &joinIndex, CondType cond, Query &qr) {
		return Join(JoinType::LeftJoin, index, joinIndex, cond, OpAnd, qr);
	}

	/// OrInnerJoin of this namespace with another one.
	/// @param index - name of the field in the namespace of this Query object.
	/// @param joinIndex - name of the field in the namespace of qr Query object.
	/// @param cond - condition type (Eq, Leq, Geq, etc).
	/// @param qr - query of the namespace that is going to be joined with this one.
	/// @return Not a reference to a query object ready to be executed.
	Query OrInnerJoin(const string &index, const string &joinIndex, CondType cond, Query &qr) {
		Query &joinQr = Join(JoinType::OrInnerJoin, index, joinIndex, cond, OpAnd, qr);
		joinQr.nextOp_ = OpOr;
		Query innerJoinQr(joinQr);
		joinQr.nextOp_ = OpAnd;
		return innerJoinQr;
	}

	/// Changes debug level.
	/// @param level - debug level.
	/// @return Query object.
	Query &Debug(int level) {
		debugLevel = level;
		return *this;
	}

	/// Performs sorting by certain column. Analog to sql ORDER BY.
	/// @param sort - sorting column name.
	/// @param desc - is sorting direction descending or ascending.
	/// @return Query object.
	Query &Sort(const string &sort, bool desc) {
		if (sort.length()) sortingEntries_.push_back({sort, desc});
		return *this;
	}

	/// Performs distinct for a certain index.
	/// @param indexName - name of index for distict operation.
	Query &Distinct(const string &indexName) {
		if (indexName.length()) {
			QueryEntry qentry;
			qentry.index = indexName;
			qentry.distinct = true;
			entries.push_back(qentry);
		}
		return *this;
	}

	/// Sets list of columns in this namespace to be finally selected.
	/// @param l - list of columns to be selected.
	Query &Select(std::initializer_list<const char *> l) {
		selectFilter_.insert(selectFilter_.begin(), l.begin(), l.end());
		return *this;
	};

	/// Adds an aggregate function for certain column.
	/// Analog to sql aggregate functions (min, max, avg, etc).
	/// @param idx - name of the field to be aggregated.
	/// @param type - aggregation function type (Sum, Avg).
	/// @return Query object ready to be executed.
	Query &Aggregate(const string &idx, AggType type) {
		aggregations_.push_back({idx, type});
		return *this;
	}

	/// Sets next operation type to Or.
	/// @return Query object.
	Query &Or() {
		nextOp_ = OpOr;
		return *this;
	}

	/// Sets next operation type to Not.
	/// @return Query object.
	Query &Not() {
		nextOp_ = OpNot;
		return *this;
	}

	/// Sets the limit of selected rows.
	/// Analog to sql LIMIT rowsNumber.
	/// @param limit - number of rows to get from result set.
	/// @return Query object.
	Query &Limit(unsigned limit) {
		count = limit;
		return *this;
	}

	/// Sets the number of the first selected row from result query.
	/// Analog to sql LIMIT OFFSET.
	/// @param offset - index of the first row to get from result set.
	/// @return Query object.
	Query &Offset(unsigned offset) {
		start = offset;
		return *this;
	}

	/// Set the total count calculation mode to Accurate
	/// @return Query object
	Query &ReqTotal() {
		calcTotal = ModeAccurateTotal;
		return *this;
	}

	/// Set the total count calculation mode to Cached.
	/// It will be use LRUCache for total count result
	/// @return Query object
	Query &CachedTotal() {
		calcTotal = ModeCachedTotal;
		return *this;
	}

	/// Serializes query data to stream.
	/// @param ser - serializer object for write.
	/// @param mode - serialization mode.
	void Serialize(WrSerializer &ser, uint8_t mode = Normal) const;

	/// Deserializes query data from stream.
	/// @param ser - serializer object.
	void Deserialize(Serializer &ser);

	/// returns structure of a query in JSON format
	string GetJSON() const;

	/// Get  readaby Join Type
	/// @param type - join tyoe
	/// @return string with join type name
	static const char *JoinTypeName(JoinType type);

	void WalkNested(bool withSelf, bool withMerged, std::function<void(const Query &q)> visitor) const {
		if (withSelf) visitor(*this);
		if (withMerged)
			for (auto &mq : mergeQueries_) visitor(mq);
		for (auto &jq : joinQueries_) visitor(jq);
		for (auto &mq : mergeQueries_)
			for (auto &jq : mq.joinQueries_) visitor(jq);
	}

protected:
	/// Parses query.
	/// @param tok - tokenizer object instance.
	/// @return always returns zero.
	int Parse(tokenizer &tok);

	/// Parses filter part of sql query.
	/// @param tok - tokenizer object instance.
	/// @return always returns zero.
	int selectParse(tokenizer &tok);

	/// Parses JSON dsl set.
	/// @param dsl - dsl set.
	void parseJson(const string &dsl);

	/// Deserializes query data from stream.
	/// @param ser - serializer object.
	void deserialize(Serializer &ser);

	/// Parse join entries
	void parseJoin(JoinType type, tokenizer &tok);

	/// Parse join entries
	void parseJoinEntries(tokenizer &tok, const string &mainNs);

	/// Parse merge entries
	void parseMerge(tokenizer &parser);

	/// Calculates QueryEntries indexes for EqualPosition context.
	/// @param fields - equal position context.
	template <typename T>
	EqualPosition determineEqualPositionIndexes(const T &fields) {
		EqualPosition ep;
		for (size_t i = 0; i < entries.size(); ++i) {
			for (auto it = fields.begin(); it != fields.end(); ++it)
				if (entries[i].index == *it) {
					ep.push_back(i);
					break;
				}
		}
		if (fields.size() < 2) throw Error(errLogic, "Amount of fields with equal index position should be 2 or more!");
		return ep;
	}

	/// Builds print version of a query with join in sql format.
	/// @param ser - serializer to store SQL string
	/// @param stripArgs - replace condition values with '?'
	void dumpJoined(WrSerializer &ser, bool stripArgs) const;

	/// Builds a print version of a query with merge queries in sql format.
	/// @param ser - serializer to store SQL string
	/// @param stripArgs - replace condition values with '?'
	void dumpMerged(WrSerializer &ser, bool stripArgs) const;

	/// Builds a print version of a query's order by statement
	/// @param ser - serializer to store SQL string
	/// @param stripArgs - replace condition values with '?'
	void dumpOrderBy(WrSerializer &ser, bool stripArgs) const;

public:
	/// Next operation constant.
	OpType nextOp_ = OpAnd;

	/// Name of the namespace.
	string _namespace;

	/// Sorting data.
	SortingEntries sortingEntries_;

	/// Calculation mode.
	CalcTotalMode calcTotal = ModeNoTotal;

	/// First row index from result set.
	unsigned start = 0;

	/// Number of rows from result set.
	unsigned count = UINT_MAX;

	/// Debug level.
	int debugLevel = 0;

	/// Default join type.
	JoinType joinType = JoinType::LeftJoin;

	/// Keys that always go first - before any ordered values.
	VariantArray forcedSortOrder;

	/// List of queries for join.
	vector<Query> joinQueries_;

	/// List of merge queries.
	vector<Query> mergeQueries_;

	/// List of columns in a final result set.
	h_vector<string, 4> selectFilter_;

	/// List of sql functions
	h_vector<string, 1> selectFunctions_;

	/// List of same position fields for queries with arrays
	h_vector<EqualPosition, 1> equalPositions_;

	/// Explain query if true
	bool explain_ = false;
};

}  // namespace reindexer
