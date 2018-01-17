#pragma once

#include <climits>
#include <initializer_list>
#include "querywhere.h"
#include "tools/errors.h"
#include "tools/serializer.h"

using std::string;
using std::vector;

/**
 * @namespace reindexer
 * The base namespace
 */
namespace reindexer {

/**
 * @class Query.
 * Allows to select data from DB.
 * Analog to ansi-sql select query.
 */
class Query : public QueryWhere {
public:
	/**
	 * Creates an object for certain namespace with appropriate settings.
	 * @param __namespace - name of the namespace the data to be selected from.
	 * @param _start - number of the first row to get from selected set. Analog to sql LIMIT Offset.
	 * @param _count - number of rows to get from result set. Analog to sql LIMIT RowsCount.
	 * @param _calcTotal - calculation mode.
	 */
	Query(const string &__namespace, unsigned _start = 0, unsigned _count = UINT_MAX, CalcTotalMode _calcTotal = ModeNoTotal);
	/**
	 * Creates an empty object.
	 */
	Query() {}

	/**
	 * @public
	 * Parses pure sql select query and initializes Query object data members as a result.
	 * @param q - sql query.
	 * @return always returns 0.
	 */
	int Parse(const string &q);
	/**
	 * @public
	 * Parses JSON dsl set.
	 * @param dsl - dsl set.
	 * @return always returns errOk or throws an exception.
	 */
	Error ParseJson(const string &dsl);

	/**
	 * @public
	 * Logs query in 'Select field1, ... field N from namespace ...' format.
	 */
	string Dump() const;
	/**
	 * @public
	 * Builds print version of a query with join in sql format.
	 * @return query sql string.
	 */
	string DumpJoined() const;
	/**
	 * @public
	 * Builds a print version of a query with merge queries in sql format.
	 * @return query sql string.
	 */
	string DumpMerged() const;

	/**
	 * @public
	 * Adds a condition with a single value. Analog to sql Where clause.
	 * @param idx - index used in condition clause.
	 * @param cond - type of condition.
	 * @param val - value of index to be compared with.
	 * @return Query object ready to be executed.
	 */
	template <typename Input>
	Query &Where(const char *idx, CondType cond, Input val) {
		return Where(idx, cond, {val});
	}
	/**
	 * @public
	 * Adds a condition with several values. Analog to sql Where clause.
	 * @param idx - index used in condition clause.
	 * @param cond - type of condition.
	 * @param l - list of indexes' values to be compared with.
	 * @return Query object ready to be executed.
	 */
	template <typename T>
	Query &Where(const char *idx, CondType cond, std::initializer_list<T> l) {
		QueryEntry qe;
		qe.condition = cond;
		qe.index = idx;
		qe.op = nextOp_;
		for (auto it = l.begin(); it != l.end(); it++) qe.values.push_back(KeyValue(*it));
		entries.push_back(qe);
		nextOp_ = OpAnd;
		return *this;
	}

	/**
	 * @public
	 * Adds a condition with several values. Analog to sql Where clause.
	 * @param idx - index used in condition clause.
	 * @param cond - type of condition.
	 * @param l - vector of indexes' values to be compared with.
	 * @return Query object ready to be executed.
	 */
	template <typename T>
	Query &Where(const char *idx, CondType cond, const std::vector<T> &l) {
		QueryEntry qe;
		qe.condition = cond;
		qe.index = idx;
		qe.op = nextOp_;
		for (auto it = l.begin(); it != l.end(); it++) qe.values.push_back(KeyValue(*it));
		entries.push_back(qe);
		nextOp_ = OpAnd;
		return *this;
	}

	/**
	 * @public
	 * Joins namespace with another namespace. Analog to sql JOIN.
	 * @param joinType - type of Join (Inner, Left or OrInner).
	 * @param index - name of the field in the namespace of this Query object.
	 * @param joinIndex - name of the field in the namespace of qr Query object.
	 * @param cond - condition type (Eq, Leq, Geq, etc).
	 * @param op - operation type (and, or, not).
	 * @param qr - query of the namespace that is going to be joined with this one.
	 * @return Query object ready to be executed.
	 */
	Query &Join(JoinType joinType, const char *index, const char *joinIndex, CondType cond, OpType op, Query &qr) {
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

	/**
	 * @public
	 * Inner Join of this namespace with another one.
	 * @param index - name of the field in the namespace of this Query object.
	 * @param joinIndex - name of the field in the namespace of qr Query object.
	 * @param cond - condition type (Eq, Leq, Geq, etc).
	 * @param qr - query of the namespace that is going to be joined with this one.
	 * @return Query object ready to be executed.
	 */
	Query &InnerJoin(const char *index, const char *joinIndex, CondType cond, Query &qr) {
		return Join(JoinType::InnerJoin, index, joinIndex, cond, OpAnd, qr);
	}

	/**
	 * @public
	 * Left Join of this namespace with another one.
	 * @param index - name of the field in the namespace of this Query object.
	 * @param joinIndex - name of the field in the namespace of qr Query object.
	 * @param cond - condition type (Eq, Leq, Geq, etc).
	 * @param qr - query of the namespace that is going to be joined with this one.
	 * @return Query object ready to be executed.
	 */
	Query &LeftJoin(const char *index, const char *joinIndex, CondType cond, Query &qr) {
		return Join(JoinType::LeftJoin, index, joinIndex, cond, OpAnd, qr);
	}

	/**
	 * @public
	 * OrInnerJoin of this namespace with another one.
	 * @param index - name of the field in the namespace of this Query object.
	 * @param joinIndex - name of the field in the namespace of qr Query object.
	 * @param cond - condition type (Eq, Leq, Geq, etc).
	 * @param qr - query of the namespace that is going to be joined with this one.
	 * @return Not a reference to a query object ready to be executed.
	 */
	Query OrInnerJoin(const char *index, const char *joinIndex, CondType cond, Query &qr) {
		Query &joinQr = Join(JoinType::OrInnerJoin, index, joinIndex, cond, OpAnd, qr);
		joinQr.nextOp_ = OpOr;
		Query innerJoinQr(joinQr);
		joinQr.nextOp_ = OpAnd;
		return innerJoinQr;
	}

	/**
	 * @public
	 * Changes debug level.
	 * @param level - debug level.
	 * @return Query object.
	 */
	Query &Debug(int level) {
		debugLevel = level;
		return *this;
	}
	/**
	 * @public
	 * Performs sorting by certain column. Analog to sql ORDER BY.
	 * @param sort - sorting column name.
	 * @param desc - is sorting direction descending or ascending.
	 * @return Query object.
	 */
	Query &Sort(const char *sort, bool desc) {
		sortBy = sort;
		sortDirDesc = desc;
		return *this;
	}
	Query &Distinct(const char *indexName) {
		QueryEntry qentry;
		qentry.index = indexName;
		qentry.distinct = true;
		entries.push_back(qentry);
		return *this;
	}
	/**
	 * @public
	 * Sets list of columns in this namespace to be finally selected.
	 * @param l - list of columns to be selected.
	 */
	Query &Select(std::initializer_list<const char *> l) {
		selectFilter_.insert(selectFilter_.begin(), l.begin(), l.end());
		return *this;
	};
	/**
	 * @public
	 * Adds an aggregate function for certain column.
	 * Analog to sql aggregate functions (min, max, avg, etc).
	 * @param idx - name of the field to be aggregated.
	 * @param type - aggregation function type (Sum, Avg).
	 * @return Query object ready to be executed.
	 */
	Query &Aggregate(const char *idx, AggType type) {
		aggregations_.push_back({idx, type});
		return *this;
	}
	/**
	 * @public
	 * Sets next operation type to Or.
	 * @return Query object.
	 */
	Query &Or() {
		nextOp_ = OpOr;
		return *this;
	}
	/**
	 * @public
	 * Sets next operation type to Not.
	 * @return Query object.
	 */
	Query &Not() {
		nextOp_ = OpNot;
		return *this;
	}
	/**
	 * @public
	 * Sets the limit of selected rows.
	 * Analog to sql LIMIT rowsNumber.
	 * @param limit - number of rows to get from result set.
	 * @return Query object.
	 */
	Query &Limit(unsigned limit) {
		count = limit;
		return *this;
	}
	/**
	 * @public
	 * Sets the number of the first selected row from result query.
	 * Analog to sql LIMIT OFFSET.
	 * @param offset - index of the first row to get from result set.
	 * @return Query object.
	 */
	Query &Offset(unsigned offset) {
		start = offset;
		return *this;
	}

	/**
	 * @public
	 * Serializes query data to stream.
	 * @param ser - serializer object for write.
	 * @param mode - serialization mode.
	 */
	void Serialize(WrSerializer &ser, uint8_t mode = Normal) const;
	/**
	 * @public
	 * Deserializes query data from stream.
	 * @param ser - serializer object.
	 */
	void Deserialize(Serializer &ser);

protected:
	/**
	 * @protected
	 * Parses query.
	 * @param tok - tokenizer object instance.
	 * @return always returns zero.
	 */
	int Parse(tokenizer &tok);

	/**
	 * @protected
	 * Parses filter part of sql query.
	 * @param tok - tokenizer object instance.
	 * @return always returns zero.
	 */
	int selectParse(tokenizer &tok);
	/**
	 * @protected
	 * Parses namespace part of sql query.
	 * @param tok - tokenizer object instance.
	 * @return always returns zero.
	 */
	int describeParse(tokenizer &tok);
	/**
	 * @protected
	 * Parses JSON dsl set.
	 * @param dsl - dsl set.
	 */
	void parseJson(const string &dsl);

	/**
	 * @protected
	 * Deserializes query data from stream.
	 * @param ser - serializer object.
	 */
	void deserialize(Serializer &ser);

	/**
	 * @protected
	 * Next operation constant.
	 */
	OpType nextOp_ = OpAnd;

public:
	/**
	 * @public
	 * Name of the namespace.
	 */
	string _namespace;
	/**
	 * @protected
	 * name of the column to be sorted by.
	 */
	string sortBy;
	/**
	 * @protected
	 * Sorting direction type: asc or desc.
	 */
	bool sortDirDesc = false;
	/**
	 * @protected
	 * Calculation mode.
	 */
	CalcTotalMode calcTotal = ModeNoTotal;
	/**
	 * @protected
	 * Need to describe query.
	 */
	bool describe = false;
	/**
	 * @protected
	 * First row index from result set.
	 */
	unsigned start = 0;
	/**
	 * @protected
	 * Number of rows from result set.
	 */
	unsigned count = UINT_MAX;
	/**
	 * @protected
	 * Debug level.
	 */
	int debugLevel = 0;
	/**
	 * @protected
	 * Default join type.
	 */
	JoinType joinType = JoinType::LeftJoin;

	/**
	 * @protected
	 */
	KeyValues forcedSortOrder;

	/**
	 * @protected
	 * Container or namespaces in a describe part.
	 */
	vector<string> namespacesNames_;

	/**
	 * @protected
	 * List of queries for join.
	 */
	vector<Query> joinQueries_;
	/**
	 * @protected
	 * List of merge queries.
	 */
	vector<Query> mergeQueries_;
	/**
	 * @protected
	 * List of columns in a final result set.
	 */
	h_vector<string, 4> selectFilter_;
};

}  // namespace reindexer
