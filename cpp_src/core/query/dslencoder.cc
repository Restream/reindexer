#include "core/query/dslencoder.h"
#include <unordered_map>
#include "core/cjson/jsonbuilder.h"
#include "core/keyvalue/key_string.h"
#include "core/keyvalue/p_string.h"
#include "core/query/dslparsetools.h"
#include "core/query/query.h"

using std::unordered_map;

struct EnumClassHash {
	template <typename T>
	size_t operator()(T t) const {
		return static_cast<size_t>(t);
	}
};

namespace reindexer {
namespace dsl {

const unordered_map<JoinType, string, EnumClassHash> join_types = {{InnerJoin, "inner"}, {LeftJoin, "left"}, {OrInnerJoin, "orinner"}};

const unordered_map<Filter, string, EnumClassHash> filter_map = {
	{Filter::Cond, "cond"}, {Filter::Op, "op"}, {Filter::Field, "field"}, {Filter::Value, "value"}};

const unordered_map<CondType, string, EnumClassHash> cond_map = {
	{CondAny, "any"},	 {CondEq, "eq"},   {CondLt, "lt"},			{CondLe, "le"},		  {CondGt, "gt"},	{CondGe, "ge"},
	{CondRange, "range"}, {CondSet, "set"}, {CondAllSet, "allset"}, {CondEmpty, "empty"}, {CondEq, "match"},
};

const unordered_map<OpType, string, EnumClassHash> op_map = {{OpOr, "or"}, {OpAnd, "and"}, {OpNot, "not"}};

const unordered_map<CalcTotalMode, string, EnumClassHash> reqtotal_values = {
	{ModeNoTotal, "disabled"}, {ModeAccurateTotal, "enabled"}, {ModeCachedTotal, "cached"}};

const unordered_map<AggType, string, EnumClassHash> aggregation_types = {
	{AggSum, "sum"}, {AggAvg, "avg"}, {AggMax, "max"}, {AggMin, "min"}, {AggFacet, "facet"}};

template <typename T>
string get(unordered_map<T, string, EnumClassHash> const& m, const T& key) {
	auto it = m.find(key);
	if (it != m.end()) return it->second;
	assert(it != m.end());
	return string();
}

void encodeSorting(const Query& query, JsonBuilder& builder) {
	auto arrNode = builder.Array("sort");

	for (const SortingEntry& sortingEntry : query.sortingEntries_) {
		arrNode.Object().Put("field", sortingEntry.column).Put("desc", sortingEntry.desc);
	}
}

void encodeFilter(const QueryEntry& qentry, JsonBuilder& builder) {
	builder.Put("op", get(op_map, qentry.op));
	builder.Put("cond", get(cond_map, qentry.condition));
	builder.Put("field", qentry.index);

	if (qentry.values.size() > 1 || ((qentry.values.size() == 1) && qentry.values[0].Type() == KeyValueTuple)) {
		auto arrNode = builder.Array("value");
		for (const Variant& kv : qentry.values) {
			arrNode.Put(nullptr, kv);
		}
	} else {
		builder.Put("value", qentry.values[0]);
	}
}

void encodeFilters(const Query& query, JsonBuilder& builder) {
	auto arrNode = builder.Array("filters");

	for (const QueryEntry& qe : query.entries) {
		auto node = arrNode.Object();
		encodeFilter(qe, node);
	}
}

void toDsl(const Query& query, JsonBuilder& builder);

void encodeMergedQueries(const Query& query, JsonBuilder& builder) {
	auto arrNode = builder.Array("merge_queries");

	for (const Query& mq : query.mergeQueries_) {
		auto node = arrNode.Object();
		toDsl(mq, node);
	}
}

void encodeSelectFilter(const Query& query, JsonBuilder& builder) {
	auto arrNode = builder.Array("select_filter");
	for (auto& str : query.selectFilter_) arrNode.Put(nullptr, str);
}

void encodeSelectFunctions(const Query& query, JsonBuilder& builder) {
	auto arrNode = builder.Array("select_functions");
	for (auto& str : query.selectFunctions_) arrNode.Put(nullptr, str);
}

void encodeAggregationFunctions(const Query& query, JsonBuilder& builder) {
	auto arrNode = builder.Array("aggregations");

	for (auto& entry : query.aggregations_) {
		arrNode.Object().Put("field", entry.index_).Put("type", get(aggregation_types, entry.type_));
	}
}

void encodeJoinEntry(const QueryJoinEntry& joinEntry, JsonBuilder& builder) {
	builder.Put("left_field", joinEntry.index_);
	builder.Put("right_field", joinEntry.joinIndex_);
	builder.Put("cond", get(cond_map, joinEntry.condition_));
	builder.Put("op", get(op_map, joinEntry.op_));
}

void encodeJoins(const Query& query, JsonBuilder& builder) {
	if (query.joinQueries_.empty()) return;

	auto arrNode = builder.Array("join_queries");

	for (auto& joinQuery : query.joinQueries_) {
		auto node = arrNode.Object();

		node.Put("type", get(join_types, joinQuery.joinType));
		node.Put("op", get(op_map, joinQuery.nextOp_));
		node.Put("namespace", joinQuery._namespace);
		node.Put("limit", joinQuery.count);
		node.Put("offset", joinQuery.start);

		encodeFilters(joinQuery, node);
		encodeSorting(joinQuery, node);

		auto arr1 = node.Array("on");

		for (auto& joinEntry : joinQuery.joinEntries_) {
			auto obj1 = arr1.Object();
			encodeJoinEntry(joinEntry, obj1);
		}
	}
}

void toDsl(const Query& query, JsonBuilder& builder) {
	builder.Put("namespace", query._namespace);
	builder.Put("limit", query.count);
	builder.Put("offset", query.start);
	// builder.Put("distinct", "");
	builder.Put("req_total", get(reqtotal_values, query.calcTotal));
	builder.Put("next_op", get(op_map, query.nextOp_));
	builder.Put("explain", query.explain_);

	encodeSelectFilter(query, builder);
	encodeSelectFunctions(query, builder);
	encodeSorting(query, builder);
	encodeFilters(query, builder);
	encodeMergedQueries(query, builder);
	encodeAggregationFunctions(query, builder);
	encodeJoins(query, builder);
}

std::string toDsl(const Query& query) {
	WrSerializer ser;
	JsonBuilder builder(ser);
	toDsl(query, builder);

	builder.End();
	return ser.Slice().ToString();
}

}  // namespace dsl
}  // namespace reindexer
