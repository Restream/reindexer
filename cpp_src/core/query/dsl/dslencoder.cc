#include "dslencoder.h"
#include <unordered_map>
#include "core/cjson/jsonbuilder.h"
#include "core/keyvalue/key_string.h"
#include "core/keyvalue/p_string.h"
#include "core/query/query.h"
#include "core/queryresults/aggregationresult.h"
#include "dslparser.h"

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

const unordered_map<CondType, string, EnumClassHash> cond_map = {
	{CondAny, "any"},	  {CondEq, "eq"},	{CondLt, "lt"},			{CondLe, "le"},		  {CondGt, "gt"},	  {CondGe, "ge"},
	{CondRange, "range"}, {CondSet, "set"}, {CondAllSet, "allset"}, {CondEmpty, "empty"}, {CondLike, "like"},
};

const unordered_map<OpType, string, EnumClassHash> op_map = {{OpOr, "or"}, {OpAnd, "and"}, {OpNot, "not"}};

const unordered_map<CalcTotalMode, string, EnumClassHash> reqtotal_values = {
	{ModeNoTotal, "disabled"}, {ModeAccurateTotal, "enabled"}, {ModeCachedTotal, "cached"}};

template <typename T>
string get(unordered_map<T, string, EnumClassHash> const& m, const T& key) {
	auto it = m.find(key);
	if (it != m.end()) return it->second;
	assert(it != m.end());
	return string();
}

void encodeSorting(const SortingEntries& sortingEntries, JsonBuilder& builder) {
	auto arrNode = builder.Array("sort");

	for (const SortingEntry& sortingEntry : sortingEntries) {
		arrNode.Object().Put("field", sortingEntry.expression).Put("desc", sortingEntry.desc);
	}
}

void encodeSingleJoinQuery(const JoinedQuery& joinQuery, JsonBuilder& builder);

void encodeJoins(const Query& query, JsonBuilder& builder) {
	for (const auto& joinQuery : query.joinQueries_) {
		if (joinQuery.joinType == LeftJoin) {
			auto node = builder.Object();
			encodeSingleJoinQuery(joinQuery, node);
		}
	}
}

void encodeEqualPositions(const Query& query, JsonBuilder& builder) {
	if (query.equalPositions_.empty()) return;
	for (auto it = query.equalPositions_.begin(); it != query.equalPositions_.end(); ++it) {
		auto epNode = builder.Array("equal_position");
		for (auto it2 = it->second.begin(); it2 != it->second.end(); ++it2) {
			assert(query.entries.IsValue(*it2));
			epNode.Put(nullptr, query.entries[*it2].index);
		}
	}
}

void encodeFilters(const Query& query, JsonBuilder& builder) {
	auto arrNode = builder.Array("filters");
	query.entries.ToDsl(query, arrNode);
	encodeJoins(query, arrNode);
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

void encodeDistinct(const Query& query, JsonBuilder& builder) {
	auto arrNode = builder.Array("distinct");
	for (auto it = query.entries.begin(); it != query.entries.end(); ++it) {
		if (it->IsLeaf() && it->Value().distinct) {
			arrNode.Put(nullptr, it->Value().index);
		}
	}
}

void encodeAggregationFunctions(const Query& query, JsonBuilder& builder) {
	auto arrNode = builder.Array("aggregations");

	for (auto& entry : query.aggregations_) {
		auto aggNode = arrNode.Object();
		aggNode.Put("type", AggregationResult::aggTypeToStr(entry.type_));
		encodeSorting(entry.sortingEntries_, aggNode);
		if (entry.limit_ != UINT_MAX) aggNode.Put("limit", entry.limit_);
		if (entry.offset_ != 0) aggNode.Put("offset", entry.offset_);
		auto fldNode = aggNode.Array("fields");
		for (const auto& field : entry.fields_) {
			fldNode.Put(nullptr, field);
		}
	}
}

void encodeJoinEntry(const QueryJoinEntry& joinEntry, JsonBuilder& builder) {
	builder.Put("left_field", joinEntry.index_);
	builder.Put("right_field", joinEntry.joinIndex_);
	builder.Put("cond", get(cond_map, joinEntry.condition_));
	builder.Put("op", get(op_map, joinEntry.op_));
}

void encodeSingleJoinQuery(const JoinedQuery& joinQuery, JsonBuilder& builder) {
	auto node = builder.Object("join_query"_sv);

	node.Put("type", get(join_types, joinQuery.joinType));
	node.Put("namespace", joinQuery._namespace);
	node.Put("limit", joinQuery.count);
	node.Put("offset", joinQuery.start);

	encodeFilters(joinQuery, node);
	encodeSorting(joinQuery.sortingEntries_, node);

	auto arr1 = node.Array("on");

	for (auto& joinEntry : joinQuery.joinEntries_) {
		auto obj1 = arr1.Object();
		encodeJoinEntry(joinEntry, obj1);
	}
}

void encodeFilter(const Query& parentQuery, const QueryEntry& qentry, JsonBuilder& builder) {
	if (qentry.distinct) return;
	if (qentry.joinIndex == QueryEntry::kNoJoins) {
		builder.Put("cond", get(cond_map, CondType(qentry.condition)));
		builder.Put("field", qentry.index);

		if (qentry.values.empty()) return;
		if (qentry.values.size() > 1 || qentry.values[0].Type() == KeyValueTuple) {
			auto arrNode = builder.Array("value");
			for (const Variant& kv : qentry.values) {
				arrNode.Put(nullptr, kv);
			}
		} else {
			builder.Put("value", qentry.values[0]);
		}
	} else {
		assert(qentry.joinIndex < int(parentQuery.joinQueries_.size()));
		encodeSingleJoinQuery(parentQuery.joinQueries_[qentry.joinIndex], builder);
	}
}

void toDsl(const Query& query, JsonBuilder& builder) {
	builder.Put("namespace", query._namespace);
	builder.Put("limit", query.count);
	builder.Put("offset", query.start);
	builder.Put("req_total", get(reqtotal_values, query.calcTotal));
	builder.Put("explain", query.explain_);

	encodeDistinct(query, builder);
	encodeSelectFilter(query, builder);
	encodeSelectFunctions(query, builder);
	encodeSorting(query.sortingEntries_, builder);
	encodeFilters(query, builder);
	encodeMergedQueries(query, builder);
	encodeAggregationFunctions(query, builder);
	encodeEqualPositions(query, builder);
}

std::string toDsl(const Query& query) {
	WrSerializer ser;
	JsonBuilder builder(ser);
	toDsl(query, builder);

	builder.End();
	return string(ser.Slice());
}

}  // namespace dsl

void QueryEntries::toDsl(const_iterator it, const_iterator to, const Query& parentQuery, JsonBuilder& builder) {
	for (; it != to; ++it) {
		auto node = builder.Object();
		if (it->IsLeaf()) {
			if (it->Value().distinct) continue;
			if (it->Value().joinIndex == QueryEntry::kNoJoins) {
				node.Put("op", dsl::get(dsl::op_map, it->operation));
			}
			dsl::encodeFilter(parentQuery, it->Value(), node);
		} else {
			auto arrNode = node.Array("filters");
			toDsl(it.cbegin(), it.cend(), parentQuery, arrNode);
		}
	}
}

}  // namespace reindexer
