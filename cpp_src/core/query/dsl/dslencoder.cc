#include "dslencoder.h"

#include "core/cjson/jsonbuilder.h"
#include "core/keyvalue/p_string.h"
#include "core/query/query.h"
#include "core/queryresults/aggregationresult.h"
#include "dslparser.h"
#include "tools/logger.h"
#include "vendor/frozen/unordered_map.h"

namespace reindexer {
namespace dsl {

constexpr static auto kJoinTypes =
	frozen::make_unordered_map<JoinType, std::string_view>({{InnerJoin, "inner"}, {LeftJoin, "left"}, {OrInnerJoin, "orinner"}});

constexpr static auto kCondMap = frozen::make_unordered_map<CondType, std::string_view>({
	{CondAny, "any"},
	{CondEq, "eq"},
	{CondLt, "lt"},
	{CondLe, "le"},
	{CondGt, "gt"},
	{CondGe, "ge"},
	{CondRange, "range"},
	{CondSet, "set"},
	{CondAllSet, "allset"},
	{CondEmpty, "empty"},
	{CondLike, "like"},
	{CondDWithin, "dwithin"},
});

constexpr static auto kOpMap = frozen::make_unordered_map<OpType, std::string_view>({{OpOr, "or"}, {OpAnd, "and"}, {OpNot, "not"}});

constexpr static auto kReqTotalValues = frozen::make_unordered_map<CalcTotalMode, std::string_view>(
	{{ModeNoTotal, "disabled"}, {ModeAccurateTotal, "enabled"}, {ModeCachedTotal, "cached"}});

enum class QueryScope { Main, Subquery };

template <typename T, size_t N>
std::string_view get(const frozen::unordered_map<T, std::string_view, N>& m, const T& key) {
	auto it = m.find(key);
	if (it != m.end()) {
		return it->second;
	}
	assertrx(it != m.end());
	return std::string_view();
}

static void encodeSorting(const SortingEntries& sortingEntries, JsonBuilder& builder) {
	auto arrNode = builder.Array("sort");

	for (const SortingEntry& sortingEntry : sortingEntries) {
		arrNode.Object().Put("field", sortingEntry.expression).Put("desc", sortingEntry.desc);
	}
}

static void encodeSingleJoinQuery(const JoinedQuery& joinQuery, JsonBuilder& builder);

static void encodeJoins(const Query& query, JsonBuilder& builder) {
	for (const auto& joinQuery : query.GetJoinQueries()) {
		if (joinQuery.joinType == LeftJoin) {
			auto node = builder.Object();
			encodeSingleJoinQuery(joinQuery, node);
		}
	}
}

static void encodeEqualPositions(const EqualPositions_t& equalPositions, JsonBuilder& builder) {
	if (equalPositions.empty()) {
		return;
	}
	auto node = builder.Object();
	auto epNodePositions = node.Array("equal_positions");
	for (const auto& eqPos : equalPositions) {
		auto epNodePosition = epNodePositions.Object(std::string_view());
		auto epNodePositionArr = epNodePosition.Array("positions");
		for (const auto& field : eqPos) {
			epNodePositionArr.Put(nullptr, field);
		}
	}
}

static void encodeFilters(const Query& query, JsonBuilder& builder) {
	auto arrNode = builder.Array("filters");
	query.Entries().ToDsl(query, arrNode);
	encodeJoins(query, arrNode);
	encodeEqualPositions(query.Entries().equalPositions, arrNode);
}

static void toDsl(const Query& query, QueryScope scope, JsonBuilder& builder);

static void encodeMergedQueries(const Query& query, JsonBuilder& builder) {
	auto arrNode = builder.Array("merge_queries");

	for (const Query& mq : query.GetMergeQueries()) {
		auto node = arrNode.Object();
		toDsl(mq, QueryScope::Main, node);
	}
}

static void encodeSelectFilter(const Query& query, JsonBuilder& builder) {
	auto arrNode = builder.Array("select_filter");
	for (auto& str : query.SelectFilters()) {
		arrNode.Put(nullptr, str);
	}
}

static void encodeSelectFunctions(const Query& query, JsonBuilder& builder) {
	auto arrNode = builder.Array("select_functions");
	for (auto& str : query.selectFunctions_) {
		arrNode.Put(nullptr, str);
	}
}

static void encodeAggregationFunctions(const Query& query, JsonBuilder& builder) {
	auto arrNode = builder.Array("aggregations");

	for (const auto& entry : query.aggregations_) {
		auto aggNode = arrNode.Object();
		aggNode.Put("type", AggTypeToStr(entry.Type()));
		switch (entry.Type()) {
			case AggDistinct:
			case AggFacet:
				encodeSorting(entry.Sorting(), aggNode);
			case AggSum:
			case AggAvg:
			case AggMin:
			case AggMax:
			case AggCount:
			case AggCountCached:
			case AggUnknown:
			default:
				break;
		}

		if (entry.Limit() != QueryEntry::kDefaultLimit) {
			aggNode.Put("limit", entry.Limit());
		}
		if (entry.Offset() != QueryEntry::kDefaultOffset) {
			aggNode.Put("offset", entry.Offset());
		}
		auto fldNode = aggNode.Array("fields");
		for (const auto& field : entry.Fields()) {
			fldNode.Put(nullptr, field);
		}
	}
}

static void encodeJoinEntry(const QueryJoinEntry& joinEntry, JsonBuilder& builder) {
	builder.Put("left_field", joinEntry.LeftFieldName());
	builder.Put("right_field", joinEntry.RightFieldName());
	builder.Put("cond", get(kCondMap, joinEntry.Condition()));
	builder.Put("op", get(kOpMap, joinEntry.Operation()));
}

static void encodeSingleJoinQuery(const JoinedQuery& joinQuery, JsonBuilder& builder) {
	using namespace std::string_view_literals;
	auto node = builder.Object("join_query"sv);

	node.Put("type", get(kJoinTypes, joinQuery.joinType));
	node.Put("namespace", joinQuery.NsName());
	node.Put("limit", joinQuery.Limit());
	node.Put("offset", joinQuery.Offset());

	encodeFilters(joinQuery, node);
	encodeSorting(joinQuery.sortingEntries_, node);

	auto arr1 = node.Array("on");

	for (auto& joinEntry : joinQuery.joinEntries_) {
		auto obj1 = arr1.Object();
		encodeJoinEntry(joinEntry, obj1);
	}
	arr1.End();	 // Close array

	auto selectFilters = node.Array("select_filter");
	for (const auto& str : joinQuery.SelectFilters()) {
		selectFilters.Put(nullptr, str);
	}
}

static void putValues(JsonBuilder& builder, const VariantArray& values) {
	if (values.empty()) {
		return;
	} else if (values.size() > 1 || values[0].Type().Is<KeyValueType::Tuple>()) {
		auto arrNode = builder.Array("value");
		for (const Variant& kv : values) {
			arrNode.Put(nullptr, kv);
		}
	} else {
		builder.Put("value", values[0]);
	}
}

static void encodeFilter(const QueryEntry& qentry, JsonBuilder& builder) {
	if (qentry.Distinct()) {
		return;
	}
	builder.Put("cond", get(kCondMap, CondType(qentry.Condition())));
	builder.Put("field", qentry.FieldName());
	putValues(builder, qentry.Values());
}

static void encodeDropFields(const Query& query, JsonBuilder& builder) {
	auto dropFields = builder.Array("drop_fields");
	for (const UpdateEntry& updateEntry : query.UpdateFields()) {
		if (updateEntry.Mode() == FieldModeDrop) {
			dropFields.Put(0, updateEntry.Column());
		}
	}
}

static void encodeUpdateFields(const Query& query, JsonBuilder& builder) {
	auto updateFields = builder.Array("update_fields");
	for (const UpdateEntry& updateEntry : query.UpdateFields()) {
		if (updateEntry.Mode() == FieldModeSet || updateEntry.Mode() == FieldModeSetJson) {
			bool isObject = (updateEntry.Mode() == FieldModeSetJson);
			auto field = updateFields.Object(0);
			if (isObject) {
				field.Put("type", "object");
			} else if (updateEntry.IsExpression()) {
				field.Put("type", "expression");
			} else {
				field.Put("type", "value");
			}
			field.Put("name", updateEntry.Column());
			field.Put("is_array", updateEntry.Values().IsArrayValue());
			auto values = field.Array("values");
			for (const Variant& v : updateEntry.Values()) {
				if (isObject) {
					values.Json(nullptr, p_string(v));
				} else {
					values.Put(0, v);
				}
			}
		}
	}
}

static void toDsl(const Query& query, QueryScope scope, JsonBuilder& builder) {
	switch (query.Type()) {
		case QueryType::QuerySelect: {
			builder.Put("namespace", query.NsName());
			builder.Put("limit", query.Limit());
			builder.Put("offset", query.Offset());
			builder.Put("req_total", get(kReqTotalValues, query.CalcTotal()));
			if (scope != QueryScope::Subquery) {
				builder.Put("explain", query.NeedExplain());
				if (query.IsLocal()) {
					builder.Put("local", true);
				}
				builder.Put("type", "select");
				auto strictMode = strictModeToString(query.GetStrictMode());
				if (!strictMode.empty()) {
					builder.Put("strict_mode", strictMode);
				}
				builder.Put("select_with_rank", query.IsWithRank());
			}

			encodeSelectFilter(query, builder);
			if (scope != QueryScope::Subquery) {
				encodeSelectFunctions(query, builder);
			}
			encodeSorting(query.sortingEntries_, builder);
			encodeFilters(query, builder);
			if (scope != QueryScope::Subquery) {
				encodeMergedQueries(query, builder);
			}
			encodeAggregationFunctions(query, builder);
			break;
		}
		case QueryType::QueryUpdate: {
			builder.Put("namespace", query.NsName());
			builder.Put("explain", query.NeedExplain());
			builder.Put("type", "update");
			encodeFilters(query, builder);
			bool withDropEntries = false, withUpdateEntries = false;
			for (const UpdateEntry& updateEntry : query.UpdateFields()) {
				if (updateEntry.Mode() == FieldModeDrop) {
					if (!withDropEntries) {
						withDropEntries = true;
					}
				}
				if (updateEntry.Mode() == FieldModeSet || updateEntry.Mode() == FieldModeSetJson) {
					if (!withUpdateEntries) {
						withUpdateEntries = true;
					}
				}
			}
			if (withDropEntries) {
				encodeDropFields(query, builder);
			}
			if (withUpdateEntries) {
				encodeUpdateFields(query, builder);
			}
			break;
		}
		case QueryType::QueryDelete: {
			builder.Put("namespace", query.NsName());
			builder.Put("explain", query.NeedExplain());
			builder.Put("type", "delete");
			encodeFilters(query, builder);
			break;
		}
		case QueryType::QueryTruncate: {
			builder.Put("namespace", query.NsName());
			builder.Put("type", "truncate");
			break;
		}
	}
}

std::string toDsl(const Query& query) {
	WrSerializer ser;
	JsonBuilder builder(ser);
	toDsl(query, QueryScope::Main, builder);

	builder.End();
	return std::string(ser.Slice());
}

}  // namespace dsl

void QueryEntries::toDsl(const_iterator it, const_iterator to, const Query& parentQuery, JsonBuilder& builder) {
	for (; it != to; ++it) {
		auto node = builder.Object();
		node.Put("op", dsl::get(dsl::kOpMap, it->operation));
		it->Visit(
			[&node](const AlwaysFalse&) {
				logPrintf(LogTrace, "Not normalized query to dsl");
				node.Put("always", false);
			},
			[&node](const AlwaysTrue&) {
				logPrintf(LogTrace, "Not normalized query to dsl");
				node.Put("always", true);
			},
			[&node, &parentQuery](const SubQueryEntry& sqe) {
				node.Put("cond", dsl::get(dsl::kCondMap, CondType(sqe.Condition())));
				{
					auto subquery = node.Object("subquery");
					dsl::toDsl(parentQuery.GetSubQuery(sqe.QueryIndex()), dsl::QueryScope::Subquery, subquery);
				}
				dsl::putValues(node, sqe.Values());
			},
			[&node, &parentQuery](const SubQueryFieldEntry& sqe) {
				node.Put("cond", dsl::get(dsl::kCondMap, CondType(sqe.Condition())));
				node.Put("field", sqe.FieldName());
				auto subquery = node.Object("subquery");
				dsl::toDsl(parentQuery.GetSubQuery(sqe.QueryIndex()), dsl::QueryScope::Subquery, subquery);
			},
			[&it, &node, &parentQuery](const QueryEntriesBracket& bracket) {
				auto arrNode = node.Array("filters");
				toDsl(it.cbegin(), it.cend(), parentQuery, arrNode);
				dsl::encodeEqualPositions(bracket.equalPositions, arrNode);
			},
			[&node](const QueryEntry& qe) {
				if (qe.Distinct()) {
					return;
				}
				dsl::encodeFilter(qe, node);
			},
			[&node, &parentQuery](const JoinQueryEntry& jqe) {
				assertrx(jqe.joinIndex < parentQuery.GetJoinQueries().size());
				dsl::encodeSingleJoinQuery(parentQuery.GetJoinQueries()[jqe.joinIndex], node);
			},
			[&node](const BetweenFieldsQueryEntry& qe) {
				node.Put("cond", dsl::get(dsl::kCondMap, CondType(qe.Condition())));
				node.Put("first_field", qe.LeftFieldName());
				node.Put("second_field", qe.RightFieldName());
			});
	}
}

}  // namespace reindexer
