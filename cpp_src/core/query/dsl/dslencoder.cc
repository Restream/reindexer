#include "dslencoder.h"

#include "core/cjson/jsonbuilder.h"
#include "core/keyvalue/p_string.h"
#include "core/query/query.h"
#include "core/queryresults/aggregationresult.h"
#include "dslparser.h"
#include "tools/logger.h"
#include "vendor/frozen/unordered_map.h"

namespace reindexer {

using namespace std::string_view_literals;

namespace dsl {

constexpr static auto kJoinTypes =
	frozen::make_unordered_map<JoinType, std::string_view>({{InnerJoin, "inner"sv}, {LeftJoin, "left"sv}, {OrInnerJoin, "orinner"sv}});

constexpr static auto kCondMap = frozen::make_unordered_map<CondType, std::string_view>({
	{CondAny, "any"sv},
	{CondEq, "eq"sv},
	{CondLt, "lt"sv},
	{CondLe, "le"sv},
	{CondGt, "gt"sv},
	{CondGe, "ge"sv},
	{CondRange, "range"sv},
	{CondSet, "set"sv},
	{CondAllSet, "allset"sv},
	{CondEmpty, "empty"sv},
	{CondLike, "like"sv},
	{CondDWithin, "dwithin"sv},
	{CondKnn, "knn"sv},
});

constexpr static auto kOpMap = frozen::make_unordered_map<OpType, std::string_view>({{OpOr, "or"sv}, {OpAnd, "and"sv}, {OpNot, "not"sv}});

constexpr static auto kReqTotalValues = frozen::make_unordered_map<CalcTotalMode, std::string_view>(
	{{ModeNoTotal, "disabled"sv}, {ModeAccurateTotal, "enabled"sv}, {ModeCachedTotal, "cached"sv}});

enum class [[nodiscard]] QueryScope { Main, Subquery };

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
	auto arrNode = builder.Array("sort"sv);

	for (const SortingEntry& sortingEntry : sortingEntries) {
		auto obj = arrNode.Object();
		obj.Put("field"sv, sortingEntry.expression);
		obj.Put("desc"sv, *sortingEntry.desc);
		obj.End();
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
	auto epNodePositions = node.Array("equal_positions"sv);
	for (const auto& eqPos : equalPositions) {
		auto epNodePosition = epNodePositions.Object(std::string_view());
		auto epNodePositionArr = epNodePosition.Array("positions"sv);
		for (const auto& field : eqPos) {
			epNodePositionArr.Put(TagName::Empty(), field);
		}
	}
}

static void encodeFilters(const Query& query, JsonBuilder& builder) {
	auto arrNode = builder.Array("filters"sv);
	query.Entries().ToDsl(query, arrNode);
	encodeJoins(query, arrNode);
	encodeEqualPositions(query.Entries().equalPositions, arrNode);
}

static void toDsl(const Query& query, QueryScope scope, JsonBuilder& builder);

static void encodeMergedQueries(const Query& query, JsonBuilder& builder) {
	auto arrNode = builder.Array("merge_queries"sv);

	for (const Query& mq : query.GetMergeQueries()) {
		auto node = arrNode.Object();
		toDsl(mq, QueryScope::Main, node);
	}
}

static void encodeSelectFilter(const FieldsNamesFilter& filter, JsonBuilder& builder) {
	auto arrNode = builder.Array("select_filter"sv);
	if (filter.Empty()) {
		return;
	}
	if (filter.AllRegularFields() && !filter.OnlyAllRegularFields()) {
		arrNode.Put(TagName::Empty(), FieldsNamesFilter::kAllRegularFieldsName);
	}
	for (const auto& str : filter.Fields()) {
		arrNode.Put(TagName::Empty(), str);
	}
	if (filter.AllVectorFields()) {
		arrNode.Put(TagName::Empty(), FieldsNamesFilter::kAllVectorFieldsName);
	}
}

static void encodeSelectFunctions(const Query& query, JsonBuilder& builder) {
	auto arrNode = builder.Array("select_functions"sv);
	for (auto& str : query.selectFunctions_) {
		arrNode.Put(TagName::Empty(), str);
	}
}

static void encodeAggregationFunctions(const Query& query, JsonBuilder& builder) {
	auto arrNode = builder.Array("aggregations"sv);

	for (const auto& entry : query.aggregations_) {
		auto aggNode = arrNode.Object();
		aggNode.Put("type"sv, AggTypeToStr(entry.Type()));
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
			aggNode.Put("limit"sv, entry.Limit());
		}
		if (entry.Offset() != QueryEntry::kDefaultOffset) {
			aggNode.Put("offset"sv, entry.Offset());
		}
		auto fldNode = aggNode.Array("fields"sv);
		for (const auto& field : entry.Fields()) {
			fldNode.Put(TagName::Empty(), field);
		}
	}
}

static void encodeJoinEntry(const QueryJoinEntry& joinEntry, JsonBuilder& builder) {
	builder.Put("op"sv, get(kOpMap, joinEntry.Operation()));
	builder.Put("left_field"sv, joinEntry.LeftFieldName());
	builder.Put("right_field"sv, joinEntry.RightFieldName());
	builder.Put("cond"sv, get(kCondMap, joinEntry.Condition()));
}

static void encodeSingleJoinQuery(const JoinedQuery& joinQuery, JsonBuilder& builder) {
	auto node = builder.Object("join_query"sv);

	node.Put("type"sv, get(kJoinTypes, joinQuery.joinType));
	node.Put("namespace"sv, joinQuery.NsName());
	node.Put("limit"sv, joinQuery.Limit());
	node.Put("offset"sv, joinQuery.Offset());

	encodeFilters(joinQuery, node);
	encodeSorting(joinQuery.GetSortingEntries(), node);

	auto arr1 = node.Array("on"sv);

	for (auto& joinEntry : joinQuery.joinEntries_) {
		auto obj1 = arr1.Object();
		encodeJoinEntry(joinEntry, obj1);
	}
	arr1.End();	 // Close array

	encodeSelectFilter(joinQuery.SelectFilters(), node);
}

static void putValues(JsonBuilder& builder, const VariantArray& values) {
	if (values.empty()) {
		return;
	} else if (values.size() > 1 || values[0].Type().Is<KeyValueType::Tuple>()) {
		auto arrNode = builder.Array("value"sv);
		for (const Variant& kv : values) {
			arrNode.Put(TagName::Empty(), kv);
		}
	} else {
		builder.Put("value"sv, values[0]);
	}
}

static void encodeFilter(const QueryEntry& qentry, JsonBuilder& builder) {
	if (qentry.Distinct()) {
		return;
	}
	builder.Put("cond"sv, get(kCondMap, CondType(qentry.Condition())));
	builder.Put("field"sv, qentry.FieldName());
	putValues(builder, qentry.Values());
}

static void encodeDropFields(const Query& query, JsonBuilder& builder) {
	auto dropFields = builder.Array("drop_fields"sv);
	for (const UpdateEntry& updateEntry : query.UpdateFields()) {
		if (updateEntry.Mode() == FieldModeDrop) {
			dropFields.Put(TagName::Empty(), updateEntry.Column());
		}
	}
}

static void encodeUpdateFields(const Query& query, JsonBuilder& builder) {
	auto updateFields = builder.Array("update_fields"sv);
	for (const UpdateEntry& updateEntry : query.UpdateFields()) {
		if (updateEntry.Mode() == FieldModeSet || updateEntry.Mode() == FieldModeSetJson) {
			bool isObject = (updateEntry.Mode() == FieldModeSetJson);
			auto field = updateFields.Object();
			if (isObject) {
				field.Put("type"sv, "object"sv);
			} else if (updateEntry.IsExpression()) {
				field.Put("type"sv, "expression"sv);
			} else {
				field.Put("type"sv, "value"sv);
			}
			field.Put("name"sv, updateEntry.Column());
			field.Put("is_array"sv, updateEntry.Values().IsArrayValue());
			auto values = field.Array("values"sv);
			for (const Variant& v : updateEntry.Values()) {
				if (isObject) {
					values.Json(p_string(v));
				} else {
					values.Put(TagName::Empty(), v);
				}
			}
		}
	}
}

static void toDsl(const Query& query, QueryScope scope, JsonBuilder& builder) {
	switch (query.Type()) {
		case QueryType::QuerySelect: {
			builder.Put("namespace"sv, query.NsName());
			builder.Put("limit"sv, query.Limit());
			builder.Put("offset"sv, query.Offset());
			builder.Put("req_total"sv, get(kReqTotalValues, query.CalcTotal()));
			if (scope != QueryScope::Subquery) {
				builder.Put("explain"sv, query.NeedExplain());
				if (query.IsLocal()) {
					builder.Put("local"sv, true);
				}
				builder.Put("type"sv, "select"sv);
				auto strictMode = strictModeToString(query.GetStrictMode());
				if (!strictMode.empty()) {
					builder.Put("strict_mode"sv, strictMode);
				}
				builder.Put("select_with_rank"sv, query.IsWithRank());
			}

			encodeSelectFilter(query.SelectFilters(), builder);
			if (scope != QueryScope::Subquery) {
				encodeSelectFunctions(query, builder);
			}
			encodeSorting(query.GetSortingEntries(), builder);
			encodeFilters(query, builder);
			if (scope != QueryScope::Subquery) {
				encodeMergedQueries(query, builder);
			}
			encodeAggregationFunctions(query, builder);
			break;
		}
		case QueryType::QueryUpdate: {
			builder.Put("namespace"sv, query.NsName());
			builder.Put("explain"sv, query.NeedExplain());
			builder.Put("type"sv, "update"sv);
			encodeFilters(query, builder);
			bool withDropEntries = false, withUpdateEntries = false;
			for (const UpdateEntry& updateEntry : query.UpdateFields()) {
				if (updateEntry.Mode() == FieldModeDrop) {
					withDropEntries = true;
				}
				if (updateEntry.Mode() == FieldModeSet || updateEntry.Mode() == FieldModeSetJson) {
					withUpdateEntries = true;
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
			builder.Put("namespace"sv, query.NsName());
			builder.Put("explain"sv, query.NeedExplain());
			builder.Put("type"sv, "delete"sv);
			encodeFilters(query, builder);
			break;
		}
		case QueryType::QueryTruncate: {
			builder.Put("namespace"sv, query.NsName());
			builder.Put("type"sv, "truncate"sv);
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
		node.Put("op"sv, dsl::get(dsl::kOpMap, it->operation));
		it->Visit(
			[&node](const AlwaysFalse&) {
				logFmt(LogTrace, "Not normalized query to dsl"sv);
				node.Put("always"sv, false);
			},
			[&node](const AlwaysTrue&) {
				logFmt(LogTrace, "Not normalized query to dsl"sv);
				node.Put("always"sv, true);
			},
			[&node, &parentQuery](const SubQueryEntry& sqe) {
				node.Put("cond"sv, dsl::get(dsl::kCondMap, CondType(sqe.Condition())));
				{
					auto subquery = node.Object("subquery"sv);
					dsl::toDsl(parentQuery.GetSubQuery(sqe.QueryIndex()), dsl::QueryScope::Subquery, subquery);
				}
				dsl::putValues(node, sqe.Values());
			},
			[&node, &parentQuery](const SubQueryFieldEntry& sqe) {
				node.Put("cond"sv, dsl::get(dsl::kCondMap, CondType(sqe.Condition())));
				node.Put("field"sv, sqe.FieldName());
				auto subquery = node.Object("subquery"sv);
				dsl::toDsl(parentQuery.GetSubQuery(sqe.QueryIndex()), dsl::QueryScope::Subquery, subquery);
			},
			[&it, &node, &parentQuery](const QueryEntriesBracket& bracket) {
				auto arrNode = node.Array("filters"sv);
				toDsl(it.cbegin(), it.cend(), parentQuery, arrNode);
				dsl::encodeEqualPositions(bracket.equalPositions, arrNode);
			},
			[&node](const QueryEntry& qe) { dsl::encodeFilter(qe, node); },
			[&node, &parentQuery](const JoinQueryEntry& jqe) {
				assertrx(jqe.joinIndex < parentQuery.GetJoinQueries().size());
				dsl::encodeSingleJoinQuery(parentQuery.GetJoinQueries()[jqe.joinIndex], node);
			},
			[&node](const BetweenFieldsQueryEntry& qe) {
				node.Put("cond"sv, dsl::get(dsl::kCondMap, CondType(qe.Condition())));
				node.Put("first_field"sv, qe.LeftFieldName());
				node.Put("second_field"sv, qe.RightFieldName());
			},
			[](const MultiDistinctQueryEntry&) {}, [&node](const KnnQueryEntry& qe) { qe.ToDsl(node); });
	}
}

}  // namespace reindexer
