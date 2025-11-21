#include "dslparser.h"
#include "core/cjson/jschemachecker.h"
#include "core/query/query.h"
#include "core/type_consts_helpers.h"
#include "gason/gason.h"
#include "tools/errors.h"
#include "tools/frozen_str_tools.h"
#include "tools/json2kv.h"
#include "tools/jsontools.h"
#include "tools/scope_guard.h"
#include "tools/stringstools.h"
#include "vendor/frozen/unordered_map.h"

namespace reindexer {
using namespace gason;
using namespace std::string_view_literals;
namespace dsl {
using gason::JsonValue;

enum class [[nodiscard]] Root {
	Namespace,
	Limit,
	Offset,
	Filters,
	Sort,
	Merged,
	SelectFilter,
	SelectFunctions,
	ReqTotal,
	Aggregations,
	Explain,
	EqualPositions,
	WithRank,
	StrictMode,
	QueryType,
	DropFields,
	UpdateFields,
	Local,
};

enum class [[nodiscard]] Sort { Desc, Field, Values };
enum class [[nodiscard]] JoinRoot { Type, On, Namespace, Filters, Sort, Limit, Offset, SelectFilter };
enum class [[nodiscard]] JoinEntry { LeftField, RightField, Cond, Op };
enum class [[nodiscard]] Aggregation { Fields, Type, Sort, Limit, Offset };
enum class [[nodiscard]] EqualPosition { Positions };
enum class [[nodiscard]] UpdateField { Name, Type, Values, IsArray };
enum class [[nodiscard]] UpdateFieldType { Object, Expression, Value };

template <typename T, std::size_t N>
constexpr auto MakeFastStrMap(const std::pair<std::string_view, T> (&items)[N]) {
	return frozen::make_unordered_map<std::string_view, T>(items, frozen::nocase_hash_str{}, frozen::nocase_equal_str{});
}

// additional for parse root DSL fields
constexpr static auto kRootMap = MakeFastStrMap<Root>({
	{"namespace", Root::Namespace},
	{"limit", Root::Limit},
	{"offset", Root::Offset},
	{"filters", Root::Filters},
	{"sort", Root::Sort},
	{"merge_queries", Root::Merged},
	{"select_filter", Root::SelectFilter},
	{"select_functions", Root::SelectFunctions},
	{"req_total", Root::ReqTotal},
	{"aggregations", Root::Aggregations},
	{"explain", Root::Explain},
	{"local", Root::Local},
	{"equal_positions", Root::EqualPositions},
	{"select_with_rank", Root::WithRank},
	{"strict_mode", Root::StrictMode},
	{"type", Root::QueryType},
	{"drop_fields", Root::DropFields},
	{"update_fields", Root::UpdateFields},
});

// additional for parse field 'sort'
constexpr static auto kSortMap = MakeFastStrMap<Sort>({{"desc", Sort::Desc}, {"field", Sort::Field}, {"values", Sort::Values}});

// additional for parse field 'joined'
constexpr static auto joins_map = MakeFastStrMap<JoinRoot>({{"type", JoinRoot::Type},
															{"namespace", JoinRoot::Namespace},
															{"filters", JoinRoot::Filters},
															{"sort", JoinRoot::Sort},
															{"limit", JoinRoot::Limit},
															{"offset", JoinRoot::Offset},
															{"on", JoinRoot::On},
															{"select_filter", JoinRoot::SelectFilter}});

constexpr static auto joined_entry_map = MakeFastStrMap<JoinEntry>(
	{{"left_field", JoinEntry::LeftField}, {"right_field", JoinEntry::RightField}, {"cond", JoinEntry::Cond}, {"op", JoinEntry::Op}});

constexpr static auto join_types = MakeFastStrMap<JoinType>({{"inner", InnerJoin}, {"left", LeftJoin}, {"orinner", OrInnerJoin}});

// additional for 'filter::cond' field
constexpr static auto cond_map = MakeFastStrMap<CondType>({
	{"any", CondAny},
	{"eq", CondEq},
	{"lt", CondLt},
	{"le", CondLe},
	{"gt", CondGt},
	{"ge", CondGe},
	{"range", CondRange},
	{"set", CondSet},
	{"allset", CondAllSet},
	{"empty", CondEmpty},
	{"match", CondEq},
	{"like", CondLike},
	{"dwithin", CondDWithin},
	{"knn", CondKnn},
});

constexpr static auto kOpMap = MakeFastStrMap<OpType>({{"or", OpOr}, {"and", OpAnd}, {"not", OpNot}});

// additional for 'Root::ReqTotal' field

constexpr static auto kReqTotalValues =
	MakeFastStrMap<CalcTotalMode>({{"disabled", ModeNoTotal}, {"enabled", ModeAccurateTotal}, {"cached", ModeCachedTotal}});

// additional for 'Root::Aggregations' field
constexpr static auto kAggregationMap = MakeFastStrMap<Aggregation>({{"fields", Aggregation::Fields},
																	 {"type", Aggregation::Type},
																	 {"sort", Aggregation::Sort},
																	 {"limit", Aggregation::Limit},
																	 {"offset", Aggregation::Offset}});
constexpr static auto kAggregationTypes = MakeFastStrMap<AggType>({
	{"sum", AggSum},
	{"avg", AggAvg},
	{"max", AggMax},
	{"min", AggMin},
	{"facet", AggFacet},
	{"distinct", AggDistinct},
	{"count", AggCount},
	{"count_cached", AggCountCached},
});

// additional for parse field 'equation_positions'
constexpr static auto kEquationPositionMap = MakeFastStrMap<EqualPosition>({{"positions", EqualPosition::Positions}});

// additional for 'Root::QueryType' field
constexpr static auto kQueryTypes = MakeFastStrMap<QueryType>({
	{"select", QuerySelect},
	{"update", QueryUpdate},
	{"delete", QueryDelete},
	{"truncate", QueryTruncate},
});

// additional for 'Root::UpdateField' field
constexpr static auto kUpdateFieldMap = MakeFastStrMap<UpdateField>({
	{"name", UpdateField::Name},
	{"type", UpdateField::Type},
	{"values", UpdateField::Values},
	{"is_array", UpdateField::IsArray},
});

// additional for 'Root::UpdateFieldType' field
constexpr static auto kUpdateFieldTypeMap = MakeFastStrMap<UpdateFieldType>({
	{"object", UpdateFieldType::Object},
	{"expression", UpdateFieldType::Expression},
	{"value", UpdateFieldType::Value},
});

template <typename... JsonTags>
void checkJsonValueType(const JsonValue& val, std::string_view name, JsonTags... possibleTags) {
	if (((val.getTag() != possibleTags) && ...)) {
		throw Error(errParseJson, "Wrong type of field '{}'", name);
	}
}

template <typename T, size_t N>
T get(const frozen::unordered_map<std::string_view, T, N, frozen::nocase_hash_str, frozen::nocase_equal_str>& m, std::string_view name,
	  std::string_view mapName) {
	auto it = m.find(name);
	if (it == m.end()) {
		throw Error(errParseDSL, "Element [{}] not allowed in object of type [{}]", name, mapName);
	}
	return it->second;
}

template <typename Arr>
void parseStringArray(const JsonValue& stringArray, Arr& array) {
	for (const auto& element : stringArray) {
		auto& value = element.value;
		checkJsonValueType(value, "string array item"sv, JsonTag::STRING);
		array.emplace_back(value.toString());
	}
}

template <typename Array>
void parseValues(const JsonValue& values, Array& kvs, std::string_view fieldName) {
	if (values.getTag() == JsonTag::ARRAY) {
		uint32_t objectsCount = 0;
		for (const auto& elem : values) {
			Variant kv;
			if (elem.value.getTag() == JsonTag::OBJECT) {
				kv = Variant(stringifyJson(elem));
				++objectsCount;
			} else if (elem.value.getTag() != JsonTag::JSON_NULL) {
				kv = jsonValue2Variant(elem.value, KeyValueType::Undefined{}, fieldName, nullptr, ConvertToString_False, ConvertNull_False);
				std::ignore = kv.EnsureHold();
			}
			kvs.emplace_back(std::move(kv));
		}

		if ((0 < objectsCount) && (objectsCount < kvs.size())) {
			throw Error(errParseJson, "Array with objects must be homogeneous");
		}
	} else if (values.getTag() != JsonTag::JSON_NULL) {
		Variant kv(jsonValue2Variant(values, KeyValueType::Undefined{}, fieldName, nullptr, ConvertToString_False, ConvertNull_False));
		std::ignore = kv.EnsureHold();
		kvs.emplace_back(std::move(kv));
	}
}

void parse(const JsonValue& root, Query& q);

static void parseSortEntry(const JsonValue& entry, SortingEntries& sortingEntries, std::vector<Variant>& forcedSortOrder) {
	checkJsonValueType(entry, "Sort"sv, JsonTag::OBJECT);
	SortingEntry sortingEntry;
	for (const auto& subelement : entry) {
		auto& v = subelement.value;
		std::string_view name = subelement.key;
		switch (get<Sort>(kSortMap, name, "sort"sv)) {
			case Sort::Desc:
				if ((v.getTag() != JsonTag::JTRUE) && (v.getTag() != JsonTag::JFALSE)) {
					throw Error(errParseJson, "Wrong type of field '{}'", name);
				}
				sortingEntry.desc = Desc(v.getTag() == JsonTag::JTRUE);
				break;

			case Sort::Field:
				checkJsonValueType(v, name, JsonTag::STRING);
				sortingEntry.expression.assign(std::string(v.toString()));
				break;

			case Sort::Values:
				if (!sortingEntries.empty()) {
					throw Error(errParseJson, "Forced sort order is allowed for the first sorting entry only");
				}
				parseValues(v, forcedSortOrder, name);
				break;
		}
	}
	sortingEntries.push_back(std::move(sortingEntry));
}

static void parseSort(const JsonValue& v, SortingEntries& sortingEntries, std::vector<Variant>& forcedSortOrder) {
	if (v.getTag() == JsonTag::ARRAY) {
		for (auto entry : v) {
			parseSort(entry.value, sortingEntries, forcedSortOrder);
		}
	} else if (v.getTag() == JsonTag::OBJECT) {
		parseSortEntry(v, sortingEntries, forcedSortOrder);
	} else {
		throw Error(errConflict, "Wrong type of field 'Sort'");
	}
}

static void parseSort(const JsonValue& v, Query& query) {
	SortingEntries sortingEntries;
	std::vector<Variant> forcedSortOrder;
	parseSort(v, sortingEntries, forcedSortOrder);
	for (size_t i = 0, s = sortingEntries.size(); i < s; ++i) {
		auto& sortingEntry = sortingEntries[i];
		if (i == 0) {
			// NOLINTNEXTLINE (bugprone-use-after-move)
			query.Sort(std::move(sortingEntry.expression), *sortingEntry.desc, std::move(forcedSortOrder));
		} else {
			query.Sort(std::move(sortingEntry.expression), *sortingEntry.desc);
		}
	}
}

static void parseSingleJoinQuery(const JsonValue& join, Query& query, const JsonNode& parent);
static void parseEqualPositions(const JsonValue& dsl, Query& query);

VariantArray getValues(const JsonNode& dsl) {
	const auto valuesJson = dsl.findCaseInsensitive("value"sv);
	if (valuesJson.empty()) {
		return {};
	}
	VariantArray values;
	parseValues(valuesJson.value, values, "values"sv);
	return values;
}

static OpType parseOptionalOperation(const JsonNode& parser) {
	if (const auto opJson = parser.findCaseInsensitive("op"sv); !opJson.empty()) {
		return get<OpType>(kOpMap, opJson.As<std::string_view>(), "operation enum");
	} else {
		return OpAnd;
	}
}

static KnnSearchParams parseKnnParams(const JsonNode& json) {
	const auto paramsJson = json.findCaseInsensitive("params"sv);
	if (paramsJson.empty()) {
		throw Error{errParseDSL, "Wrong DSL format: KNN query should contain 'params'"};
	}
	const auto kJson = paramsJson.findCaseInsensitive(KnnSearchParams::kKName);
	const auto radiusJson = paramsJson.findCaseInsensitive(KnnSearchParams::kRadiusName);
	std::optional<size_t> k;
	std::optional<float> radius;
	if (!kJson.empty()) {
		k = kJson.As<size_t>(CheckUnsigned_True, 0, 1);
	}
	if (!radiusJson.empty()) {
		radius = radiusJson.As<float>();
	}

	if (!k && !radius) {
		throw Error{errParseDSL, "Wrong DSL format: KNN query should contain one of 'params.{}' or 'params.{}'", KnnSearchParams::kKName,
					KnnSearchParams::kRadiusName};
	}

	if (const auto efJson = paramsJson.findCaseInsensitive(KnnSearchParams::kEfName); !efJson.empty()) {
		if (const auto nprobeJson = paramsJson.findCaseInsensitive(KnnSearchParams::kNProbeName); !nprobeJson.empty()) {
			throw Error{errParseDSL, "Wrong DSL format: KNN query cannot contain both of 'params.{}' and 'params.{}",
						KnnSearchParams::kEfName, KnnSearchParams::kNProbeName};
		}
		return HnswSearchParams{}.K(k).Radius(radius).Ef(efJson.As<size_t>(CheckUnsigned_True, 1, k ? *k : 1));
	} else if (const auto nprobeJson = paramsJson.findCaseInsensitive(KnnSearchParams::kNProbeName); !nprobeJson.empty()) {
		if (nprobeJson.value.isNegative()) {
			throw Error{errParseDSL, "Wrong DSL format: KNN query parameter 'params.{}' cannot be negative", KnnSearchParams::kNProbeName};
		}
		return IvfSearchParams{}.K(k).Radius(radius).NProbe(nprobeJson.As<size_t>(CheckUnsigned_True, 0, 1));
	} else {
		return KnnSearchParamsBase{}.K(k).Radius(radius);
	}
}

void addWhereKNN(const JsonNode& fieldNode, const JsonNode& filter, Query& q) {
	auto knnParams = parseKnnParams(filter);

	const auto valuesDsl = filter.findCaseInsensitive("value"sv);
	if (valuesDsl.empty()) {
		throw Error{errParseDSL, "Wrong DSL format: Knn condition without value"};
	}

	if (valuesDsl.value.getTag() != JsonTag::ARRAY) {
		auto value = valuesDsl.As<std::string>();
		if (value.empty()) {
			throw Error{errParseDSL, "Wrong DSL format: Knn condition with not array or string value"};
		}

		q.WhereKNN(fieldNode.As<std::string>(), std::move(value), std::move(knnParams));
	} else {
		thread_local static std::vector<float> values;
		values.resize(0);
		values.reserve(kMaxThreadLocalJSONVector);
		auto guard = MakeScopeGuard([]() noexcept {
			if (values.capacity() > kMaxThreadLocalJSONVector) {
				values = std::vector<float>();
			}
		});
		for (const auto vDsl : valuesDsl) {
			values.push_back(vDsl.As<double>());
		}

		auto vector = ConstFloatVector{std::span<float>{values}};
		if (vector.Span().empty()) {
			throw Error{errParseDSL, "Wrong DSL format: Knn condition with empty vector"};
		}

		q.WhereKNN(fieldNode.As<std::string_view>(), std::move(vector), knnParams);
	}
}

static void parseFilter(const JsonNode& filter, Query& q) {
	checkJsonValueType(filter.value, "filter"sv, JsonTag::OBJECT);
	if (const auto ep = filter.findCaseInsensitive("equal_positions"sv); !ep.empty()) {
		checkJsonValueType(ep.value, "equal_positions"sv, JsonTag::ARRAY);
		parseEqualPositions(ep.value, q);
		return;
	} else if (const auto joinQuery = filter.findCaseInsensitive("join_query"sv); !joinQuery.empty()) {
		checkJsonValueType(joinQuery.value, "join_query"sv, JsonTag::OBJECT);
		parseSingleJoinQuery(joinQuery.value, q, filter);
		return;
	}
	const OpType op = parseOptionalOperation(filter);
	if (const auto always = filter.findCaseInsensitive("always"sv); !always.empty()) {
		if (always.As<bool>()) {
			q.AppendQueryEntry<AlwaysTrue>(op);
		} else {
			q.AppendQueryEntry<AlwaysFalse>(op);
		}
		return;
	} else if (const auto bracket = filter.findCaseInsensitive("filters"sv); !bracket.empty()) {
		checkJsonValueType(bracket.value, bracket.key, JsonTag::ARRAY);
		q.NextOp(op).OpenBracket();
		for (const auto& f : bracket) {
			parseFilter(f, q);
		}
		q.CloseBracket();
		return;
	}

	std::optional<CondType> condition;
	if (const auto condNode = filter.findCaseInsensitive("cond"sv); !condNode.empty()) {
		condition = get<CondType>(cond_map, condNode.As<std::string_view>(), "condition enum"sv);
	}
	auto getCondition = [&condition] {
		// Filter may contain an empty object, which is fine
		if (!condition.has_value()) {
			throw Error{errParseDSL, "Condition is not set for non-empty filter"};
		}
		return *condition;
	};
	q.NextOp(op);
	if (const auto firstField = filter.findCaseInsensitive("first_field"sv); !firstField.empty()) {
		q.WhereBetweenFields(firstField.As<std::string_view>(), getCondition(),
							 filter.findCaseInsensitive("second_field"sv).As<std::string_view>());
	} else if (const auto subQueryJson = filter.findCaseInsensitive("subquery"sv); !subQueryJson.empty()) {
		Query subQuery;
		parse(subQueryJson.value, subQuery);
		if (const auto field = filter.findCaseInsensitive("field"sv); !field.empty()) {
			q.Where(field.As<std::string_view>(), getCondition(), std::move(subQuery));
			if (const auto valuesJson = filter.findCaseInsensitive("value"sv); !valuesJson.empty()) {
				throw Error{errParseDSL, "Wrong DSL format: 'value', 'subquery' and 'field' fields in one filter"};
			}
		} else {
			q.Where(std::move(subQuery), getCondition(), getValues(filter));
		}
	} else if (const auto fieldNode = filter.findCaseInsensitive("field"sv); !fieldNode.empty()) {
		if (auto cond = getCondition(); cond == CondKnn) {
			addWhereKNN(fieldNode, filter, q);
		} else {
			q.Where(fieldNode.As<std::string_view>(), cond, getValues(filter));
		}
	} else if (condition.has_value()) {
		throw Error{errParseDSL, "Condition is set, but appropriate field/subquery was not found in filter"};
	}
}

static void parseJoinedEntries(const JsonValue& joinEntries, JoinedQuery& qjoin) {
	checkJsonValueType(joinEntries, "Joined"sv, JsonTag::ARRAY);
	for (const auto& element : joinEntries) {
		auto& joinEntry = element.value;
		checkJsonValueType(joinEntry, "Joined"sv, JsonTag::OBJECT);

		OpType op{OpAnd};
		CondType cond{};
		std::string leftField, rightField;
		for (const auto& subelement : joinEntry) {
			auto& value = subelement.value;
			std::string_view name = subelement.key;
			switch (get<JoinEntry>(joined_entry_map, name, "join_query.on"sv)) {
				case JoinEntry::LeftField:
					checkJsonValueType(value, name, JsonTag::STRING);
					leftField = std::string(value.toString());
					break;
				case JoinEntry::RightField:
					checkJsonValueType(value, name, JsonTag::STRING);
					rightField = std::string(value.toString());
					break;
				case JoinEntry::Cond:
					checkJsonValueType(value, name, JsonTag::STRING);
					cond = get<CondType>(cond_map, value.toString(), "condition enum"sv);
					break;
				case JoinEntry::Op:
					checkJsonValueType(value, name, JsonTag::STRING);
					op = get<OpType>(kOpMap, value.toString(), "operation enum"sv);
					break;
			}
		}
		qjoin.joinEntries_.emplace_back(op, cond, std::move(leftField), std::move(rightField));
	}
}

static void parseSingleJoinQuery(const JsonValue& join, Query& query, const JsonNode& parent) {
	JoinedQuery qjoin;
	std::vector<std::pair<size_t, EqualPosition_t>> equalPositions;
	for (const auto& subelement : join) {
		auto& value = subelement.value;
		std::string_view name = subelement.key;
		switch (get<JoinRoot>(joins_map, name, "join_query"sv)) {
			case JoinRoot::Type:
				checkJsonValueType(value, name, JsonTag::STRING);
				qjoin.joinType = get<JoinType>(join_types, value.toString(), "join_types enum"sv);
				break;
			case JoinRoot::Namespace:
				checkJsonValueType(value, name, JsonTag::STRING);
				qjoin.SetNsName(value.toString());
				break;
			case JoinRoot::Filters:
				checkJsonValueType(value, name, JsonTag::ARRAY);
				for (const auto& filter : value) {
					parseFilter(filter, qjoin);
				}
				break;
			case JoinRoot::Sort:
				parseSort(value, qjoin);
				break;
			case JoinRoot::Limit:
				checkJsonValueType(value, name, JsonTag::NUMBER, JsonTag::DOUBLE);
				qjoin.Limit(static_cast<unsigned>(value.toNumber()));
				break;
			case JoinRoot::Offset:
				checkJsonValueType(value, name, JsonTag::NUMBER, JsonTag::DOUBLE);
				qjoin.Offset(static_cast<unsigned>(value.toNumber()));
				break;
			case JoinRoot::On:
				parseJoinedEntries(value, qjoin);
				break;
			case JoinRoot::SelectFilter: {
				checkJsonValueType(value, name, JsonTag::ARRAY);
				if (!qjoin.CanAddSelectFilter()) {
					throw Error(errConflict, kAggregationWithSelectFieldsMsgError);
				}
				std::vector<std::string> selectFilters;
				parseStringArray(value, selectFilters);
				qjoin.Select(std::move(selectFilters));
				break;
			}
		}
	}
	OpType op = parseOptionalOperation(parent);
	if (qjoin.joinType == JoinType::LeftJoin) {
		if (op != OpAnd) {
			throw Error(errParseJson, "Operation {} is not allowed with LeftJoin", OpTypeToStr(op));
		}
		query.AddJoinQuery(std::move(qjoin));
	} else {
		if (qjoin.joinType == JoinType::OrInnerJoin) {
			if (op == OpNot) {
				throw Error(errParseJson, "Operation NOT is not allowed with OrInnerJoin");
			}
			op = OpOr;
		}
		query.AddJoinQuery(std::move(qjoin));
		query.AppendQueryEntry<JoinQueryEntry>(op, query.GetJoinQueries().size() - 1);
	}
}

static void parseMergeQueries(const JsonValue& mergeQueries, Query& query) {
	for (const auto& element : mergeQueries) {
		auto& merged = element.value;
		checkJsonValueType(merged, "Merged"sv, JsonTag::OBJECT);
		JoinedQuery qmerged;
		parse(merged, qmerged);
		qmerged.joinType = Merge;
		query.Merge(std::move(qmerged));
	}
}

static void parseAggregation(const JsonValue& aggregation, Query& query) {
	checkJsonValueType(aggregation, "Aggregation"sv, JsonTag::OBJECT);
	h_vector<std::string, 1> fields;
	AggType type = AggUnknown;
	SortingEntries sortingEntries;
	unsigned limit{QueryEntry::kDefaultLimit};
	unsigned offset{QueryEntry::kDefaultOffset};
	for (const auto& element : aggregation) {
		auto& value = element.value;
		std::string_view name = element.key;
		switch (get<Aggregation>(kAggregationMap, name, "aggregations"sv)) {
			case Aggregation::Fields:
				checkJsonValueType(value, name, JsonTag::ARRAY);
				for (const auto& subElem : value) {
					if (subElem.value.getTag() != JsonTag::STRING) {
						throw Error(errParseJson, "Expected string in array 'fields'");
					}
					fields.emplace_back(subElem.value.toString());
				}
				break;
			case Aggregation::Type:
				checkJsonValueType(value, name, JsonTag::STRING);
				type = get<AggType>(kAggregationTypes, value.toString(), "aggregation type enum"sv);
				if (!query.CanAddAggregation(type)) {
					throw Error(errConflict, kAggregationWithSelectFieldsMsgError);
				}
				break;
			case Aggregation::Sort: {
				std::vector<Variant> forcedSortOrder;
				parseSort(value, sortingEntries, forcedSortOrder);
				if (!forcedSortOrder.empty()) {
					throw Error(errConflict, "Fixed values not available in aggregation sort");
				}
			} break;
			case Aggregation::Limit:
				checkJsonValueType(value, name, JsonTag::NUMBER, JsonTag::DOUBLE);
				limit = value.toNumber();
				break;
			case Aggregation::Offset:
				checkJsonValueType(value, name, JsonTag::NUMBER, JsonTag::DOUBLE);
				offset = value.toNumber();
				break;
		}
	}
	query.aggregations_.emplace_back(type, std::move(fields), std::move(sortingEntries), limit, offset);
}

static void parseEqualPositions(const JsonValue& dsl, Query& query) {
	for (const auto& ar : dsl) {
		auto subArray = ar.value;
		checkJsonValueType(subArray, ar.key, JsonTag::OBJECT);
		for (const auto& element : subArray) {
			auto& value = element.value;
			std::string_view name = element.key;
			switch (get<EqualPosition>(kEquationPositionMap, name, "equal_positions"sv)) {
				case EqualPosition::Positions: {
					EqualPosition_t ep;
					for (const auto& f : value) {
						checkJsonValueType(f.value, f.key, JsonTag::STRING);
						ep.emplace_back(f.value.toString());
					}
					query.EqualPositions(std::move(ep));
				}
			}
		}
	}
}

static void parseUpdateFields(const JsonValue& updateFields, Query& query) {
	for (const auto& item : updateFields) {
		auto& field = item.value;
		checkJsonValueType(field, item.key, JsonTag::OBJECT);
		std::string fieldName;
		bool isObject = false, isExpression = false;
		VariantArray values;
		for (const auto& v : field) {
			auto& value = v.value;
			std::string_view name = v.key;
			switch (get<UpdateField>(kUpdateFieldMap, name, "update_fields"sv)) {
				case UpdateField::Name:
					checkJsonValueType(value, name, JsonTag::STRING);
					fieldName.assign(value.sval.data(), value.sval.size());
					break;
				case UpdateField::Type: {
					checkJsonValueType(value, name, JsonTag::STRING);
					switch (get<UpdateFieldType>(kUpdateFieldTypeMap, value.toString(), "update_fields_type"sv)) {
						case UpdateFieldType::Object:
							isObject = true;
							break;
						case UpdateFieldType::Expression:
							isExpression = true;
							break;
						case UpdateFieldType::Value:
							isObject = isExpression = false;
							break;
					}
					break;
				}
				case UpdateField::IsArray:
					checkJsonValueType(value, name, JsonTag::JTRUE, JsonTag::JFALSE);
					// NOLINTNEXTLINE (bugprone-unused-return-value)
					values.MarkArray(value.getTag() == JsonTag::JTRUE);
					break;
				case UpdateField::Values:
					checkJsonValueType(value, name, JsonTag::ARRAY);
					parseValues(value, values, name);
					break;
			}
		}
		if (isExpression && (values.size() != 1 || !values.front().Type().template Is<KeyValueType::String>())) {
			throw Error(errParseDSL, R"(The array "values" must contain only a string type value for the type "expression")");
		}

		if (isObject) {
			query.SetObject(fieldName, std::move(values));
		} else {
			query.Set(fieldName, std::move(values), isExpression);
		}
	}
}

void parse(const JsonValue& root, Query& q) {
	if (root.getTag() != JsonTag::OBJECT) {
		throw Error(errParseJson, "Json is malformed: {}", root.getTag());
	}
	for (const auto& elem : root) {
		auto& v = elem.value;
		auto name = elem.key;
		switch (get<Root>(kRootMap, name, "root"sv)) {
			case Root::Namespace:
				checkJsonValueType(v, name, JsonTag::STRING);
				q.SetNsName(v.toString());
				break;

			case Root::Limit:
				checkJsonValueType(v, name, JsonTag::NUMBER, JsonTag::DOUBLE);
				q.Limit(static_cast<unsigned>(v.toNumber()));
				break;

			case Root::Offset:
				checkJsonValueType(v, name, JsonTag::NUMBER, JsonTag::DOUBLE);
				q.Offset(static_cast<unsigned>(v.toNumber()));
				break;

			case Root::Filters:
				checkJsonValueType(v, name, JsonTag::ARRAY);
				for (const auto& filter : v) {
					parseFilter(filter, q);
				}
				break;

			case Root::Sort:
				parseSort(v, q);
				break;
			case Root::Merged:
				checkJsonValueType(v, name, JsonTag::ARRAY);
				parseMergeQueries(v, q);
				break;
			case Root::SelectFilter: {
				if (!q.CanAddSelectFilter()) {
					throw Error(errConflict, kAggregationWithSelectFieldsMsgError);
				}
				checkJsonValueType(v, name, JsonTag::ARRAY);
				std::vector<std::string> selectFilters;
				parseStringArray(v, selectFilters);
				q.Select(std::move(selectFilters));
				break;
			}
			case Root::SelectFunctions:
				checkJsonValueType(v, name, JsonTag::ARRAY);
				parseStringArray(v, q.selectFunctions_);
				break;
			case Root::ReqTotal:
				checkJsonValueType(v, name, JsonTag::STRING);
				q.CalcTotal(get<CalcTotalMode>(kReqTotalValues, v.toString(), "req_total enum"sv));
				break;
			case Root::Aggregations:
				checkJsonValueType(v, name, JsonTag::ARRAY);
				for (const auto& aggregation : v) {
					parseAggregation(aggregation.value, q);
				}
				break;
			case Root::Explain:
				checkJsonValueType(v, name, JsonTag::JFALSE, JsonTag::JTRUE);
				q.Explain(v.getTag() == JsonTag::JTRUE);
				break;
			case Root::Local:
				checkJsonValueType(v, name, JsonTag::JFALSE, JsonTag::JTRUE);
				q.Local(v.getTag() == JsonTag::JTRUE);
				break;
			case Root::WithRank:
				checkJsonValueType(v, name, JsonTag::JFALSE, JsonTag::JTRUE);
				if (v.getTag() == JsonTag::JTRUE) {
					q.WithRank();
				}
				break;
			case Root::StrictMode:
				checkJsonValueType(v, name, JsonTag::STRING);
				q.Strict(strictModeFromString(std::string(v.toString())));
				if (q.GetStrictMode() == StrictModeNotSet) {
					throw Error(errParseDSL, "Unexpected strict mode value: {}", v.toString());
				}
				break;
			case Root::EqualPositions:
				throw Error(errParseDSL, "Unsupported old DSL format. Equal positions should be in filters.");
			case Root::QueryType:
				checkJsonValueType(v, name, JsonTag::STRING);
				q.type_ = get<QueryType>(kQueryTypes, v.toString(), "query_type"sv);
				break;
			case Root::DropFields:
				checkJsonValueType(v, name, JsonTag::ARRAY);
				for (const auto& element : v) {
					auto& value = element.value;
					checkJsonValueType(value, "string array item"sv, JsonTag::STRING);
					q.Drop(std::string(value.toString()));
				}
				break;
			case Root::UpdateFields:
				checkJsonValueType(v, name, JsonTag::ARRAY);
				parseUpdateFields(v, q);
				break;
		}
	}
}

#include "query.json.h"

void Parse(std::string_view str, Query& q) {
	static JsonSchemaChecker schemaChecker(kQueryJson, "query");
	try {
		gason::JsonParser parser;
		auto root = parser.Parse(str);
		Error err = schemaChecker.Check(root);
		if (!err.ok()) {
			throw err;
		}
		dsl::parse(root.value, q);
	} catch (const gason::Exception& ex) {
		throw Error(errParseJson, "Query: {}", ex.what());
	} catch (const Error& err) {
		throw err;
	} catch (const std::exception& ex) {
		throw Error(errParseJson, "Exception: {}", ex.what());
	} catch (...) {
		throw Error(errParseJson, "Unknown Exception");
	}
}

}  // namespace dsl
}  // namespace reindexer
