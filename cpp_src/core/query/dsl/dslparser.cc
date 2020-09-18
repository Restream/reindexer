#include "dslparser.h"
#include "core/cjson/jschemachecker.h"
#include "core/cjson/jsonbuilder.h"
#include "core/query/query.h"
#include "estl/fast_hash_map.h"
#include "gason/gason.h"
#include "tools/errors.h"
#include "tools/json2kv.h"
#include "tools/jsontools.h"
#include "tools/stringstools.h"

namespace reindexer {
using namespace gason;
namespace dsl {
using gason::JsonValue;

enum class Root {
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
};

enum class Sort { Desc, Field, Values };
enum class JoinRoot { Type, On, Namespace, Filters, Sort, Limit, Offset };
enum class JoinEntry { LeftField, RightField, Cond, Op };
enum class Filter { Cond, Op, Field, Value, Filters, JoinQuery };
enum class Aggregation { Fields, Type, Sort, Limit, Offset };
enum class EqualPosition { Positions };
enum class UpdateField { Name, Type, Values };
enum class UpdateFieldType { Object, Expression, Value };

// additional for parse root DSL fields
template <typename T>
using fast_str_map = fast_hash_map<string, T, nocase_hash_str, nocase_equal_str>;

static const fast_str_map<Root> root_map = {
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
	{"equal_positions", Root::EqualPositions},
	{"select_with_rank", Root::WithRank},
	{"strict_mode", Root::StrictMode},
	{"type", Root::QueryType},
	{"drop_fields", Root::DropFields},
	{"update_fields", Root::UpdateFields},
};

// additional for parse field 'sort'

static const fast_str_map<Sort> sort_map = {{"desc", Sort::Desc}, {"field", Sort::Field}, {"values", Sort::Values}};

// additional for parse field 'joined'

static const fast_str_map<JoinRoot> joins_map = {
	{"type", JoinRoot::Type}, {"namespace", JoinRoot::Namespace}, {"filters", JoinRoot::Filters},
	{"sort", JoinRoot::Sort}, {"limit", JoinRoot::Limit},		  {"offset", JoinRoot::Offset},
	{"on", JoinRoot::On}};

static const fast_str_map<JoinEntry> joined_entry_map = {
	{"left_field", JoinEntry::LeftField}, {"right_field", JoinEntry::RightField}, {"cond", JoinEntry::Cond}, {"op", JoinEntry::Op}};

static const fast_str_map<JoinType> join_types = {{"inner", InnerJoin}, {"left", LeftJoin}, {"orinner", OrInnerJoin}};

// additionalfor parse field 'filters'

static const fast_str_map<Filter> filter_map = {{"cond", Filter::Cond},	  {"op", Filter::Op},			{"field", Filter::Field},
												{"value", Filter::Value}, {"filters", Filter::Filters}, {"join_query", Filter::JoinQuery}};

// additional for 'filter::cond' field

static const fast_str_map<CondType> cond_map = {
	{"any", CondAny},	  {"eq", CondEq},	{"lt", CondLt},			{"le", CondLe},		  {"gt", CondGt},	 {"ge", CondGe},
	{"range", CondRange}, {"set", CondSet}, {"allset", CondAllSet}, {"empty", CondEmpty}, {"match", CondEq}, {"like", CondLike},
};

static const fast_str_map<OpType> op_map = {{"or", OpOr}, {"and", OpAnd}, {"not", OpNot}};

// additional for 'Root::ReqTotal' field

static const fast_str_map<CalcTotalMode> reqtotal_values = {
	{"disabled", ModeNoTotal}, {"enabled", ModeAccurateTotal}, {"cached", ModeCachedTotal}};

// additional for 'Root::Aggregations' field

static const fast_str_map<Aggregation> aggregation_map = {{"fields", Aggregation::Fields},
														  {"type", Aggregation::Type},
														  {"sort", Aggregation::Sort},
														  {"limit", Aggregation::Limit},
														  {"offset", Aggregation::Offset}};
static const fast_str_map<AggType> aggregation_types = {{"sum", AggSum}, {"avg", AggAvg},	  {"max", AggMax},
														{"min", AggMin}, {"facet", AggFacet}, {"distinct", AggDistinct}};

// additionalfor parse field 'equation_positions'
static const fast_str_map<EqualPosition> equationPosition_map = {{"positions", EqualPosition::Positions}};

// additional for 'Root::QueryType' field
static const fast_str_map<QueryType> query_types = {
	{"select", QuerySelect},
	{"update", QueryUpdate},
	{"delete", QueryDelete},
	{"truncate", QueryTruncate},
};

// additional for 'Root::UpdateField' field
static const fast_str_map<UpdateField> update_field_map = {
	{"name", UpdateField::Name},
	{"type", UpdateField::Type},
	{"values", UpdateField::Values},
};

// additional for 'Root::UpdateFieldType' field
static const fast_str_map<UpdateFieldType> update_field_type_map = {
	{"object", UpdateFieldType::Object},
	{"expression", UpdateFieldType::Expression},
	{"value", UpdateFieldType::Value},
};

bool checkTag(JsonValue& val, JsonTag tag) { return val.getTag() == tag; }

template <typename... Tags>
bool checkTag(JsonValue& val, JsonTag tag, Tags... tags) {
	return std::max(tag == val.getTag(), checkTag(val, tags...));
}

template <typename... JsonTags>
void checkJsonValueType(JsonValue& val, string_view name, JsonTags... possibleTags) {
	if (!checkTag(val, possibleTags...)) throw Error(errParseJson, "Wrong type of field '%s'", name);
}

template <typename T>
T get(fast_str_map<T> const& m, string_view name, string_view mapName) {
	auto it = m.find(name);
	if (it == m.end()) throw Error(errParseDSL, "Element [%s] not allowed in object of type [%s]", name, mapName);
	return it->second;
}

template <typename T, int holdSize>
void parseStringArray(JsonValue& stringArray, h_vector<T, holdSize>& array) {
	for (auto element : stringArray) {
		auto& value = element->value;
		checkJsonValueType(value, "string array item", JSON_STRING);
		array.push_back(string(value.toString()));
	}
}

template <typename Array>
void parseValues(JsonValue& values, Array& kvs) {
	if (values.getTag() == JSON_ARRAY) {
		for (auto elem : values) {
			Variant kv;
			if (elem->value.getTag() == JSON_OBJECT) {
                kv = Variant(stringifyJson(*elem));
			} else if (elem->value.getTag() != JSON_NULL) {
				kv = jsonValue2Variant(elem->value, KeyValueUndefined);
				kv.EnsureHold();
			}
			if (kvs.size() > 1 && kvs.back().Type() != kv.Type()) throw Error(errParseJson, "Array of filter values must be homogeneous.");
			kvs.push_back(std::move(kv));
		}
	} else if (values.getTag() != JSON_NULL) {
		Variant kv(jsonValue2Variant(values, KeyValueUndefined));
		kv.EnsureHold();
		kvs.push_back(std::move(kv));
	}
}

void parse(JsonValue& root, Query& q);

void parseSortEntry(JsonValue& entry, Query& q) {
	checkJsonValueType(entry, "Sort", JSON_OBJECT);
	SortingEntry sortingEntry;
	for (auto subelement : entry) {
		auto& v = subelement->value;
		string_view name = subelement->key;
		switch (get(sort_map, name, "sort"_sv)) {
			case Sort::Desc:
				if ((v.getTag() != JSON_TRUE) && (v.getTag() != JSON_FALSE)) throw Error(errParseJson, "Wrong type of field '%s'", name);
				sortingEntry.desc = (v.getTag() == JSON_TRUE);
				break;

			case Sort::Field:
				checkJsonValueType(v, name, JSON_STRING);
				sortingEntry.expression.assign(string(v.toString()));
				break;

			case Sort::Values:
				parseValues(v, q.forcedSortOrder_);
				break;
		}
	}
	if (!sortingEntry.expression.empty()) {
		q.sortingEntries_.push_back(std::move(sortingEntry));
	}
}

void parseSortEntry(JsonValue& entry, AggregateEntry& agg) {
	checkJsonValueType(entry, "Sort", JSON_OBJECT);
	SortingEntry sortingEntry;
	for (auto subelement : entry) {
		auto& v = subelement->value;
		string_view name = subelement->key;
		switch (get(sort_map, name, "sort"_sv)) {
			case Sort::Desc:
				if ((v.getTag() != JSON_TRUE) && (v.getTag() != JSON_FALSE)) throw Error(errParseJson, "Wrong type of field '%s'", name);
				sortingEntry.desc = (v.getTag() == JSON_TRUE);
				break;

			case Sort::Field:
				checkJsonValueType(v, name, JSON_STRING);
				sortingEntry.expression.assign(string(v.toString()));
				break;

			case Sort::Values:
				throw Error(errConflict, "Fixed values not available in aggregation sort");
		}
	}
	if (!sortingEntry.expression.empty()) {
		agg.sortingEntries_.push_back(std::move(sortingEntry));
	}
}
template <typename T>
void parseSort(JsonValue& v, T& q) {
	if (v.getTag() == JSON_ARRAY) {
		for (auto entry : v) parseSort(entry->value, q);
	} else if (v.getTag() == JSON_OBJECT) {
		parseSortEntry(v, q);
	} else {
		throw Error(errConflict, "Wrong type of field 'Sort'");
	}
}

void parseSingleJoinQuery(JsonValue& join, Query& query);

void parseFilter(JsonValue& filter, Query& q) {
	QueryEntry qe;
	OpType op = OpAnd;
	checkJsonValueType(filter, "filter", JSON_OBJECT);
	bool joinEntry = false;
	enum { ENTRY, BRACKET } entryOrBracket = ENTRY;
	int elemsParsed = 0;
	for (auto elem : filter) {
		auto& v = elem->value;
		auto name = elem->key;
		switch (get(filter_map, name, "filter"_sv)) {
			case Filter::Cond:
				checkJsonValueType(v, name, JSON_STRING);
				qe.condition = get(cond_map, v.toString(), "condition enum"_sv);
				break;

			case Filter::Op:
				checkJsonValueType(v, name, JSON_STRING);
				op = get(op_map, v.toString(), "operation enum"_sv);
				break;

			case Filter::Value:
				parseValues(v, qe.values);
				break;

			case Filter::JoinQuery:
				checkJsonValueType(v, name, JSON_OBJECT);
				parseSingleJoinQuery(v, q);
				joinEntry = true;
				break;

			case Filter::Field:
				checkJsonValueType(v, name, JSON_STRING);
				qe.index = string(v.toString());
				break;

			case Filter::Filters:
				checkJsonValueType(v, name, JSON_ARRAY);
				q.entries.OpenBracket(op);
				for (auto f : v) parseFilter(f->value, q);
				q.entries.CloseBracket();
				entryOrBracket = BRACKET;
				break;
		}
		++elemsParsed;
	}
	if (elemsParsed == 0) return;
	if (entryOrBracket == BRACKET) {
		q.entries.SetLastOperation(op);
		return;
	}
	switch (qe.condition) {
		case CondGe:
		case CondGt:
		case CondEq:
		case CondLt:
		case CondLe:
		case CondLike:
			if (qe.values.size() != 1) {
				throw Error(errLogic, "Condition %d must have exact 1 value, but %d values was provided", qe.condition, qe.values.size());
			}
			break;
		case CondRange:
			if (qe.values.size() != 2) {
				throw Error(errLogic, "Condition RANGE must have exact 2 values, but %d values was provided", qe.values.size());
			}
			break;
		case CondSet:
			if (qe.values.size() < 1) {
				throw Error(errLogic, "Condition SET must have at least 1 value, but %d values was provided", qe.values.size());
			}
			break;
		case CondAny:
		case CondAllSet:
			if (qe.values.size() != 0) {
				throw Error(errLogic, "Condition ANY must have 0 values, but %d values was provided", qe.values.size());
			}
			break;
		default:
			break;
	}

	if (joinEntry) {
		assert(q.joinQueries_.size() > 0);
		const auto& qjoin = q.joinQueries_.back();
		if (qjoin.joinType != JoinType::LeftJoin) {
			q.entries.Append((qjoin.joinType == JoinType::InnerJoin) ? OpAnd : OpOr, QueryEntry(q.joinQueries_.size() - 1));
		}
	} else {
		q.entries.Append(op, std::move(qe));
	}
}

void parseJoinedEntries(JsonValue& joinEntries, JoinedQuery& qjoin) {
	checkJsonValueType(joinEntries, "Joined", JSON_ARRAY);
	for (auto element : joinEntries) {
		auto& joinEntry = element->value;
		checkJsonValueType(joinEntry, "Joined", JSON_OBJECT);

		QueryJoinEntry qjoinEntry;
		for (auto subelement : joinEntry) {
			auto& value = subelement->value;
			string_view name = subelement->key;
			switch (get(joined_entry_map, name, "join_query.on"_sv)) {
				case JoinEntry::LeftField:
					checkJsonValueType(value, name, JSON_STRING);
					qjoinEntry.index_ = string(value.toString());
					break;
				case JoinEntry::RightField:
					checkJsonValueType(value, name, JSON_STRING);
					qjoinEntry.joinIndex_ = string(value.toString());
					break;
				case JoinEntry::Cond:
					checkJsonValueType(value, name, JSON_STRING);
					qjoinEntry.condition_ = get(cond_map, value.toString(), "condition enum"_sv);
					break;
				case JoinEntry::Op:
					checkJsonValueType(value, name, JSON_STRING);
					qjoinEntry.op_ = get(op_map, value.toString(), "operation enum"_sv);
					break;
			}
		}
		qjoin.joinEntries_.emplace_back(qjoinEntry);
	}
}

void parseSingleJoinQuery(JsonValue& join, Query& query) {
	JoinedQuery qjoin;
	for (auto subelement : join) {
		auto& value = subelement->value;
		string_view name = subelement->key;
		switch (get(joins_map, name, "join_query"_sv)) {
			case JoinRoot::Type:
				checkJsonValueType(value, name, JSON_STRING);
				qjoin.joinType = get(join_types, value.toString(), "join_types enum"_sv);
				break;
			case JoinRoot::Namespace:
				checkJsonValueType(value, name, JSON_STRING);
				qjoin._namespace = string(value.toString());
				break;
			case JoinRoot::Filters:
				checkJsonValueType(value, name, JSON_ARRAY);
				for (auto filter : value) parseFilter(filter->value, qjoin);
				break;
			case JoinRoot::Sort:
				parseSort(value, qjoin);
				break;
			case JoinRoot::Limit:
				checkJsonValueType(value, name, JSON_NUMBER, JSON_DOUBLE);
				qjoin.count = static_cast<unsigned>(value.toNumber());
				break;
			case JoinRoot::Offset:
				checkJsonValueType(value, name, JSON_NUMBER, JSON_DOUBLE);
				qjoin.start = static_cast<unsigned>(value.toNumber());
				break;
			case JoinRoot::On:
				parseJoinedEntries(value, qjoin);
				break;
		}
	}
	query.joinQueries_.emplace_back(std::move(qjoin));
}

void parseMergeQueries(JsonValue& mergeQueries, Query& query) {
	for (auto element : mergeQueries) {
		auto& merged = element->value;
		checkJsonValueType(merged, "Merged", JSON_OBJECT);
		JoinedQuery qmerged;
		parse(merged, qmerged);
		query.mergeQueries_.emplace_back(std::move(qmerged));
	}
}

void parseAggregation(JsonValue& aggregation, Query& query) {
	checkJsonValueType(aggregation, "Aggregation", JSON_OBJECT);
	AggregateEntry aggEntry;
	for (auto element : aggregation) {
		auto& value = element->value;
		string_view name = element->key;
		switch (get(aggregation_map, name, "aggregations"_sv)) {
			case Aggregation::Fields:
				checkJsonValueType(value, name, JSON_ARRAY);
				for (auto subElem : value) {
					if (subElem->value.getTag() != JSON_STRING) throw Error(errParseJson, "Expected string in array 'fields'");
					aggEntry.fields_.push_back(string(subElem->value.toString()));
				}
				break;
			case Aggregation::Type:
				checkJsonValueType(value, name, JSON_STRING);
				aggEntry.type_ = get(aggregation_types, value.toString(), "aggregation type enum"_sv);
				break;
			case Aggregation::Sort:
				parseSort(value, aggEntry);
				break;
			case Aggregation::Limit:
				checkJsonValueType(value, name, JSON_NUMBER, JSON_DOUBLE);
				aggEntry.limit_ = value.toNumber();
				break;
			case Aggregation::Offset:
				checkJsonValueType(value, name, JSON_NUMBER, JSON_DOUBLE);
				aggEntry.offset_ = value.toNumber();
				break;
		}
	}
	query.aggregations_.push_back(aggEntry);
}

void parseEqualPositions(JsonValue& egualPositions, Query& query) {
	for (auto ar : egualPositions) {
		auto subArray = ar->value;
		checkJsonValueType(subArray, ar->key, JSON_OBJECT);
		for (auto element : subArray) {
			auto& value = element->value;
			string_view name = element->key;
			switch (get(equationPosition_map, name, "equal_positions"_sv)) {
				case EqualPosition::Positions: {
					h_vector<string, 4> ep;
					for (auto f : value) {
						checkJsonValueType(f->value, f->key, JSON_STRING);
						ep.emplace_back(f->value.toString());
					}
					if (ep.size() < 2)
						throw Error(errLogic, "equal_position() is supposed to have at least 2 arguments. Arguments: [%s]",
									ep.size() == 1 ? ep[0] : "");
					query.equalPositions_.emplace(query.entries.DetermineEqualPositionIndexes(ep));
				}
			}
		}
	}
}

void parseUpdateFields(JsonValue& updateFields, Query& query) {
	for (auto item : updateFields) {
		auto& field = item->value;
		checkJsonValueType(field, item->key, JSON_OBJECT);
		string fieldName;
		bool isObject = false, isExpression = false;
		VariantArray values;
		for (auto v : field) {
			auto& value = v->value;
			string_view name = v->key;
			switch (get(update_field_map, name, "update_fields"_sv)) {
				case UpdateField::Name:
					checkJsonValueType(value, name, JSON_STRING);
					fieldName.assign(value.sval.data(), value.sval.size());
					break;
				case UpdateField::Type: {
					checkJsonValueType(value, name, JSON_STRING);
					switch (get(update_field_type_map, value.toString(), "update_fields_type"_sv)) {
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
				case UpdateField::Values:
					checkJsonValueType(value, name, JSON_ARRAY);
					parseValues(value, values);
					break;
			}
		}
		if (isObject) {
			query.SetObject(fieldName, values);
		} else {
			query.Set(fieldName, values, isExpression);
		}
	}
}

void parse(JsonValue& root, Query& q) {
	if (root.getTag() != JSON_OBJECT) {
		throw Error(errParseJson, "Json is malformed: %d", root.getTag());
	}

	for (auto elem : root) {
		auto& v = elem->value;
		auto name = elem->key;
		switch (get(root_map, name, "root"_sv)) {
			case Root::Namespace:
				checkJsonValueType(v, name, JSON_STRING);
				q._namespace = string(v.toString());
				break;

			case Root::Limit:
				checkJsonValueType(v, name, JSON_NUMBER, JSON_DOUBLE);
				q.count = static_cast<unsigned>(v.toNumber());
				break;

			case Root::Offset:
				checkJsonValueType(v, name, JSON_NUMBER, JSON_DOUBLE);
				q.start = static_cast<unsigned>(v.toNumber());
				break;

			case Root::Filters:
				checkJsonValueType(v, name, JSON_ARRAY);
				for (auto filter : v) parseFilter(filter->value, q);
				break;

			case Root::Sort:
				parseSort(v, q);
				break;
			case Root::Merged:
				checkJsonValueType(v, name, JSON_ARRAY);
				parseMergeQueries(v, q);
				break;
			case Root::SelectFilter:
				checkJsonValueType(v, name, JSON_ARRAY);
				parseStringArray(v, q.selectFilter_);
				break;
			case Root::SelectFunctions:
				checkJsonValueType(v, name, JSON_ARRAY);
				parseStringArray(v, q.selectFunctions_);
				break;
			case Root::ReqTotal:
				checkJsonValueType(v, name, JSON_STRING);
				q.calcTotal = get(reqtotal_values, v.toString(), "req_total enum"_sv);
				break;
			case Root::Aggregations:
				checkJsonValueType(v, name, JSON_ARRAY);
				for (auto aggregation : v) parseAggregation(aggregation->value, q);
				break;
			case Root::Explain:
				checkJsonValueType(v, name, JSON_FALSE, JSON_TRUE);
				q.explain_ = v.getTag() == JSON_TRUE;
				break;
			case Root::WithRank:
				checkJsonValueType(v, name, JSON_FALSE, JSON_TRUE);
				if (v.getTag() == JSON_TRUE) q.WithRank();
				break;
			case Root::StrictMode:
				checkJsonValueType(v, name, JSON_STRING);
				q.strictMode = strictModeFromString(string(v.toString()));
				if (q.strictMode == StrictModeNotSet) {
					throw Error(errParseDSL, "Unexpected strict mode value: %s", v.toString());
				}
				break;
			case Root::EqualPositions: {
				checkJsonValueType(v, name, JSON_ARRAY);
				parseEqualPositions(v, q);
			} break;
			case Root::QueryType:
				checkJsonValueType(v, name, JSON_STRING);
				q.type_ = get(query_types, v.toString(), "query_type"_sv);
				break;
			case Root::DropFields:
				checkJsonValueType(v, name, JSON_ARRAY);
				for (auto element : v) {
					auto& value = element->value;
					checkJsonValueType(value, "string array item", JSON_STRING);
					q.Drop(string(value.toString()));
				}
				break;
			case Root::UpdateFields:
				checkJsonValueType(v, name, JSON_ARRAY);
				parseUpdateFields(v, q);
				break;
			default:
				throw Error(errParseDSL, "incorrect tag '%'", name);
		}
	}
}

#include "query.json.h"

Error Parse(const string& str, Query& q) {
	static JsonSchemaChecker schemaChecker(kQueryJson, "query");
	try {
		gason::JsonParser parser;
		auto root = parser.Parse(giftStr(str));
		Error err = schemaChecker.Check(root);
		if (!err.ok()) return err;
		dsl::parse(root.value, q);
	} catch (const gason::Exception& ex) {
		return Error(errParseJson, "Query: %s", ex.what());
	} catch (const Error& err) {
		return err;
	}
	return errOK;
}

}  // namespace dsl
}  // namespace reindexer
