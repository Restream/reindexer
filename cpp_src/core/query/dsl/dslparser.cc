#include "dslparser.h"
#include "core/query/query.h"
#include "estl/fast_hash_map.h"
#include "gason/gason.h"
#include "tools/errors.h"
#include "tools/json2kv.h"
#include "tools/stringstools.h"

namespace reindexer {
using namespace gason;
namespace dsl {
using gason::JsonValue;

enum class Root {
	Namespace,
	Limit,
	Offset,
	Distinct,
	Filters,
	Sort,
	Merged,
	SelectFilter,
	SelectFunctions,
	ReqTotal,
	Aggregations,
	Explain,
	EqualPosition,
};

enum class Sort { Desc, Field, Values };
enum class JoinRoot { Type, On, Namespace, Filters, Sort, Limit, Offset };
enum class JoinEntry { LetfField, RightField, Cond, Op };
enum class Filter { Cond, Op, Field, Value, Filters, JoinQuery };
enum class Aggregation { Fields, Type, Sort, Limit, Offset };

// additional for parse root DSL fields
template <typename T>
using fast_str_map = fast_hash_map<string, T, nocase_hash_str, nocase_equal_str>;

static const fast_str_map<Root> root_map = {{"namespace", Root::Namespace},
											{"limit", Root::Limit},
											{"offset", Root::Offset},
											{"distinct", Root::Distinct},
											{"filters", Root::Filters},
											{"sort", Root::Sort},
											{"merge_queries", Root::Merged},
											{"select_filter", Root::SelectFilter},
											{"select_functions", Root::SelectFunctions},
											{"req_total", Root::ReqTotal},
											{"aggregations", Root::Aggregations},
											{"explain", Root::Explain},
											{"equal_position", Root::EqualPosition}};

// additional for parse field 'sort'

static const fast_str_map<Sort> sort_map = {{"desc", Sort::Desc}, {"field", Sort::Field}, {"values", Sort::Values}};

// additional for parse field 'joined'

static const fast_str_map<JoinRoot> joins_map = {
	{"type", JoinRoot::Type}, {"namespace", JoinRoot::Namespace}, {"filters", JoinRoot::Filters},
	{"sort", JoinRoot::Sort}, {"limit", JoinRoot::Limit},		  {"offset", JoinRoot::Offset},
	{"on", JoinRoot::On}};

static const fast_str_map<JoinEntry> joined_entry_map = {
	{"left_field", JoinEntry::LetfField}, {"right_field", JoinEntry::RightField}, {"cond", JoinEntry::Cond}, {"op", JoinEntry::Op}};

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
static const fast_str_map<AggType> aggregation_types = {
	{"sum", AggSum}, {"avg", AggAvg}, {"max", AggMax}, {"min", AggMin}, {"facet", AggFacet}};

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
T get(fast_str_map<T> const& m, string_view name) {
	auto it = m.find(name);
	return it != m.end() ? it->second : T(-1);
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
			if (elem->value.getTag() != JSON_NULL) {
				kv = jsonValue2Variant(elem->value, KeyValueUndefined);
			}
			if (kvs.size() > 1 && kvs.back().Type() != kv.Type()) throw Error(errParseJson, "Array of filter values must be homogeneous.");
			kvs.push_back(std::move(kv));
		}
	} else if (values.getTag() != JSON_NULL) {
		kvs.push_back(jsonValue2Variant(values, KeyValueUndefined));
	}
}
void parse(JsonValue& root, Query& q);

void parseSortEntry(JsonValue& entry, Query& q) {
	checkJsonValueType(entry, "Sort", JSON_OBJECT);
	SortingEntry sortingEntry;
	for (auto subelement : entry) {
		auto& v = subelement->value;
		string_view name = subelement->key;
		switch (get(sort_map, name)) {
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
		switch (get(sort_map, name)) {
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
		switch (get(filter_map, name)) {
			case Filter::Cond:
				checkJsonValueType(v, name, JSON_STRING);
				qe.condition = get(cond_map, v.toString());
				break;

			case Filter::Op:
				checkJsonValueType(v, name, JSON_STRING);
				op = get(op_map, v.toString());
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
			switch (get(joined_entry_map, name)) {
				case JoinEntry::LetfField:
					checkJsonValueType(value, name, JSON_STRING);
					qjoinEntry.index_ = string(value.toString());
					break;
				case JoinEntry::RightField:
					checkJsonValueType(value, name, JSON_STRING);
					qjoinEntry.joinIndex_ = string(value.toString());
					break;
				case JoinEntry::Cond:
					checkJsonValueType(value, name, JSON_STRING);
					qjoinEntry.condition_ = get(cond_map, value.toString());
					break;
				case JoinEntry::Op:
					checkJsonValueType(value, name, JSON_STRING);
					qjoinEntry.op_ = get(op_map, value.toString());
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
		switch (get(joins_map, name)) {
			case JoinRoot::Type:
				checkJsonValueType(value, name, JSON_STRING);
				qjoin.joinType = get(join_types, value.toString());
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
	query.joinQueries_.emplace_back(qjoin);
}

void parseMergeQueries(JsonValue& mergeQueries, Query& query) {
	for (auto element : mergeQueries) {
		auto& merged = element->value;
		checkJsonValueType(merged, "Merged", JSON_OBJECT);
		JoinedQuery qmerged;
		parse(merged, qmerged);
		query.mergeQueries_.emplace_back(qmerged);
	}
}

void parseAggregation(JsonValue& aggregation, Query& query) {
	checkJsonValueType(aggregation, "Aggregation", JSON_OBJECT);
	AggregateEntry aggEntry;
	for (auto element : aggregation) {
		auto& value = element->value;
		string_view name = element->key;
		switch (get(aggregation_map, name)) {
			case Aggregation::Fields:
				checkJsonValueType(value, name, JSON_ARRAY);
				for (auto subElem : value) {
					if (subElem->value.getTag() != JSON_STRING) throw Error(errParseJson, "Expected string in array 'fields'");
					aggEntry.fields_.push_back(string(subElem->value.toString()));
				}
				break;
			case Aggregation::Type:
				checkJsonValueType(value, name, JSON_STRING);
				aggEntry.type_ = get(aggregation_types, value.toString());
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

void parse(JsonValue& root, Query& q) {
	if (root.getTag() != JSON_OBJECT) {
		throw Error(errParseJson, "Json is malformed: %d", root.getTag());
	}

	for (auto elem : root) {
		auto& v = elem->value;
		auto name = elem->key;
		switch (get(root_map, name)) {
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

			case Root::Distinct:
				checkJsonValueType(v, name, JSON_ARRAY);
				for (auto f : v) {
					checkJsonValueType(f->value, f->key, JSON_STRING);
					if (f->value.toString().size()) q.Distinct(string(f->value.toString()));
				}
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
				q.calcTotal = get(reqtotal_values, v.toString());
				break;
			case Root::Aggregations:
				checkJsonValueType(v, name, JSON_ARRAY);
				for (auto aggregation : v) parseAggregation(aggregation->value, q);
				break;
			case Root::Explain:
				checkJsonValueType(v, name, JSON_FALSE, JSON_TRUE);
				q.explain_ = v.getTag() == JSON_TRUE;
				break;
			case Root::EqualPosition: {
				checkJsonValueType(v, name, JSON_ARRAY);
				vector<string> ep;
				for (auto f : v) {
					checkJsonValueType(f->value, f->key, JSON_STRING);
					ep.emplace_back(f->value.toString());
				}
				if (ep.size() < 2) throw Error(errLogic, "equal_position() is supposed to have at least 2 arguments");
				q.equalPositions_.emplace(q.entries.DetermineEqualPositionIndexes(ep));
			} break;
		}
	}
}

Error Parse(const string& str, Query& q) {
	try {
		gason::JsonParser parser;

		auto root = parser.Parse(giftStr(str));
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
