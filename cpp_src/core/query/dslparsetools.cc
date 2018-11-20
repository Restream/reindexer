#include "dslparsetools.h"
#include <string>
#include "estl/fast_hash_map.h"
#include "tools/errors.h"
#include "tools/json2kv.h"
#include "tools/stringstools.h"

namespace reindexer {
namespace dsl {

void parseValues(JsonValue& values, VariantArray& kvs);

// additional for parse root DSL fields

static const fast_hash_map<string, Root> root_map = {{"namespace", Root::Namespace},
													 {"limit", Root::Limit},
													 {"offset", Root::Offset},
													 {"distinct", Root::Distinct},
													 {"filters", Root::Filters},
													 {"sort", Root::Sort},
													 {"join_queries", Root::Joined},
													 {"merge_queries", Root::Merged},
													 {"select_filter", Root::SelectFilter},
													 {"select_functions", Root::SelectFunctions},
													 {"req_total", Root::ReqTotal},
													 {"aggregations", Root::Aggregations},
													 {"next_op", Root::NextOp},
													 {"explain", Root::Explain}};

// additional for parse field 'sort'

static const fast_hash_map<string, Sort> sort_map = {{"desc", Sort::Desc}, {"field", Sort::Field}, {"values", Sort::Values}};

// additional for parse field 'joined'

static const fast_hash_map<string, JoinRoot> joins_map = {
	{"type", JoinRoot::Type},   {"namespace", JoinRoot::Namespace}, {"filters", JoinRoot::Filters}, {"sort", JoinRoot::Sort},
	{"limit", JoinRoot::Limit}, {"offset", JoinRoot::Offset},		{"on", JoinRoot::On},			{"op", JoinRoot::Op}};

static const fast_hash_map<string, JoinEntry> joined_entry_map = {
	{"left_field", JoinEntry::LetfField}, {"right_field", JoinEntry::RightField}, {"cond", JoinEntry::Cond}, {"op", JoinEntry::Op}};

static const fast_hash_map<string, JoinType> join_types = {{"inner", InnerJoin}, {"left", LeftJoin}, {"orinner", OrInnerJoin}};

// additionalfor parse field 'filters'

static const fast_hash_map<string, Filter> filter_map = {
	{"cond", Filter::Cond}, {"op", Filter::Op}, {"field", Filter::Field}, {"value", Filter::Value}};

// additional for 'filter::cond' field

static const fast_hash_map<string, CondType> cond_map = {
	{"any", CondAny},	 {"eq", CondEq},   {"lt", CondLt},			{"le", CondLe},		  {"gt", CondGt},	{"ge", CondGe},
	{"range", CondRange}, {"set", CondSet}, {"allset", CondAllSet}, {"empty", CondEmpty}, {"match", CondEq},
};

static const fast_hash_map<string, OpType> op_map = {{"or", OpOr}, {"and", OpAnd}, {"not", OpNot}};

// additional for 'Root::ReqTotal' field

static const fast_hash_map<string, CalcTotalMode> reqtotal_values = {
	{"disabled", ModeNoTotal}, {"enabled", ModeAccurateTotal}, {"cached", ModeCachedTotal}};

// additional for 'Root::Aggregations' field

static const fast_hash_map<string, Aggregation> aggregation_map = {{"field", Aggregation::Field}, {"type", Aggregation::Type}};
static const fast_hash_map<string, AggType> aggregation_types = {
	{"sum", AggSum}, {"avg", AggAvg}, {"max", AggMax}, {"min", AggMin}, {"facet", AggFacet}};

bool checkTag(JsonValue& val, JsonTag tag) { return val.getTag() == tag; }

template <typename... Tags>
bool checkTag(JsonValue& val, JsonTag tag, Tags... tags) {
	return std::max(tag == val.getTag(), checkTag(val, tags...));
}

template <typename... JsonTags>
void checkJsonValueType(JsonValue& val, const string& name, JsonTags... possibleTags) {
	if (!checkTag(val, possibleTags...)) throw Error(errParseJson, "Wrong type of field '%s'", name.c_str());
}

template <typename T>
T get(fast_hash_map<string, T> const& m, string const& name) {
	auto it = m.find(name);
	return it != m.end() ? it->second : T(-1);
}

template <typename T, int holdSize>
void parseStringArray(JsonValue& stringArray, h_vector<T, holdSize>& array) {
	for (auto element : stringArray) {
		auto& value = element->value;
		checkJsonValueType(value, "string array item", JSON_STRING);
		array.push_back(value.toString());
	}
}

void parseSortEntry(JsonValue& entry, Query& q) {
	checkJsonValueType(entry, "Sort", JSON_OBJECT);
	SortingEntry sortingEntry;
	for (auto subelement : entry) {
		auto& v = subelement->value;
		string name = lower(subelement->key);
		switch (get(sort_map, name)) {
			case Sort::Desc:
				if ((v.getTag() != JSON_TRUE) && (v.getTag() != JSON_FALSE))
					throw Error(errParseJson, "Wrong type of field '%s'", name.c_str());
				sortingEntry.desc = (v.getTag() == JSON_TRUE);
				break;

			case Sort::Field:
				checkJsonValueType(v, name, JSON_STRING);
				sortingEntry.column.assign(v.toString());
				break;

			case Sort::Values:
				parseValues(v, q.forcedSortOrder);
				break;
		}
	}
	if (!sortingEntry.column.empty()) {
		q.sortingEntries_.push_back(std::move(sortingEntry));
	}
}

void parseSort(JsonValue& v, Query& q) {
	if (v.getTag() == JSON_ARRAY) {
		for (auto entry : v) parseSort(entry->value, q);
	} else if (v.getTag() == JSON_OBJECT) {
		parseSortEntry(v, q);
	} else {
		throw Error(errConflict, "Wrong type of field 'Sort'");
	}
}

void parseValues(JsonValue& values, VariantArray& kvs) {
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

void parseFilter(JsonValue& filter, Query& q) {
	QueryEntry qe;
	checkJsonValueType(filter, "filter", JSON_OBJECT);
	for (auto elem : filter) {
		auto& v = elem->value;
		auto name = lower(elem->key);
		switch (get(filter_map, name)) {
			case Filter::Cond:
				checkJsonValueType(v, name, JSON_STRING);
				qe.condition = get(cond_map, lower(v.toString()));
				break;

			case Filter::Op:
				checkJsonValueType(v, name, JSON_STRING);
				qe.op = get(op_map, lower(v.toString()));
				break;

			case Filter::Value:
				parseValues(v, qe.values);
				break;

			case Filter::Field:
				checkJsonValueType(v, name, JSON_STRING);
				qe.index.assign(v.toString());
				break;
		}
	}
	switch (qe.condition) {
		case CondGe:
		case CondGt:
		case CondEq:
		case CondLt:
		case CondLe:
			if (qe.values.size() != 1) {
				throw Error(errLogic, "Condition %d must have exact 1 value, but %d values was provided", qe.condition,
							int(qe.values.size()));
			}
			break;
		case CondRange:
			if (qe.values.size() != 2) {
				throw Error(errLogic, "Condition RANGE must have exact 2 values, but %d values was provided", int(qe.values.size()));
			}
			break;
		case CondSet:
			if (qe.values.size() < 1) {
				throw Error(errLogic, "Condition SET must have at least 1 value, but %d values was provided", int(qe.values.size()));
			}
			break;
		case CondAny:
		case CondAllSet:
			if (qe.values.size() != 0) {
				throw Error(errLogic, "Condition ANY must have 0 values, but %d values was provided", int(qe.values.size()));
			}
			break;
		default:
			break;
	}

	q.entries.push_back(qe);
}

void parseJoinedEntries(JsonValue& joinEntries, Query& qjoin) {
	checkJsonValueType(joinEntries, "Joined", JSON_ARRAY);
	for (auto element : joinEntries) {
		auto& joinEntry = element->value;
		checkJsonValueType(joinEntry, "Joined", JSON_OBJECT);

		QueryJoinEntry qjoinEntry;
		for (auto subelement : joinEntry) {
			auto& value = subelement->value;
			string name = lower(subelement->key);
			switch (get(joined_entry_map, name)) {
				case JoinEntry::LetfField:
					checkJsonValueType(value, name, JSON_STRING);
					qjoinEntry.index_ = value.toString();
					break;
				case JoinEntry::RightField:
					checkJsonValueType(value, name, JSON_STRING);
					qjoinEntry.joinIndex_ = value.toString();
					break;
				case JoinEntry::Cond:
					checkJsonValueType(value, name, JSON_STRING);
					qjoinEntry.condition_ = get(cond_map, lower(value.toString()));
					break;
				case JoinEntry::Op:
					checkJsonValueType(value, name, JSON_STRING);
					qjoinEntry.op_ = get(op_map, lower(value.toString()));
					break;
			}
		}
		qjoin.joinEntries_.emplace_back(qjoinEntry);
	}
}

void parseJoins(JsonValue& joins, Query& query) {
	for (auto element : joins) {
		auto& join = element->value;
		checkJsonValueType(join, "Joined", JSON_OBJECT);

		Query qjoin;
		for (auto subelement : join) {
			auto& value = subelement->value;
			string name = lower(subelement->key);
			switch (get(joins_map, name)) {
				case JoinRoot::Type:
					checkJsonValueType(value, name, JSON_STRING);
					qjoin.joinType = get(join_types, lower(value.toString()));
					break;
				case JoinRoot::Namespace:
					checkJsonValueType(value, name, JSON_STRING);
					qjoin._namespace = value.toString();
					break;
				case JoinRoot::Op:
					checkJsonValueType(value, name, JSON_STRING);
					qjoin.nextOp_ = get(op_map, lower(value.toString()));
					break;
				case JoinRoot::Filters:
					checkJsonValueType(value, name, JSON_ARRAY);
					for (auto filter : value) parseFilter(filter->value, qjoin);
					break;
				case JoinRoot::Sort:
					parseSort(value, qjoin);
					break;
				case JoinRoot::Limit:
					checkJsonValueType(value, name, JSON_NUMBER);
					qjoin.count = static_cast<unsigned>(value.toNumber());
					break;
				case JoinRoot::Offset:
					checkJsonValueType(value, name, JSON_NUMBER);
					qjoin.start = static_cast<unsigned>(value.toNumber());
					break;
				case JoinRoot::On:
					parseJoinedEntries(value, qjoin);
					break;
			}
		}
		query.joinQueries_.emplace_back(qjoin);
	}
}

void parseMergeQueries(JsonValue& mergeQueries, Query& query) {
	for (auto element : mergeQueries) {
		auto& merged = element->value;
		checkJsonValueType(merged, "Merged", JSON_OBJECT);
		Query qmerged;
		parse(merged, qmerged);
		query.mergeQueries_.emplace_back(qmerged);
	}
}

void parseAggregation(JsonValue& aggregation, Query& query) {
	checkJsonValueType(aggregation, "Aggregation", JSON_OBJECT);
	AggregateEntry aggEntry;
	for (auto element : aggregation) {
		auto& value = element->value;
		string name = lower(element->key);
		switch (get(aggregation_map, name)) {
			case Aggregation::Field:
				checkJsonValueType(value, name, JSON_STRING);
				aggEntry.index_ = value.toString();
				break;
			case Aggregation::Type:
				checkJsonValueType(value, name, JSON_STRING);
				aggEntry.type_ = get(aggregation_types, lower(value.toString()));
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
		auto name = lower(elem->key);
		switch (get(root_map, name)) {
			case Root::Namespace:
				checkJsonValueType(v, name, JSON_STRING);
				q._namespace.assign(v.toString());
				break;

			case Root::Limit:
				checkJsonValueType(v, name, JSON_NUMBER);
				q.count = static_cast<unsigned>(v.toNumber());
				break;

			case Root::Offset:
				checkJsonValueType(v, name, JSON_NUMBER);
				q.start = static_cast<unsigned>(v.toNumber());
				break;

			case Root::Distinct:
				checkJsonValueType(v, name, JSON_STRING);
				if (*v.toString()) q.Distinct(v.toString());
				break;

			case Root::Filters:
				checkJsonValueType(v, name, JSON_ARRAY);
				for (auto filter : v) parseFilter(filter->value, q);
				break;

			case Root::NextOp:
				checkJsonValueType(v, name, JSON_STRING);
				q.nextOp_ = get(op_map, v.toString());
				break;

			case Root::Sort:
				parseSort(v, q);
				break;
			case Root::Joined:
				checkJsonValueType(v, name, JSON_ARRAY);
				parseJoins(v, q);
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
		}
	}
}

}  // namespace dsl
}  // namespace reindexer
