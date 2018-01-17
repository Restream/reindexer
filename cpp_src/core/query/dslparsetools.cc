#include "dslparsetools.h"
#include <string>
#include "estl/fast_hash_map.h"
#include "tools/errors.h"
#include "tools/json2kv.h"
#include "tools/stringstools.h"

namespace reindexer {
namespace dsl {

void parseValues(JsonValue& values, KeyValues& kvs);

// additional for parse root DSL fields

enum class Root { Namespace, Limit, Offset, Distinct, Filters, Sort };

static const fast_hash_map<string, Root> root_map = {{"namespace", Root::Namespace}, {"limit", Root::Limit},	 {"offset", Root::Offset},
													 {"distinct", Root::Distinct},   {"filters", Root::Filters}, {"sort", Root::Sort}};

// additional for parse field 'sort'

enum class Sort { Desc, Field, Values };

static const fast_hash_map<string, Sort> sort_map = {{"desc", Sort::Desc}, {"field", Sort::Field}, {"values", Sort::Values}};

// additionalfor parse field 'filters'

enum class Filter { Cond, Op, Field, Value };

static const fast_hash_map<string, Filter> filter_map = {
	{"cond", Filter::Cond}, {"op", Filter::Op}, {"field", Filter::Field}, {"value", Filter::Value}};

// additional for 'filter::cond' field

static const fast_hash_map<string, CondType> cond_map = {
	{"any", CondAny}, {"eq", CondEq},		{"lt", CondLt},   {"le", CondLe},		  {"gt", CondGt},
	{"ge", CondGe},   {"range", CondRange}, {"set", CondSet}, {"allset", CondAllSet}, {"empty", CondEmpty}};

static const fast_hash_map<string, OpType> op_map = {{"or", OpOr}, {"and", OpAnd}, {"not", OpNot}};

template <typename T>
T get(fast_hash_map<string, T> const& m, string const& name) {
	auto it = m.find(name);
	return it != m.end() ? it->second : T(-1);
}

void parseSort(JsonValue& sort, Query& q) {
	for (auto elem : sort) {
		auto& v = elem->value;
		auto name = lower(elem->key);
		switch (get(sort_map, name)) {
			case Sort::Desc:
				if ((v.getTag() != JSON_TRUE) && (v.getTag() != JSON_FALSE))
					throw Error(errParseJson, "Wrong type of field '%s'", name.c_str());
				q.sortDirDesc = (v.getTag() == JSON_TRUE);
				break;

			case Sort::Field:
				if (v.getTag() != JSON_STRING) throw Error(errParseJson, "Wrong type of field '%s'", name.c_str());
				q.sortBy.assign(v.toString());
				break;

			case Sort::Values:
				parseValues(v, q.forcedSortOrder);
				break;
		}
	}
}

void parseValues(JsonValue& values, KeyValues& kvs) {
	if (values.getTag() == JSON_ARRAY) {
		for (auto elem : values) {
			if (elem->value.getTag() != JSON_NULL) {
				KeyValue kv(jsonValue2KeyRef(elem->value, KeyValueUndefined));
				if (kvs.size() > 1 && kvs.back().Type() != kv.Type())
					throw Error(errParseJson, "Array of filter values must be homogeneous.");
				kvs.push_back(kv);
			}
		}
	} else if (values.getTag() != JSON_NULL) {
		kvs.push_back(KeyValue(jsonValue2KeyRef(values, KeyValueUndefined)));
	}
}

void parseFilter(JsonValue& filter, Query& q) {
	QueryEntry qe;
	if (filter.getTag() != JSON_OBJECT) throw Error(errParseJson, "Wrong field filters element type");
	for (auto elem : filter) {
		auto& v = elem->value;
		auto name = lower(elem->key);
		switch (get(filter_map, name)) {
			case Filter::Cond:
				if (v.getTag() != JSON_STRING) throw Error(errParseJson, "Wrong type of field '%s'", name.c_str());
				qe.condition = get(cond_map, lower(v.toString()));
				break;

			case Filter::Op:
				if (v.getTag() != JSON_STRING) throw Error(errParseJson, "Wrong type of field '%s'", name.c_str());
				qe.op = get(op_map, lower(v.toString()));
				break;

			case Filter::Value:
				parseValues(v, qe.values);
				break;

			case Filter::Field:
				if (v.getTag() != JSON_STRING) throw Error(errParseJson, "Wrong type of field '%s'", name.c_str());
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

void parse(JsonValue& root, Query& q) {
	for (auto elem : root) {
		auto& v = elem->value;
		auto name = lower(elem->key);
		switch (get(root_map, name)) {
			case Root::Namespace:
				if (v.getTag() != JSON_STRING) throw Error(errParseJson, "Wrong type of field '%s'", name.c_str());
				q._namespace.assign(v.toString());
				break;

			case Root::Limit:
				if (v.getTag() != JSON_NUMBER) throw Error(errParseJson, "Wrong type of field '%s'", name.c_str());
				q.count = static_cast<unsigned>(v.toNumber());
				break;

			case Root::Offset:
				if (v.getTag() != JSON_NUMBER) throw Error(errParseJson, "Wrong type of field '%s'", name.c_str());
				q.start = static_cast<unsigned>(v.toNumber());
				break;

			case Root::Distinct:
				// nothing?????
				break;

			case Root::Filters:
				if (v.getTag() != JSON_ARRAY) throw Error(errParseJson, "Wrong type of field '%s'", name.c_str());
				for (auto filter : v) parseFilter(filter->value, q);
				break;

			case Root::Sort:
				if (v.getTag() != JSON_OBJECT) throw Error(errParseJson, "Wrong type of field '%s'", name.c_str());
				parseSort(v, q);
				break;
		}
	}
}

}  // namespace dsl
}  // namespace reindexer
