#include "core/query/dslencoder.h"
#include "core/query/dslparsetools.h"
#include "core/query/query.h"

#include <unordered_map>

using std::unordered_map;

const char coma = ',';
const char colon = ':';
const char leftBracket = '{';
const char rightBracket = '}';
const char leftSquareBracket = '[';
const char rightSquareBracket = ']';

struct EnumClassHash {
	template <typename T>
	size_t operator()(T t) const {
		return static_cast<size_t>(t);
	}
};

namespace reindexer {
namespace dsl {

const unordered_map<Root, string, EnumClassHash> root_map = {{Root::Namespace, "namespace"},
															 {Root::Limit, "limit"},
															 {Root::Offset, "offset"},
															 {Root::Distinct, "distinct"},
															 {Root::Filters, "filters"},
															 {Root::Sort, "sort"},
															 {Root::Joined, "join_queries"},
															 {Root::Merged, "merge_queries"},
															 {Root::SelectFilter, "select_filter"},
															 {Root::SelectFunctions, "select_functions"},
															 {Root::ReqTotal, "req_total"},
															 {Root::Aggregations, "aggregations"},
															 {Root::NextOp, "next_op"}};

const unordered_map<Sort, string, EnumClassHash> sort_map = {{Sort::Desc, "desc"}, {Sort::Field, "field"}, {Sort::Values, "values"}};

const unordered_map<JoinRoot, string, EnumClassHash> joins_map = {
	{JoinRoot::Type, "type"},   {JoinRoot::Namespace, "namespace"}, {JoinRoot::Filters, "filters"}, {JoinRoot::Sort, "sort"},
	{JoinRoot::Limit, "limit"}, {JoinRoot::Offset, "offset"},		{JoinRoot::On, "on"},			{JoinRoot::Op, "op"}};

const unordered_map<JoinEntry, string, EnumClassHash> joined_entry_map = {
	{JoinEntry::LetfField, "left_field"}, {JoinEntry::RightField, "right_field"}, {JoinEntry::Cond, "cond"}, {JoinEntry::Op, "op"}};

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

const unordered_map<Aggregation, string, EnumClassHash> aggregation_map = {{Aggregation::Field, "field"}, {Aggregation::Type, "type"}};
const unordered_map<AggType, string, EnumClassHash> aggregation_types = {{AggSum, "sum"}, {AggAvg, "avg"}};

template <typename T>
string get(unordered_map<T, string, EnumClassHash> const& m, const T& key) {
	auto it = m.find(key);
	if (it != m.end()) return it->second;
	assert(it != m.end());
	return string();
}

void encodeString(const string& str, string& dsl) {
	const char* s = str.data();
	size_t l = str.size();
	dsl += '"';

	while (l--) {
		int c = *s++;
		switch (c) {
			case '\b':
				dsl += '\\';
				dsl += 'b';
				break;
			case '\f':
				dsl += '\\';
				dsl += 'f';
				break;
			case '\n':
				dsl += '\\';
				dsl += 'n';
				break;
			case '\r':
				dsl += '\\';
				dsl += 'r';
				break;
			case '\t':
				dsl += '\\';
				dsl += 't';
				break;
			case '\\':
				dsl += '\\';
				dsl += '\\';
				break;
			case '"':
				dsl += '\\';
				dsl += '"';
				break;
			case '&':
				dsl += '\\';
				dsl += 'u';
				dsl += '0';
				dsl += '0';
				dsl += '2';
				dsl += '6';
				break;
			default:
				dsl += c;
		}
	}
	dsl += '"';
}

void addComa(string& dsl) { dsl += coma; }

void encodeNodeName(const string& nodeName, string& dsl) {
	encodeString(nodeName, dsl);
	dsl += colon;
}

template <typename T>
void encodeNumericField(const string& nodeName, const T nodeValue, string& dsl) {
	encodeNodeName(nodeName, dsl);
	dsl += std::to_string(nodeValue);
}

void encodeStringField(const string& nodeName, const string& nodeValue, string& dsl) {
	encodeNodeName(nodeName, dsl);
	encodeString(nodeValue, dsl);
}

void encodeBooleanField(const string& nodeName, bool nodeValue, string& dsl) {
	encodeNodeName(nodeName, dsl);
	dsl += nodeValue ? "true" : "false";
}

void encodeKeyValue(const KeyValue& kv, string& dsl) {
	switch (kv.Type()) {
		case KeyValueInt:
			dsl += std::to_string(static_cast<int>(kv));
			break;
		case KeyValueInt64:
			dsl += std::to_string(static_cast<int64_t>(kv));
			break;
		case KeyValueDouble:
			dsl += std::to_string(static_cast<double>(kv));
			break;
		case KeyValueString:
			encodeString(*static_cast<key_string>(kv), dsl);
			break;
		case KeyValueComposite: {
			dsl += leftSquareBracket;
			const vector<KeyValue>& values(kv.getCompositeValues());
			for (size_t i = 0; i < values.size(); ++i) {
				encodeKeyValue(values[i], dsl);
				if (i != values.size() - 1) addComa(dsl);
			}
			dsl += rightSquareBracket;
			break;
		}
		default:
			break;
	}
}

template <typename T, int holdSize>
void encodeStringArray(const h_vector<T, holdSize>& array, string& dsl) {
	dsl += leftSquareBracket;
	for (size_t i = 0; i < array.size(); ++i) {
		encodeString(array[i], dsl);
		if (i != array.size() - 1) addComa(dsl);
	}
	dsl += rightSquareBracket;
}

void encodeSorting(const Query& query, string& dsl) {
	if (query.sortingEntries_.empty()) return;
	encodeNodeName(get(root_map, Root::Sort), dsl);
	bool multicolumnSort = query.sortingEntries_.size() > 1;
	if (multicolumnSort) dsl += leftSquareBracket;
	for (size_t i = 0; i < query.sortingEntries_.size(); i++) {
		dsl += leftBracket;
		const SortingEntry& sortingEntry(query.sortingEntries_[i]);
		encodeStringField(get(sort_map, Sort::Field), sortingEntry.column, dsl);
		addComa(dsl);
		encodeBooleanField(get(sort_map, Sort::Desc), sortingEntry.desc, dsl);
		dsl += rightBracket;
		if (i != query.sortingEntries_.size() - 1) addComa(dsl);
	}
	if (multicolumnSort) dsl += rightSquareBracket;
}

void encodeFilter(const QueryEntry& qentry, string& dsl) {
	dsl += leftBracket;
	encodeStringField(get(filter_map, Filter::Op), get(op_map, qentry.op), dsl);
	addComa(dsl);
	encodeStringField(get(filter_map, Filter::Cond), get(cond_map, qentry.condition), dsl);
	addComa(dsl);
	encodeStringField(get(filter_map, Filter::Field), qentry.index, dsl);
	addComa(dsl);

	encodeNodeName(get(filter_map, Filter::Value), dsl);
	if (qentry.values.size() > 1 || ((qentry.values.size() == 1) && qentry.values[0].Type() == KeyValueComposite)) {
		dsl += leftSquareBracket;
		for (size_t i = 0; i < qentry.values.size(); i++) {
			const KeyValue& kv(qentry.values[i]);
			encodeKeyValue(kv, dsl);
			if (i != qentry.values.size() - 1) addComa(dsl);
		}
		dsl += rightSquareBracket;
	} else {
		encodeKeyValue(qentry.values[0], dsl);
	}

	dsl += rightBracket;
}

void encodeFilters(const Query& query, string& dsl) {
	if (query.entries.empty()) return;
	encodeNodeName(get(root_map, Root::Filters), dsl);
	dsl += leftSquareBracket;
	for (size_t i = 0; i < query.entries.size(); ++i) {
		const QueryEntry& qe(query.entries[i]);
		encodeFilter(qe, dsl);
		if (i != query.entries.size() - 1) addComa(dsl);
	}
	dsl += rightSquareBracket;
}

void encodeMergedQueries(const Query& query, string& dsl) {
	if (query.mergeQueries_.empty()) return;
	encodeNodeName(get(root_map, Root::Merged), dsl);
	dsl += leftSquareBracket;
	for (size_t i = 0; i < query.mergeQueries_.size(); i++) {
		dsl += toDsl(query.mergeQueries_[i]);
		if (i != query.mergeQueries_.size() - 1) addComa(dsl);
	}
	dsl += rightSquareBracket;
}

void encodeSelectFilter(const Query& query, string& dsl) {
	if (query.selectFilter_.empty()) return;
	encodeNodeName(get(root_map, Root::SelectFilter), dsl);
	encodeStringArray(query.selectFilter_, dsl);
}

void encodeSelectFunctions(const Query& query, string& dsl) {
	if (query.selectFunctions_.empty()) return;
	encodeNodeName(get(root_map, Root::SelectFunctions), dsl);
	encodeStringArray(query.selectFunctions_, dsl);
}

void encodeAggregationFunctions(const Query& query, string& dsl) {
	if (query.aggregations_.empty()) return;
	encodeNodeName(get(root_map, Root::Aggregations), dsl);
	dsl += leftSquareBracket;
	for (size_t i = 0; i < query.aggregations_.size(); i++) {
		const AggregateEntry& entry(query.aggregations_[i]);
		dsl += leftBracket;
		encodeStringField(get(aggregation_map, Aggregation::Field), entry.index_, dsl);
		addComa(dsl);
		encodeStringField(get(aggregation_map, Aggregation::Type), get(aggregation_types, entry.type_), dsl);
		dsl += rightBracket;
		if (i != query.aggregations_.size() - 1) addComa(dsl);
	}
	dsl += rightSquareBracket;
}

void encodeJoinEntry(const QueryJoinEntry& joinEntry, string& dsl) {
	dsl += leftBracket;
	encodeStringField(get(joined_entry_map, JoinEntry::LetfField), joinEntry.index_, dsl);
	addComa(dsl);
	encodeStringField(get(joined_entry_map, JoinEntry::RightField), joinEntry.joinIndex_, dsl);
	addComa(dsl);
	encodeStringField(get(joined_entry_map, JoinEntry::Cond), get(cond_map, joinEntry.condition_), dsl);
	addComa(dsl);
	encodeStringField(get(joined_entry_map, JoinEntry::Op), get(op_map, joinEntry.op_), dsl);
	dsl += rightBracket;
}

void encodeJoins(const Query& query, string& dsl) {
	if (query.joinQueries_.empty()) return;
	encodeNodeName(get(root_map, Root::Joined), dsl);
	dsl += leftSquareBracket;
	for (size_t i = 0; i < query.joinQueries_.size(); ++i) {
		const Query& joinQuery(query.joinQueries_[i]);
		dsl += leftBracket;
		encodeStringField(get(joins_map, JoinRoot::Type), get(join_types, joinQuery.joinType), dsl);
		addComa(dsl);
		encodeStringField(get(joins_map, JoinRoot::Op), get(op_map, joinQuery.nextOp_), dsl);
		addComa(dsl);
		encodeStringField(get(joins_map, JoinRoot::Namespace), joinQuery._namespace, dsl);
		addComa(dsl);
		encodeNumericField(get(joins_map, JoinRoot::Limit), joinQuery.count, dsl);
		addComa(dsl);
		encodeNumericField(get(joins_map, JoinRoot::Offset), joinQuery.start, dsl);

		if (!joinQuery.entries.empty()) addComa(dsl);
		encodeFilters(joinQuery, dsl);

		if (!joinQuery.sortingEntries_.empty()) addComa(dsl);
		encodeSorting(joinQuery, dsl);

		addComa(dsl);
		encodeNodeName(get(joins_map, JoinRoot::On), dsl);
		dsl += leftSquareBracket;
		for (size_t j = 0; j < joinQuery.joinEntries_.size(); ++j) {
			encodeJoinEntry(joinQuery.joinEntries_[j], dsl);
			if (j != joinQuery.joinEntries_.size() - 1) addComa(dsl);
		}
		dsl += rightSquareBracket;
		dsl += rightBracket;
		if (i != query.joinQueries_.size() - 1) addComa(dsl);
	}
	dsl += rightSquareBracket;
}

std::string toDsl(const Query& query) {
	string dsl;
	dsl += leftBracket;

	encodeStringField(get(root_map, Root::Namespace), query._namespace, dsl);
	addComa(dsl);
	encodeNumericField(get(root_map, Root::Limit), query.count, dsl);
	addComa(dsl);
	encodeNumericField(get(root_map, Root::Offset), query.start, dsl);
	addComa(dsl);
	encodeStringField(get(root_map, Root::Distinct), "", dsl);
	addComa(dsl);
	encodeStringField(get(root_map, Root::ReqTotal), get(reqtotal_values, query.calcTotal), dsl);
	addComa(dsl);
	encodeStringField(get(root_map, Root::NextOp), get(op_map, query.nextOp_), dsl);

	if (!query.selectFilter_.empty()) addComa(dsl);
	encodeSelectFilter(query, dsl);

	if (!query.selectFunctions_.empty()) addComa(dsl);
	encodeSelectFunctions(query, dsl);

	if (!query.sortingEntries_.empty()) addComa(dsl);
	encodeSorting(query, dsl);

	if (!query.entries.empty()) addComa(dsl);
	encodeFilters(query, dsl);

	if (!query.mergeQueries_.empty()) addComa(dsl);
	encodeMergedQueries(query, dsl);

	if (!query.aggregations_.empty()) addComa(dsl);
	encodeAggregationFunctions(query, dsl);

	if (!query.joinQueries_.empty()) addComa(dsl);
	encodeJoins(query, dsl);

	dsl += rightBracket;
	return dsl;
}

}  // namespace dsl
}  // namespace reindexer
