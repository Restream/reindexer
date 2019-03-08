#include "querywhere.h"
#include <stdlib.h>
#include "core/keyvalue/key_string.h"
#include "estl/tokenizer.h"
#include "tools/errors.h"
#include "tools/serializer.h"
#include "tools/stringstools.h"

namespace reindexer {

bool QueryEntry::operator==(const QueryEntry &obj) const {
	if (op != obj.op) return false;
	if (condition != obj.condition) return false;
	if (index != obj.index) return false;
	if (idxNo != obj.idxNo) return false;
	if (distinct != obj.distinct) return false;
	if (values != obj.values) return false;
	return true;
}

bool QueryEntry::operator!=(const QueryEntry &obj) const { return !operator==(obj); }

bool QueryJoinEntry::operator==(const QueryJoinEntry &obj) const {
	if (op_ != obj.op_) return false;
	if (condition_ != obj.condition_) return false;
	if (index_ != obj.index_) return false;
	if (joinIndex_ != obj.joinIndex_) return false;
	if (idxNo != obj.idxNo) return false;
	return true;
}

bool AggregateEntry::operator==(const AggregateEntry &obj) const {
	if (index_ != obj.index_) return false;
	if (type_ != obj.type_) return false;
	return true;
}

bool AggregateEntry::operator!=(const AggregateEntry &obj) const { return !operator==(obj); }

bool SortingEntry::operator==(const SortingEntry &obj) const {
	if (column != obj.column) return false;
	if (desc != obj.desc) return false;
	if (index != obj.index) return false;
	return true;
}

bool SortingEntry::operator!=(const SortingEntry &obj) const { return !operator==(obj); }

bool QueryWhere::operator==(const QueryWhere &obj) const {
	if (entries != obj.entries) return false;
	if (aggregations_ != obj.aggregations_) return false;
	if (joinEntries_ != obj.joinEntries_) return false;
	return true;
}

CondType QueryWhere::getCondType(string_view cond) {
	if (cond == "="_sv || cond == "=="_sv || cond == "is"_sv) {
		return CondEq;
	} else if (cond == ">"_sv) {
		return CondGt;
	} else if (cond == ">="_sv) {
		return CondGe;
	} else if (cond == "<"_sv) {
		return CondLt;
	} else if (cond == "<="_sv) {
		return CondLe;
	} else if (iequals(cond, "in"_sv)) {
		return CondSet;
	} else if (iequals(cond, "range"_sv)) {
		return CondRange;
	}
	throw Error(errParseSQL, "Expected condition operator, but found '%s' in query", cond);
}

const char *condNames[] = {"IS NOT NULL", "=", "<", "<=", ">", "=>", "RANGE", "IN", "ALLSET", "IS NULL"};
const char *opNames[] = {"-", "OR", "AND", "AND NOT"};

void QueryWhere::dumpWhere(WrSerializer &ser, bool stripArgs) const {
	if (entries.size()) ser << " WHERE";

	for (auto &e : entries) {
		if (&e != &*entries.begin() && unsigned(e.op) < sizeof(opNames) / sizeof(opNames[0])) {
			ser << " " << opNames[e.op];
		} else if (&e == &*entries.begin() && e.op == OpNot) {
			ser << " NOT";
		}
		ser << " " << e.index << " ";
		if (e.condition < sizeof(condNames) / sizeof(condNames[0]))
			ser << condNames[e.condition] << " ";
		else
			ser << "<unknown cond> ";
		if (e.condition == CondEmpty || e.condition == CondAny) {
		} else if (stripArgs) {
			ser << '?';
		} else {
			if (e.values.size() > 1) ser << '(';
			for (auto &v : e.values) {
				if (&v != &*e.values.begin()) ser << ',';
				if (v.Type() == KeyValueString)
					ser << '\'' << v.As<string>() << '\'';
				else
					ser << v.As<string>();
			}
			ser << ((e.values.size() > 1) ? ")" : "");
		}
	}
}

string QueryEntry::Dump() const {
	string result;
	if (distinct) {
		result = "Distinct index: " + index;
	} else {
		switch (op) {
			case OpOr:
				result = "Or";
				break;
			case OpAnd:
				result = "And";
				break;
			case OpNot:
				result = "Not";
				break;
			default:
				break;
		}
		result += " ";
		result += index;
		result += " ";

		if (condition < sizeof(condNames) / sizeof(condNames[0])) result += string(condNames[condition]) + " ";

		bool severalValues = (values.size() > 1);
		if (severalValues) result += "(";
		for (auto &v : values) {
			if (&v != &*values.begin()) result += ",";
			result += "'" + v.As<string>() + "'";
		}
		result += (severalValues) ? ") " : " ";
	}
	return result;
}

}  // namespace reindexer
