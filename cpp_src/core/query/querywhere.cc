#include "querywhere.h"
#include "tools/serializer.h"
#include "tools/stringstools.h"

namespace reindexer {

bool QueryEntry::operator==(const QueryEntry &obj) const {
	if (condition != obj.condition) return false;
	if (index != obj.index) return false;
	if (idxNo != obj.idxNo) return false;
	if (distinct != obj.distinct) return false;
	if (values != obj.values) return false;
	return true;
}

bool QueryEntry::operator!=(const QueryEntry &obj) const { return !operator==(obj); }

template <typename T>
EqualPosition QueryEntries::DetermineEqualPositionIndexes(unsigned start, const T &fields) const {
	if (fields.size() < 2) throw Error(errLogic, "Amount of fields with equal index position should be 2 or more!");
	EqualPosition result;
	for (size_t i = start; i < Size(); i = Next(i)) {
		if (!container_[i]->IsLeaf()) continue;
		for (const auto &f : fields) {
			if (container_[i]->Value().index == f) {
				result.push_back(i);
				break;
			}
		}
	}
	return result;
}

template <typename T>
std::pair<unsigned, EqualPosition> QueryEntries::DetermineEqualPositionIndexes(const T &fields) const {
	const unsigned start = activeBrackets_.empty() ? 0u : activeBrackets_.back() + 1u;
	return {start, DetermineEqualPositionIndexes(start, fields)};
}

// Explicit instantiations
template EqualPosition QueryEntries::DetermineEqualPositionIndexes<vector<string>>(unsigned, const vector<string> &) const;
template std::pair<unsigned, EqualPosition> QueryEntries::DetermineEqualPositionIndexes<vector<string>>(const vector<string> &) const;
template std::pair<unsigned, EqualPosition> QueryEntries::DetermineEqualPositionIndexes<h_vector<string, 4>>(
	const h_vector<string, 4> &) const;
template std::pair<unsigned, EqualPosition> QueryEntries::DetermineEqualPositionIndexes<std::initializer_list<string>>(
	const std::initializer_list<string> &) const;

void QueryEntries::serialize(const_iterator it, const_iterator to, WrSerializer &ser) {
	for (; it != to; ++it) {
		if (it->IsLeaf()) {
			const QueryEntry &entry = it->Value();
			entry.distinct ? ser.PutVarUint(QueryDistinct) : ser.PutVarUint(QueryCondition);
			ser.PutVString(entry.index);
			if (entry.distinct) continue;
			ser.PutVarUint(it->Op);
			ser.PutVarUint(entry.condition);
			ser.PutVarUint(entry.values.size());
			for (auto &kv : entry.values) ser.PutVariant(kv);
		} else {
			ser.PutVarUint(QueryOpenBracket);
			ser.PutVarUint(it->Op);
			serialize(it->cbegin(it), it->cend(it), ser);
			ser.PutVarUint(QueryCloseBracket);
		}
	}
}

bool UpdateEntry::operator==(const UpdateEntry &obj) const {
	if (column != obj.column) return false;
	if (values != obj.values) return false;
	if (isExpression != obj.isExpression) return false;
	return true;
}

bool UpdateEntry::operator!=(const UpdateEntry &obj) const { return !operator==(obj); }

bool QueryJoinEntry::operator==(const QueryJoinEntry &obj) const {
	if (op_ != obj.op_) return false;
	if (condition_ != obj.condition_) return false;
	if (index_ != obj.index_) return false;
	if (joinIndex_ != obj.joinIndex_) return false;
	if (idxNo != obj.idxNo) return false;
	return true;
}

bool AggregateEntry::operator==(const AggregateEntry &obj) const {
	return fields_ == obj.fields_ && type_ == obj.type_ && sortingEntries_ == obj.sortingEntries_ && limit_ == obj.limit_ &&
		   offset_ == obj.offset_;
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
	} else if (iequals(cond, "like"_sv)) {
		return CondLike;
	}
	throw Error(errParseSQL, "Expected condition operator, but found '%s' in query", cond);
}

const char *condNames[] = {"IS NOT NULL", "=", "<", "<=", ">", ">=", "RANGE", "IN", "ALLSET", "IS NULL", "LIKE"};
const char *opNames[] = {"-", "OR", "AND", "AND NOT"};

void QueryEntries::WriteSQLWhere(WrSerializer &ser, bool stripArgs) const {
	if (Empty()) return;
	ser << " WHERE ";
	writeSQL(cbegin(), cend(), ser, stripArgs);
}

void QueryEntries::writeSQL(const_iterator from, const_iterator to, WrSerializer &ser, bool stripArgs) {
	for (const_iterator it = from; it != to; ++it) {
		if (it != from) {
			ser << ' ';
			if (unsigned(it->Op) < sizeof(opNames) / sizeof(opNames[0])) ser << opNames[it->Op] << ' ';  // -V547
		} else if (it->Op == OpNot) {
			ser << "NOT ";
		}
		if (!it->IsLeaf()) {
			ser << '(';
			writeSQL(it->cbegin(it), it->cend(it), ser, stripArgs);
			ser << ')';
		} else {
			const QueryEntry &entry = it->Value();
			if (entry.index.find('.') == string::npos)
				ser << entry.index << ' ';
			else
				ser << '\'' << entry.index << "\' ";

			if (entry.condition < sizeof(condNames) / sizeof(condNames[0]))
				ser << condNames[entry.condition] << ' ';
			else
				ser << "<unknown cond> ";
			if (entry.condition == CondEmpty || entry.condition == CondAny) {
			} else if (stripArgs) {
				ser << '?';
			} else {
				if (entry.values.size() != 1) ser << '(';
				for (auto &v : entry.values) {
					if (&v != &entry.values[0]) ser << ',';
					if (v.Type() == KeyValueString) {
						ser << '\'' << v.As<string>() << '\'';
					} else {
						ser << v.As<string>();
					}
				}
				ser << ((entry.values.size() != 1) ? ")" : "");
			}
		}
	}
}

string QueryEntry::Dump() const {
	string result;
	if (distinct) {
		result = "Distinct index: " + index;
	} else {
		result = index + ' ';

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
