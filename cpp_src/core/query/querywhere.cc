#include "querywhere.h"
#include "estl/tokenizer.h"
#include "tools/errors.h"

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

bool QueryWhere::operator==(const QueryWhere &obj) const {
	if (entries != obj.entries) return false;
	if (aggregations_ != obj.aggregations_) return false;
	if (joinEntries_ != obj.joinEntries_) return false;
	return true;
}

CondType QueryWhere::getCondType(const string &cond) {
	if (cond == "=" || cond == "==" || cond == "is") {
		return CondEq;
	} else if (cond == ">") {
		return CondGt;
	} else if (cond == ">=") {
		return CondGe;
	} else if (cond == "<") {
		return CondLt;
	} else if (cond == "<=") {
		return CondLe;
	} else if (cond == "in") {
		return CondSet;
	} else if (cond == "range") {
		return CondRange;
	}
	throw Error(errParseSQL, "Expected condition operator, but found '%s' in query", cond.c_str());
}

int QueryWhere::ParseWhere(tokenizer &parser) {
	token tok;
	OpType nextOp = OpAnd;

	tok = parser.peek_token();

	if (tok.text == "not") {
		nextOp = OpNot;
		parser.next_token();
	}

	while (!parser.end()) {
		QueryEntry entry;
		entry.op = nextOp;
		// Just skip token.
		tok = parser.next_token();

		if (tok.text == "(") {
			throw Error(errParseSQL, "Found '(' - nestqed queries are not supported, %s", parser.where().c_str());

		} else if (tok.type == TokenName || tok.type == TokenString) {
			// Index name
			entry.index = tok.text;

			// Operator
			tok = parser.next_token();

			if (tok.text == "<>") {
				entry.condition = CondEq;
				if (entry.op == OpAnd)
					entry.op = OpNot;
				else if (entry.op == OpNot)
					entry.op = OpAnd;
				else {
					throw Error(errParseSQL, "<> condition with OR is not supported, %s", parser.where().c_str());
				}
			} else {
				entry.condition = getCondType(tok.text);
			}
			// Value
			tok = parser.next_token();
			if (tok.text == "null") {
				entry.condition = CondEmpty;
			} else if (tok.text == "(") {
				for (;;) {
					tok = parser.next_token();
					if (tok.type != TokenNumber && tok.type != TokenString)
						throw Error(errParseSQL, "Expected parameter, but found '%s' in query, %s", tok.text.c_str(),
									parser.where().c_str());
					entry.values.push_back(KeyValue(tok.text));
					tok = parser.next_token();
					if (tok.text == ")") break;
					if (tok.text != ",")
						throw Error(errParseSQL, "Expected ')' or ',', but found '%s' in query, %s", tok.text.c_str(),
									parser.where().c_str());
				}
			} else {
				if (tok.type != TokenNumber && tok.type != TokenString)
					throw Error(errParseSQL, "Expected parameter, but found %s in query, %s", tok.text.c_str(), parser.where().c_str());
				entry.values.push_back(KeyValue(tok.text));
			}
		}
		// Push back parsed entry
		entries.push_back(entry);

		tok = parser.peek_token();

		if (tok.text == "and") {
			nextOp = OpAnd;
			parser.next_token();
			tok = parser.peek_token();
			if (tok.text == "not") {
				nextOp = OpNot;
			} else
				continue;
		} else if (tok.text == "or") {
			nextOp = OpOr;
		} else
			break;

		parser.next_token();
	}
	return 0;
}

const char *condNames[] = {"ANY", "=", "<", "<=", ">", "=>", "RANGE", "IN", "ALLSET", "EMPTY"};
const char *opNames[] = {"-", "OR", "AND", "AND NOT"};

string QueryWhere::toString() const {
	string res;
	if (entries.size()) res = " WHERE";

	for (auto &e : entries) {
		if (&e != &*entries.begin() && unsigned(e.op) < sizeof(opNames) / sizeof(opNames[0])) {
			res += " " + string(opNames[e.op]);
		}
		res += " " + e.index + " ";
		if (e.condition < sizeof(condNames) / sizeof(condNames[0]))
			res += string(condNames[e.condition]) + " ";
		else
			res += "<unknown cond> ";
		if (e.values.size() > 1) res += "(";
		for (auto &v : e.values) {
			if (&v != &*e.values.begin()) res += ",";
			res += "'" + v.As<string>() + "'";
		}
		res += (e.values.size() > 1) ? ")" : "";
	}

	return res;
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
