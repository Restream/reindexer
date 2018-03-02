#include "querywhere.h"
#include "estl/tokenizer.h"
#include "tools/errors.h"
#include "tools/logger.h"

namespace reindexer {

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
			throw Error(errParseSQL, "Found '(' - nestqed queries are not supported\n");

		} else if (tok.type == TokenName || tok.type == TokenString) {
			// Index name
			entry.index = tok.text;

			// Operator
			tok = parser.next_token();
			const string op = tok.text;

			if (op == "=" || op == "==" || op == "is") {
				entry.condition = CondEq;
			} else if (op == ">") {
				entry.condition = CondGt;
			} else if (op == ">=") {
				entry.condition = CondGe;
			} else if (op == "<") {
				entry.condition = CondLt;
			} else if (op == "<=") {
				entry.condition = CondLe;
			} else if (op == "in") {
				entry.condition = CondSet;
			} else if (op == "<>") {
				entry.condition = CondEq;
				if (entry.op == OpAnd)
					entry.op = OpNot;
				else if (entry.op == OpNot)
					entry.op = OpAnd;
				else {
					throw Error(errParseSQL, "<> condition with OR is not supported");
				}
			} else {
				throw Error(errParseSQL, "Expected condition operator, but found '%s' in query", tok.text.c_str());
			}

			// Value
			tok = parser.next_token();
			if (tok.text == "null") {
				entry.condition = CondEmpty;
			} else if (tok.text == "(") {
				for (;;) {
					tok = parser.next_token();
					if (tok.type != TokenNumber && tok.type != TokenString)
						throw Error(errParseSQL, "Expected parameter, but found '%s' in query", tok.text.c_str());
					entry.values.push_back(KeyValue(tok.text));
					tok = parser.next_token();
					if (tok.text == ")") break;
					if (tok.text != ",") throw Error(errParseSQL, "Expected ')' or ',', but found '%s' in query", tok.text.c_str());
				}
			} else {
				if (tok.type != TokenNumber && tok.type != TokenString)
					throw Error(errParseSQL, "Expected parameter, but found %s in query", tok.text.c_str());
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

string QueryWhere::toString() const {
	string res;
	if (entries.size()) res = joinEntries_.size() ? "AND " : "WHERE ";

	for (auto &e : entries) {
		if (&e != &*entries.begin()) res += (e.op == OpOr) ? "OR " : (e.op == OpAnd) ? "AND " : (e.op == OpNot) ? "AND NOT " : " ";
		res += e.index + " ";
		if (e.condition < sizeof(condNames) / sizeof(condNames[0]))
			res += string(condNames[e.condition]) + " ";
		else
			res += "? ";
		if (e.values.size() > 1) res += "(";
		for (auto &v : e.values) {
			if (&v != &*e.values.begin()) res += ",";
			res += "'" + v.As<string>() + "'";
		}
		res += (e.values.size() > 1) ? ") " : " ";
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
