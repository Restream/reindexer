#include "query/query.h"
#include "cbinding/reindexer_ctypes.h"
#include "query/tokenizer.h"
#include "tools/errors.h"
#include "tools/logger.h"

namespace reindexer {

Query::Query(const string &__namespace, int _start, int _count, bool _calcTotal)
	: _namespace(__namespace), calcTotal(_calcTotal), start(_start), count(_count) {}

int Query::Parse(const string &q) {
	tokenizer parser(q);
	return Parse(parser);
}

int Query::Parse(tokenizer &parser) {
	token tok = parser.next_token();

	if (tok.text != "select") throw Error(errParseSQL, "Expected 'SELECT', but found '%s' in query", tok.text.c_str());

	// Just skip token
	tok = parser.next_token();

	if (parser.next_token().text != "from") throw Error(errParams, "Expected 'FROM', but found '%s' in query", tok.text.c_str());

	_namespace = parser.next_token().text;
	parser.skip_space();

	while (!parser.end()) {
		tok = parser.next_token();
		if (tok.text == "where") {
			ParseWhere(parser);
		} else if (tok.text == "limit") {
			tok = parser.next_token();
			if (tok.type != TokenNumber) return -1;
			count = stoi(tok.text);
		} else if (tok.text == "offset") {
			tok = parser.next_token();
			if (tok.type != TokenNumber) return -1;
			start = stoi(tok.text);
		} else if (tok.text == "order") {
			// Just skip token (BY)
			parser.next_token();
			tok = parser.next_token();
			if (tok.type != TokenName) throw Error(errParseSQL, "Expected name, but found '%s' in query", tok.text.c_str());
			sortBy = tok.text;
			tok = parser.peek_token();
			if (tok.text == "asc" || tok.text == "desc") {
				sortDirDesc = bool(tok.text == "desc");
				parser.next_token();
			}
		} else {
			throw Error(errParseSQL, "Unexpected '%s' in query", tok.text.c_str());
		}
	}
	return 0;
}

string Query::DumpJoined() const {
	extern const char *condNames[];
	string ret;
	for (auto &je : joinQueries_) {
		switch (je.joinType) {
			case InnerJoin:
				ret += "INNER JOIN ";
				break;
			case OrInnerJoin:
				ret += "OR INNER JOIN ";
				break;
			case LeftJoin:
				ret += "LEFT JOIN ";
				break;
		}
		ret += je._namespace + " ON ";
		for (auto &e : je.joinEntries_) {
			if (&e != &*je.joinEntries_.begin()) ret += "AND ";
			ret += je._namespace + "." + e.joinIndex_ + " " + condNames[e.condition_] + " " + _namespace + "." + e.index_ + " ";
		}
		ret += je.QueryWhere::toString();
	}

	return ret;
}

void Query::Dump() const {
	string lim;
	if (start != 0) lim += "OFFSET " + to_string(start);
	if (count != 0) lim += "LIMIT " + to_string(count);

	logPrintf(LogInfo, "SELECT * FROM %s %s%s%s%s%s%s", _namespace.c_str(), QueryWhere::toString().c_str(), DumpJoined().c_str(),
			  sortBy.length() ? (string("ORDER BY ") + sortBy).c_str() : "", sortDirDesc ? " DESC" : "", lim.c_str(),
			  calcTotal ? " REQTOTAL" : "");
}

}  // namespace reindexer