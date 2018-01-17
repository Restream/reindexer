#include "core/sqlfunc/sqlfunc.h"
#include <ctime>
#include <string>
#include <vector>
#include "tools/errors.h"
#include "tools/logger.h"

using std::string;
using std::vector;
using std::next;
using std::move;

namespace reindexer {

const SqlFuncStruct &SqlFunc::Parse(string query) {
	tokenizer parser(query);

	token tok = parser.next_token(false);

	sqlFuncStruct_.field = tok.text;

	tok = parser.next_token(false);
	if (tok.text != "=") {
		throw Error(errParams, "`=` is expected, but found `%s`", tok.text.c_str());
	}

	parseFunction(parser);

	if (!sqlFuncStruct_.isFunction) {
		size_t equalPos = query.find('=');
		sqlFuncStruct_.value = query.substr(equalPos + 1);
	}

	return sqlFuncStruct_;
}

void SqlFunc::parseFunction(tokenizer &parser) {
	token tok = parser.next_token(true);
	sqlFuncStruct_.funcName = tok.text;

	tok = parser.next_token(false);
	if (tok.text == "(") {
		while (!parser.end()) {
			tok = parser.next_token(false);
			if (tok.text == ")") {
				token nextTok = parser.next_token(false);
				if (nextTok.text.length()) {
					throw Error(errParseDSL, "Unexpected character `%s` after close parenthesis", nextTok.text.c_str());
				}

				sqlFuncStruct_.isFunction = true;
				break;
			}

			if (tok.text == ",") {
				continue;
			}

			sqlFuncStruct_.funcArgs.push_back(tok.text);
		}
	} else {
		throw Error(errParseDSL, "An open parenthesis is required, but found `%s`", tok.text.c_str());
	}
}

}  // namespace reindexer
