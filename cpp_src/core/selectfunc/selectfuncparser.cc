#include "core/selectfunc/selectfuncparser.h"
#include <ctime>
#include <string>
#include <vector>
#include "tools/errors.h"
#include "tools/logger.h"

using std::move;
using std::next;
using std::string;
using std::vector;

namespace reindexer {

SelectFuncStruct &SelectFuncParser::Parse(string query) {
	tokenizer parser(query);

	token tok = parser.next_token(false);

	selectFuncStruct_.field = tok.text().ToString();

	tok = parser.next_token(false);
	if (tok.text() != "=" && tok.text() != ".") {
		throw Error(errParams, "`=` or '.' is expected, but found `%s`", tok.text().data());
	}

	parseFunction(parser);

	if (!selectFuncStruct_.isFunction) {
		size_t equalPos = query.find('=');
		selectFuncStruct_.value = query.substr(equalPos + 1);
	}

	return selectFuncStruct_;
}

void SelectFuncParser::parseFunction(tokenizer &parser) {
	token tok = parser.next_token(true);
	if (tok.text() == "snippet") {
		selectFuncStruct_.type = SelectFuncStruct::kSelectFuncSnippet;
	} else if (tok.text() == "highlight") {
		selectFuncStruct_.type = SelectFuncStruct::kSelectFuncHighlight;
	}
	selectFuncStruct_.funcName = tok.text().ToString();

	tok = parser.next_token(false);
	if (tok.text() == "(") {
		string agr;
		while (!parser.end()) {
			tok = parser.next_token(false);
			if (tok.text() == ")") {
				token nextTok = parser.next_token(false);
				if (nextTok.text().length()) {
					throw Error(errParseDSL, "Unexpected character `%s` after close parenthesis", nextTok.text().data());
				}
				selectFuncStruct_.funcArgs.push_back(agr);

				selectFuncStruct_.isFunction = true;
				break;
			}

			if (tok.text() == ",") {
				selectFuncStruct_.funcArgs.push_back(agr);
				agr.clear();

			} else {
				agr += tok.text().ToString();
			}
		}
	} else {
		throw Error(errParseDSL, "An open parenthesis is required, but found `%s`", tok.text().data());
	}
}

}  // namespace reindexer
