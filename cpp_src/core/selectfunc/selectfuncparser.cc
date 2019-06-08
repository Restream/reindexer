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

	selectFuncStruct_.field = string(tok.text());

	tok = parser.next_token(false);
	if (tok.text() != "=" && tok.text() != ".") {
		throw Error(errParams, "`=` or '.' is expected, but found `%s`", tok.text());
	}

	ParseFunction(parser, false);

	if (!selectFuncStruct_.isFunction) {
		size_t equalPos = query.find('=');
		selectFuncStruct_.value = query.substr(equalPos + 1);
	}

	return selectFuncStruct_;
}

SelectFuncStruct &SelectFuncParser::ParseFunction(tokenizer &parser, bool partOfExpression) {
	token tok = parser.next_token(true);
	if (tok.text() == "snippet") {
		selectFuncStruct_.type = SelectFuncStruct::kSelectFuncSnippet;
	} else if (tok.text() == "highlight") {
		selectFuncStruct_.type = SelectFuncStruct::kSelectFuncHighlight;
	}
	selectFuncStruct_.funcName = string(tok.text());

	tok = parser.next_token(false);
	if (tok.text() == "(") {
		string agr;
		while (!parser.end()) {
			tok = parser.next_token(false);
			if (tok.text() == ")") {
				if (!partOfExpression) {
					token nextTok = parser.next_token(false);
					if (nextTok.text().length() > 0) {
						throw Error(errParseDSL, "Unexpected character `%s` after close parenthesis", nextTok.text());
					}
				}
				selectFuncStruct_.funcArgs.push_back(agr);
				selectFuncStruct_.isFunction = true;
				break;
			}
			if (tok.text() == ",") {
				selectFuncStruct_.funcArgs.push_back(agr);
				agr.clear();
			} else {
				agr += string(tok.text());
			}
		}
	} else {
		throw Error(errParseDSL, "An open parenthesis is required, but found `%s`", tok.text());
	}

	return selectFuncStruct_;
}

bool SelectFuncParser::IsFunction(const string_view &val) {
	if (val.length() < 3) return false;

	size_t i = 0;
	if (!isalpha(val[i++])) return false;

	int openParenthesis = 0, closeParenthesis = 0;
	for (; i < val.length(); ++i) {
		char ch = val[i];
		switch (ch) {
			case '(':
				if (openParenthesis++ > 0) return false;
				if (closeParenthesis > 0) return false;
				break;
			case ')':
				if (openParenthesis != 1) return false;
				if (closeParenthesis++ > 0) return false;
				if (i == val.length() - 1) return true;
				break;
			case ',':
				if (openParenthesis != 1) return false;
				if (closeParenthesis != 0) return false;
				if (i == val.length() - 1) return false;
				break;
			default:
				if (openParenthesis > 1) return false;
				if (closeParenthesis > 0) return false;
				break;
		}
	}

	return false;
}

bool SelectFuncParser::IsFunction(const VariantArray &val) {
	if (val.size() != 1) return false;
	if (val.front().Type() != KeyValueString) return false;
	return IsFunction(static_cast<string_view>(val.front()));
}

}  // namespace reindexer
