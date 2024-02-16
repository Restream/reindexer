#include "core/selectfunc/selectfuncparser.h"
#include <ctime>
#include <string>
#include "tools/errors.h"
#include "tools/logger.h"

namespace reindexer {

SelectFuncStruct &SelectFuncParser::Parse(const std::string &query) {
	tokenizer parser(query);

	token tok = parser.next_token(tokenizer::flags::no_flags);

	auto dotPos = tok.text().find('.');
	if (dotPos == std::string_view::npos || (parser.peek_token(tokenizer::flags::no_flags).text() == "=")) {
		selectFuncStruct_.field = std::string(tok.text());
		tok = parser.next_token(tokenizer::flags::no_flags);
		if (tok.text() != "=") {
			if (tok.text() == ".") {
				throw Error(errParams, "Unexpected space symbol before `.` (select function delimiter)");
			}
			throw Error(errParams, "Expected `=` or `.` as a select function delimiter, but found `%s`", tok.text());
		}
		token ftok;
		ParseFunction(parser, false, ftok);
	} else {
		if (dotPos == tok.text_.size() - 1) {
			throw Error(errParams, "Unexpected space symbol or token after `.` (select function delimiter): `%s`", tok.text());
		}
		selectFuncStruct_.field = std::string(tok.text_.begin(), tok.text_.begin() + dotPos);
		token ftok(TokenName);
		ftok.text_.assign(tok.text_.begin() + dotPos + 1, tok.text_.end());
		ParseFunction(parser, false, ftok);
	}

	if (!selectFuncStruct_.isFunction) {
		size_t equalPos = query.find('=');
		selectFuncStruct_.value = query.substr(equalPos + 1);
	}

	return selectFuncStruct_;
}

void SelectFuncParser::parsePositionalAndNamedArgs(tokenizer &parser, const Args &args) {
	using namespace std::string_view_literals;
	token tok;
	tok = parser.next_token(tokenizer::flags::no_flags);
	if (!(tok.type == TokenSymbol && tok.text() == "("sv)) {
		throw Error(errParseDSL, "%s: An open parenthesis is required, but found `%s`", selectFuncStruct_.funcName, tok.text());
	}
	std::string argFirstPart;
	std::string argSecondPart;
	enum class NamedArgState { Name = 0, Eq = 1, Val = 2, End = 3, End2 = 4 };
	NamedArgState expectedToken = NamedArgState::Name;

	while (!parser.end()) {
		tok = parser.next_token(tokenizer::flags::no_flags);
		switch (tok.type) {
			case TokenSymbol:
				if (tok.text() == ")"sv) {
					token nextTok = parser.next_token(tokenizer::flags::no_flags);
					if (nextTok.text().length() > 0) {
						throw Error(errParseDSL, "%s: Unexpected character `%s` after closing parenthesis.", selectFuncStruct_.funcName,
									nextTok.text());
					}

					switch (expectedToken) {
						case NamedArgState::End2:  // add last argument position
							selectFuncStruct_.funcArgs.emplace_back(std::move(argFirstPart));
							argFirstPart.clear();
							if (selectFuncStruct_.funcArgs.size() != args.posArgsCount) {
								throw Error(errParseDSL, "%s: Incorrect count of position arguments. Found %d required %d.",
											selectFuncStruct_.funcName, selectFuncStruct_.funcArgs.size(), args.posArgsCount,
											args.posArgsCount);
							}
							break;
						case NamedArgState::End: {	// add last argument named
							if (selectFuncStruct_.funcArgs.size() != args.posArgsCount) {
								throw Error(errParseDSL, "%s: Incorrect count of position arguments. Found %d required %d.",
											selectFuncStruct_.funcName, selectFuncStruct_.funcArgs.size(), args.posArgsCount,
											args.posArgsCount);
							}
							if (args.namedArgs.find(argFirstPart) == args.namedArgs.end()) {
								throw Error(errParseDSL, "%s: Unknown argument name '%s'.", selectFuncStruct_.funcName, argFirstPart);
							}
							auto r = selectFuncStruct_.namedArgs.emplace(std::move(argFirstPart), "");
							argFirstPart.clear();
							if (!r.second) {
								throw Error(errParseDSL, "%s: Argument already added '%s'.", selectFuncStruct_.funcName, r.first->first);
							}
							r.first->second = std::move(argSecondPart);
							argSecondPart.clear();
						} break;
						case NamedArgState::Name:
						case NamedArgState::Eq:
						case NamedArgState::Val:
							throw Error(errParseDSL, "%s: Unexpected token '%s'.", selectFuncStruct_.funcName, argFirstPart);
					}

					selectFuncStruct_.isFunction = true;
					break;
				} else if (tok.text() == ","sv) {
					if (selectFuncStruct_.funcArgs.size() >= args.posArgsCount) {
						if (expectedToken != NamedArgState::End) {
							throw Error(errParseDSL, "%s: Unexpected token '%s'.", selectFuncStruct_.funcName, tok.text());
						}
						if (args.namedArgs.find(argFirstPart) == args.namedArgs.end()) {
							throw Error(errParseDSL, "%s: Unknown argument name '%s'.", selectFuncStruct_.funcName, argFirstPart);
						}
						auto r = selectFuncStruct_.namedArgs.emplace(std::move(argFirstPart), "");
						if (!r.second) {
							throw Error(errParseDSL, "%s: Argument already added '%s'.", selectFuncStruct_.funcName, r.first->first);
						}
						r.first->second = std::move(argSecondPart);
					} else {  // posible only posArgs
						if (expectedToken != NamedArgState::End2) {
							throw Error(errParseDSL,
										"%s: Unexpected token '%s', expecting positional argument (%d more positional args required)",
										selectFuncStruct_.funcName, tok.text(), args.posArgsCount - selectFuncStruct_.funcArgs.size());
						}
						selectFuncStruct_.funcArgs.emplace_back(std::move(argFirstPart));
					}
					argFirstPart.clear();
					argSecondPart.clear();
					expectedToken = NamedArgState::Name;
				} else {
					throw Error(errParseDSL, "%s: Unexpected token '%s'", selectFuncStruct_.funcName, tok.text());
				}
				break;
			case TokenOp:
				if (tok.text() == "="sv) {
					if (argFirstPart.empty()) {
						throw Error(errParseDSL, "%s: Argument name is empty.", selectFuncStruct_.funcName);
					} else if (expectedToken != NamedArgState::Eq) {
						throw Error(errParseDSL, "%s: Unexpected token '%s'.", selectFuncStruct_.funcName, tok.text());
					}
					expectedToken = NamedArgState::Val;
				} else {
					throw Error(errParseDSL, "%s: Unexpected token '%s'", selectFuncStruct_.funcName, tok.text());
				}
				break;
			case TokenNumber:
			case TokenString:
				switch (expectedToken) {
					case NamedArgState::Val:
						argSecondPart = tok.text();
						expectedToken = NamedArgState::End;
						break;

					case NamedArgState::Name:
						argFirstPart = tok.text();
						expectedToken = NamedArgState::End2;
						break;

					case NamedArgState::Eq:
					case NamedArgState::End:
					case NamedArgState::End2:
						throw Error(errParseDSL, "%s: Unexpected token '%s'.", selectFuncStruct_.funcName, tok.text());
				}
				break;
			case TokenName:
				if (expectedToken != NamedArgState::Name) {
					throw Error(errParseDSL, "%s: Unexpected token '%s'.", selectFuncStruct_.funcName, tok.text());
				} else {
					argFirstPart = tok.text();
					expectedToken = NamedArgState::Eq;
				}
				break;
			case TokenSign:
			case TokenEnd:
				throw Error(errParseDSL, "%s: Unexpected token '%s'", selectFuncStruct_.funcName, tok.text());
		}
	}
	if (!selectFuncStruct_.isFunction) {
		throw Error(errParseDSL, "%s: The closing parenthesis is required, but found `%s`", selectFuncStruct_.funcName, tok.text());
	}
}

SelectFuncStruct &SelectFuncParser::ParseFunction(tokenizer &parser, bool partOfExpression, token &tok) {
	using namespace std::string_view_literals;
	if (tok.text().empty()) {
		tok = parser.next_token();
	}
	if (tok.text() == "snippet") {
		selectFuncStruct_.func = Snippet();
	} else if (tok.text() == "highlight") {
		selectFuncStruct_.func = Highlight();
	} else if (tok.text() == "snippet_n") {
		selectFuncStruct_.func = SnippetN();
		selectFuncStruct_.funcName = std::string(tok.text());
		static const Args args(4, {"pre_delim", "post_delim", "with_area", "left_bound", "right_bound"});
		parsePositionalAndNamedArgs(parser, args);
		return selectFuncStruct_;
	}
	selectFuncStruct_.funcName = std::string(tok.text());

	tok = parser.next_token(tokenizer::flags::no_flags);
	if (tok.text() == "(") {
		std::string arg;
		while (!parser.end()) {
			tok = parser.next_token(tokenizer::flags::no_flags);
			if (tok.text() == ")") {
				if (!partOfExpression) {
					token nextTok = parser.next_token(tokenizer::flags::no_flags);
					if (nextTok.text().length() > 0) {
						throw Error(errParseDSL, "%s: Unexpected character `%s` after closing parenthesis", selectFuncStruct_.funcName,
									nextTok.text());
					}
				}
				selectFuncStruct_.funcArgs.emplace_back(std::move(arg));
				selectFuncStruct_.isFunction = true;
				arg.clear();
				break;
			}
			if (tok.text() == "," && tok.type == TokenSymbol) {
				selectFuncStruct_.funcArgs.emplace_back(std::move(arg));
				arg.clear();
			} else {
				arg += tok.text();
			}
		}
		if (!selectFuncStruct_.isFunction) {
			throw Error(errParseDSL, "%s: The closing parenthesis is required, but found `%s`. Select function name: `%s`",
						selectFuncStruct_.funcName, tok.text(), selectFuncStruct_.funcName);
		}
	} else {
		throw Error(errParseDSL, "%s: An open parenthesis is required, but found `%s`. Select function name: `%s`",
					selectFuncStruct_.funcName, tok.text(), selectFuncStruct_.funcName);
	}

	return selectFuncStruct_;
}

bool SelectFuncParser::IsFunction(std::string_view val) noexcept {
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

bool SelectFuncParser::IsFunction(const VariantArray &val) noexcept {
	if (val.size() != 1) return false;
	if (!val.front().Type().Is<KeyValueType::String>()) return false;
	return IsFunction(static_cast<std::string_view>(val.front()));
}

}  // namespace reindexer
