#include "function_parser.h"
#include <ctime>
#include <string>
#include "estl/tokenizer.h"
#include "tools/errors.h"

namespace reindexer {

using namespace std::string_view_literals;

ParsedQueryFunction QueryFunctionParser::Parse(std::string_view query) {
	tokenizer parser(query);

	token tok = parser.next_token(tokenizer::flags::no_flags);

	ParsedQueryFunction parsedQueryFunction;
	auto dotPos = tok.text().find('.');
	if (dotPos == std::string_view::npos || (parser.peek_token(tokenizer::flags::no_flags).text() == "=")) {
		parsedQueryFunction.field = std::string(tok.text());
		tok = parser.next_token(tokenizer::flags::no_flags);
		if (tok.text() != "="sv) {
			if (tok.text() == "."sv) {
				throw Error(errParams, "Unexpected space symbol before `.` (select function delimiter)");
			}
			throw Error(errParams, "Expected `=` or `.` as a select function delimiter, but found `{}`", tok.text());
		}
		token ftok;
		parseFunction(parser, parsedQueryFunction, ftok);
	} else {
		if (dotPos == tok.text_.size() - 1) {
			throw Error(errParams, "Unexpected space symbol or token after `.` (select function delimiter): `{}`", tok.text());
		}
		parsedQueryFunction.field = std::string(tok.text_.begin(), tok.text_.begin() + dotPos);
		token ftok(TokenName);
		ftok.text_.assign(tok.text_.begin() + dotPos + 1, tok.text_.end());
		parseFunction(parser, parsedQueryFunction, ftok);
	}

	if (!parsedQueryFunction.isFunction) {
		size_t equalPos = query.find('=');
		parsedQueryFunction.value = query.substr(equalPos + 1);
	}

	return parsedQueryFunction;
}

void QueryFunctionParser::parsePositionalAndNamedArgs(tokenizer& parser, ParsedQueryFunction& parsedQueryFunction, const Args& args) {
	token tok;
	tok = parser.next_token(tokenizer::flags::no_flags);
	if (!(tok.type == TokenSymbol && tok.text() == "("sv)) {
		throw Error(errParseDSL, "{}: An open parenthesis is required, but found `{}`", parsedQueryFunction.funcName, tok.text());
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
						throw Error(errParseDSL, "{}: Unexpected character `{}` after closing parenthesis.", parsedQueryFunction.funcName,
									nextTok.text());
					}

					switch (expectedToken) {
						case NamedArgState::End2:  // add last argument position
							parsedQueryFunction.funcArgs.emplace_back(std::move(argFirstPart));
							argFirstPart.clear();
							if (parsedQueryFunction.funcArgs.size() != args.posArgsCount) {
								throw Error(errParseDSL, "{}: Incorrect count of position arguments. Found {} required {}.",
											parsedQueryFunction.funcName, parsedQueryFunction.funcArgs.size(), args.posArgsCount,
											args.posArgsCount);
							}
							break;
						case NamedArgState::End: {	// add last argument named
							if (parsedQueryFunction.funcArgs.size() != args.posArgsCount) {
								throw Error(errParseDSL, "{}: Incorrect count of position arguments. Found {} required {}.",
											parsedQueryFunction.funcName, parsedQueryFunction.funcArgs.size(), args.posArgsCount,
											args.posArgsCount);
							}
							if (args.namedArgs.find(argFirstPart) == args.namedArgs.end()) {
								throw Error(errParseDSL, "{}: Unknown argument name '{}'.", parsedQueryFunction.funcName, argFirstPart);
							}
							auto r = parsedQueryFunction.namedArgs.emplace(std::move(argFirstPart), "");
							argFirstPart.clear();
							if (!r.second) {
								throw Error(errParseDSL, "{}: Argument already added '{}'.", parsedQueryFunction.funcName, r.first->first);
							}
							r.first->second = std::move(argSecondPart);
							argSecondPart.clear();
						} break;
						case NamedArgState::Name:
						case NamedArgState::Eq:
						case NamedArgState::Val:
							throw Error(errParseDSL, "{}: Unexpected token '{}'.", parsedQueryFunction.funcName, argFirstPart);
					}

					parsedQueryFunction.isFunction = true;
					break;
				} else if (tok.text() == ","sv) {
					if (parsedQueryFunction.funcArgs.size() >= args.posArgsCount) {
						if (expectedToken != NamedArgState::End) {
							throw Error(errParseDSL, "{}: Unexpected token '{}'.", parsedQueryFunction.funcName, tok.text());
						}
						if (args.namedArgs.find(argFirstPart) == args.namedArgs.end()) {
							throw Error(errParseDSL, "{}: Unknown argument name '{}'.", parsedQueryFunction.funcName, argFirstPart);
						}
						auto r = parsedQueryFunction.namedArgs.emplace(std::move(argFirstPart), "");
						if (!r.second) {
							throw Error(errParseDSL, "{}: Argument already added '{}'.", parsedQueryFunction.funcName, r.first->first);
						}
						r.first->second = std::move(argSecondPart);
					} else {  // possible only posArgs
						if (expectedToken != NamedArgState::End2) {
							throw Error(errParseDSL,
										"{}: Unexpected token '{}', expecting positional argument ({} more positional args required)",
										parsedQueryFunction.funcName, tok.text(), args.posArgsCount - parsedQueryFunction.funcArgs.size());
						}
						parsedQueryFunction.funcArgs.emplace_back(std::move(argFirstPart));
					}
					argFirstPart.clear();
					argSecondPart.clear();
					expectedToken = NamedArgState::Name;
				} else {
					throw Error(errParseDSL, "{}: Unexpected token '{}'", parsedQueryFunction.funcName, tok.text());
				}
				break;
			case TokenOp:
				if (tok.text() == "="sv) {
					if (argFirstPart.empty()) {
						throw Error(errParseDSL, "{}: Argument name is empty.", parsedQueryFunction.funcName);
					} else if (expectedToken != NamedArgState::Eq) {
						throw Error(errParseDSL, "{}: Unexpected token '{}'.", parsedQueryFunction.funcName, tok.text());
					}
					expectedToken = NamedArgState::Val;
				} else {
					throw Error(errParseDSL, "{}: Unexpected token '{}'", parsedQueryFunction.funcName, tok.text());
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
						throw Error(errParseDSL, "{}: Unexpected token '{}'.", parsedQueryFunction.funcName, tok.text());
				}
				break;
			case TokenName:
				if (expectedToken != NamedArgState::Name) {
					throw Error(errParseDSL, "{}: Unexpected token '{}'.", parsedQueryFunction.funcName, tok.text());
				} else {
					argFirstPart = tok.text();
					expectedToken = NamedArgState::Eq;
				}
				break;
			case TokenSign:
			case TokenEnd:
				throw Error(errParseDSL, "{}: Unexpected token '{}'", parsedQueryFunction.funcName, tok.text());
		}
	}
	if (!parsedQueryFunction.isFunction) {
		throw Error(errParseDSL, "{}: The closing parenthesis is required, but found `{}`", parsedQueryFunction.funcName, tok.text());
	}
}

ParsedQueryFunction QueryFunctionParser::ParseFunction(tokenizer& parser, token& tok) {
	ParsedQueryFunction parsedQueryFunction;
	parseFunctionImpl(parser, parsedQueryFunction, tok);
	return parsedQueryFunction;
}

void QueryFunctionParser::parseFunction(tokenizer& parser, ParsedQueryFunction& parsedQueryFunction, token& tok) {
	parseFunctionImpl(parser, parsedQueryFunction, tok);
	token nextTok = parser.next_token(tokenizer::flags::no_flags);
	if (nextTok.text().length() > 0) {
		throw Error(errParseDSL, "{}: Unexpected character `{}` after closing parenthesis", parsedQueryFunction.funcName, nextTok.text());
	}
}

void QueryFunctionParser::parseFunctionImpl(tokenizer& parser, ParsedQueryFunction& parsedQueryFunction, token& tok) {
	using namespace std::string_view_literals;
	if (tok.text().empty()) {
		tok = parser.next_token();
	}
	parsedQueryFunction.funcName = std::string(tok.text());
	if (tok.text() == "snippet_n"sv) {
		static const Args args(4, {"pre_delim"sv, "post_delim"sv, "with_area"sv, "left_bound"sv, "right_bound"sv});
		parsePositionalAndNamedArgs(parser, parsedQueryFunction, args);
		return;
	}

	tok = parser.next_token(tokenizer::flags::no_flags);
	if (tok.text() == "("sv) {
		if (parser.peek_token(tokenizer::flags::no_flags).text() == ")"sv) {
			parser.next_token(tokenizer::flags::no_flags);
			parsedQueryFunction.isFunction = true;
		} else {
			std::string arg;
			while (!parser.end()) {
				tok = parser.next_token(tokenizer::flags::no_flags);
				if (tok.text() == ")"sv) {
					parsedQueryFunction.funcArgs.emplace_back(std::move(arg));
					parsedQueryFunction.isFunction = true;
					arg.clear();
					break;
				}
				if (tok.text() == ","sv && tok.type == TokenSymbol) {
					parsedQueryFunction.funcArgs.emplace_back(std::move(arg));
					arg.clear();
				} else {
					arg += tok.text();
				}
			}
			if (!parsedQueryFunction.isFunction) {
				throw Error(errParseDSL, "{}: The closing parenthesis is required, but found `{}`", parsedQueryFunction.funcName,
							tok.text());
			}
		}
	} else {
		throw Error(errParseDSL, "{}: An open parenthesis is required, but found `{}`. Select function name: `{}`",
					parsedQueryFunction.funcName, tok.text(), parsedQueryFunction.funcName);
	}

	return;
}

bool QueryFunctionParser::IsFunction(std::string_view val) noexcept {
	if (val.length() < 3) {
		return false;
	}

	size_t i = 0;
	if (!isalpha(val[i++])) {
		return false;
	}

	unsigned openParenthesis = 0;
	for (; i < val.length(); ++i) {
		char ch = val[i];
		switch (ch) {
			case '(':
				if (++openParenthesis != 1) {
					return false;
				}
				break;
			case ')':
				return openParenthesis == 1 && ++i == val.length();
			case ',':
				if (openParenthesis != 1) {
					return false;
				}
				break;
			default:
				break;
		}
	}

	return false;
}

bool QueryFunctionParser::IsFunction(const VariantArray& val) noexcept {
	return val.size() == 1 && val.front().Type().Is<KeyValueType::String>() && IsFunction(static_cast<std::string_view>(val.front()));
}

}  // namespace reindexer
