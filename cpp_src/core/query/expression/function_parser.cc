#include "function_parser.h"
#include <ctime>
#include <string>
#include "estl/tokenizer.h"
#include "tools/errors.h"

namespace reindexer {

using namespace std::string_view_literals;

ParsedQueryFunction QueryFunctionParser::Parse(std::string_view query) {
	Tokenizer parser(query);

	Token tok = parser.NextToken(Tokenizer::Flags::NoFlags);

	ParsedQueryFunction parsedQueryFunction;
	auto dotPos = tok.Text().find('.');
	if (dotPos == std::string_view::npos || (parser.PeekToken(Tokenizer::Flags::NoFlags).Text() == "=")) {
		parsedQueryFunction.field = std::string(tok.Text());
		tok = parser.NextToken(Tokenizer::Flags::NoFlags);
		if (tok.Text() != "="sv) {
			if (tok.Text() == "."sv) {
				throw Error(errParams, "Unexpected space symbol before `.` (select function delimiter)");
			}
			throw Error(errParams, "Expected `=` or `.` as a select function delimiter, but found `{}`", tok.Text());
		}
		Token ftok;
		parseFunction(parser, parsedQueryFunction, ftok);
	} else {
		if (dotPos == tok.Text().size() - 1) {
			throw Error(errParams, "Unexpected space symbol or token after `.` (select function delimiter): `{}`", tok.Text());
		}
		parsedQueryFunction.field = std::string(tok.Text().begin(), tok.Text().begin() + dotPos);
		Token ftok(TokenName, tok.Text().begin() + dotPos + 1, tok.Text().end());
		parseFunction(parser, parsedQueryFunction, ftok);
	}

	if (!parsedQueryFunction.isFunction) {
		size_t equalPos = query.find('=');
		parsedQueryFunction.value = query.substr(equalPos + 1);
	}

	return parsedQueryFunction;
}

void QueryFunctionParser::parsePositionalAndNamedArgs(Tokenizer& parser, ParsedQueryFunction& parsedQueryFunction, const Args& args) {
	Token tok;
	tok = parser.NextToken(Tokenizer::Flags::NoFlags);
	if (!(tok.Type() == TokenSymbol && tok.Text() == "("sv)) {
		throw Error(errParseDSL, "{}: An open parenthesis is required, but found `{}`", parsedQueryFunction.funcName, tok.Text());
	}
	std::string argFirstPart;
	std::string argSecondPart;
	enum class [[nodiscard]] NamedArgState { Name = 0, Eq = 1, Val = 2, End = 3, End2 = 4 };
	NamedArgState expectedToken = NamedArgState::Name;

	while (!parser.End()) {
		tok = parser.NextToken(Tokenizer::Flags::NoFlags);
		switch (tok.Type()) {
			case TokenSymbol:
				if (tok.Text() == ")"sv) {
					Token nextTok = parser.NextToken(Tokenizer::Flags::NoFlags);
					if (nextTok.Text().length() > 0) {
						throw Error(errParseDSL, "{}: Unexpected character `{}` after closing parenthesis.", parsedQueryFunction.funcName,
									nextTok.Text());
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
				} else if (tok.Text() == ","sv) {
					if (parsedQueryFunction.funcArgs.size() >= args.posArgsCount) {
						if (expectedToken != NamedArgState::End) {
							throw Error(errParseDSL, "{}: Unexpected token '{}'.", parsedQueryFunction.funcName, tok.Text());
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
										parsedQueryFunction.funcName, tok.Text(), args.posArgsCount - parsedQueryFunction.funcArgs.size());
						}
						parsedQueryFunction.funcArgs.emplace_back(std::move(argFirstPart));
					}
					argFirstPart.clear();
					argSecondPart.clear();
					expectedToken = NamedArgState::Name;
				} else {
					throw Error(errParseDSL, "{}: Unexpected token '{}'", parsedQueryFunction.funcName, tok.Text());
				}
				break;
			case TokenOp:
				if (tok.Text() == "="sv) {
					if (argFirstPart.empty()) {
						throw Error(errParseDSL, "{}: Argument name is empty.", parsedQueryFunction.funcName);
					} else if (expectedToken != NamedArgState::Eq) {
						throw Error(errParseDSL, "{}: Unexpected token '{}'.", parsedQueryFunction.funcName, tok.Text());
					}
					expectedToken = NamedArgState::Val;
				} else {
					throw Error(errParseDSL, "{}: Unexpected token '{}'", parsedQueryFunction.funcName, tok.Text());
				}
				break;
			case TokenNumber:
			case TokenString:
				switch (expectedToken) {
					case NamedArgState::Val:
						argSecondPart = tok.Text();
						expectedToken = NamedArgState::End;
						break;

					case NamedArgState::Name:
						argFirstPart = tok.Text();
						expectedToken = NamedArgState::End2;
						break;

					case NamedArgState::Eq:
					case NamedArgState::End:
					case NamedArgState::End2:
						throw Error(errParseDSL, "{}: Unexpected token '{}'.", parsedQueryFunction.funcName, tok.Text());
				}
				break;
			case TokenName:
				if (expectedToken != NamedArgState::Name) {
					throw Error(errParseDSL, "{}: Unexpected token '{}'.", parsedQueryFunction.funcName, tok.Text());
				} else {
					argFirstPart = tok.Text();
					expectedToken = NamedArgState::Eq;
				}
				break;
			case TokenSign:
			case TokenEnd:
				throw Error(errParseDSL, "{}: Unexpected token '{}'", parsedQueryFunction.funcName, tok.Text());
		}
	}
	if (!parsedQueryFunction.isFunction) {
		throw Error(errParseDSL, "{}: The closing parenthesis is required, but found `{}`", parsedQueryFunction.funcName, tok.Text());
	}
}

ParsedQueryFunction QueryFunctionParser::ParseFunction(Tokenizer& parser, Token& tok) {
	ParsedQueryFunction parsedQueryFunction;
	parseFunctionImpl(parser, parsedQueryFunction, tok);
	return parsedQueryFunction;
}

void QueryFunctionParser::parseFunction(Tokenizer& parser, ParsedQueryFunction& parsedQueryFunction, Token& tok) {
	parseFunctionImpl(parser, parsedQueryFunction, tok);
	Token nextTok = parser.NextToken(Tokenizer::Flags::NoFlags);
	if (nextTok.Text().length() > 0) {
		throw Error(errParseDSL, "{}: Unexpected character `{}` after closing parenthesis", parsedQueryFunction.funcName, nextTok.Text());
	}
}

void QueryFunctionParser::parseFunctionImpl(Tokenizer& parser, ParsedQueryFunction& parsedQueryFunction, Token& tok) {
	using namespace std::string_view_literals;
	if (tok.Text().empty()) {
		tok = parser.NextToken();
	}
	parsedQueryFunction.funcName = std::string(tok.Text());
	if (tok.Text() == "snippet_n"sv) {
		static const Args args(4, {"pre_delim"sv, "post_delim"sv, "with_area"sv, "left_bound"sv, "right_bound"sv});
		parsePositionalAndNamedArgs(parser, parsedQueryFunction, args);
		return;
	}

	tok = parser.NextToken(Tokenizer::Flags::NoFlags);
	if (tok.Text() == "("sv) {
		if (parser.PeekToken(Tokenizer::Flags::NoFlags).Text() == ")"sv) {
			parser.SkipToken(Tokenizer::Flags::NoFlags);
			parsedQueryFunction.isFunction = true;
		} else {
			std::string arg;
			while (!parser.End()) {
				tok = parser.NextToken(Tokenizer::Flags::NoFlags);
				if (tok.Text() == ")"sv) {
					parsedQueryFunction.funcArgs.emplace_back(std::move(arg));
					parsedQueryFunction.isFunction = true;
					arg.clear();
					break;
				}
				if (tok.Text() == ","sv && tok.Type() == TokenSymbol) {
					parsedQueryFunction.funcArgs.emplace_back(std::move(arg));
					arg.clear();
				} else {
					arg += tok.Text();
				}
			}
			if (!parsedQueryFunction.isFunction) {
				throw Error(errParseDSL, "{}: The closing parenthesis is required, but found `{}`", parsedQueryFunction.funcName,
							tok.Text());
			}
		}
	} else {
		throw Error(errParseDSL, "{}: An open parenthesis is required, but found `{}`. Select function name: `{}`",
					parsedQueryFunction.funcName, tok.Text(), parsedQueryFunction.funcName);
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
