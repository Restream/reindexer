#include "core/function/expression_ast.h"

#include <fmt/format.h>

#ifndef _MSC_VER
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wswitch-enum"
#pragma GCC diagnostic ignored "-Wold-style-cast"
#endif
#include "core/function/expression_yy.hh"
#ifndef _MSC_VER
#pragma GCC diagnostic pop
#endif
#include "estl/tokenizer.h"

namespace reindexer::expr_yy {

using namespace std::string_view_literals;

const Tokenizer::Flags kExprTokFlags{Tokenizer::Flags::TreatSignAsToken | Tokenizer::Flags::NoLineComments};

Parser::symbol_type yylex(ExprParseContext& ctx) {
	auto& tokener = ctx.Tok();

	tokener.SkipSpace(kExprTokFlags);
	if (tokener.End()) {
		return Parser::make_END();
	}

	Token peek = tokener.PeekToken(kExprTokFlags);
	const auto text = peek.Text();

	if (text == "+"sv) {
		tokener.SkipToken(kExprTokFlags);
		return Parser::make_PLUS();
	}
	if (text == "-"sv) {
		tokener.SkipToken(kExprTokFlags);
		return Parser::make_MINUS();
	}
	if (text == "*"sv) {
		tokener.SkipToken(kExprTokFlags);
		return Parser::make_MUL();
	}
	if (text == "/"sv) {
		tokener.SkipToken(kExprTokFlags);
		return Parser::make_DIV();
	}
	if (text == "("sv) {
		tokener.SkipToken(kExprTokFlags);
		return Parser::make_LPAREN();
	}
	if (text == ")"sv) {
		tokener.SkipToken(kExprTokFlags);
		return Parser::make_RPAREN();
	}
	if (text == "["sv) {
		tokener.SkipToken(kExprTokFlags);
		return Parser::make_LBRACK();
	}
	if (text == "]"sv) {
		tokener.SkipToken(kExprTokFlags);
		return Parser::make_RBRACK();
	}
	if (text == ","sv) {
		tokener.SkipToken(kExprTokFlags);
		return Parser::make_COMMA();
	}
	if (text == "|"sv) {
		tokener.SkipToken(kExprTokFlags);
		Token second = tokener.PeekToken(kExprTokFlags);
		if (second.Text() != "|"sv) {
			ctx.ThrowError("Unexpected token in expression: '|'");
		}
		tokener.SkipToken(kExprTokFlags);
		return Parser::make_OROR();
	}

	Token tok = tokener.NextToken(kExprTokFlags);
	switch (tok.Type()) {
		case TokenNumber:
			return Parser::make_NUMBER(GetVariantFromToken(tok));
		case TokenString:
			return Parser::make_STRING(std::string{tok.Text()});
		case TokenName:
			if (tok.IsKeyword("null"sv)) {
				return Parser::make_NULL_VALUE();
			}
			if (tok.IsKeyword("true"sv)) {
				return Parser::make_TRUE();
			}
			if (tok.IsKeyword("false"sv)) {
				return Parser::make_FALSE();
			}
			if (tok.Quoted()) {
				return Parser::make_QUOTED_NAME(std::string{tok.Text()});
			}
			if (tokener.PeekToken(kExprTokFlags).Text() == "("sv) {
				ctx.PushFunctionCall(tok.Text());
				return Parser::make_FUNCTION(std::string{tok.Text()});
			}
			return Parser::make_NAME(std::string{tok.Text()});
		case TokenEnd:
			return Parser::make_END();
		case TokenOp:
		case TokenSymbol:
		case TokenSign:
			break;
	}
	ctx.ThrowError(fmt::format("Unexpected token in expression: '{}'", tok.Text()));
}

}  // namespace reindexer::expr_yy
