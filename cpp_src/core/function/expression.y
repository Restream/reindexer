%require "3.8"
// Regenerate expression_yy.cc/.hh with:
// ninja -C build regenerate_expression_parser
// or: cpp_src/core/function/regenerate_expression_parser.sh
%language "C++"
%define api.namespace {reindexer::expr_yy}
%define api.parser.class {Parser}
%define api.value.type variant
%define api.token.constructor
%define parse.error custom
%define parse.assert
%parse-param { reindexer::ExprParseContext& ctx }
%lex-param { reindexer::ExprParseContext& ctx }

%code top {
	#if defined(__GNUC__) && !defined(__clang__) && __GNUC__ >= 15
	// GCC 15 reports false-positive maybe-uninitialized warnings in Bison 3.8.2's variant symbol move/destruction code.
	#pragma GCC diagnostic push
	#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
	#endif
}

%code requires {
	#include <memory>
	#include <string>
	#include "core/function/expression_ast.h"
	#include "core/keyvalue/variant.h"
}

%code {
	#include <utility>
	namespace reindexer::expr_yy {
	Parser::symbol_type yylex(reindexer::ExprParseContext& ctx);
	}  // namespace reindexer::expr_yy
}

%token END 0 "end of expression"
%token <reindexer::Variant> NUMBER
%token <std::string> NAME
%token <std::string> QUOTED_NAME
%token <std::string> FUNCTION
%token <std::string> STRING
%token TRUE FALSE NULL_VALUE
%token OROR "||"
%token PLUS "+"
%token MINUS "-"
%token MUL "*"
%token DIV "/"
%token LPAREN "("
%token RPAREN ")"
%token LBRACK "["
%token RBRACK "]"
%token COMMA ","

%nterm <reindexer::ExprNodePtr> input expr primary array_lit
%nterm <reindexer::ExprNodeArgs> opt_expr_args expr_args
%nterm <reindexer::VariantArray> array_elems opt_array_elems
%nterm <reindexer::Variant> array_elem

%left PLUS MINUS
%left MUL DIV
%left OROR
%precedence UMINUS

%%

input
	: expr END {
		ctx.SetResult(std::move($1));
	}
	;

expr
	: expr PLUS expr {
		$$ = std::make_unique<reindexer::ExprBinary>(reindexer::ExprBinOp::Add, std::move($1), std::move($3));
	}
	| expr MINUS expr {
		$$ = std::make_unique<reindexer::ExprBinary>(reindexer::ExprBinOp::Sub, std::move($1), std::move($3));
	}
	| expr MUL expr {
		$$ = std::make_unique<reindexer::ExprBinary>(reindexer::ExprBinOp::Mul, std::move($1), std::move($3));
	}
	| expr DIV expr {
		$$ = std::make_unique<reindexer::ExprBinary>(reindexer::ExprBinOp::Div, std::move($1), std::move($3));
	}
	| expr OROR expr {
		if (ctx.WhereMode()) {
			ctx.ThrowError("Unsupported construct in WHERE arithmetic expression: '||'");
		}
		$$ = std::make_unique<reindexer::ExprBinary>(reindexer::ExprBinOp::Concat, std::move($1), std::move($3));
	}
	| MINUS expr %prec UMINUS {
		$$ = std::make_unique<reindexer::ExprUnaryMinus>(std::move($2));
	}
	| primary { $$ = std::move($1); }
	;

primary
	: NUMBER {
		$$ = std::make_unique<reindexer::ExprNumber>(std::move($1));
	}
	| STRING {
		if (ctx.WhereMode() && ctx.InternFieldNames()) {
			ctx.ThrowError("Unsupported construct in WHERE arithmetic expression: string literal");
		}
		$$ = std::make_unique<reindexer::ExprNumber>(reindexer::Variant{std::move($1)});
	}
	| TRUE {
		if (ctx.WhereMode() && ctx.InternFieldNames()) {
			ctx.ThrowError("Unsupported construct in WHERE arithmetic expression: boolean literal");
		}
		$$ = std::make_unique<reindexer::ExprNumber>(reindexer::Variant{true});
	}
	| FALSE {
		if (ctx.WhereMode() && ctx.InternFieldNames()) {
			ctx.ThrowError("Unsupported construct in WHERE arithmetic expression: boolean literal");
		}
		$$ = std::make_unique<reindexer::ExprNumber>(reindexer::Variant{false});
	}
	| NULL_VALUE {
		if (ctx.WhereMode() && ctx.InternFieldNames()) {
			ctx.ThrowError("Unsupported construct in WHERE arithmetic expression: null literal");
		}
		$$ = std::make_unique<reindexer::ExprNumber>(reindexer::Variant{});
	}
	| NAME {
		$$ = ctx.MakeField(std::move($1));
	}
	| QUOTED_NAME {
		$$ = ctx.MakeField(std::move($1), true);
	}
	| FUNCTION LPAREN opt_expr_args RPAREN {
		$$ = ctx.MakeFunction(std::move($1), std::move($3));
	}
	| LPAREN expr RPAREN { $$ = std::move($2); }
	| array_lit {
		if (ctx.WhereMode()) {
			ctx.ThrowError("Unsupported construct in WHERE arithmetic expression: '['");
		}
		$$ = std::move($1);
	}
	;

array_lit
	: LBRACK opt_array_elems RBRACK {
		$$ = std::make_unique<reindexer::ExprArrayLiteral>(std::move($2));
	}
	;

opt_array_elems
	: %empty { $$ = reindexer::VariantArray{}; }
	| array_elems { $$ = std::move($1); }
	;

array_elems
	: array_elem {
		$$ = reindexer::VariantArray{};
		$$.emplace_back(std::move($1));
	}
	| array_elems COMMA array_elem { $$ = std::move($1); $$.emplace_back(std::move($3)); }
	;

array_elem
	: NUMBER { $$ = std::move($1); }
	| MINUS NUMBER {
		if ($2.Type().IsOneOf<reindexer::KeyValueType::Int, reindexer::KeyValueType::Int64>()) {
			$$ = reindexer::Variant{-$2.As<int64_t>()};
		} else {
			$$ = reindexer::Variant{-$2.As<double>()};
		}
	}
	| PLUS NUMBER { $$ = std::move($2); }
	| STRING { $$ = reindexer::Variant{std::move($1)}; }
	| TRUE { $$ = reindexer::Variant{true}; }
	| FALSE { $$ = reindexer::Variant{false}; }
	| NULL_VALUE { $$ = reindexer::Variant{}; }
	;

opt_expr_args
	: %empty { $$ = reindexer::ExprNodeArgs{}; }
	| expr_args { $$ = std::move($1); }
	;

expr_args
	: expr {
		$$ = reindexer::ExprNodeArgs{};
		$$.emplace_back(std::move($1));
	}
	| expr_args COMMA expr { $$ = std::move($1); $$.emplace_back(std::move($3)); }
	;

%%

void reindexer::expr_yy::Parser::error(const std::string& msg) {
	ctx.ThrowError(msg);
}

void reindexer::expr_yy::Parser::report_syntax_error(const context& yyctx) const {
	const auto& la = yyctx.lookahead();
	std::string tok;
	switch (la.kind()) {
		case symbol_kind::S_YYEMPTY:
		case symbol_kind::S_YYEOF:
			tok = "end of expression";
			break;
		case symbol_kind::S_NAME:
		case symbol_kind::S_QUOTED_NAME:
		case symbol_kind::S_FUNCTION:
		case symbol_kind::S_STRING:
			tok = la.value.as<std::string>();
			break;
		case symbol_kind::S_NUMBER:
			tok = std::string{la.value.as<reindexer::Variant>().As<std::string>()};
			break;
		default:
			tok = la.name();
			break;
	}
	ctx.ThrowError("Unexpected token in expression: '" + tok + "'");
}

#if defined(__GNUC__) && !defined(__clang__) && __GNUC__ >= 15
#pragma GCC diagnostic pop
#endif
