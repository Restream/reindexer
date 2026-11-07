#pragma once

#include <optional>
#include <string>
#include "core/function/function.h"
#include "core/query/expression/arithmetic_expression.h"
#include "estl/tokenizer.h"
#include "estl/tokenizer_range.h"
#include "tools/assertrx.h"

namespace reindexer {

// Without TreatSignAsToken `y-1` is tokenized as `y` and `-1`
constexpr Tokenizer::Flags::Values kArithmeticTokFlags = Tokenizer::Flags::TreatSignAsToken;

inline bool isSymbol(const Token& tok, std::string_view symbol) noexcept { return tok.Type() == TokenSymbol && tok.Text() == symbol; }
bool isFieldNameToken(const Token& tok) noexcept;
bool isArithmeticOperator(const Token& tok) noexcept;

// Operand, which starts from the current position, is a primary (name, number, function call or brackets group) followed by
// an arithmetic operator, or it is a name/brackets group with an unary sign
bool operandHasArithmetic(Tokenizer& parser) noexcept;

// `left`, `inner` and `join` are field names, if they are followed by an operator: `left = 1`, `inner*2 > 5`
bool isJoinKeyword(const Token& tok, std::string_view kw, const Token& next) noexcept;
// Expects that the parser is positioned right after the `kw` token
bool isJoinKeyword(Tokenizer& parser, const Token& tok, std::string_view kw);

// Expects that the parser is positioned right after '(' at the beginning of the condition.
// The brackets group is arithmetic if it is followed by an arithmetic or a comparison operator
bool isArithmeticBracket(Tokenizer& parser) noexcept;

// Expects that the parser is positioned at '(' after the condition operator.
// Distinguishes operand in brackets like `(b+1)*2`, `(b)` or `(now())` from the values list like `(1, 2, 3)`, `(-5)` or `(b, 1)`.
// `{...}` is one composite literal: a sign or operator inside it (`{-1}`, `{1+2, 3}`) does not make this bracket arithmetic.
bool isArithmeticValueBracket(Tokenizer& parser) noexcept;

/// One side of a WHERE condition `<operand> <condition> <operand>`. Each side may be an arithmetic expression.
/// SQLParser::parseArithmeticCondition() chooses the query entry by the kinds of both operands:
/// - Field (`id`, `"a+b"`, `(id)`) is stored in `field`. A field and a literal give QueryEntry, two fields give BetweenFieldsQueryEntry.
/// - Literal (`5`, `-5`, `'str'`, `true`, `null`, `{1, 2}`) is stored in `literal`. A literal on the left side inverts the condition:
///   `5 < a` is stored as `a > 5`. Two literals are an error.
/// - Function (`now()`, `flat_array_len(arr)`) is stored in `function` and gives QueryFunctionEntry, if the entry supports it on this side
///   (flat_array_len() on the left, now() on the right). Otherwise the function is compared as the arithmetic `expression`.
/// - Expression (`a+1`, `2*3`, `now()-ts`) is stored in `expression` and gives QueryArithmeticEntry. Constants are not folded:
///   `x > 2*3` is not `x > 6`.
/// `source` and [sourceBegin, sourceEnd) locate the operand in the query for error messages.
struct [[nodiscard]] ArithmeticOperand {
	enum class [[nodiscard]] Kind : uint8_t { Field, Literal, Function, Expression };

	Kind kind{Kind::Expression};
	std::string field;
	Variant literal;
	std::optional<functions::FunctionVariant> function;
	expressions::ArithmeticExpression expression;
	std::string source;
	size_t sourceBegin{0};
	size_t sourceEnd{0};

	TokenizerRange Range(const Tokenizer& parser) const noexcept { return parser.Where(sourceBegin, sourceEnd); }
	functions::FunctionVariant& Function() & {
		if (!function) [[unlikely]] {
			throw_as_assert;
		}
		return *function;
	}
};

/// Parses the operand from the current position up to the condition operator or the end of the condition (`AND`, `OR`, `ORDER BY`,
/// `)` and so on). A single literal or field name is parsed directly, anything else goes through the expression grammar.
ArithmeticOperand parseArithmeticOperand(Tokenizer& parser);

}  // namespace reindexer
