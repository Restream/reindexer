#pragma once

#include <optional>
#include <string>
#include "core/function/function.h"
#include "core/query/comparison.h"
#include "core/query/expression/arithmetic_expression.h"
#include "estl/h_vector.h"
#include "estl/tokenizer.h"
#include "estl/tokenizer_range.h"
#include "tools/assertrx.h"

namespace reindexer {

// Without TreatSignAsToken `y-1` is tokenized as `y` and `-1`
constexpr Tokenizer::Flags::Values kArithmeticTokFlags = Tokenizer::Flags::TreatSignAsToken;

bool isFieldNameToken(const Token& tok) noexcept;
bool isArithmeticOperator(const Token& tok) noexcept;

// Operand, which starts from the current position, is a primary (name, number, function call or brackets group) followed by
// an arithmetic operator, or it is a name/brackets group with an unary sign
bool operandHasArithmetic(Tokenizer& parser) noexcept;

// `left`, `inner` and `join` are field names, if they are followed by an operator: `left = 1`, `inner*2 > 5`
bool isJoinKeyword(const Token& tok, std::string_view kw, const Token& next) noexcept;
// Expects that the parser is positioned right after the `kw` token
bool isJoinKeyword(Tokenizer& parser, const Token& tok, std::string_view kw);

class [[nodiscard]] ArithmeticBracketDetector {
public:
	// Expects that the parser is positioned right after '(' at the beginning of the condition.
	// The brackets group is arithmetic if it is followed by an arithmetic or a comparison operator
	bool IsArithmetic(Tokenizer& parser) noexcept;

private:
	struct [[nodiscard]] Bracket {
		size_t contentPos;
		bool arithmetic;
	};

	void scanGroup(Tokenizer& parser);

	h_vector<Bracket, 8> brackets_;
};

// Expects that the parser is positioned at '(' after the condition operator.
// Distinguishes operand in brackets like `(b+1)*2`, `(b)` or `(now())` from the values list like `(1, 2, 3)`, `(-5)` or `(b, 1)`.
// `{...}` is one composite literal: a sign or operator inside it (`{-1}`, `{1+2, 3}`) does not make this bracket arithmetic.
bool isArithmeticValueBracket(Tokenizer& parser) noexcept;
bool isValueListBracket(Tokenizer& parser) noexcept;

/// One side of a WHERE condition `<operand> <condition> <operand>`. Each side may be an arithmetic expression.
/// [sourceBegin, sourceEnd) locates the operand in the query for error messages.
struct [[nodiscard]] ArithmeticOperand {
	comparison::Operand value;
	size_t sourceBegin{0};
	size_t sourceEnd{0};

	TokenizerRange Range(const Tokenizer& parser) const noexcept { return parser.Where(sourceBegin, sourceEnd); }
	std::string_view Source(const Tokenizer& parser) const noexcept { return {parser.Begin() + sourceBegin, sourceEnd - sourceBegin}; }
};

/// Parses the operand from the current position up to the condition operator or the end of the condition (`AND`, `OR`, `ORDER BY`,
/// `)` and so on). A single literal or field name is parsed directly, anything else goes through the expression grammar.
ArithmeticOperand parseArithmeticOperand(Tokenizer& parser);

}  // namespace reindexer
