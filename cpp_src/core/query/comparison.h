#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <variant>
#include "core/function/function.h"
#include "core/keyvalue/variant.h"
#include "core/query/expression/arithmetic_expression.h"
#include "tools/errors.h"

namespace reindexer {

class Query;

namespace comparison {

struct [[nodiscard]] Field {
	std::string name;
};
struct [[nodiscard]] Literal {
	Variant value;
	std::optional<expressions::ArithmeticExpression> expression{};
};
struct [[nodiscard]] Values {
	VariantArray values;
};
struct [[nodiscard]] Function {
	functions::FunctionVariant function;
	std::optional<expressions::ArithmeticExpression> expression{};
};
struct [[nodiscard]] Expression {
	expressions::ArithmeticExpression expression;
};
struct [[nodiscard]] SubQuery {
	Query* query{nullptr};
};

/// A parsed operand of `<operand> <condition> <operand>`.
///
/// Query::addComparison() selects the specialized query entry, normalizes operand
/// order, inverts the condition when required, and validates the combination.
///
/// SQL parses plain fields and literals directly. Remaining non-subquery scalar
/// operands are parsed as arithmetic expressions and classified with Classify().
/// Subqueries and value lists are parsed separately. Explicit API/DSL Expression
/// operands remain arithmetic unless either side is a SubQuery; in that case they
/// are also classified as a field, literal, function, or arithmetic expression.
///
/// Main rules:
/// - Field + Field produces BetweenFieldsQueryEntry.
/// - Field on the left + Literal/Values on the right produces QueryEntry.
/// - Literal/Values on the left of a Field, Function, Expression or SubQuery move
///   to the right with the condition inverted when it is invertible. A
///   non-invertible condition keeps them on the left: QueryArithmeticEntry next
///   to a Field, Function or Expression, SubQueryEntry next to a SubQuery. RANGE
///   keeps them on the left only next to a SubQuery and is an error otherwise.
///   ALLSET next to a flat_array_len() Function is an error too. The entries
///   reject the conditions they do not support.
/// - Two value operands produce QueryArithmeticEntry. A Literal in such a pair
///   must be numeric: `5 = (1, 5)` is valid, `'a' = 5` and `5 = 'a'` are errors.
/// - A supported function produces QueryFunctionEntry or SubQueryFunctionEntry
///   when its side and the other operand allow it. Otherwise it is compared as
///   an arithmetic expression. An unsupported function is always rejected: by
///   the side check on a function-entry path, otherwise by the expression parser.
/// - An Expression that remains arithmetic produces QueryArithmeticEntry or
///   SubQueryArithmeticEntry.
/// - One SubQuery produces the corresponding SubQuery*Entry when the condition
///   is valid for that pair. A SubQuery on the left of a Field moves to the right
///   with the condition inverted when it is invertible; otherwise ALLSET keeps it
///   on the left and RANGE is an error. Two subqueries are invalid.
using Operand = std::variant<Field, Literal, Values, Function, Expression, SubQuery>;

Operand Classify(expressions::ArithmeticExpression&&);

class [[nodiscard]] OperandError : public Error {
public:
	enum class [[nodiscard]] Site : uint8_t { Left, Condition, Right };

	OperandError(Site site, Error&& err) noexcept : Error(std::move(err)), site_{site} {}

	Site GetSite() const noexcept { return site_; }

private:
	Site site_;
};

}  // namespace comparison
}  // namespace reindexer
