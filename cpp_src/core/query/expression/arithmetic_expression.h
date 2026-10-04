#pragma once

#include "core/function/expression_ast.h"
#include "core/type_consts.h"

#include <string>
#include <string_view>

namespace reindexer {

class WrSerializer;

namespace expressions {

/// Arithmetic expression: Bison-parsed AST + source string.
class [[nodiscard]] ArithmeticExpression {
public:
	ArithmeticExpression() = default;
	explicit ArithmeticExpression(std::string_view expr);

	void Serialize(WrSerializer& ser) const;

	std::string Dump() const { return source_; }
	std::string_view Source() const noexcept { return source_; }
	const ExpressionAst& Ast() const noexcept { return ast_; }
	bool UsesNow() const noexcept { return usesNow_; }

	bool operator==(const ArithmeticExpression& other) const noexcept { return source_ == other.source_; }

private:
	ExpressionAst ast_;
	std::string source_;
	bool usesNow_{false};
};

}  // namespace expressions
}  // namespace reindexer
