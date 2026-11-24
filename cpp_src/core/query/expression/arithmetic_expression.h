#pragma once

#include "core/function/expression_ast.h"
#include "core/function/function.h"

#include <string>
#include <string_view>

namespace reindexer {

class WrSerializer;

namespace expressions {

/// Arithmetic expression: Bison-parsed AST + its text.
class [[nodiscard]] ArithmeticExpression {
public:
	ArithmeticExpression() = default;
	explicit ArithmeticExpression(std::string_view expr);

	void Serialize(WrSerializer& ser) const;

	const std::string& Dump() const& noexcept { return text_; }
	const ExpressionAst& Ast() const noexcept { return ast_; }
	bool UsesNow() const noexcept { return usesNow_; }
	std::optional<functions::FunctionVariant> AsWhereFunction() const;
	std::optional<std::string_view> AsField() const noexcept;
	std::optional<Variant> AsLiteral() const;

	bool operator==(const ArithmeticExpression& other) const noexcept { return text_ == other.text_; }

	const std::string& Dump() const&& = delete;

private:
	ExpressionAst ast_;
	std::string text_;
	bool usesNow_{false};
};

inline expressions::ArithmeticExpression operator""_expr(const char* str, size_t len) {
	return expressions::ArithmeticExpression(std::string_view(str, len));
}

}  // namespace expressions
}  // namespace reindexer
