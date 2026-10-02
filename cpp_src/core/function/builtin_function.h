#pragma once

#include "core/function/expression_ast.h"

#include <string_view>

namespace reindexer {

class PayloadType;
class TagsMatcher;

class [[nodiscard]] BuiltinFunction {
public:
	virtual ~BuiltinFunction() = default;
	BuiltinFunction(const BuiltinFunction&) = delete;
	BuiltinFunction& operator=(const BuiltinFunction&) = delete;
	BuiltinFunction(BuiltinFunction&&) = delete;
	BuiltinFunction& operator=(BuiltinFunction&&) = delete;

	bool ExprArgs() const noexcept { return constraints_.exprArgs; }
	bool ReturnsArray() const noexcept { return constraints_.returnsArray; }
	std::string_view Name() const noexcept { return name_; }

	void Validate(const ExprNodeArgs& args, ExprParseContext& ctx) const;
	virtual ExprNodePtr Bind(ExprNodeArgs args, ExprParseContext& ctx) const = 0;
	virtual VariantArray Execute(ExprEvalContext& ctx, const ExprFunction& func) const;
	virtual ExprScalar ExecuteScalar(ExprEvalContext& ctx, const ExprFunction& func) const;
	virtual void CollectReferencedFields(const ExprFunctionArgs& args, ExprReferencedFields& fields) const;

	static const BuiltinFunction* Find(std::string_view name) noexcept;
	static ExprNodePtr Create(std::string_view name, ExprNodeArgs args, ExprParseContext& ctx);

protected:
	static constexpr uint8_t kArgName = 1u << static_cast<uint8_t>(ExprParseContext::FunctionArg::Kind::Name);
	static constexpr uint8_t kArgQuotedName = 1u << static_cast<uint8_t>(ExprParseContext::FunctionArg::Kind::QuotedName);
	static constexpr uint8_t kArgString = 1u << static_cast<uint8_t>(ExprParseContext::FunctionArg::Kind::String);

	struct [[nodiscard]] Constraints {
		uint8_t minArgs{0};
		uint8_t maxArgs{0};
		uint8_t allowedArgKinds{0};
		bool allowedInWhere{true};
		bool exprArgs{false};
		bool returnsArray{false};
	};

	BuiltinFunction(std::string_view name, Constraints constraints) noexcept : name_{name}, constraints_{constraints} {}

	void ValidateIdentifierArgs(const ExprParseContext::FunctionArgs& args, ExprParseContext& ctx) const;

	static const PayloadType& PayloadTypeOf(const NamespaceImpl& ns) noexcept;
	static const TagsMatcher& TagsMatcherOf(const NamespaceImpl& ns) noexcept;

private:
	std::string_view name_;
	Constraints constraints_;
};

}  // namespace reindexer
