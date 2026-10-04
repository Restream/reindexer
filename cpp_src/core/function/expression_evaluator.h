#pragma once

#include "core/function/expression_ast.h"
#include "core/keyvalue/variant.h"

namespace reindexer {

class NamespaceImpl;
class NsContext;
class PayloadValue;

namespace functions {
class FunctionInvoker;
}

/// Evaluates UPDATE SET expressions via Bison-parsed AST.
class [[nodiscard]] ExpressionEvaluator {
public:
	ExpressionEvaluator(NamespaceImpl& ns, functions::FunctionInvoker& funcInvoker) noexcept : ns_(ns), functionInvoker_(funcInvoker) {}

	VariantArray Evaluate(const ExpressionAst& ast, const PayloadValue& v, std::string_view forField, const NsContext& ctx);

private:
	NamespaceImpl& ns_;
	functions::FunctionInvoker& functionInvoker_;
};

}  // namespace reindexer
