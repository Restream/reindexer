#pragma once

#include "core/function/expression_ast.h"
#include "core/keyvalue/variant.h"

namespace reindexer {

class IItemValuesExtractor;
class NamespaceImpl;
class NsContext;

namespace functions {
class FunctionInvoker;
}

/// Evaluates UPDATE SET expressions via Bison-parsed AST.
class [[nodiscard]] ExpressionEvaluator {
public:
	ExpressionEvaluator(NamespaceImpl& ns, functions::FunctionInvoker& funcInvoker) noexcept : ns_(ns), functionInvoker_(funcInvoker) {}

	VariantArray Evaluate(const ExpressionAst& ast, IItemValuesExtractor& dataExtractor, std::string_view forField, const NsContext& ctx,
						  std::span<int> updateIndexBindings);

private:
	NamespaceImpl& ns_;
	functions::FunctionInvoker& functionInvoker_;
};

}  // namespace reindexer
