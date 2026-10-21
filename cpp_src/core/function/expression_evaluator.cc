#include "expression_evaluator.h"

#include "core/function/expression_ast.h"
#include "core/namespace/namespaceimpl.h"
#include "core/payload/payloadiface.h"

namespace reindexer {

namespace {
constexpr char kArrayNullOutsideConcatError[] = "Unable to use array and null values outside of the arrays concatenation";

bool allowsArrayResult(const ExprNode* node) noexcept {
	if (!node) {
		return false;
	}
	if (node->Type() == ExprNodeType::Function) {
		return static_cast<const ExprFunction*>(node)->ReturnsArray();
	}
	return node->Type() == ExprNodeType::Binary && static_cast<const ExprBinary*>(node)->op == ExprBinOp::Concat;
}
}  // namespace

VariantArray ExpressionEvaluator::Evaluate(const ExpressionAst& ast, const PayloadValue& v, std::string_view forField,
										   const NsContext& ctx) {
	ConstPayload payload{ns_.payloadType(), v};
	VariantArray fieldScratch;
	ExprEvalContext evalCtx{
		.ns = ns_,
		.functionInvoker = &functionInvoker_,
		.ctx = &ctx,
		.forField = forField,
		.whereMode = false,
		.fieldBindings = {},
		.nowTimes = nullptr,
		.payload = payload,
		.fieldScratch = fieldScratch,
	};
	if (allowsArrayResult(ast.Root())) {
		return ast.Evaluate(evalCtx);
	}
	const auto result = ast.EvaluateScalar(evalCtx);
	if (result.empty()) {
		throw Error(errParams, kArrayNullOutsideConcatError);
	}
	return VariantArray{result.ToVariant()};
}

}  // namespace reindexer
