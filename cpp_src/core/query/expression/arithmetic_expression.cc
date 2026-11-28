#include "arithmetic_expression.h"
#include "core/function/builtin_function.h"
#include "tools/overflow.h"
#include "tools/serilize/wrserializer.h"

namespace reindexer::expressions {

namespace {

const ExprNumber* numberNode(const ExprNode* node) noexcept {
	return node && node->Type() == ExprNodeType::Number ? static_cast<const ExprNumber*>(node) : nullptr;
}

ExpressionAst flatArrayLenAst(const functions::FlatArrayLen& function) {
	const BuiltinFunction* builtin = BuiltinFunction::Find(function.Name());
	assertrx_throw(builtin);
	ExprFunctionArgs args;
	args.emplace_back(ExprFunctionArg{.value = function.FieldNames().front(), .kind = ExprFunctionArg::Kind::Name});
	return {std::make_unique<ExprFunction>(*builtin, ExprFunction::Kind::FlatArrayLen, std::move(args)), {}};
}

}  // namespace

ArithmeticExpression::ArithmeticExpression(std::string_view expr)
	: ast_{ExpressionAst::Parse(expr, /*whereMode=*/true)}, text_{ast_.Root()->Dump()}, usesNow_{ast_.UsesNow()} {}

ArithmeticExpression::ArithmeticExpression(const functions::FlatArrayLen& function)
	: ast_{flatArrayLenAst(function)}, text_{ast_.Root()->Dump()}, usesNow_{false} {}

std::optional<functions::FunctionVariant> ArithmeticExpression::AsWhereFunction() const {
	const ExprNode* root = ast_.Root();
	if (!root || root->Type() != ExprNodeType::Function) {
		return std::nullopt;
	}
	const auto& fn = static_cast<const ExprFunction&>(*root);
	if (!fn.exprArgs.empty()) {
		return std::nullopt;
	}
	switch (fn.kind) {
		case ExprFunction::Kind::Now:
			return functions::Now{fn.timeUnit};
		case ExprFunction::Kind::FlatArrayLen:
			if (fn.stringArgs.size() != 1) {
				return std::nullopt;
			}
			return functions::FlatArrayLen{fn.stringArgs.front().value};
		case ExprFunction::Kind::Serial:
		case ExprFunction::Kind::ArrayRemove:
		case ExprFunction::Kind::ArrayRemoveOnce:
			return std::nullopt;
	}
	return std::nullopt;
}

std::optional<std::string_view> ArithmeticExpression::AsField() const noexcept {
	const ExprNode* root = ast_.Root();
	if (!root || root->Type() != ExprNodeType::Field) {
		return std::nullopt;
	}
	return static_cast<const ExprField&>(*root).name;
}

std::optional<Variant> ArithmeticExpression::AsLiteral() const {
	const ExprNode* root = ast_.Root();
	if (const ExprNumber* number = numberNode(root)) {
		return number->value;
	}
	if (!root || root->Type() != ExprNodeType::UnaryMinus) {
		return std::nullopt;
	}
	const ExprNumber* number = numberNode(static_cast<const ExprUnaryMinus&>(*root).child.get());
	if (!number) {
		return std::nullopt;
	}
	const Variant& value = number->value;
	if (value.Type().Is<KeyValueType::Int64>()) {
		int64_t negated = 0;
		if (SubOverflow(int64_t{0}, value.As<int64_t>(), negated)) {
			throw Error(errLogic, kIntegerOverflowError);
		}
		return Variant{negated};
	}
	if (value.Type().Is<KeyValueType::Double>()) {
		return Variant{-value.As<double>()};
	}
	return std::nullopt;
}

void ArithmeticExpression::Serialize(WrSerializer& ser) const {
	ser.PutVarUint(ExpressionTypeArithmetic);
	ser.PutVString(text_);
}

}  // namespace reindexer::expressions
