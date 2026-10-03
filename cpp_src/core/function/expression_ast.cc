#include "expression_ast.h"

#include <algorithm>

#ifndef _MSC_VER
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wswitch-enum"
#pragma GCC diagnostic ignored "-Wold-style-cast"
#endif
#include "core/function/expression_yy.hh"
#ifndef _MSC_VER
#pragma GCC diagnostic pop
#endif
#include "core/function/builtin_function.h"
#include "core/namespace/namespaceimpl.h"
#include "core/payload/payloadiface.h"
#include "core/queryresults/fields_filter.h"
#include "core/type_consts_helpers.h"
#include "tools/float_comparison.h"
#include "tools/overflow.h"

namespace reindexer {

using namespace std::string_view_literals;

namespace {
constexpr char kWrongFieldTypeError[] = "Only integral type non-array fields are supported in arithmetical expressions: {}";
constexpr char kScalarsInConcatenationError[] = "Unable to use scalar values in the arrays concatenation expressions: {}";
constexpr char kIntegerOverflowError[] = "Integer overflow in arithmetic expression";
constexpr char kArrayNullOutsideConcatError[] = "Unable to use array and null values outside of the arrays concatenation";
constexpr char kArrayInArithmeticError[] = "Array result is not allowed in arithmetic expression";
constexpr char kMixConcatArithmeticError[] = "Unable to mix arrays concatenation and arithmetic operations. Got token: '{}'";

[[noreturn]] void throwWrongFieldType(std::string_view name) { throw Error(errParams, kWrongFieldTypeError, name); }

[[noreturn]] void throwMultidimensionalArray(std::string_view name) {
	throw Error(errParams, "Concatenation and remove are not supported for multidimensional arrays: '{}'", name);
}

bool isConcatNode(const ExprNode& node) noexcept {
	return node.Type() == ExprNodeType::Binary && static_cast<const ExprBinary&>(node).op == ExprBinOp::Concat;
}

std::string_view tokenForOp(ExprBinOp op) noexcept {
	switch (op) {
		case ExprBinOp::Add:
			return "+";
		case ExprBinOp::Sub:
			return "-";
		case ExprBinOp::Mul:
			return "*";
		case ExprBinOp::Div:
			return "/";
		case ExprBinOp::Concat:
			return "||";
	}
	return {};
}

VariantArray asArrayResult(VariantArray values) {
	std::ignore = values.MarkArray();
	return values;
}

VariantArray& fieldBuf(ExprEvalContext& ctx) {
	ctx.fieldScratch.Clear();
	return ctx.fieldScratch;
}

ExprScalar scalarFromNumeric(const Variant& v, std::string_view name) {
	if (auto scalar = ExprScalar::TryFromVariant(v)) {
		return *scalar;
	}
	throwWrongFieldType(name);
}

bool hasExplicitElementIndex(std::string_view path) noexcept {
	for (size_t i = 0; i + 1 < path.size(); ++i) {
		if (path[i] == '[' && path[i + 1] != '*') {
			return true;
		}
	}
	return false;
}

ExprScalar scalarFromIndexedField(const ConstPayload& pv, int fieldIdx, std::string_view name, bool whereMode) {
	const auto& fieldType = pv.Type().Field(fieldIdx);
	if (fieldType.IsArray()) {
		if (whereMode) {
			throwWrongFieldType(name);
		}
		throw Error(errParams, kArrayNullOutsideConcatError);
	}
	if (fieldType.Type().Is<KeyValueType::FloatVector>()) {
		throwWrongFieldType(name);
	}
	return fieldType.Type().EvaluateOneOf(
		[&](KeyValueType::Int) { return ExprScalar::FromInt(pv.GetView<int>(fieldIdx).front()); },
		[&](KeyValueType::Int64) { return ExprScalar::FromInt(pv.GetView<int64_t>(fieldIdx).front()); },
		[&](KeyValueType::Double) { return ExprScalar::FromDouble(pv.GetView<double>(fieldIdx).front()); },
		[&](KeyValueType::Float) { return ExprScalar::FromDouble(pv.GetView<float>(fieldIdx).front()); },
		[&](concepts::OneOf<KeyValueType::Bool, KeyValueType::String, KeyValueType::Uuid, KeyValueType::FloatVector, KeyValueType::Tuple,
							KeyValueType::Undefined, KeyValueType::Composite, KeyValueType::Null> auto) -> ExprScalar {
			throwWrongFieldType(name);
		});
}

ExprScalar evaluateScalar(const ExprNode& node, ExprEvalContext& ctx);

std::string literalToken(const Variant& v) {
	if (v.Type().Is<KeyValueType::Bool>()) {
		return v.As<bool>() ? "true" : "false";
	}
	if (v.Type().Is<KeyValueType::Null>()) {
		return "null";
	}
	return v.Dump();
}

ExprScalar evaluateScalarNumber(const ExprNumber& node) {
	if (auto scalar = ExprScalar::TryFromVariant(node.value)) {
		return *scalar;
	}
	throwWrongFieldType(literalToken(node.value));
}

ExprScalar evaluateScalarUnary(const ExprUnaryMinus& node, ExprEvalContext& ctx) {
	const auto v = evaluateScalar(*node.child, ctx);
	if (v.empty()) {
		return {};
	}
	if (v.kind == ExprScalar::Kind::Int) {
		int64_t result = 0;
		if (SubOverflow(int64_t{0}, v.i, result)) {
			throw Error(errLogic, kIntegerOverflowError);
		}
		return ExprScalar::FromInt(result);
	}
	return ExprScalar::FromDouble(-v.d);
}

ExprScalar evaluateScalarBinary(const ExprBinary& node, ExprEvalContext& ctx) {
	if (node.op == ExprBinOp::Concat) {
		throw Error(errParams, kArrayInArithmeticError);
	}
	if (isConcatNode(*node.left) || isConcatNode(*node.right)) {
		throw Error(errParams, kMixConcatArithmeticError, tokenForOp(node.op));
	}
	const auto lhs = evaluateScalar(*node.left, ctx);
	const auto rhs = evaluateScalar(*node.right, ctx);
	if (lhs.empty() || rhs.empty()) {
		return {};
	}
	if (node.op != ExprBinOp::Div && lhs.kind == ExprScalar::Kind::Int && rhs.kind == ExprScalar::Kind::Int) {
		int64_t result = 0;
		bool overflow = false;
		switch (node.op) {
			case ExprBinOp::Add:
				overflow = AddOverflow(lhs.i, rhs.i, result);
				break;
			case ExprBinOp::Sub:
				overflow = SubOverflow(lhs.i, rhs.i, result);
				break;
			case ExprBinOp::Mul:
				overflow = MulOverflow(lhs.i, rhs.i, result);
				break;
			case ExprBinOp::Div:
			case ExprBinOp::Concat:
				assertrx_throw(false);
		}
		if (overflow) {
			throw Error(errLogic, kIntegerOverflowError);
		}
		return ExprScalar::FromInt(result);
	}
	const double l = lhs.AsDouble();
	const double r = rhs.AsDouble();
	switch (node.op) {
		case ExprBinOp::Add:
			return ExprScalar::FromDouble(l + r);
		case ExprBinOp::Sub:
			return ExprScalar::FromDouble(l - r);
		case ExprBinOp::Mul:
			return ExprScalar::FromDouble(l * r);
		case ExprBinOp::Div:
			if (fp::IsZero(r)) {
				throw Error(errLogic, "Division by zero!");
			}
			return ExprScalar::FromDouble(l / r);
		case ExprBinOp::Concat:
			break;
	}
	assertrx_throw(false);
	return {};
}

ExprScalar evaluateScalar(const ExprNode& node, ExprEvalContext& ctx) {
	switch (node.Type()) {
		case ExprNodeType::Number:
			return evaluateScalarNumber(static_cast<const ExprNumber&>(node));
		case ExprNodeType::Field:
			return static_cast<const ExprField&>(node).EvaluateScalar(ctx);
		case ExprNodeType::UnaryMinus:
			return evaluateScalarUnary(static_cast<const ExprUnaryMinus&>(node), ctx);
		case ExprNodeType::Binary:
			return evaluateScalarBinary(static_cast<const ExprBinary&>(node), ctx);
		case ExprNodeType::Function:
			return static_cast<const ExprFunction&>(node).EvaluateScalar(ctx);
		case ExprNodeType::ArrayLiteral:
			throw Error(errParams, kArrayInArithmeticError);
	}
	assertrx_throw(false);
	return {};
}

void appendConcatOperand(const ExprNode& node, ExprEvalContext& ctx, VariantArray& out) {
	if (isConcatNode(node)) {
		const auto& binary = static_cast<const ExprBinary&>(node);
		appendConcatOperand(*binary.left, ctx, out);
		appendConcatOperand(*binary.right, ctx, out);
		return;
	}
	if (node.Type() == ExprNodeType::ArrayLiteral) {
		const auto& values = static_cast<const ExprArrayLiteral&>(node).values;
		out.reserve(out.size() + values.size());
		out.insert(out.end(), values.begin(), values.end());
		return;
	}
	auto values = node.Evaluate(ctx);
	if (!values.IsArrayValue() && !values.empty()) {
		throw Error(errParams, kScalarsInConcatenationError, node.Dump());
	}
	out.reserve(out.size() + values.size());
	for (auto& value : values) {
		out.emplace_back(std::move(value));
	}
}
}  // namespace

ExprParseContext::ExprParseContext(std::string_view expr, bool whereMode) : input{expr}, whereMode_{whereMode}, tokenizer_{expr} {}

ExprNodePtr ExprParseContext::MakeField(std::string name, bool quoted) {
	size_t slot = 0;
	if (InternFieldNames()) {
		const auto existing = std::find(fieldNames_.begin(), fieldNames_.end(), name);
		slot = existing == fieldNames_.end() ? fieldNames_.size() : size_t(existing - fieldNames_.begin());
		if (existing == fieldNames_.end()) {
			fieldNames_.push_back(name);
		}
	}
	return std::make_unique<ExprField>(std::move(name), slot, quoted);
}

void ExprParseContext::PushFunctionCall(std::string_view name) {
	const auto* fn = BuiltinFunction::Find(name);
	internFieldArgs_.emplace_back(!fn || fn->ExprArgs() ? uint8_t{1} : uint8_t{0});
}

void ExprParseContext::PopFunctionCall() noexcept {
	if (!internFieldArgs_.empty()) {
		internFieldArgs_.pop_back();
	}
}

ExprNodePtr ExprParseContext::MakeFunction(std::string_view name, ExprNodeArgs args) {
	PopFunctionCall();
	return BuiltinFunction::Create(name, std::move(args), *this);
}

void ExprParseContext::ThrowError(std::string_view msg) const { throw Error(errParams, "{} (expression: '{}')", msg, input); }

ExpressionAst ExpressionAst::Parse(std::string_view expr, bool whereMode) {
	if (expr.empty()) {
		throw Error(errParams, whereMode ? "Empty WHERE arithmetic expression" : "Empty expression");
	}
	ExprParseContext ctx{expr, whereMode};
	expr_yy::Parser parser{ctx};
	if (parser.parse() != 0) {
		ctx.ThrowError("Failed to parse expression");
	}
	auto root = ctx.TakeResult();
	if (!root) {
		ctx.ThrowError("Empty parse result");
	}
	return ExpressionAst{std::move(root), ctx.TakeFieldNames()};
}

VariantArray ExpressionAst::Evaluate(ExprEvalContext& ctx) const {
	assertrx_throw(root_);
	assertrx_throw(!ctx.whereMode);
	return root_->Evaluate(ctx);
}

ExprScalar ExpressionAst::EvaluateScalar(ExprEvalContext& ctx) const {
	assertrx_throw(root_);
	return evaluateScalar(*root_, ctx);
}

VariantArray ExprNumber::Evaluate(ExprEvalContext&) const { return VariantArray{value}; }

std::string ExprNumber::Dump() const { return literalToken(value); }

ExprField::FieldAccess ExprField::resolveAccess(ExprEvalContext& ctx, int& fieldIdx, const ExprFieldBinding*& binding) const {
	binding = nullptr;
	if (!ctx.fieldBindings.empty()) {
		assertrx_throw(slot < ctx.fieldBindings.size());
		binding = &ctx.fieldBindings[slot];
		if (!binding->compositeFieldsTypes.empty()) {
			throwWrongFieldType(name);
		}
		if (binding->payloadFieldIdx) {
			fieldIdx = *binding->payloadFieldIdx;
			return FieldAccess::Indexed;
		}
		return FieldAccess::BindingFields;
	}
	if (ctx.ns.payloadType().FieldByName(name, fieldIdx)) {
		return FieldAccess::Indexed;
	}
	return FieldAccess::JsonPath;
}

void ExprField::getByJsonPath(const PayloadIface<const PayloadValue>& pv, const NamespaceImpl& ns, VariantArray& out) const {
	if (pv.ContainsMultidimensionalArray(FieldsFilter{std::string_view{name}, ns})) {
		throwMultidimensionalArray(name);
	}
	pv.GetByJsonPath(jsonPathForEval(ns), ns.tagsMatcher(), out, KeyValueType::Undefined{});
}

std::string_view ExprField::jsonPathForEval(const NamespaceImpl& ns) const {
	std::string_view jsonPath{name};
	if (name.find('[') != std::string::npos) {
		return jsonPath;
	}
	int fieldIdx = 0;
	if (ns.tryGetIndexByNameOrJsonPath(jsonPath, fieldIdx)) {
		const auto& index{ns.indexes()[fieldIdx]};
		if (IsComposite(index->Type())) {
			throwWrongFieldType(name);
		}
		if (index->Opts().IsSparse()) {
			const auto& fields{index->Fields()};
			if (fields.getJsonPathsLength() > 0) {
				jsonPath = fields.getJsonPath(0);
			} else {
				throw Error(errParams, "Field '{}' doesn't have json path", name);
			}
		}
	}
	return jsonPath;
}

VariantArray ExprField::Evaluate(ExprEvalContext& ctx) const {
	const auto& pv = ctx.payload;
	int fieldIdx = 0;
	[[maybe_unused]] const ExprFieldBinding* binding = nullptr;
	const auto access = resolveAccess(ctx, fieldIdx, binding);
	assertrx_throw(access != FieldAccess::BindingFields);

	VariantArray fieldValues;
	if (access == FieldAccess::Indexed) {
		const auto& fieldType = pv.Type().Field(fieldIdx);
		if (fieldType.Type().Is<KeyValueType::FloatVector>()) {
			throwWrongFieldType(name);
		}
		if (fieldType.IsArray()) {
			if (pv.ContainsMultidimensionalArray(FieldsFilter{fieldType.JsonPaths(), ctx.ns})) {
				throwMultidimensionalArray(name);
			}
			pv.Get(fieldIdx, fieldValues);
			return asArrayResult(std::move(fieldValues));
		}
		return fieldType.Type().EvaluateOneOf(
			[&](concepts::OneOf<KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double, KeyValueType::Float> auto) -> VariantArray {
				pv.Get(fieldIdx, fieldValues);
				if (fieldValues.empty() || fieldValues.IsNullValue()) {
					throw Error(errParams, "Calculating value of an empty field is impossible: '{}'", name);
				}
				return VariantArray{fieldValues.front()};
			},
			[&](concepts::OneOf<KeyValueType::Bool, KeyValueType::String, KeyValueType::Uuid> auto) -> VariantArray {
				pv.Get(fieldIdx, fieldValues);
				if (fieldValues.empty()) {
					return VariantArray{};
				}
				return VariantArray{fieldValues.front()};
			},
			[&](KeyValueType::FloatVector) -> VariantArray { throwWrongFieldType(name); },
			[](concepts::OneOf<KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Composite, KeyValueType::Null> auto)
				-> VariantArray {
				assertrx_throw(false);
				abort();
			});
	}

	getByJsonPath(pv, ctx.ns, fieldValues);
	if (fieldValues.IsNullValue()) {
		return {};
	}
	if (fieldValues.IsArrayValue()) {
		return asArrayResult(std::move(fieldValues));
	}
	if (fieldValues.empty()) {
		return {};
	}
	if (fieldValues.size() == 1) {
		return VariantArray{fieldValues.front()};
	}
	return VariantArray{};
}

std::string ExprField::Dump() const { return quoted ? std::string("\"") + name + "\"" : name; }

ExprScalar ExprField::EvaluateScalar(ExprEvalContext& ctx) const {
	const auto& pv = ctx.payload;
	int fieldIdx = 0;
	const ExprFieldBinding* binding = nullptr;
	const auto access = resolveAccess(ctx, fieldIdx, binding);
	if (access == FieldAccess::Indexed) {
		return scalarFromIndexedField(pv, fieldIdx, name, ctx.whereMode);
	}

	auto& fieldValues = fieldBuf(ctx);
	if (access == FieldAccess::BindingFields) {
		assertrx_throw(binding);
		pv.GetByFieldsSet(binding->fields, fieldValues, binding->fieldType, binding->compositeFieldsTypes);
		if (fieldValues.empty() || fieldValues.IsNullValue()) {
			return {};
		}
		if (fieldValues.size() != 1) {
			throwWrongFieldType(name);
		}
		return scalarFromNumeric(fieldValues.front(), name);
	}

	getByJsonPath(pv, ctx.ns, fieldValues);
	if (fieldValues.IsNullValue() || fieldValues.empty()) {
		return {};
	}
	if (fieldValues.IsArrayValue()) {
		if (fieldValues.size() == 1 && hasExplicitElementIndex(name)) {
			return scalarFromNumeric(fieldValues.front(), name);
		}
		if (ctx.whereMode) {
			throwWrongFieldType(name);
		}
		throw Error(errParams, kArrayNullOutsideConcatError);
	}
	if (fieldValues.size() != 1) {
		throwWrongFieldType(name);
	}
	return scalarFromNumeric(fieldValues.front(), name);
}

std::string ExprUnaryMinus::Dump() const {
	assertrx_throw(child);
	if (child->Type() == ExprNodeType::Binary) {
		return std::string("-(") + child->Dump() + ")";
	}
	return std::string("-") + child->Dump();
}

VariantArray ExprUnaryMinus::Evaluate(ExprEvalContext& ctx) const {
	const auto result = evaluateScalarUnary(*this, ctx);
	if (result.empty()) {
		throw Error(errParams, kArrayNullOutsideConcatError);
	}
	return VariantArray{result.ToVariant()};
}

std::string ExprBinary::Dump() const {
	assertrx_throw(left && right);
	return left->Dump() + std::string{tokenForOp(op)} + right->Dump();
}

VariantArray ExprBinary::Evaluate(ExprEvalContext& ctx) const {
	if (op != ExprBinOp::Concat) {
		const auto result = evaluateScalarBinary(*this, ctx);
		if (result.empty()) {
			throw Error(errParams, kArrayNullOutsideConcatError);
		}
		return VariantArray{result.ToVariant()};
	}

	VariantArray out;
	std::ignore = out.MarkArray();
	appendConcatOperand(*left, ctx, out);
	appendConcatOperand(*right, ctx, out);
	return out;
}

std::string ExprArrayLiteral::Dump() const {
	std::string out = "[";
	for (size_t i = 0; i < values.size(); ++i) {
		if (i != 0) {
			out += ",";
		}
		out += literalToken(values[i]);
	}
	return out + "]";
}

VariantArray ExprArrayLiteral::Evaluate(ExprEvalContext&) const {
	VariantArray out = values;
	std::ignore = out.MarkArray();
	return out;
}

std::string ExprFunctionArg::Dump() const {
	switch (kind) {
		case Kind::QuotedName:
			return "\"" + value + "\"";
		case Kind::String:
			return Variant{value}.Dump();
		case Kind::Name:
			return value;
	}
	return value;
}

ExprFunction::ExprFunction(const BuiltinFunction& fn, Kind functionKind, ExprFunctionArgs stringArguments, TimeUnit unit)
	: ExprNode{ExprNodeType::Function}, kind{functionKind}, stringArgs{std::move(stringArguments)}, timeUnit{unit}, fn_{&fn} {}

ExprFunction::ExprFunction(const BuiltinFunction& fn, Kind functionKind, ExprNodeArgs expressionArguments)
	: ExprNode{ExprNodeType::Function}, kind{functionKind}, exprArgs{std::move(expressionArguments)}, fn_{&fn} {}

std::string ExprFunction::Dump() const {
	assertrx_throw(fn_);
	std::string out = std::string{fn_->Name()} + "(";
	if (!stringArgs.empty()) {
		for (size_t i = 0; i < stringArgs.size(); ++i) {
			if (i != 0) {
				out += ",";
			}
			out += stringArgs[i].Dump();
		}
	} else {
		for (size_t i = 0; i < exprArgs.size(); ++i) {
			if (i != 0) {
				out += ",";
			}
			if (exprArgs[i]) {
				out += exprArgs[i]->Dump();
			}
		}
	}
	return out + ")";
}

VariantArray ExprFunction::Evaluate(ExprEvalContext& ctx) const { return fn_->Execute(ctx, *this); }

ExprScalar ExprFunction::EvaluateScalar(ExprEvalContext& ctx) const { return fn_->ExecuteScalar(ctx, *this); }

void ExprFunction::CollectReferencedFields(ExprReferencedFields& fields) const {
	fn_->CollectReferencedFields(stringArgs, fields);
	for (const auto& arg : exprArgs) {
		if (arg) {
			arg->CollectReferencedFields(fields);
		}
	}
}

bool ExprFunction::UsesNow() const noexcept {
	if (kind == Kind::Now) {
		return true;
	}
	for (const auto& arg : exprArgs) {
		if (arg && arg->UsesNow()) {
			return true;
		}
	}
	return false;
}

bool ExprFunction::ReturnsArray() const noexcept { return fn_->ReturnsArray(); }

}  // namespace reindexer
