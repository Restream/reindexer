#include "builtin_function.h"

#include <algorithm>
#include <optional>

#include "core/function/error.h"
#include "core/function/function.h"
#include "core/function/function_invoker.h"
#include "core/function/function_parser.h"
#include "core/namespace/namespaceimpl.h"
#include "tools/errors.h"
#include "tools/frozen_str_tools.h"
#include "tools/timetools.h"
#include "vendor/frozen/unordered_map.h"

namespace reindexer {

namespace {

ExprScalar scalarFromVariant(const Variant& value) {
	if (auto scalar = ExprScalar::TryFromVariant(value)) {
		return *scalar;
	}
	throw Error(errParams, "Only integral type non-array fields are supported in arithmetical expressions: {}", value.Dump());
}

ExprScalar invokeUpdateScalarFunction(ExprEvalContext& ctx, const functions::FunctionVariant& fn) {
	if (!ctx.functionInvoker || !ctx.ctx) [[unlikely]] {
		throw Error(errLogic, "FunctionInvoker and NsContext are required to invoke UPDATE functions");
	}
	assertrx_throw(ctx.payload.Value());
	return scalarFromVariant(ctx.functionInvoker->Invoke(fn, *ctx.ctx, *ctx.payload.Value()));
}

std::optional<ExprParseContext::FunctionArg> identFromArg(const ExprNode& node) {
	if (node.Type() == ExprNodeType::Field) {
		const auto& field = static_cast<const ExprField&>(node);
		const auto kind = field.quoted ? ExprParseContext::FunctionArg::Kind::QuotedName : ExprParseContext::FunctionArg::Kind::Name;
		return ExprParseContext::FunctionArg{field.name, kind};
	}
	if (node.Type() == ExprNodeType::Number) {
		const auto& number = static_cast<const ExprNumber&>(node);
		if (number.value.Type().Is<KeyValueType::String>()) {
			return ExprParseContext::FunctionArg{number.value.As<std::string>(), ExprParseContext::FunctionArg::Kind::String};
		}
	}
	return std::nullopt;
}

ExprParseContext::FunctionArgs identsFromArgs(const ExprNodeArgs& args, ExprParseContext& ctx) {
	ExprParseContext::FunctionArgs idents;
	for (const auto& arg : args) {
		auto ident = identFromArg(*arg);
		if (!ident) {
			ctx.ThrowError("Unexpected argument type in expression function");
		}
		idents.emplace_back(std::move(*ident));
	}
	return idents;
}

class [[nodiscard]] IdentifierBuiltinFunction : public BuiltinFunction {
public:
	IdentifierBuiltinFunction(std::string_view name, ExprFunction::Kind kind, Constraints constraints) noexcept
		: BuiltinFunction{name, constraints}, kind_{kind} {}

	ExprNodePtr Bind(ExprNodeArgs args, ExprParseContext& ctx) const final {
		auto idents = identsFromArgs(args, ctx);
		ValidateIdentifierArgs(idents, ctx);
		return CreateNode(std::move(idents));
	}

protected:
	virtual ExprNodePtr CreateNode(ExprFunctionArgs values) const {
		return std::make_unique<ExprFunction>(*this, kind_, std::move(values));
	}

	ExprFunction::Kind kind_;
};

class [[nodiscard]] FlatArrayLenFunction final : public IdentifierBuiltinFunction {
public:
	FlatArrayLenFunction() noexcept
		: IdentifierBuiltinFunction{"flat_array_len", ExprFunction::Kind::FlatArrayLen,
									Constraints{.minArgs = 1, .maxArgs = 1, .allowedArgKinds = kArgName | kArgQuotedName}} {}

	ExprScalar ExecuteScalar(ExprEvalContext& ctx, const ExprFunction& func) const override {
		assertrx_throw(func.stringArgs.size() == 1);
		functions::FlatArrayLen fn{func.stringArgs.front().value};
		assertrx_throw(ctx.payload.Value());
		return ExprScalar::FromInt(static_cast<int64_t>(fn.Evaluate(*ctx.payload.Value(), PayloadTypeOf(ctx.ns), TagsMatcherOf(ctx.ns))));
	}

	void CollectReferencedFields(const ExprFunctionArgs& args, ExprReferencedFields& fields) const override {
		for (const auto& arg : args) {
			fields.emplace_back(arg.value);
		}
	}
};

class [[nodiscard]] NowFunction final : public IdentifierBuiltinFunction {
public:
	NowFunction() noexcept
		: IdentifierBuiltinFunction{"now", ExprFunction::Kind::Now,
									Constraints{.minArgs = 0, .maxArgs = 1, .allowedArgKinds = kArgName | kArgString}} {}

	ExprScalar ExecuteScalar(ExprEvalContext& ctx, const ExprFunction& func) const override {
		if (ctx.functionInvoker) {
			return invokeUpdateScalarFunction(ctx, functions::Now{func.timeUnit});
		}
		if (!ctx.nowTimes) [[unlikely]] {
			throw Error(errLogic, "Timestamp is not provided in expression evaluation context");
		}
		return ExprScalar::FromInt((*ctx.nowTimes)[static_cast<size_t>(func.timeUnit)]);
	}

protected:
	ExprNodePtr CreateNode(ExprFunctionArgs values) const override {
		const TimeUnit unit = values.empty() ? TimeUnit::sec : ToTimeUnit(values.front().value);
		return std::make_unique<ExprFunction>(*this, kind_, std::move(values), unit);
	}
};

class [[nodiscard]] SerialFunction final : public IdentifierBuiltinFunction {
public:
	SerialFunction() noexcept
		: IdentifierBuiltinFunction{"serial", ExprFunction::Kind::Serial,
									Constraints{.minArgs = 0, .maxArgs = 0, .allowedInWhere = false}} {}

	ExprScalar ExecuteScalar(ExprEvalContext& ctx, const ExprFunction&) const override {
		functions::ParsedFunction parsed;
		parsed.funcName = "serial";
		parsed.field = std::string{ctx.forField};
		return invokeUpdateScalarFunction(ctx, functions::Create(std::move(parsed)));
	}
};

class [[nodiscard]] ArrayRemoveFunction final : public BuiltinFunction {
public:
	explicit ArrayRemoveFunction(bool once) noexcept
		: BuiltinFunction{once ? "array_remove_once" : "array_remove",
						  Constraints{.minArgs = 2, .maxArgs = 2, .allowedInWhere = false, .exprArgs = true, .returnsArray = true}},
		  once_{once} {}

	ExprNodePtr Bind(ExprNodeArgs args, ExprParseContext&) const override {
		return std::make_unique<ExprFunction>(*this, once_ ? ExprFunction::Kind::ArrayRemoveOnce : ExprFunction::Kind::ArrayRemove,
											  std::move(args));
	}

	VariantArray Execute(ExprEvalContext& ctx, const ExprFunction& func) const override {
		assertrx_throw(func.exprArgs.size() == 2);
		auto values = func.exprArgs[0]->Evaluate(ctx);
		auto remove = func.exprArgs[1]->Evaluate(ctx);
		if (!values.IsArrayValue()) {
			if (values.empty()) {
				std::ignore = values.MarkArray();
				return values;
			}
			throw Error(errParams, "Only an array field is expected as first parameter of command 'array_remove_once/array_remove'");
		}
		std::ignore = values.MarkArray();
		if (remove.IsArrayValue() || remove.size() != 1) {
			std::ignore = remove.MarkArray();
		}
		for (const auto& item : remove) {
			const auto matches = [&item](const auto& elem) {
				return item.RelaxCompare<WithString::Yes, NotComparable::Return, kDefaultNullsHandling>(elem) == ComparationResult::Eq;
			};
			if (once_) {
				if (auto it = std::find_if(values.begin(), values.end(), matches); it != values.end()) {
					std::ignore = values.erase(it);
				}
			} else {
				std::ignore = values.erase(std::remove_if(values.begin(), values.end(), matches), values.end());
			}
		}
		std::ignore = values.MarkArray();
		return values;
	}

private:
	bool once_{false};
};

const FlatArrayLenFunction kFlatArrayLenFn;
const NowFunction kNowFn;
const SerialFunction kSerialFn;
const ArrayRemoveFunction kArrayRemoveFn{false};
const ArrayRemoveFunction kArrayRemoveOnceFn{true};

template <std::size_t N>
constexpr auto MakeFunctionsMap(const std::pair<std::string_view, const BuiltinFunction*> (&items)[N]) {
	return frozen::make_unordered_map<std::string_view, const BuiltinFunction*>(items, frozen::nocase_hash_str{},
																				frozen::nocase_equal_str{});
}

constexpr auto kBuiltinFunctions = MakeFunctionsMap({
	{"flat_array_len", &kFlatArrayLenFn},
	{"now", &kNowFn},
	{"serial", &kSerialFn},
	{"array_remove", &kArrayRemoveFn},
	{"array_remove_once", &kArrayRemoveOnceFn},
});

}  // namespace

const PayloadType& BuiltinFunction::PayloadTypeOf(const NamespaceImpl& ns) noexcept { return ns.payloadType(); }

const TagsMatcher& BuiltinFunction::TagsMatcherOf(const NamespaceImpl& ns) noexcept { return ns.tagsMatcher(); }

void BuiltinFunction::CollectReferencedFields(const ExprFunctionArgs&, ExprReferencedFields&) const {}

VariantArray BuiltinFunction::Execute(ExprEvalContext& ctx, const ExprFunction& func) const {
	return VariantArray{ExecuteScalar(ctx, func).ToVariant()};
}

ExprScalar BuiltinFunction::ExecuteScalar(ExprEvalContext&, const ExprFunction&) const {
	throw Error(errParams, "Array result is not allowed in arithmetic expression");
}

void BuiltinFunction::Validate(const ExprNodeArgs& args, ExprParseContext& ctx) const {
	if (ctx.WhereMode() && !constraints_.allowedInWhere) {
		ctx.ThrowError("Unsupported construct in WHERE arithmetic expression: '" + std::string{name_} + "'");
	}
	if (args.size() < constraints_.minArgs || args.size() > constraints_.maxArgs) {
		if (constraints_.minArgs == 0 && constraints_.maxArgs == 1) {
			functions::errors::throwExpectsOneOrZeroArgumentsError(errParams, name_, args.size());
		}
		if (constraints_.minArgs == 1 && constraints_.maxArgs == 1) {
			functions::errors::throwExpectsOneArgumentError(errParams, name_, args.size());
		}
		if (constraints_.minArgs == constraints_.maxArgs) {
			throw Error(errParams, "'{}' expects {} arguments, but {} were provided", name_, constraints_.minArgs, args.size());
		}
		throw Error(errParams, "'{}' expects {} to {} arguments, but {} were provided", name_, constraints_.minArgs, constraints_.maxArgs,
					args.size());
	}
}

void BuiltinFunction::ValidateIdentifierArgs(const ExprParseContext::FunctionArgs& args, ExprParseContext& ctx) const {
	for (const auto& arg : args) {
		const auto bit = static_cast<uint8_t>(1u << static_cast<uint8_t>(arg.kind));
		if ((constraints_.allowedArgKinds & bit) == 0) {
			ctx.ThrowError("Unexpected argument type in expression function");
		}
	}
}

const BuiltinFunction* BuiltinFunction::Find(std::string_view name) noexcept {
	const auto it = kBuiltinFunctions.find(name);
	if (it == kBuiltinFunctions.end()) {
		return nullptr;
	}
	return it->second;
}

ExprNodePtr BuiltinFunction::Create(std::string_view name, ExprNodeArgs args, ExprParseContext& ctx) {
	const auto* fn = Find(name);
	if (!fn) {
		throw Error(errParams, "Function '{}' is not supported", name);
	}
	fn->Validate(args, ctx);
	return fn->Bind(std::move(args), ctx);
}

}  // namespace reindexer
