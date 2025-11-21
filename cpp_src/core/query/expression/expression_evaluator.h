#pragma once

#include "core/keyvalue/variant.h"

namespace reindexer {

class Token;
class Tokenizer;
class FunctionExecutor;
class TagsMatcher;
class NsContext;
class NamespaceImpl;

class [[nodiscard]] ExpressionEvaluator {
public:
	ExpressionEvaluator(NamespaceImpl& ns, FunctionExecutor& func) noexcept : ns_(ns), functionExecutor_(func) {}

	VariantArray Evaluate(std::string_view expr, const PayloadValue& v, std::string_view forField, const NsContext& ctx);

private:
	struct [[nodiscard]] PrimaryToken {
		enum class [[nodiscard]] Type { Scalar, Array, Command, Null };

		Variant value;
		Type type;
	};

	enum class [[nodiscard]] StringAllowed : bool { No = false, Yes = true };
	enum class [[nodiscard]] NonIntegralAllowed : bool { No = false, Yes = true };
	PrimaryToken getPrimaryToken(Tokenizer& parser, const PayloadValue& v, StringAllowed strAllowed, NonIntegralAllowed nonIntAllowed,
								 Token& outTok, const NsContext& ctx);
	PrimaryToken handleTokenName(Tokenizer& parser, const PayloadValue& v, NonIntegralAllowed nonIntAllowed, Token& outTok,
								 const NsContext& ctx);
	double performSumAndSubtracting(Tokenizer& parser, const PayloadValue& v, const NsContext& ctx);
	double performMultiplicationAndDivision(Tokenizer& parser, const PayloadValue& v, Token& lastTok, const NsContext& ctx);
	double performArrayConcatenation(Tokenizer& parser, const PayloadValue& v, Token& lastTok, const NsContext& ctx);
	void handleCommand(Tokenizer& parser, const PayloadValue& v, const Variant& flag, const NsContext& ctx);

	void captureArrayContent(Tokenizer& parser);
	[[noreturn]] void throwUnexpectedTokenError(Tokenizer& parser, const Token& outTok);

	enum [[nodiscard]] State { None = 0, StateArrayConcat, StateMultiplyAndDivide, StateSumAndSubtract };

	NamespaceImpl& ns_;
	FunctionExecutor& functionExecutor_;
	std::string_view forField_;
	VariantArray arrayValues_;
	State state_ = None;
};
}  // namespace reindexer
