#pragma once

#include "core/keyvalue/variant.h"

namespace reindexer {

class token;
class tokenizer;
class FunctionExecutor;
class TagsMatcher;
class NsContext;

class [[nodiscard]] ExpressionEvaluator {
public:
	ExpressionEvaluator(const PayloadType& type, TagsMatcher& tagsMatcher, FunctionExecutor& func) noexcept
		: type_(type), tagsMatcher_(tagsMatcher), functionExecutor_(func) {}

	VariantArray Evaluate(std::string_view expr, const PayloadValue& v, std::string_view forField, const NsContext& ctx);

private:
	struct [[nodiscard]] PrimaryToken {
		enum class [[nodiscard]] Type { Scalar, Array, Command, Null };

		Variant value;
		Type type;
	};

	enum class [[nodiscard]] StringAllowed : bool { No = false, Yes = true };
	enum class [[nodiscard]] NonIntegralAllowed : bool { No = false, Yes = true };
	PrimaryToken getPrimaryToken(tokenizer& parser, const PayloadValue& v, StringAllowed strAllowed, NonIntegralAllowed nonIntAllowed,
								 token& outTok, const NsContext& ctx);
	PrimaryToken handleTokenName(tokenizer& parser, const PayloadValue& v, NonIntegralAllowed nonIntAllowed, token& outTok,
								 const NsContext& ctx);
	double performSumAndSubtracting(tokenizer& parser, const PayloadValue& v, const NsContext& ctx);
	double performMultiplicationAndDivision(tokenizer& parser, const PayloadValue& v, token& lastTok, const NsContext& ctx);
	double performArrayConcatenation(tokenizer& parser, const PayloadValue& v, token& lastTok, const NsContext& ctx);
	void handleCommand(tokenizer& parser, const PayloadValue& v, const Variant& flag, const NsContext& ctx);

	void captureArrayContent(tokenizer& parser);
	[[noreturn]] void throwUnexpectedTokenError(tokenizer& parser, const token& outTok);

	enum [[nodiscard]] State { None = 0, StateArrayConcat, StateMultiplyAndDivide, StateSumAndSubtract };

	const PayloadType& type_;
	TagsMatcher& tagsMatcher_;
	FunctionExecutor& functionExecutor_;
	std::string_view forField_;
	VariantArray arrayValues_;
	State state_ = None;
};
}  // namespace reindexer
