#pragma once

#include "core/keyvalue/variant.h"

namespace reindexer {

class token;
class tokenizer;
class FunctionExecutor;
class TagsMatcher;
class NsContext;

class ExpressionEvaluator {
public:
	ExpressionEvaluator(const PayloadType& type, TagsMatcher& tagsMatcher, FunctionExecutor& func);

	VariantArray Evaluate(tokenizer& parser, const PayloadValue& v, std::string_view forField, const NsContext& ctx);
	VariantArray Evaluate(std::string_view expr, const PayloadValue& v, std::string_view forField, const NsContext& ctx);

private:
	double getPrimaryToken(tokenizer& parser, const PayloadValue& v, const NsContext& ctx);
	double performSumAndSubtracting(tokenizer& parser, const PayloadValue& v, const NsContext& ctx);
	double performMultiplicationAndDivision(tokenizer& parser, const PayloadValue& v, token& lastTok, const NsContext& ctx);
	double performArrayConcatenation(tokenizer& parser, const PayloadValue& v, token& lastTok, const NsContext& ctx);

	void captureArrayContent(tokenizer& parser);

	enum State { None = 0, StateArrayConcat, StateMultiplyAndDivide, StateSumAndSubtract };

	const PayloadType& type_;
	TagsMatcher& tagsMatcher_;
	FunctionExecutor& functionExecutor_;
	std::string forField_;
	VariantArray arrayValues_;
	State state_ = None;
};
}  // namespace reindexer
