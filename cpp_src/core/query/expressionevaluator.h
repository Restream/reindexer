#pragma once

#include "core/keyvalue/variant.h"

namespace reindexer {

class token;
class tokenizer;
class FunctionExecutor;
class TagsMatcher;

class ExpressionEvaluator {
public:
	ExpressionEvaluator(const PayloadType& type, TagsMatcher& tagsMatcher, FunctionExecutor& func);

	VariantArray Evaluate(tokenizer& parser, const PayloadValue& v, string_view forField);
	VariantArray Evaluate(const string_view& expr, const PayloadValue& v, string_view forField);

private:
	double getPrimaryToken(tokenizer& parser, const PayloadValue& v);
	double performSumAndSubtracting(tokenizer& parser, const PayloadValue& v);
	double performMultiplicationAndDivision(tokenizer& parser, const PayloadValue& v, token& lastTok);
	double performArrayConcatenation(tokenizer& parser, const PayloadValue& v, token& lastTok);

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
