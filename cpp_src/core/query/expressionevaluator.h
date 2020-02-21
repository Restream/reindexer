#pragma once

#include "core/keyvalue/variant.h"

namespace reindexer {

class token;
class tokenizer;
class FunctionExecutor;
class TagsMatcher;

class ExpressionEvaluator {
public:
	ExpressionEvaluator(const PayloadType& type, TagsMatcher& tagsMatcher, FunctionExecutor& func, const string& forField);

	Variant Evaluate(tokenizer& parser, const PayloadValue& v);
	Variant Evaluate(const string_view& expr, const PayloadValue& v);

private:
	double getPrimaryToken(tokenizer& parser, const PayloadValue& v);
	double performSumAndSubtracting(tokenizer& parser, const PayloadValue& v);
	double performMultiplicationAndDivision(tokenizer& parser, const PayloadValue& v, token& lastTok);

	const PayloadType& type_;
	TagsMatcher& tagsMatcher_;
	FunctionExecutor& functionExecutor_;
	string forField_;
};
}  // namespace reindexer
