#pragma once

#include "core/keyvalue/variant.h"

namespace reindexer {

class tokenizer;
class FunctionExecutor;

class ExpressionEvaluator {
public:
	ExpressionEvaluator(const PayloadType& type, FunctionExecutor& func, const string& forField);

	Variant Evaluate(tokenizer& parser, const PayloadValue& v);
	Variant Evaluate(const string_view& expr, const PayloadValue& v);

private:
	double getPrimaryToken(tokenizer& parser, const PayloadValue& v);
	double performSumAndSubtracting(tokenizer& parser, const PayloadValue& v);
	double performMultiplicationAndDivision(tokenizer& parser, const PayloadValue& v);

	const PayloadType& type_;
	FunctionExecutor& functionExecutor_;
	string forField_;
};
}  // namespace reindexer
