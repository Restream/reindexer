#include "arithmetic_expression.h"
#include "tools/serilize/wrserializer.h"

namespace reindexer::expressions {

ArithmeticExpression::ArithmeticExpression(std::string_view expr)
	: ast_{ExpressionAst::Parse(expr, /*whereMode=*/true)}, source_{expr}, usesNow_{ast_.UsesNow()} {}

void ArithmeticExpression::Serialize(WrSerializer& ser) const {
	ser.PutVarUint(ExpressionTypeArithmetic);
	ser.PutVString(Source());
}

}  // namespace reindexer::expressions
