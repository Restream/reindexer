#include "comparison.h"
#include "core/query/condition_inversion.h"
#include "core/query/query.h"
#include "core/query/query_impl.h"
#include "core/type_consts_helpers.h"
#include "estl/concepts.h"
#include "tools/serilize/wrserializer.h"

namespace reindexer {
namespace comparison {

Operand Classify(expressions::ArithmeticExpression&& expression) {
	if (const auto field = expression.AsField()) {
		return Field{std::string{*field}};
	}
	if (auto function = expression.AsWhereFunction()) {
		return Function{std::move(*function), std::move(expression)};
	}
	if (auto literal = expression.AsLiteral()) {
		return Literal{std::move(*literal), std::move(expression)};
	}
	return Expression{std::move(expression)};
}

namespace {

bool isFlatArrayLen(const functions::FunctionVariant& function) noexcept {
	return std::holds_alternative<functions::FlatArrayLen>(function);
}
bool isNow(const functions::FunctionVariant& function) noexcept { return std::holds_alternative<functions::Now>(function); }
bool isNumeric(const Literal& literal) noexcept {
	return literal.value.Type().IsOneOf<KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double, KeyValueType::Float>();
}

VariantArray takeValues(Literal& literal) {
	VariantArray values;
	values.emplace_back(std::move(literal.value));
	return values;
}
VariantArray&& takeValues(Values& values) noexcept { return std::move(values.values); }

std::string literalText(const Literal& literal) {
	return literal.expression ? literal.expression->Dump() : literal.value.As<std::string>();
}
std::string valuesText(const Values& values) {
	WrSerializer ser;
	ser << '(';
	for (const auto& value : values.values) {
		if (&value != &values.values[0]) {
			ser << ", ";
		}
		value.Dump(ser, CheckIsStringPrintable::No);
	}
	ser << ')';
	return std::string{ser.Slice()};
}

expressions::ArithmeticExpression takeExpression(Function& function) {
	if (function.expression) {
		return std::move(*function.expression);
	}
	return std::visit([](const auto& fn) { return expressions::ArithmeticExpression{fn.ToString()}; }, function.function);
}

class [[nodiscard]] Builder {
public:
	Builder(QueryImpl query, OpType op, CondType cond) noexcept : q_{query}, op_{op}, cond_{cond} {}

	void operator()(Field& l, Field& r) { q_.AddCondition<BetweenFieldsQueryEntry>(op_, std::move(l.name), cond_, std::move(r.name)); }
	void operator()(Field& l, concepts::OneOf<Literal, Values> auto& r) {
		q_.AddCondition<QueryEntry>(op_, std::move(l.name), cond_, takeValues(r));
	}
	void operator()(Field& l, Function& r) {
		if (isFlatArrayLen(r.function)) {
			q_.AddCondition<QueryArithmeticEntry>(op_, std::move(l.name), cond_, takeExpression(r));
		} else {
			q_.AddConditionFunction(op_, std::move(l.name), cond_, std::move(r.function));
		}
	}
	void operator()(Field& l, Expression& r) {
		q_.AddCondition<QueryArithmeticEntry>(op_, std::move(l.name), cond_, std::move(r.expression));
	}
	void operator()(Field& l, SubQuery& r) { q_.AddConditionSubQuery(op_, std::move(l.name), cond_, std::move(*r.query)); }

	void operator()(concepts::OneOf<Literal, Values> auto& l, Field& r) {
		auto values = takeValues(l);
		if (const auto inverted = TryInvertCondition(cond_)) {
			VerifyQueryEntryValues(cond_, values);
			q_.AddCondition<QueryEntry>(op_, std::move(r.name), *inverted, std::move(values));
		} else {
			checkLeftValuesNotEmpty(values);
			q_.AddCondition<QueryArithmeticEntry>(op_, std::move(values), cond_, std::move(r.name));
		}
	}
	void operator()(Literal& l, Literal& r) {
		if (!isNumeric(l) || !isNumeric(r)) [[unlikely]] {
			throwFieldExpected(OperandError::Site::Right, literalText(r));
		}
		addValuesComparison(takeValues(l), takeValues(r));
	}
	void operator()(Literal& l, Values& r) {
		if (!isNumeric(l)) [[unlikely]] {
			throwFieldExpected(OperandError::Site::Right, valuesText(r));
		}
		addValuesComparison(takeValues(l), takeValues(r));
	}
	void operator()(Values& l, Literal& r) {
		if (!isNumeric(r)) [[unlikely]] {
			throwFieldExpected(OperandError::Site::Right, literalText(r));
		}
		addValuesComparison(takeValues(l), takeValues(r));
	}
	void operator()(Values& l, Values& r) { addValuesComparison(takeValues(l), takeValues(r)); }
	void operator()(concepts::OneOf<Literal, Values> auto& l, Function& r) {
		auto values = takeValues(l);
		const auto inverted = TryInvertCondition(cond_);
		if (inverted && isFlatArrayLen(r.function)) {
			verifyFlatArrayLenCondition();
			VerifyQueryEntryValues(cond_, values);
			q_.AddConditionFunction(op_, std::move(r.function), *inverted, std::move(values));
		} else {
			addLeftValues(std::move(values), takeExpression(r));
		}
	}
	void operator()(concepts::OneOf<Literal, Values> auto& l, Expression& r) { addLeftValues(takeValues(l), std::move(r.expression)); }
	void operator()(concepts::OneOf<Literal, Values> auto& l, SubQuery& r) {
		auto values = takeValues(l);
		if (const auto inverted = TryInvertCondition(cond_)) {
			VerifyQueryEntryValues(cond_, values);
			q_.AddConditionSubQuery(op_, std::move(*r.query), *inverted, std::move(values));
		} else {
			checkLeftValuesNotEmpty(values);
			q_.AddConditionValuesSubQuery(op_, std::move(values), cond_, std::move(*r.query));
		}
	}

	void operator()(Function& l, Field& r) { q_.AddCondition<QueryArithmeticEntry>(op_, takeExpression(l), cond_, std::move(r.name)); }
	void operator()(Function& l, concepts::OneOf<Literal, Values> auto& r) {
		if (isNow(l.function)) {
			q_.AddCondition<QueryArithmeticEntry>(op_, takeExpression(l), cond_, takeValues(r));
		} else {
			q_.AddConditionFunction(op_, std::move(l.function), cond_, takeValues(r));
		}
	}
	void operator()(Function& l, Function& r) { q_.AddCondition<QueryArithmeticEntry>(op_, takeExpression(l), cond_, takeExpression(r)); }
	void operator()(Function& l, Expression& r) {
		q_.AddCondition<QueryArithmeticEntry>(op_, takeExpression(l), cond_, std::move(r.expression));
	}
	void operator()(Function& l, SubQuery& r) {
		if (!isNow(l.function)) {
			q_.AddConditionFunctionSubQuery(op_, std::move(l.function), cond_, std::move(*r.query));
		} else if (const auto inverted = TryInvertCondition(cond_)) {
			q_.AddConditionSubQueryFunction(op_, std::move(*r.query), *inverted, std::move(l.function));
		} else {
			addArithmeticSubQuery(takeExpression(l), r);
		}
	}

	void operator()(Expression& l, Field& r) {
		q_.AddCondition<QueryArithmeticEntry>(op_, std::move(l.expression), cond_, std::move(r.name));
	}
	void operator()(Expression& l, concepts::OneOf<Literal, Values> auto& r) {
		q_.AddCondition<QueryArithmeticEntry>(op_, std::move(l.expression), cond_, takeValues(r));
	}
	void operator()(Expression& l, Function& r) {
		q_.AddCondition<QueryArithmeticEntry>(op_, std::move(l.expression), cond_, takeExpression(r));
	}
	void operator()(Expression& l, Expression& r) {
		q_.AddCondition<QueryArithmeticEntry>(op_, std::move(l.expression), cond_, std::move(r.expression));
	}
	void operator()(Expression& l, SubQuery& r) { addArithmeticSubQuery(std::move(l.expression), r); }

	void operator()(SubQuery& l, Field& r) {
		if (const auto inverted = TryInvertCondition(cond_)) {
			q_.AddConditionSubQuery(op_, std::move(r.name), *inverted, std::move(*l.query));
		} else {
			q_.AddConditionSubQueryField(op_, std::move(*l.query), cond_, std::move(r.name));
		}
	}
	void operator()(SubQuery& l, concepts::OneOf<Literal, Values> auto& r) {
		q_.AddConditionSubQuery(op_, std::move(*l.query), cond_, takeValues(r));
	}
	void operator()(SubQuery& l, Function& r) {
		if (!isFlatArrayLen(r.function)) {
			q_.AddConditionSubQueryFunction(op_, std::move(*l.query), cond_, std::move(r.function));
		} else if (const auto inverted = TryInvertCondition(cond_)) {
			q_.AddConditionFunctionSubQuery(op_, std::move(r.function), *inverted, std::move(*l.query));
		} else {
			addSubQueryArithmetic(l, takeExpression(r));
		}
	}
	void operator()(SubQuery& l, Expression& r) { addSubQueryArithmetic(l, std::move(r.expression)); }
	[[noreturn]] void operator()(SubQuery&, SubQuery&) { throw Error(errParams, "Comparison of two subqueries is not supported"); }

private:
	void checkLeftValuesNotEmpty(const VariantArray& values) const {
		if (values.empty()) [[unlikely]] {
			throw Error(errParams, "Condition {} requires at least one value on the left side", CondTypeToStrShort(cond_));
		}
	}
	void verifyFlatArrayLenCondition() const {
		try {
			functions::FlatArrayLen::VerifyCondition(cond_);
		} catch (Error& err) {
			throw OperandError(OperandError::Site::Condition, std::move(err));
		}
	}
	void addValuesComparison(VariantArray&& left, VariantArray&& right) {
		checkLeftValuesNotEmpty(left);
		q_.AddCondition<QueryArithmeticEntry>(op_, std::move(left), cond_, std::move(right));
	}
	void addLeftValues(VariantArray&& values, expressions::ArithmeticExpression&& expression) {
		if (const auto inverted = TryInvertCondition(cond_)) {
			q_.AddCondition<QueryArithmeticEntry>(op_, std::move(expression), *inverted, std::move(values));
		} else {
			checkLeftValuesNotEmpty(values);
			q_.AddCondition<QueryArithmeticEntry>(op_, std::move(values), cond_, std::move(expression));
		}
	}
	[[noreturn]] static void throwFieldExpected(OperandError::Site site, std::string_view found) {
		throw OperandError(site, Error(errParams, "Expected field or index name, but found '{}' in query", found));
	}
	void addArithmeticSubQuery(expressions::ArithmeticExpression&& expression, SubQuery& subQuery) {
		SubQueryArithmeticEntry::VerifyCondition(cond_, SubQueryArithmeticEntry::Right);
		q_.AddSubQueryArithmeticEntry(op_, std::move(expression), cond_, std::move(*subQuery.query));
	}
	void addSubQueryArithmetic(SubQuery& subQuery, expressions::ArithmeticExpression&& expression) {
		SubQueryArithmeticEntry::VerifyCondition(cond_, SubQueryArithmeticEntry::Left);
		q_.AddSubQueryArithmeticEntry(op_, std::move(*subQuery.query), cond_, std::move(expression));
	}

	QueryImpl q_;
	OpType op_;
	CondType cond_;
};

void classifyExpression(Operand& operand) {
	if (auto* expression = std::get_if<Expression>(&operand)) {
		operand = Classify(std::move(expression->expression));
	}
}

void verifyRangeBounds(const Operand& left, const Operand& right) {
	if (std::holds_alternative<Literal>(right) || std::holds_alternative<Values>(right) || std::holds_alternative<SubQuery>(right) ||
		(std::holds_alternative<Field>(left) && std::holds_alternative<Field>(right))) {
		return;
	}
	const auto [leftSide, rightSide] = std::visit(
		[]<typename T>(const T&) -> std::pair<std::string_view, std::string_view> {
			if constexpr (std::is_same_v<T, Field>) {
				return {"a field", "two values, a subquery or a field"};
			} else if constexpr (std::is_same_v<T, SubQuery>) {
				return {"a subquery", "two values"};
			} else if constexpr (concepts::OneOf<T, Function, Expression>) {
				return {"an expression", "two values or a subquery"};
			} else {
				return {"a value", "two values or a subquery"};
			}
		},
		left);
	throw OperandError(OperandError::Site::Condition,
					   Error(errParams, "Condition {} with {} on the left side requires {} on the right side",
							 CondTypeToStrShort(CondRange), leftSide, rightSide));
}

}  // namespace
}  // namespace comparison

void Query::addComparison(OpType op, comparison::Operand&& left, CondType cond, comparison::Operand&& right) {
	if (std::holds_alternative<comparison::SubQuery>(left) || std::holds_alternative<comparison::SubQuery>(right)) {
		comparison::classifyExpression(left);
		comparison::classifyExpression(right);
	}
	if (cond == CondRange) {
		comparison::verifyRangeBounds(left, right);
	}
	std::visit(comparison::Builder{QueryImpl{*this}, op, cond}, left, right);
}

}  // namespace reindexer
