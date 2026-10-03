#include "arithmetic_comparator.h"

#include <algorithm>

#include "core/namespace/namespaceimpl.h"
#include "core/nsselecter/querypreprocessor.h"
#include "helpers.h"
#include "tools/compare.h"
#include "tools/timetools.h"

namespace reindexer {

namespace {
std::vector<ExprFieldBinding> resolveExpressionFields(const expressions::ArithmeticExpression& expression, const PayloadType& pt,
													  const NamespaceImpl& ns) {
	std::vector<ExprFieldBinding> bindings;
	bindings.reserve(expression.Ast().FieldNames().size());
	for (const auto& fieldName : expression.Ast().FieldNames()) {
		int fieldIdx = 0;
		if (pt.FieldByName(fieldName, fieldIdx)) {
			bindings.emplace_back(fieldIdx);
			continue;
		}
		QueryField field{fieldName};
		QueryPreprocessor::SetQueryField(field, ns);
		bindings.emplace_back(field.Fields(), field.FieldType(), field.CompositeFieldsTypes());
	}
	return bindings;
}

ComparationResult compareScalars(const ExprScalar& lhs, const ExprScalar& rhs) noexcept {
	if (lhs.empty() || rhs.empty()) [[unlikely]] {
		return ComparationResult::NotComparable;
	}
	if (lhs.kind == ExprScalar::Kind::Int && rhs.kind == ExprScalar::Kind::Int) {
		return compare(lhs.i, rhs.i);
	}
	if (lhs.kind == ExprScalar::Kind::Int) {
		return compare(lhs.i, rhs.d);
	}
	if (rhs.kind == ExprScalar::Kind::Int) {
		return compare(lhs.d, rhs.i);
	}
	return compare(lhs.d, rhs.d);
}

bool matchCondition(ComparationResult res, CondType cond) {
	switch (cond) {
		case CondEq:
		case CondSet:
		case CondAllSet:
			return res == ComparationResult::Eq;
		case CondLt:
			return res == ComparationResult::Lt;
		case CondLe:
			return res & ComparationResult::Le;
		case CondGt:
			return res == ComparationResult::Gt;
		case CondGe:
			return res & ComparationResult::Ge;
		case CondRange:
		case CondAny:
		case CondEmpty:
		case CondLike:
		case CondDWithin:
		case CondKnn:
			break;
	}
	assertrx_throw(false);
	return false;
}

ComparationResult compareVariants(const Variant& lhs, const Variant& rhs) {
	return (rhs.IsNullValue() || lhs.IsNullValue())
			   ? lhs.RelaxCompare<WithString::Yes, NotComparable::Return, kWhereCompareNullHandling>(rhs)
			   : lhs.RelaxCompare<WithString::Yes, NotComparable::Throw, kWhereCompareNullHandling>(rhs);
}

ComparationResult compareScalarAndVariant(const ExprScalar& lhs, const Variant& rhs) {
	if (auto scalar = ExprScalar::TryFromVariant(rhs)) {
		return compareScalars(lhs, *scalar);
	}
	return compareVariants(lhs.ToVariant(), rhs);
}

ComparationResult compareVariantAndScalar(const Variant& lhs, const ExprScalar& rhs) {
	if (auto scalar = ExprScalar::TryFromVariant(lhs)) {
		return compareScalars(*scalar, rhs);
	}
	return compareVariants(lhs, rhs.ToVariant());
}

bool satisfy(const ExprScalar& left, CondType cond, const ExprScalar& right) { return matchCondition(compareScalars(left, right), cond); }

template <typename Range, typename Compare>
bool satisfyScalarVsRange(const ExprScalar& left, CondType cond, const Range& right, Compare compare) {
	switch (cond) {
		case CondEq:
		case CondSet:
			return std::ranges::any_of(right, [&](const auto& rhs) { return compare(left, rhs) == ComparationResult::Eq; });
		case CondAllSet:
			return right.size() <= 1 && (right.empty() || compare(left, right.front()) == ComparationResult::Eq);
		case CondLt:
		case CondLe:
		case CondGt:
		case CondGe:
			return std::ranges::any_of(right, [&](const auto& rhs) { return matchCondition(compare(left, rhs), cond); });
		case CondRange:
			assertrx_throw(right.size() == 2);
			return (compare(left, right[0]) & ComparationResult::Ge) && (compare(left, right[1]) & ComparationResult::Le);
		case CondAny:
		case CondEmpty:
		case CondLike:
		case CondDWithin:
		case CondKnn:
			break;
	}
	assertrx_throw(false);
	return false;
}

template <typename Range, typename Compare>
bool satisfyRangeVsScalar(const Range& left, CondType cond, const ExprScalar& right, Compare compare) {
	switch (cond) {
		case CondEq:
		case CondSet:
		case CondAllSet:
			return std::ranges::any_of(left, [&](const auto& lhs) { return compare(lhs, right) == ComparationResult::Eq; });
		case CondLt:
		case CondLe:
		case CondGt:
		case CondGe:
			return std::ranges::any_of(left, [&](const auto& lhs) { return matchCondition(compare(lhs, right), cond); });
		case CondRange:
		case CondAny:
		case CondEmpty:
		case CondLike:
		case CondDWithin:
		case CondKnn:
			break;
	}
	assertrx_throw(false);
	return false;
}
}  // namespace

ArithmeticComparator::ArithmeticComparator(QueryArithmeticEntry&& entry, PayloadType pt, const NamespaceImpl& ns,
										   std::optional<int64_t> nowNsec)
	: entry_{std::move(entry)}, pt_{std::move(pt)}, ns_{&ns} {
	if (entry_.GetLeftKind() == QueryArithmeticEntry::LeftKind::Arithmetic) {
		leftExprFieldBindings_ = resolveExpressionFields(entry_.LeftExpr(), pt_, ns);
	}
	if (entry_.GetRightKind() == QueryArithmeticEntry::RightKind::Arithmetic) {
		rightExprFieldBindings_ = resolveExpressionFields(entry_.RightExpr(), pt_, ns);
	}
	if (entry_.UsesNow()) {
		if (!nowNsec.has_value()) [[unlikely]] {
			throw Error(errLogic, "now() snapshot is not provided for arithmetic condition '{}'", entry_.Dump());
		}
		auto& nowTimes = nowTimes_.emplace();
		nowTimes[static_cast<size_t>(TimeUnit::sec)] = ConvertTime(*nowNsec, TimeUnit::nsec, TimeUnit::sec);
		nowTimes[static_cast<size_t>(TimeUnit::msec)] = ConvertTime(*nowNsec, TimeUnit::nsec, TimeUnit::msec);
		nowTimes[static_cast<size_t>(TimeUnit::usec)] = ConvertTime(*nowNsec, TimeUnit::nsec, TimeUnit::usec);
		nowTimes[static_cast<size_t>(TimeUnit::nsec)] = *nowNsec;
	}
	if (entry_.GetRightKind() == QueryArithmeticEntry::RightKind::Values) {
		rightValueScalars_.reserve(entry_.Values().size());
		const bool allScalar = std::ranges::all_of(entry_.Values(), [&](const Variant& value) {
			auto scalar = ExprScalar::TryFromVariant(value);
			if (scalar) {
				rightValueScalars_.emplace_back(*scalar);
			}
			return scalar.has_value();
		});
		if (!allScalar) {
			rightValueScalars_.clear();
		}
	}
}

std::string ArithmeticComparator::ConditionStr() const {
	switch (entry_.Condition()) {
		case CondGt:
			return std::string{comparators::CondToStr<CondGt>()};
		case CondGe:
			return std::string{comparators::CondToStr<CondGe>()};
		case CondLt:
			return std::string{comparators::CondToStr<CondLt>()};
		case CondLe:
			return std::string{comparators::CondToStr<CondLe>()};
		case CondEq:
			return std::string{comparators::CondToStr<CondEq>()};
		case CondSet:
			return "IN";
		case CondRange:
			return "RANGE";
		case CondAllSet:
			return "ALLSET";
		case CondAny:
		case CondEmpty:
		case CondLike:
		case CondDWithin:
		case CondKnn:
			break;
	}
	return {};
}

const VariantArray& ArithmeticComparator::fieldValues(ConstPayload item, const QueryField& field) const {
	fieldValuesScratch_.Clear();
	item.GetByFieldsSet(field.Fields(), fieldValuesScratch_, field.FieldType(), field.CompositeFieldsTypes());
	if (fieldValuesScratch_.IsNullValue()) {
		fieldValuesScratch_.Clear();
		return fieldValuesScratch_;
	}
	if (!fieldValuesScratch_.empty() &&
		(fieldValuesScratch_[0].Type().Is<KeyValueType::Composite>() || fieldValuesScratch_[0].Type().Is<KeyValueType::Tuple>())) {
		throw Error(errQueryExec, "Composite or tuple field in WHERE arithmetic expression: {}", field.FieldName());
	}
	return fieldValuesScratch_;
}

ExprScalar ArithmeticComparator::evalExpr(const expressions::ArithmeticExpression& expr, const ConstPayload& item,
										  std::span<const ExprFieldBinding> fieldBindings) const {
	assertrx_throw(ns_);
	ExprEvalContext ctx{.ns = *ns_,
						.functionInvoker = nullptr,
						.ctx = nullptr,
						.forField = {},
						.whereMode = true,
						.fieldBindings = fieldBindings,
						.nowTimes = nowTimes_ ? &*nowTimes_ : nullptr,
						.payload = item,
						.fieldScratch = exprScratch_};
	return expr.Ast().EvaluateScalar(ctx);
}

bool ArithmeticComparator::compare(ConstPayload item) const {
	if (entry_.GetLeftKind() == QueryArithmeticEntry::LeftKind::Field) {
		const auto& left = fieldValues(item, entry_.LeftField());
		const auto right = evalExpr(entry_.RightExpr(), item, rightExprFieldBindings_);
		if (left.empty() || right.empty()) {
			return false;
		}
		return satisfyRangeVsScalar(left, entry_.Condition(), right, compareVariantAndScalar);
	}

	const auto left = evalExpr(entry_.LeftExpr(), item, leftExprFieldBindings_);
	switch (entry_.GetRightKind()) {
		case QueryArithmeticEntry::RightKind::Values:
			if (left.empty()) {
				return false;
			}
			return rightValueScalars_.size() == entry_.Values().size()
					   ? satisfyScalarVsRange(left, entry_.Condition(), rightValueScalars_, compareScalars)
					   : satisfyScalarVsRange(left, entry_.Condition(), entry_.Values(), compareScalarAndVariant);
		case QueryArithmeticEntry::RightKind::Field: {
			const auto& right = fieldValues(item, entry_.RightField());
			if (left.empty() || right.empty()) {
				return false;
			}
			return satisfyScalarVsRange(left, entry_.Condition(), right, compareScalarAndVariant);
		}
		case QueryArithmeticEntry::RightKind::Arithmetic: {
			const auto right = evalExpr(entry_.RightExpr(), item, rightExprFieldBindings_);
			if (left.empty() || right.empty()) {
				return false;
			}
			return satisfy(left, entry_.Condition(), right);
		}
	}
	assertrx_throw(false);
	return false;
}

}  // namespace reindexer
