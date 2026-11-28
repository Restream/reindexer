#include "arithmetic_comparator.h"

#include <algorithm>
#include <cmath>

#include "core/item_tree/data_extractor.h"
#include "core/keyvalue/condition_comparison.h"
#include "core/nsselecter/query_field_resolver.h"
#include "core/query/condition_inversion.h"
#include "helpers.h"
#include "tools/compare.h"
#include "tools/timetools.h"

namespace reindexer {

namespace {
std::vector<ExprFieldBinding> resolveExpressionFields(const expressions::ArithmeticExpression& expression, const PayloadType& pt,
													  const QueryFieldResolver& fieldResolver) {
	std::vector<ExprFieldBinding> bindings;
	bindings.reserve(expression.Ast().FieldNames().size());
	for (const auto& fieldName : expression.Ast().FieldNames()) {
		int fieldIdx = 0;
		if (pt.FieldByName(fieldName, fieldIdx)) {
			bindings.emplace_back(fieldIdx, pt.Field(fieldIdx));
			continue;
		}
		QueryField field{fieldName};
		fieldResolver.Resolve(field);
		bindings.emplace_back(field.Fields(), field.FieldType(), field.CompositeFieldsTypes());
	}
	return bindings;
}

FieldsSet resolveFlatArrayLenField(std::string_view name, const QueryFieldResolver& fieldResolver) {
	QueryField field{name};
	fieldResolver.Resolve(field);
	if (field.Fields().size() != 1) {
		throw Error(errQueryExec,
					"Conditions with user-defined functions are not allowed for composite indexes with more than one jsonpath.");
	}
	return field.Fields();
}

void collectFunctionFieldBindings(const ExprNode* node, const QueryFieldResolver& fieldResolver,
								  std::vector<ExprFunctionFieldBinding>& bindings) {
	if (!node) {
		return;
	}
	switch (node->Type()) {
		case ExprNodeType::Binary: {
			const auto& binary = static_cast<const ExprBinary&>(*node);
			collectFunctionFieldBindings(binary.left.get(), fieldResolver, bindings);
			collectFunctionFieldBindings(binary.right.get(), fieldResolver, bindings);
			return;
		}
		case ExprNodeType::UnaryMinus:
			collectFunctionFieldBindings(static_cast<const ExprUnaryMinus&>(*node).child.get(), fieldResolver, bindings);
			return;
		case ExprNodeType::Function: {
			const auto& function = static_cast<const ExprFunction&>(*node);
			if (function.kind == ExprFunction::Kind::FlatArrayLen && function.stringArgs.size() == 1) {
				bindings.push_back(
					{.function = &function, .fields = resolveFlatArrayLenField(function.stringArgs.front().value, fieldResolver)});
			}
			for (const auto& arg : function.exprArgs) {
				collectFunctionFieldBindings(arg.get(), fieldResolver, bindings);
			}
			return;
		}
		case ExprNodeType::Number:
		case ExprNodeType::Field:
		case ExprNodeType::ArrayLiteral:
			return;
	}
}

std::optional<size_t> collectSideBindings(const expressions::ArithmeticExpression& expr, const QueryFieldResolver& fieldResolver,
										  std::vector<ExprFunctionFieldBinding>& bindings) {
	const size_t first = bindings.size();
	const ExprNode* root = expr.Ast().Root();
	collectFunctionFieldBindings(root, fieldResolver, bindings);
	if (root && bindings.size() > first && bindings[first].function == root) {
		return first;
	}
	return std::nullopt;
}

QueryArithmeticEntry makeFlatArrayLenEntry(const QueryFunctionEntry& entry) {
	const auto* flatArrayLen = std::get_if<functions::FlatArrayLen>(&entry.FunctionVariant());
	if (!flatArrayLen) {
		throw Error{errQueryExec, "Function '{}' is not supported yet.",
					std::visit([](const auto& function) { return function.Name(); }, entry.FunctionVariant())};
	}
	if (entry.Fields() != 1) {
		throw Error(errQueryExec, "flat_array_len() expects values only for 1 field, but {} were provided", entry.Fields());
	}
	functions::FlatArrayLen::VerifyCondition(entry.Condition());
	return {expressions::ArithmeticExpression{*flatArrayLen}, entry.Condition(), VariantArray{entry.Values()}};
}

constexpr size_t kSortedInValuesThreshold = 16;

ComparationResult compareScalars(const ExprScalar& lhs, const ExprScalar& rhs) noexcept {
	if (lhs.empty() || rhs.empty() || (lhs.kind == ExprScalar::Kind::Double && std::isnan(lhs.d)) ||
		(rhs.kind == ExprScalar::Kind::Double && std::isnan(rhs.d))) [[unlikely]] {
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
			   ? CompareForCondition<WithString::Yes, NotComparable::Return, kWhereCompareNullHandling>(lhs, rhs)
			   : CompareForCondition<WithString::Yes, NotComparable::Throw, kWhereCompareNullHandling>(lhs, rhs);
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
			return !right.empty() &&
				   std::ranges::all_of(right, [&](const auto& rhs) { return compare(left, rhs) == ComparationResult::Eq; });
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
										   NamespaceMetadataView metadata, const TagsMatcher& tm, std::optional<int64_t> nowNsec)
	: entry_{std::move(entry)}, pt_{std::move(pt)}, metadata_{metadata}, ns_{&ns}, tm_{&tm} {
	const QueryFieldResolver fieldResolver{metadata_};
	if (entry_.GetLeftKind() == QueryArithmeticEntry::LeftKind::Arithmetic) {
		leftExprFieldBindings_ = resolveExpressionFields(entry_.LeftExpr(), pt_, fieldResolver);
		leftFlatArrayLenBinding_ = collectSideBindings(entry_.LeftExpr(), fieldResolver, functionFieldBindings_);
	}
	if (entry_.GetRightKind() == QueryArithmeticEntry::RightKind::Arithmetic) {
		rightExprFieldBindings_ = resolveExpressionFields(entry_.RightExpr(), pt_, fieldResolver);
		rightFlatArrayLenBinding_ = collectSideBindings(entry_.RightExpr(), fieldResolver, functionFieldBindings_);
	}
	if (entry_.UsesNow()) {
		if (!nowNsec.has_value()) [[unlikely]] {
			throw Error(errLogic, "now() snapshot is not provided for arithmetic condition '{}'", entry_.Dump());
		}
		nowTimes_ = MakeNowTimes(*nowNsec);
	}
	if (entry_.GetLeftKind() == QueryArithmeticEntry::LeftKind::Arithmetic &&
		entry_.GetRightKind() == QueryArithmeticEntry::RightKind::Values) {
		numericValues_ = NumericValues::Prepare(entry_.Values(), entry_.Condition(), leftFlatArrayLenBinding_);
	} else if (entry_.GetLeftKind() == QueryArithmeticEntry::LeftKind::Values &&
			   entry_.GetRightKind() == QueryArithmeticEntry::RightKind::Arithmetic) {
		numericValues_ =
			NumericValues::Prepare(entry_.LeftValues(), InvertConditionWithScalarRhs(entry_.Condition()), rightFlatArrayLenBinding_);
	}
}

ArithmeticComparator::ArithmeticComparator(const QueryFunctionEntry& entry, PayloadType pt, const NamespaceImpl& ns,
										   NamespaceMetadataView metadata, const TagsMatcher& tm)
	: entry_{makeFlatArrayLenEntry(entry)}, pt_{std::move(pt)}, metadata_{metadata}, ns_{&ns}, tm_{&tm} {
	functionFieldBindings_.push_back(
		{.function = static_cast<const ExprFunction*>(entry_.LeftExpr().Ast().Root()), .fields = entry.FieldData(0).Fields()});
	leftFlatArrayLenBinding_ = 0;
	numericValues_ = NumericValues::Prepare(entry_.Values(), entry_.Condition(), leftFlatArrayLenBinding_);
}

bool ArithmeticComparator::IsRowIndependent(const QueryArithmeticEntry& entry) noexcept {
	const auto independentOfRow = [](const expressions::ArithmeticExpression& expr) noexcept {
		return expr.Ast().FieldNames().empty() && expr.Ast().ReferencedFields().empty();
	};
	if (entry.GetLeftKind() != QueryArithmeticEntry::LeftKind::Arithmetic || !independentOfRow(entry.LeftExpr())) {
		return false;
	}
	switch (entry.GetRightKind()) {
		case QueryArithmeticEntry::RightKind::Values:
			return true;
		case QueryArithmeticEntry::RightKind::Arithmetic:
			return independentOfRow(entry.RightExpr());
		case QueryArithmeticEntry::RightKind::Field:
			break;
	}
	return false;
}

bool ArithmeticComparator::EvaluateRowIndependent(QueryArithmeticEntry&& entry, const NamespaceImpl& ns, NamespaceMetadataView metadata,
												  const TagsMatcher& tm, std::optional<int64_t> nowNsec) {
	assertrx_dbg(IsRowIndependent(entry));
	const ArithmeticComparator comparator{std::move(entry), metadata.GetPayloadType(), ns, metadata, tm, nowNsec};
	const PayloadValue noRow;
	return comparator.compare(ConstPayload{comparator.pt_, noRow});
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
	assertrx_throw(tm_);
	assertrx_throw(item.Value());
	PayloadDataExtractor dataExtractor{*item.Value(), pt_, metadata_, *tm_};
	ExprEvalContext ctx{.ns = *ns_,
						.functionInvoker = nullptr,
						.ctx = nullptr,
						.forField = {},
						.whereMode = true,
						.fieldBindings = fieldBindings,
						.functionFieldBindings = functionFieldBindings_,
						.updateIndexBindings = {},
						.nowTimes = nowTimes_ ? &*nowTimes_ : nullptr,
						.fieldScratch = exprScratch_,
						.dataExtractor = dataExtractor,
						.payloadData = item.Value()->IsFree() ? nullptr : item.Value()->Ptr()};
	return expr.Ast().EvaluateScalar(ctx);
}

ExprScalar ArithmeticComparator::evalLeft(const ConstPayload& item) const {
	return leftFlatArrayLenBinding_
			   ? ExprScalar::FromInt(functions::FlatArrayLen::Evaluate(item, functionFieldBindings_[*leftFlatArrayLenBinding_].fields))
			   : evalExpr(entry_.LeftExpr(), item, leftExprFieldBindings_);
}

ExprScalar ArithmeticComparator::evalRight(const ConstPayload& item) const {
	return rightFlatArrayLenBinding_
			   ? ExprScalar::FromInt(functions::FlatArrayLen::Evaluate(item, functionFieldBindings_[*rightFlatArrayLenBinding_].fields))
			   : evalExpr(entry_.RightExpr(), item, rightExprFieldBindings_);
}

std::optional<ArithmeticComparator::NumericValues> ArithmeticComparator::NumericValues::Prepare(const VariantArray& values, CondType cond,
																								std::optional<size_t> flatArrayLenBinding) {
	NumericValues result{.condition = cond, .flatArrayLenBinding = flatArrayLenBinding, .scalars = {}, .sortedInts = {}};
	result.scalars.reserve(values.size());
	for (const Variant& value : values) {
		const auto scalar = ExprScalar::TryFromVariant(value);
		if (!scalar) {
			return std::nullopt;
		}
		result.scalars.emplace_back(*scalar);
		result.allIntegers &= scalar->kind == ExprScalar::Kind::Int;
	}
	if (cond == CondSet && result.allIntegers && values.size() > kSortedInValuesThreshold) {
		result.sortedInts.reserve(values.size());
		for (const auto& scalar : result.scalars) {
			result.sortedInts.push_back(scalar.i);
		}
		std::ranges::sort(result.sortedInts);
		const auto duplicates = std::ranges::unique(result.sortedInts);
		result.sortedInts.erase(duplicates.begin(), duplicates.end());
	}
	return result;
}

bool ArithmeticComparator::NumericValues::Compare(ExprScalar value) const {
	if (value.empty()) {
		return false;
	}
	if (allIntegers && value.kind == ExprScalar::Kind::Int) {
		return CompareInt(value.i);
	}
	return satisfyScalarVsRange(value, condition, scalars, compareScalars);
}

bool ArithmeticComparator::compareOperands(const ConstPayload& item) const {
	if (entry_.GetLeftKind() == QueryArithmeticEntry::LeftKind::Field) {
		const auto& left = fieldValues(item, entry_.LeftField());
		const auto right = evalRight(item);
		if (left.empty() || right.empty()) {
			return false;
		}
		return satisfyRangeVsScalar(left, entry_.Condition(), right, compareVariantAndScalar);
	}
	if (entry_.GetLeftKind() == QueryArithmeticEntry::LeftKind::Values) {
		switch (entry_.GetRightKind()) {
			case QueryArithmeticEntry::RightKind::Values:
				return QueryEntries::CheckIfSatisfyCondition(entry_.LeftValues(), entry_.Condition(), entry_.Values());
			case QueryArithmeticEntry::RightKind::Field: {
				const auto& right = fieldValues(item, entry_.RightField());
				return !right.empty() && QueryEntries::CheckIfSatisfyCondition(entry_.LeftValues(), entry_.Condition(), right);
			}
			case QueryArithmeticEntry::RightKind::Arithmetic: {
				const auto right = evalRight(item);
				return !right.empty() && satisfyRangeVsScalar(entry_.LeftValues(), entry_.Condition(), right, compareVariantAndScalar);
			}
		}
	}

	const auto left = evalLeft(item);
	switch (entry_.GetRightKind()) {
		case QueryArithmeticEntry::RightKind::Values:
			return !left.empty() && satisfyScalarVsRange(left, entry_.Condition(), entry_.Values(), compareScalarAndVariant);
		case QueryArithmeticEntry::RightKind::Field: {
			const auto& right = fieldValues(item, entry_.RightField());
			if (left.empty() || right.empty()) {
				return false;
			}
			return satisfyScalarVsRange(left, entry_.Condition(), right, compareScalarAndVariant);
		}
		case QueryArithmeticEntry::RightKind::Arithmetic: {
			const auto right = evalRight(item);
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
