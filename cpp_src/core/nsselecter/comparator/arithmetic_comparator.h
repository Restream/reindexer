#pragma once

#include <algorithm>
#include "const.h"
#include "core/function/expression_ast.h"
#include "core/id_type.h"
#include "core/keyvalue/variant.h"
#include "core/namespace/namespace_metadata_view.h"
#include "core/payload/payloadiface.h"
#include "core/payload/payloadtype.h"
#include "core/query/queryentry.h"
#include "core/type_consts.h"

namespace reindexer {

class NamespaceImpl;
class TagsMatcher;

/// Evaluates WHERE arithmetic operands via ExpressionAst and applies CondType comparison.
class [[nodiscard]] ArithmeticComparator {
public:
	ArithmeticComparator(QueryArithmeticEntry&& entry, PayloadType pt, const NamespaceImpl& ns, NamespaceMetadataView metadata,
						 const TagsMatcher& tm, std::optional<int64_t> nowNsec);
	ArithmeticComparator(const QueryFunctionEntry& entry, PayloadType pt, const NamespaceImpl& ns, NamespaceMetadataView metadata,
						 const TagsMatcher& tm);

	static bool IsRowIndependent(const QueryArithmeticEntry& entry) noexcept;
	static bool EvaluateRowIndependent(QueryArithmeticEntry&& entry, const NamespaceImpl& ns, NamespaceMetadataView metadata,
									   const TagsMatcher& tm, std::optional<int64_t> nowNsec);

	RX_ALWAYS_INLINE bool Compare(const PayloadValue& pv, IdType /*rowId*/) {
		++totalCalls_;
		const ConstPayload item{pt_, pv};
		const bool matched = compare(item);
		if (matched) {
			++matchedCount_;
		}
		return matched;
	}

	int GetMatchedCount(bool invert) const noexcept {
		assertrx_dbg(totalCalls_ >= matchedCount_);
		return invert ? (totalCalls_ - matchedCount_) : matchedCount_;
	}

	double Cost(double expectedIterations) const noexcept {
		if (const auto index = numericValues_ ? numericValues_->flatArrayLenBinding : std::nullopt) {
			if (const size_t paths = functionFieldBindings_[*index].fields.getTagsPathsLength(); paths == 0) {
				return expectedIterations + 1.0;
			} else {
				return comparators::kNonIdxFieldComparatorCostMultiplier * expectedIterations + static_cast<double>(paths) + 1.0;
			}
		}
		return comparators::kNonIdxFieldComparatorCostMultiplier * expectedIterations + 1.0;
	}

	std::string ConditionStr() const;
	std::string Name() const& { return entry_.Dump(); }
	std::optional<int64_t> NowNsec() const noexcept {
		return nowTimes_ ? std::optional{(*nowTimes_)[static_cast<size_t>(TimeUnit::nsec)]} : std::nullopt;
	}
	std::string Dump() const& { return Name() + ' ' + ConditionStr(); }

	reindexer::IsDistinct IsDistinct() const noexcept { return IsDistinct_False; }
	void ExcludeDistinctValues(const PayloadValue&, IdType) const noexcept {}

private:
	struct [[nodiscard]] NumericValues {
		CondType condition;
		std::optional<size_t> flatArrayLenBinding;
		bool allIntegers{true};
		h_vector<ExprScalar, 2> scalars;
		std::vector<int64_t> sortedInts;

		static std::optional<NumericValues> Prepare(const VariantArray&, CondType, std::optional<size_t> flatArrayLenBinding);
		bool Compare(ExprScalar) const;
		RX_ALWAYS_INLINE bool CompareInt(int64_t left) const {
			switch (condition) {
				case CondEq:
				case CondSet:
					return sortedInts.empty() ? std::ranges::any_of(scalars, [left](const ExprScalar& v) noexcept { return left == v.i; })
											  : std::ranges::binary_search(sortedInts, left);
				case CondAllSet:
					return !scalars.empty() && std::ranges::all_of(scalars, [left](const ExprScalar& v) noexcept { return left == v.i; });
				case CondLt:
					return std::ranges::any_of(scalars, [left](const ExprScalar& v) noexcept { return left < v.i; });
				case CondLe:
					return std::ranges::any_of(scalars, [left](const ExprScalar& v) noexcept { return left <= v.i; });
				case CondGt:
					return std::ranges::any_of(scalars, [left](const ExprScalar& v) noexcept { return left > v.i; });
				case CondGe:
					return std::ranges::any_of(scalars, [left](const ExprScalar& v) noexcept { return left >= v.i; });
				case CondRange:
					return left >= scalars[0].i && left <= scalars[1].i;
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
	};
	RX_ALWAYS_INLINE bool compare(const ConstPayload& item) const {
		if (!numericValues_) {
			return compareOperands(item);
		}
		if (numericValues_->allIntegers && numericValues_->flatArrayLenBinding) {
			const auto& fields = functionFieldBindings_[*numericValues_->flatArrayLenBinding].fields;
			return numericValues_->CompareInt(functions::FlatArrayLen::Evaluate(item, fields));
		}
		const auto scalar = entry_.GetLeftKind() == QueryArithmeticEntry::LeftKind::Arithmetic ? evalLeft(item) : evalRight(item);
		return numericValues_->Compare(scalar);
	}
	bool compareOperands(const ConstPayload& item) const;
	ExprScalar evalExpr(const expressions::ArithmeticExpression& expr, const ConstPayload& item,
						std::span<const ExprFieldBinding> fieldBindings) const;
	ExprScalar evalLeft(const ConstPayload& item) const;
	ExprScalar evalRight(const ConstPayload& item) const;
	const VariantArray& fieldValues(ConstPayload item, const QueryField& field) const;

	QueryArithmeticEntry entry_;
	std::optional<NumericValues> numericValues_;
	std::vector<ExprFieldBinding> leftExprFieldBindings_;
	std::vector<ExprFieldBinding> rightExprFieldBindings_;
	std::vector<ExprFunctionFieldBinding> functionFieldBindings_;
	PayloadType pt_;
	NamespaceMetadataView metadata_;
	const NamespaceImpl* ns_{nullptr};
	const TagsMatcher* tm_{nullptr};
	std::optional<NowTimes> nowTimes_;
	mutable VariantArray fieldValuesScratch_;
	mutable VariantArray exprScratch_;
	std::optional<size_t> leftFlatArrayLenBinding_;
	std::optional<size_t> rightFlatArrayLenBinding_;
	int totalCalls_ = 0;
	int matchedCount_ = 0;
};

}  // namespace reindexer
