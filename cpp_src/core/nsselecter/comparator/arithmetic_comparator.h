#pragma once

#include "const.h"
#include "core/function/expression_ast.h"
#include "core/id_type.h"
#include "core/keyvalue/variant.h"
#include "core/payload/payloadiface.h"
#include "core/payload/payloadtype.h"
#include "core/query/queryentry.h"
#include "core/type_consts.h"

namespace reindexer {

class NamespaceImpl;

/// Evaluates WHERE arithmetic operands via ExpressionAst and applies CondType comparison.
class [[nodiscard]] ArithmeticComparator {
public:
	ArithmeticComparator(QueryArithmeticEntry&& entry, PayloadType pt, const NamespaceImpl& ns, std::optional<int64_t> nowNsec);

	RX_ALWAYS_INLINE bool Compare(const PayloadValue& pv, IdType /*rowId*/) {
		++totalCalls_;
		ConstPayload item{pt_, pv};
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
	bool compare(ConstPayload item) const;
	ExprScalar evalExpr(const expressions::ArithmeticExpression& expr, const ConstPayload& item,
						std::span<const ExprFieldBinding> fieldBindings) const;
	const VariantArray& fieldValues(ConstPayload item, const QueryField& field) const;

	QueryArithmeticEntry entry_;
	std::vector<ExprFieldBinding> leftExprFieldBindings_;
	std::vector<ExprFieldBinding> rightExprFieldBindings_;
	PayloadType pt_;
	const NamespaceImpl* ns_{nullptr};
	std::optional<NowTimes> nowTimes_;
	mutable VariantArray fieldValuesScratch_;
	mutable VariantArray exprScratch_;
	h_vector<ExprScalar, 2> rightValueScalars_;
	int totalCalls_ = 0;
	int matchedCount_ = 0;
};

}  // namespace reindexer
