#pragma once

#include <memory>
#include <span>
#include <vector>
#include "core/query/queryentry.h"

namespace reindexer {

template <typename T>
class PayloadIface;
class PayloadType;
using ConstPayload = PayloadIface<const PayloadValue>;

class [[nodiscard]] QueryConditionsEvaluator {
public:
	// Prepare after field binding and value normalization. Rebuild if the entries change.
	static QueryConditionsEvaluator PrepareForEvaluation(const QueryEntries&, const PayloadType&,
														 std::vector<CollateOpts> collateOpts = {});
	bool Evaluate(const QueryEntries&, const ConstPayload&) const;
	static bool CheckIfSatisfyCondition(const VariantArray& lValues, CondType, const VariantArray& rValues,
										const CollateOpts& collate = CollateOpts{});

private:
	class AllSetValues;
	class CompositeCondition;
	using ConstIterator = QueryEntries::const_iterator;
	enum class [[nodiscard]] ComparisonMode { Default, NonIndexed, BetweenFields };

	struct [[nodiscard]] PreparedAllSetData {
		std::vector<std::shared_ptr<const AllSetValues>> allSetValues;
	};

	struct [[nodiscard]] EvaluationContext {
		const ConstPayload& payload;
		const PreparedAllSetData& prepared;
		ConstIterator begin;
		std::span<const CollateOpts> collateOpts;
		std::span<const std::shared_ptr<const CompositeCondition>> compositeConditions;
	};

	QueryConditionsEvaluator() = default;

	static void validateForEvaluation(const QueryEntries&);
	static PreparedAllSetData prepareAllSetData(const QueryEntries&, std::span<const CollateOpts>);
	static bool checkIfSatisfyConditions(ConstIterator begin, ConstIterator end, const EvaluationContext&);
	static bool checkIfSatisfyCondition(const QueryEntry&, const ConstPayload&, const CollateOpts&, const AllSetValues*);
	static bool checkIfSatisfyCondition(const BetweenFieldsQueryEntry&, const ConstPayload&, const CollateOpts&);
	static bool checkIfSatisfyCondition(const VariantArray&, CondType, const VariantArray&, const CollateOpts&, ComparisonMode);

	PreparedAllSetData preparedAllSet_;
	std::vector<CollateOpts> collateOpts_;
	std::vector<std::shared_ptr<const CompositeCondition>> compositeConditions_;
};

}  // namespace reindexer
