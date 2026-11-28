#include "queryconditions_evaluator.h"

#include <algorithm>
#include <optional>
#include <tuple>
#include <utility>
#include "core/keyvalue/allset_match_state.h"
#include "core/keyvalue/condition_comparison.h"
#include "core/keyvalue/relaxed_allset.h"
#include "core/nsselecter/comparator/helpers.h"
#include "core/payload/condition_comparison.h"
#include "core/payload/payloadiface.h"
#include "estl/fast_hash_map.h"
#include "tools/customhash.h"
#include "tools/string_regexp_functions.h"

namespace reindexer {

class [[nodiscard]] QueryConditionsEvaluator::AllSetValues {
public:
	AllSetValues(const QueryEntry& entry, const CollateOpts& collate) : values_{prepare(entry, collate)} {
		if (auto* indexed = std::get_if<IndexedMap>(&values_)) {
			if (std::any_of(entry.Values().begin(), entry.Values().end(), cannotMatch)) {
				// Indexed ALLSET cannot satisfy a NULL or NaN requirement.
				canMatch_ = false;
				return;
			}
			for (const auto& value : entry.Values()) {
				std::ignore = indexed->emplace(Variant{value}.EnsureHold(), indexed->size());
			}
		}
	}

	bool Check(const VariantArray& values) const {
		if (!canMatch_) {
			return false;
		}
		if (const auto* nonIndexed = std::get_if<RelaxedAllSet>(&values_)) {
			return nonIndexed->Check(values);
		}
		const auto& required = std::get<IndexedMap>(values_);
		AllSetMatchState matched{required.size()};
		for (const auto& value : values) {
			if (IsNaN(value)) {
				continue;
			}
			const auto it = required.find(value);
			if (it != required.cend() && matched.Mark(it->second)) {
				return true;
			}
		}
		return false;
	}

private:
	struct [[nodiscard]] IndexedHasher {
		CollateMode collateMode;

		size_t operator()(const Variant& value) const {
			if (value.Type().Is<KeyValueType::String>()) {
				return collateHash(static_cast<std::string_view>(value), collateMode);
			}
			if (value.Type().Is<KeyValueType::Tuple>()) {
				const auto parts = value.As<VariantArray>();
				size_t hash = parts.size();
				for (const auto& part : parts) {
					hash = (hash * VariantArray::kHashMagic) ^ (*this)(part);
				}
				return hash;
			}
			return value.Hash();
		}
	};

	struct [[nodiscard]] IndexedEqual {
		CollateOpts collate;

		bool operator()(const Variant& lhs, const Variant& rhs) const {
			return CompareForCondition<WithString::No, NotComparable::Return, kDefaultNullsHandling>(lhs, rhs, collate) ==
				   ComparationResult::Eq;
		}
	};

	// Both operands are normalized to the index's types, so one hash index is sufficient.
	using IndexedMap = fast_hash_map<Variant, size_t, IndexedHasher, IndexedEqual>;
	using PreparedValues = std::variant<RelaxedAllSet, IndexedMap>;

	static PreparedValues prepare(const QueryEntry& entry, const CollateOpts& collate) {
		if (entry.IsFieldIndexed()) {
			return PreparedValues{std::in_place_type<IndexedMap>, entry.Values().size(), IndexedHasher{.collateMode = collate.mode},
								  IndexedEqual{.collate = collate}};
		}
		return PreparedValues{std::in_place_type<RelaxedAllSet>, entry.Values()};
	}

	static bool cannotMatch(const Variant& value) {
		if (value.IsNullValue() || IsNaN(value)) {
			return true;
		}
		if (value.Type().Is<KeyValueType::Tuple>()) {
			const auto parts = value.As<VariantArray>();
			return std::any_of(parts.begin(), parts.end(), cannotMatch);
		}
		return false;
	}

	PreparedValues values_;
	bool canMatch_{true};
};

class [[nodiscard]] QueryConditionsEvaluator::CompositeCondition {
public:
	CompositeCondition(const QueryEntry& entry, const PayloadType& type, const CollateOpts& collate)
		: payloadType_{type}, fields_{entry.Fields()}, collate_{collate}, condition_{entry.Condition()} {
		values_.reserve(entry.Values().size());
		for (const auto& value : entry.Values()) {
			if (value.IsNullValue()) {
				values_.emplace_back(std::nullopt);
			} else {
				values_.emplace_back(value.As<PayloadValue>());
			}
		}
		if (condition_ == CondAllSet) {
			canMatch_ = false;
			if (values_.empty()) {
				return;
			}
			const auto& firstValue = values_.front();
			if (!firstValue.has_value()) {
				return;
			}
			canMatch_ = true;
			const ConstPayload first{payloadType_, *firstValue};
			for (const auto& value : values_) {
				if (!value.has_value() ||
					ComparePayloadsForCondition(first, ConstPayload{payloadType_, *value}, fields_, collate_) != ComparationResult::Eq) {
					canMatch_ = false;
					break;
				}
			}
			// A document has one composite key, so ALLSET can match only one distinct requirement.
			if (canMatch_) {
				values_.resize(1);
			}
		}
	}

	bool Check(const ConstPayload& payload) const {
		const auto compare = [this, &payload](const std::optional<PayloadValue>& value) {
			return value.has_value() ? ComparePayloadsForCondition(payload, ConstPayload{payloadType_, *value}, fields_, collate_)
									 : ComparationResult::NotComparable;
		};
		ComparationResult mask = ComparationResult::Eq;
		switch (condition_) {
			case CondAny:
			case CondEmpty: {
				VariantArray values;
				if (fields_.size() == 1) {
					condition_comparison_detail::GetFieldValues(payload, fields_[0], fields_, 0, values);
				}
				const bool isNull = fields_.size() == 1 && (values.empty() || values.IsNullValue());
				return condition_ == CondEmpty ? isNull : !isNull;
			}
			case CondAllSet:
				return canMatch_ && compare(values_[0]) == ComparationResult::Eq;
			case CondRange:
				assertrx_dbg(values_.size() == 2);
				return (compare(values_[0]) & ComparationResult::Ge) && (compare(values_[1]) & ComparationResult::Le);
			case CondEq:
			case CondSet:
				break;
			case CondLt:
				mask = ComparationResult::Lt;
				break;
			case CondLe:
				mask = ComparationResult::Le;
				break;
			case CondGt:
				mask = ComparationResult::Gt;
				break;
			case CondGe:
				mask = ComparationResult::Ge;
				break;
			case CondLike:
			case CondDWithin:
			case CondKnn:
				throw_as_assert;
		}
		for (const auto& value : values_) {
			if (compare(value) & mask) {
				return true;
			}
		}
		return false;
	}

private:
	PayloadType payloadType_;
	FieldsSet fields_;
	CollateOpts collate_;
	CondType condition_;
	std::vector<std::optional<PayloadValue>> values_;
	bool canMatch_{true};
};

QueryConditionsEvaluator QueryConditionsEvaluator::PrepareForEvaluation(const QueryEntries& entries, const PayloadType& payloadType,
																		std::vector<CollateOpts> collateOpts) {
	assertrx_dbg(collateOpts.empty() || collateOpts.size() == entries.Size());
	validateForEvaluation(entries);
	QueryConditionsEvaluator evaluator;
	evaluator.collateOpts_ = std::move(collateOpts);
	evaluator.preparedAllSet_ = prepareAllSetData(entries, evaluator.collateOpts_);
	for (size_t i = 0; i < entries.Size(); ++i) {
		if (!entries.Is<QueryEntry>(i) || !entries.Get<QueryEntry>(i).FieldType().Is<KeyValueType::Composite>()) {
			continue;
		}
		if (evaluator.compositeConditions_.empty()) {
			evaluator.compositeConditions_.resize(entries.Size());
		}
		evaluator.compositeConditions_[i] = std::make_shared<const CompositeCondition>(
			entries.Get<QueryEntry>(i), payloadType, evaluator.collateOpts_.empty() ? CollateOpts{} : evaluator.collateOpts_[i]);
	}
	return evaluator;
}

void QueryConditionsEvaluator::validateForEvaluation(const QueryEntries& entries) {
	for (size_t i = 0; i < entries.Size(); ++i) {
		if (!entries.Is<QueryEntry>(i)) {
			continue;
		}
		const auto& entry = entries.Get<QueryEntry>(i);
		if (entry.Condition() == CondLike && entry.IsFieldIndexed() && !entry.FieldType().Is<KeyValueType::String>()) {
			throw Error(errQueryExec, "Condition {} with type {}", CondTypeToStr(entry.Condition()), entry.FieldType().Name());
		}
	}
}

QueryConditionsEvaluator::PreparedAllSetData QueryConditionsEvaluator::prepareAllSetData(const QueryEntries& entries,
																						 std::span<const CollateOpts> collateOpts) {
	static const CollateOpts kDefaultCollateOpts;
	PreparedAllSetData prepared;
	auto& allSetValues = prepared.allSetValues;
	for (size_t i = 0; i < entries.Size(); ++i) {
		if (!entries.Is<QueryEntry>(i)) {
			continue;
		}
		const auto& entry = entries.Get<QueryEntry>(i);
		if (entry.Condition() == CondAllSet && entry.Values().size() > 1 && !entry.FieldType().Is<KeyValueType::Composite>()) {
			if (allSetValues.empty()) {
				allSetValues.resize(entries.Size());
			}
			allSetValues[i] = std::make_shared<const AllSetValues>(entry, collateOpts.empty() ? kDefaultCollateOpts : collateOpts[i]);
		}
	}
	return prepared;
}

bool QueryConditionsEvaluator::Evaluate(const QueryEntries& entries, const ConstPayload& pl) const {
	assertrx_dbg(collateOpts_.empty() || collateOpts_.size() == entries.Size());
	assertrx_dbg(preparedAllSet_.allSetValues.empty() || preparedAllSet_.allSetValues.size() == entries.Size());
	assertrx_dbg(compositeConditions_.empty() || compositeConditions_.size() == entries.Size());
	return checkIfSatisfyConditions(entries.cbegin(), entries.cend(),
									{.payload = pl,
									 .prepared = preparedAllSet_,
									 .begin = entries.cbegin(),
									 .collateOpts = collateOpts_,
									 .compositeConditions = compositeConditions_});
}

bool QueryConditionsEvaluator::checkIfSatisfyConditions(ConstIterator begin, ConstIterator end, const EvaluationContext& ctx) {
	static const CollateOpts kDefaultCollateOpts;
	assertrx_dbg(begin != end && begin->operation != OpOr);
	bool result = true;
	for (auto it = begin; it != end; ++it) {
		if (it->operation == OpOr) {
			if (result) {
				continue;
			}
		} else if (!result) {
			break;
		}
		const auto position = static_cast<size_t>(it.PlainIterator() - ctx.begin.PlainIterator());
		const auto& collate = ctx.collateOpts.empty() ? kDefaultCollateOpts : ctx.collateOpts[position];
		const bool lastResult = it->Visit(
			[] RX_PRE_LMBD_ALWAYS_INLINE(const concepts::OneOf<SubQueryEntry, SubQueryFieldEntry, JoinQueryEntry, SubQueryFunctionEntry,
															   SubQueryArithmeticEntry> auto&)
				RX_POST_LMBD_ALWAYS_INLINE -> bool { throw_as_assert; },
			[&it, &ctx] RX_PRE_LMBD_ALWAYS_INLINE(const QueryEntriesBracket&) RX_POST_LMBD_ALWAYS_INLINE {
				return checkIfSatisfyConditions(it.cbegin(), it.cend(), ctx);
			},
			[&ctx, position] RX_PRE_LMBD_ALWAYS_INLINE(const QueryEntry& qe) RX_POST_LMBD_ALWAYS_INLINE {
				if (!ctx.compositeConditions.empty() && ctx.compositeConditions[position]) {
					return ctx.compositeConditions[position]->Check(ctx.payload);
				}
				const auto& entryCollate = ctx.collateOpts.empty() ? kDefaultCollateOpts : ctx.collateOpts[position];
				const auto* allSetValues = ctx.prepared.allSetValues.empty() ? nullptr : ctx.prepared.allSetValues[position].get();
				return checkIfSatisfyCondition(qe, ctx.payload, entryCollate, allSetValues);
			},
			[] RX_PRE_LMBD_ALWAYS_INLINE(const QueryFunctionEntry&) -> bool { throw_as_assert; }, [] RX_PRE_LMBD_ALWAYS_INLINE(const QueryArithmeticEntry&) -> bool { throw_as_assert; },
			[&ctx, &collate] RX_PRE_LMBD_ALWAYS_INLINE(
						  const BetweenFieldsQueryEntry& qe)
			RX_POST_LMBD_ALWAYS_INLINE { return checkIfSatisfyCondition(qe, ctx.payload, collate); },
			[] RX_PRE_LMBD_ALWAYS_INLINE(const AlwaysFalse&) RX_POST_LMBD_ALWAYS_INLINE noexcept { return false; },
			[] RX_PRE_LMBD_ALWAYS_INLINE(const AlwaysTrue&) RX_POST_LMBD_ALWAYS_INLINE noexcept { return true; },
			[] RX_PRE_LMBD_ALWAYS_INLINE(const MultiDistinctQueryEntry&) RX_POST_LMBD_ALWAYS_INLINE noexcept { return true; },
			[] RX_PRE_LMBD_ALWAYS_INLINE(const KnnQueryEntry&) RX_POST_LMBD_ALWAYS_INLINE -> bool { throw_as_assert; });
		result = (lastResult != (it->operation == OpNot));
	}
	return result;
}

bool QueryConditionsEvaluator::checkIfSatisfyCondition(const QueryEntry& qEntry, const ConstPayload& pl, const CollateOpts& collate,
													   const AllSetValues* allSetValues) {
	assertrx_dbg((qEntry.Condition() == CondAllSet && qEntry.Values().size() > 1) == (allSetValues != nullptr));
	VariantArray values;
	pl.GetByFieldsSet(qEntry.Fields(), values, qEntry.FieldType(), qEntry.CompositeFieldsTypes());
	if (allSetValues) {
		return allSetValues->Check(values);
	}
	return checkIfSatisfyCondition(values, qEntry.Condition(), qEntry.Values(), collate,
								   qEntry.IsFieldIndexed() ? ComparisonMode::Default : ComparisonMode::NonIndexed);
}

bool QueryConditionsEvaluator::checkIfSatisfyCondition(const BetweenFieldsQueryEntry& qEntry, const ConstPayload& pl,
													   const CollateOpts& collate) {
	const auto comparisonType = [](const QueryField& field) noexcept {
		return field.FieldType().Is<KeyValueType::Composite>() && field.CompositeFieldsTypes().size() == 1 ? field.CompositeFieldsTypes()[0]
																										   : field.FieldType();
	};
	const auto leftType = comparisonType(qEntry.LeftFieldData());
	const auto rightType = comparisonType(qEntry.RightFieldData());
	if (!leftType.Is<KeyValueType::Undefined>() && !rightType.Is<KeyValueType::Undefined>() &&
		!comparators::CompareFieldTypes(leftType, rightType)) {
		throw Error(errQueryExec, "Cannot compare fields '{}' and '{}' of types {} and {}", qEntry.LeftFieldName(), qEntry.RightFieldName(),
					leftType.Name(), rightType.Name());
	}
	VariantArray lValues;
	pl.GetByFieldsSet(qEntry.LeftFields(), lValues, qEntry.LeftFieldType(), qEntry.LeftCompositeFieldsTypes());
	VariantArray rValues;
	pl.GetByFieldsSet(qEntry.RightFields(), rValues, qEntry.RightFieldType(), qEntry.RightCompositeFieldsTypes());
	return checkIfSatisfyCondition(lValues, qEntry.Condition(), rValues, collate, ComparisonMode::BetweenFields);
}

bool QueryConditionsEvaluator::CheckIfSatisfyCondition(const VariantArray& lValues, CondType condition, const VariantArray& rValues,
													   const CollateOpts& collate) {
	return checkIfSatisfyCondition(lValues, condition, rValues, collate, ComparisonMode::Default);
}

bool QueryConditionsEvaluator::checkIfSatisfyCondition(const VariantArray& lValues, CondType condition, const VariantArray& rValues,
													   const CollateOpts& collate, ComparisonMode mode) {
	const auto compare = [mode, &collate](const Variant& lhs, const Variant& rhs) {
		if (mode == ComparisonMode::BetweenFields && !comparators::CompareFieldTypes(lhs.Type(), rhs.Type())) {
			return ComparationResult::NotComparable;
		}
		return mode == ComparisonMode::NonIndexed || lhs.IsNullValue() || rhs.IsNullValue()
				   ? CompareForCondition<WithString::Yes, NotComparable::Return, kWhereCompareNullHandling>(lhs, rhs, collate)
				   : CompareForCondition<WithString::Yes, NotComparable::Throw, kWhereCompareNullHandling>(lhs, rhs, collate);
	};
	switch (condition) {
		case CondAny:
			for (const Variant& value : lValues) {
				if (!value.IsNullValue()) {
					return true;
				}
			}
			return false;
		case CondEmpty:
			if (lValues.IsObjectValue()) [[unlikely]] {
				return false;
			}
			for (const Variant& value : lValues) {
				if (value.IsNullValue()) {
					return true;
				}
			}
			return lValues.empty();
		case CondEq:
		case CondSet:
			for (const auto& lhs : lValues) {
				for (const auto& rhs : rValues) {
					const auto res = compare(lhs, rhs);
					if (res == ComparationResult::Eq) {
						return true;
					}
				}
			}
			return false;
		case CondAllSet:
			if (rValues.empty()) {
				return false;
			}
			for (const auto& v : rValues) {
				if (mode == ComparisonMode::BetweenFields && v.IsNullValue()) {
					continue;
				}
				auto it = lValues.cbegin();
				for (; it != lValues.cend(); ++it) {
					const auto res = compare(*it, v);
					if (res == ComparationResult::Eq) {
						break;
					}
				}
				if (it == lValues.cend()) {
					return false;
				}
			}
			return true;
		case CondLt:
			for (const auto& lhs : lValues) {
				for (const auto& rhs : rValues) {
					const auto res = compare(lhs, rhs);
					if (res == ComparationResult::Lt) {
						return true;
					}
				}
			}
			return false;
		case CondLe:
			for (const auto& lhs : lValues) {
				for (const auto& rhs : rValues) {
					const auto res = compare(lhs, rhs);
					if (res & ComparationResult::Le) {
						return true;
					}
				}
			}
			return false;
		case CondGt:
			for (const auto& lhs : lValues) {
				for (const auto& rhs : rValues) {
					const auto res = compare(lhs, rhs);
					if (res == ComparationResult::Gt) {
						return true;
					}
				}
			}
			return false;
		case CondGe:
			for (const auto& lhs : lValues) {
				for (const auto& rhs : rValues) {
					const auto res = compare(lhs, rhs);
					if (res & ComparationResult::Ge) {
						return true;
					}
				}
			}
			return false;
		case CondRange:
			if (rValues.size() < 2 || (mode == ComparisonMode::BetweenFields && (rValues[0].IsNullValue() || rValues[1].IsNullValue())))
				[[unlikely]] {
				throw Error(errQueryExec, "For condition range second field should be an array of 2 values");
			}
			for (const auto& v : lValues) {
				if ((compare(v, rValues[0]) & ComparationResult::Ge) && (compare(v, rValues[1]) & ComparationResult::Le)) {
					return true;
				}
			}
			return false;
		case CondLike:
			if (mode == ComparisonMode::BetweenFields) {
				if (lValues.empty() || rValues.empty()) {
					return false;
				}
				for (const auto& value : lValues) {
					if (!value.Type().Is<KeyValueType::String>()) {
						throw Error(errQueryExec, "For condition LIKE fields should be of string type");
					}
				}
				for (const auto& value : rValues) {
					if (!value.Type().Is<KeyValueType::String>()) {
						throw Error(errQueryExec, "For condition LIKE fields should be of string type");
					}
				}
			}
			for (const auto& v : lValues) {
				if (mode == ComparisonMode::BetweenFields) {
					for (const auto& pattern : rValues) {
						if (matchLikePattern(std::string_view(v), std::string_view(pattern))) {
							return true;
						}
					}
					continue;
				}
				if (!v.Type().Is<KeyValueType::String>()) [[unlikely]] {
					if (mode == ComparisonMode::NonIndexed) {
						continue;
					}
					throw Error(errLogic, "Condition LIKE must be applied to data of string type, but {} was provided", v.Type().Name());
				}
				if (matchLikePattern(std::string_view(v), std::string_view(rValues[0]))) {
					return true;
				}
			}
			return false;
		case CondDWithin: {
			Point point;
			double distance;
			if (rValues[0].Type().Is<KeyValueType::Tuple>()) {
				point = rValues[0].As<Point>();
				distance = rValues[1].As<double>();
			} else {
				point = rValues[1].As<Point>();
				distance = rValues[0].As<double>();
			}
			return DWithin(static_cast<Point>(lValues), point, distance);
		}
		case CondKnn:
			[[unlikely]] throw_as_assert;
	}
	return false;
}

}  // namespace reindexer
