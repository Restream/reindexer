#include "expressiontree.h"
#include "core/nsselecter/querypreprocessor.h"
#include "core/sorting/sortexpression.h"

namespace reindexer {

template <typename OperationType, typename SubTree, int holdSize, typename... Ts>
template <typename Merger, typename SkippingEntries, typename InvalidEntries>
size_t ExpressionTree<OperationType, SubTree, holdSize, Ts...>::mergeEntriesImpl(Merger& merger, uint16_t dst, uint16_t srcBegin,
																				 uint16_t srcEnd, Changed& changed) {
	assertrx_dbg(dst <= srcBegin);
	size_t merged = 0;
	for (size_t src = srcBegin, nextSrc; src < srcEnd; src = nextSrc) {
		nextSrc = Next(src);
		const auto mergeResult =
			container_[src].Visit(overloaded{[](const concepts::OneOf<InvalidEntries> auto&) -> MergeResult { throw_as_assert; },
											 [dst, src, this](const concepts::OneOf<SkippingEntries> auto&) {
												 if (dst != src) {
													 container_[dst] = std::move(container_[src]);
												 }
												 return MergeResult::NotMerged;
											 },
											 [&](const SubTree&) {
												 if (dst != src) {
													 container_[dst] = std::move(container_[src]);
												 }
												 Merger subMerger{merger.MergingTree()};
												 const size_t mergedInBracket = mergeEntriesImpl<Merger, SkippingEntries, InvalidEntries>(
													 subMerger, dst + 1, src + 1, nextSrc, changed);
												 container_[dst].template Value<SubTree>().Erase(mergedInBracket);
												 merged += mergedInBracket;
												 changed |= (mergedInBracket != 0);
												 return MergeResult::NotMerged;
											 },
											 [&](Merger::MergingEntry& entry) {
												 const bool last = nextSrc >= srcEnd;
												 const auto nextOp = last ? OperationType{} : GetOperation(nextSrc);
												 const auto mergeEntryRes = merger.Merge(entry, dst, src, nextOp, last, changed);
												 switch (mergeEntryRes) {
													 case MergeResult::NotMerged:
														 if (dst != src) {
															 container_[dst] = std::move(container_[src]);
														 }
														 break;
													 case MergeResult::Merged:
														 ++merged;
														 changed = Changed_True;
														 break;
													 case MergeResult::Annihilated:
														 changed = Changed_True;
														 break;
												 }
												 return mergeEntryRes;
											 }});
		switch (mergeResult) {
			case MergeResult::NotMerged:
				dst = Next(dst);
				break;
			case MergeResult::Merged:
				break;
			case MergeResult::Annihilated:
				return merged + srcEnd - src;
		}
	}
	return merged;
}

class [[nodiscard]] QueryPreprocessor::Merger {
public:
	explicit Merger(QueryPreprocessor& qPreproc) noexcept : qPreproc_{qPreproc} {}
	QueryPreprocessor& MergingTree() noexcept { return qPreproc_; }

	using InvalidEntries = TypesPack<SubQueryEntry, SubQueryFieldEntry>;
	using SkippingEntries =
		TypesPack<JoinQueryEntry, BetweenFieldsQueryEntry, AlwaysFalse, AlwaysTrue, KnnQueryEntry, MultiDistinctQueryEntry>;
	using MergingEntry = QueryEntry;

	MergeResult Merge(QueryEntry& entry, uint16_t dst, uint16_t src, OpType nextOp, bool last, Changed) {
		if (entry.IsFieldIndexed() && !entry.ForcedSortOptEntry()) {
			const Index& index = *qPreproc_.ns_.indexes_[entry.IndexNo()];
			if (index.IsFulltext() || index.IsFloatVector()) {
				return MergeResult::NotMerged;
			}

			const auto& indexOpts = index.Opts();
			if (indexOpts.IsArray()) {
				return MergeResult::NotMerged;
			}

			// try to merge entries with AND operator
			if (qPreproc_.GetOperation(src) == OpAnd && (last || nextOp != OpOr)) {
				if (size_t(entry.IndexNo()) >= iidx_.size()) {
					const auto oldSize = iidx_.size();
					iidx_.resize(entry.IndexNo() + 1);
					std::fill(iidx_.begin() + oldSize, iidx_.begin() + iidx_.size(), 0);
				}
				auto& iidxRef = iidx_[entry.IndexNo()];
				const auto& indexOpts = index.Opts();
				if (iidxRef > 0) {
					const auto orderedFlag = index.IsOrdered() ? MergeOrdered::Yes : MergeOrdered::No;
					const auto mergeRes =
						IsComposite(index.Type())
							? qPreproc_.mergeQueryEntries<ValuesType::Composite>(iidxRef - 1, src, orderedFlag, qPreproc_.ns_.payloadType_,
																				 index.Fields())
							: qPreproc_.mergeQueryEntries<ValuesType::Scalar>(iidxRef - 1, src, orderedFlag, indexOpts.collateOpts_);
					if (mergeRes == MergeResult::Annihilated) {
						iidxRef = 0;
					}
					return mergeRes;
				} else {
					assertrx_dbg(dst < std::numeric_limits<uint16_t>::max() - 1);
					iidxRef = dst + 1;
				}
			}
		}
		return MergeResult::NotMerged;
	}

private:
	h_vector<uint16_t, kMaxIndexes> iidx_;
	QueryPreprocessor& qPreproc_;
};

std::pair<size_t, Changed> QueryPreprocessor::lookupQueryIndexes(uint16_t srcEnd) {
	Changed changed = Changed_False;
	Merger merger{*this};
	const auto count = mergeEntries(merger, 0, 0, srcEnd, changed);
	return {count, changed};
}

class [[nodiscard]] SortExpression::Merger {
public:
	explicit Merger(Base& tree) noexcept : tree_{tree} {}
	Base& MergingTree() noexcept { return tree_; }

	using InvalidEntries = TypesPack<>;
	using SkippingEntries =
		TypesPack<SortExprFuncs::Index, SortExprFuncs::JoinedIndex, SortExprFuncs::Rank, SortExprFuncs::RankNamed, SortExprFuncs::Rrf,
				  SortExprFuncs::SortHash, SortExprFuncs::DistanceFromPoint, SortExprFuncs::DistanceJoinedIndexFromPoint,
				  SortExprFuncs::DistanceBetweenIndexes, SortExprFuncs::DistanceBetweenIndexAndJoinedIndex,
				  SortExprFuncs::DistanceBetweenJoinedIndexes, SortExprFuncs::DistanceBetweenJoinedIndexesSameNs>;
	using MergingEntry = SortExprFuncs::Value;

protected:
	Base& tree_;
};

class [[nodiscard]] SortExpression::ConstantsMultiplier : public Merger {
public:
	using Merger::Merger;
	MergeResult Merge(const SortExprFuncs::Value& entry, uint16_t dst, uint16_t src, SortExpressionOperation /*nextOp*/, bool /*last*/,
					  Changed) {
		if (tree_.GetOperation(src).op == OpMult) {
			assertrx_throw(tree_.GetOperation(src).negative == false);
			assertrx_throw(dst > 0);
			assertrx_throw(tree_.Is<SortExprFuncs::Value>(dst - 1));
			tree_.Get<SortExprFuncs::Value>(dst - 1).value *= entry.value;
			return MergeResult::Merged;
		} else {
			return MergeResult::NotMerged;
		}
	}
};

Changed SortExpression::multiplyConstants() {
	Changed changed = Changed_False;
	ConstantsMultiplier constMultiplier{*this};
	const size_t deleted = mergeEntries(constMultiplier, 0, 0, Size(), changed);
	if (deleted > 0) {
		std::ignore = container_.erase(container_.end() - deleted, container_.end());
	}
	return Changed{deleted > 0} || changed;
}

class [[nodiscard]] SortExpression::ConstantsSummer : public Merger {
public:
	using Merger::Merger;
	MergeResult Merge(SortExprFuncs::Value& entry, uint16_t dst, uint16_t src, SortExpressionOperation nextOp, bool last,
					  Changed& changed) {
		if (!last && (nextOp == OpMult || nextOp == OpDiv)) {
			return MergeResult::NotMerged;
		}
		const auto op = tree_.GetOperation(src);
		switch (op.op) {
			case OpPlus:
			case OpMinus:
				if (firstConstInitialized_) {
					assertrx_throw(tree_.Is<SortExprFuncs::Value>(firstConst_));
					if ((op.op == OpMinus) != op.negative) {
						tree_.Get<SortExprFuncs::Value>(firstConst_).value -= entry.value;
					} else {
						tree_.Get<SortExprFuncs::Value>(firstConst_).value += entry.value;
					}
					changed = Changed_True;
					return MergeResult::Merged;
				} else {
					if ((op.op == OpMinus) != op.negative) {
						entry.value = -entry.value;
						changed = Changed_True;
					}
					if (op.op == OpMinus || op.negative) {
						tree_.SetOperation({OpPlus, false}, src);
						changed = Changed_True;
					}
					firstConst_ = dst;
					firstConstInitialized_ = true;
					return MergeResult::NotMerged;
				}
			case OpMult:
			case OpDiv:
				return MergeResult::NotMerged;
			default:
				throw_as_assert;
		}
	}

private:
	size_t firstConst_{0};
	bool firstConstInitialized_{false};
};

Changed SortExpression::sumConstants() {
	Changed changed = Changed_False;
	ConstantsSummer constSummer{*this};
	const size_t deleted = mergeEntries(constSummer, 0, 0, Size(), changed);
	if (deleted > 0) {
		std::ignore = container_.erase(container_.end() - deleted, container_.end());
	}
	return Changed{deleted > 0} || changed;
}

}  // namespace reindexer
