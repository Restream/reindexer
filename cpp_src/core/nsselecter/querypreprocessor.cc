#include "querypreprocessor.h"

#include "core/index/index.h"
#include "core/keyvalue/fast_hash_set_variant.h"
#include "core/namespace/namespaceimpl.h"
#include "core/nsselecter/joinedselector.h"
#include "core/nsselecter/selectiteratorcontainer.h"
#include "core/payload/fieldsset.h"
#include "core/query/dsl/dslencoder.h"
#include "core/query/queryentry.h"
#include "core/sorting/sortexpression.h"
#include "estl/overloaded.h"
#include "nsselecter.h"
#include "qresexplainholder.h"
#include "substitutionhelpers.h"

namespace reindexer {

QueryPreprocessor::QueryPreprocessor(QueryEntries&& queries, NamespaceImpl* ns, const SelectCtx& ctx)
	: QueryEntries(std::move(queries)),
	  ns_(*ns),
	  query_{ctx.query},
	  strictMode_(ctx.inTransaction ? StrictModeNone
									: ((query_.GetStrictMode() == StrictModeNotSet) ? ns_.config_.strictMode : query_.GetStrictMode())),
	  forcedSortOrder_(!query_.forcedSortOrder_.empty()),
	  reqMatchedOnce_(ctx.reqMatchedOnceFlag),
	  start_(query_.Offset()),
	  count_(query_.Limit()),
	  isMergeQuery_(ctx.isMergeQuery == IsMergeQuery::Yes) {
	if (forcedSortOrder_ && (start_ > QueryEntry::kDefaultOffset || count_ < QueryEntry::kDefaultLimit)) {
		assertrx_throw(!query_.sortingEntries_.empty());
		static const std::vector<JoinedSelector> emptyJoinedSelectors;
		const auto& sEntry = query_.sortingEntries_[0];
		if (SortExpression::Parse(sEntry.expression, emptyJoinedSelectors).ByField()) {
			VariantArray values;
			values.reserve(query_.forcedSortOrder_.size());
			for (const auto& v : query_.forcedSortOrder_) {
				values.push_back(v);
			}
			desc_ = sEntry.desc;
			QueryField fld{sEntry.expression};
			SetQueryField(fld, ns_);
			Append<QueryEntry>(desc_ ? OpNot : OpAnd, std::move(fld), query_.forcedSortOrder_.size() == 1 ? CondEq : CondSet,
							   std::move(values));
			queryEntryAddedByForcedSortOptimization_ = true;
		}
	}
	if (isMergeQuery_) {
		if (QueryEntry::kDefaultLimit - start_ > count_) {
			count_ += start_;
		} else {
			count_ = QueryEntry::kDefaultLimit;
		}
		start_ = QueryEntry::kDefaultOffset;
	}
}

void QueryPreprocessor::ExcludeFtQuery(const RdxContext& rdxCtx) {
	if (queryEntryAddedByForcedSortOptimization_ || Size() <= 1) {
		return;
	}
	for (auto it = begin(), next = it, endIt = end(); it != endIt; it = next) {
		++next;
		if (it->Is<QueryEntry>() && it->Value<QueryEntry>().IsFieldIndexed()) {
			auto& index = ns_.indexes_[it->Value<QueryEntry>().IndexNo()];
			if (!IsFastFullText(index->Type())) {
				continue;
			}
			if (it->operation != OpAnd || (next != endIt && next->operation == OpOr) || !index->EnablePreselectBeforeFt()) {
				break;
			}
			ftPreselect_ = index->FtPreselect(rdxCtx);
			start_ = QueryEntry::kDefaultOffset;
			count_ = QueryEntry::kDefaultLimit;
			forcedSortOrder_ = false;
			ftEntry_ = std::move(it->Value<QueryEntry>());
			const size_t pos = it.PlainIterator() - cbegin().PlainIterator();
			Erase(pos, pos + 1);
			break;
		}
	}
}

bool QueryPreprocessor::NeedNextEvaluation(unsigned start, unsigned count, bool& matchedAtLeastOnce,
										   QresExplainHolder& qresHolder) noexcept {
	if (evaluationsCount_++) {
		return false;
	}
	if (queryEntryAddedByForcedSortOptimization_) {
		container_.back().operation = desc_ ? OpAnd : OpNot;
		assertrx_throw(start <= start_);
		start_ = start;
		assertrx_throw(count <= count_);
		count_ = count;
		return count_ || (reqMatchedOnce_ && !matchedAtLeastOnce);
	} else if (ftEntry_) {
		if (!matchedAtLeastOnce) {
			return false;
		}
		qresHolder.BackupContainer();
		if (isMergeQuery_) {
			if (QueryEntry::kDefaultLimit - query_.Offset() > query_.Limit()) {
				count_ = query_.Limit() + query_.Offset();
			} else {
				count_ = QueryEntry::kDefaultLimit;
			}
			start_ = QueryEntry::kDefaultOffset;
		} else {
			start_ = query_.Offset();
			count_ = query_.Limit();
		}
		forcedSortOrder_ = !query_.forcedSortOrder_.empty();
		clear();
		Append(OpAnd, std::move(*ftEntry_));
		ftEntry_ = std::nullopt;
		matchedAtLeastOnce = false;
		equalPositions.clear();
		return true;
	}
	return false;
}

void QueryPreprocessor::checkStrictMode(const QueryField& field) const {
	if (field.IsFieldIndexed()) {
		return;
	}
	switch (strictMode_) {
		case StrictModeIndexes:
			throw Error(errStrictMode,
						"Current query strict mode allows filtering by indexes only. There are no indexes with name '%s' in namespace '%s'",
						field.FieldName(), ns_.name_);
		case StrictModeNames:
			if (field.HaveEmptyField()) {
				throw Error(errStrictMode,
							"Current query strict mode allows filtering by existing fields only. There are no fields with name '%s' in "
							"namespace '%s'",
							field.FieldName(), ns_.name_);
			}
		case StrictModeNotSet:
		case StrictModeNone:
			return;
	}
}

class JoinOnExplainEnabled;
class JoinOnExplainDisabled;

int QueryPreprocessor::calculateMaxIterations(const size_t from, const size_t to, int maxMaxIters, span<int>& maxIterations,
											  bool inTransaction, bool enableSortOrders, const RdxContext& rdxCtx) const {
	int res = maxMaxIters;
	int current = maxMaxIters;
	for (size_t cur = from; cur < to; cur = Next(cur)) {
		maxIterations[cur] = std::min(
			maxMaxIters,
			Visit(
				cur,
				[&](const QueryEntriesBracket&) {
					return calculateMaxIterations(cur + 1, Next(cur), maxMaxIters, maxIterations, inTransaction, enableSortOrders, rdxCtx);
				},
				[&](const QueryEntry& qe) {
					if (qe.IndexNo() >= 0) {
						Index& index = *ns_.indexes_[qe.IndexNo()];
						if (IsFullText(index.Type()) || isStore(index.Type())) {
							return maxMaxIters;
						}

						Index::SelectOpts opts;
						opts.itemsCountInNamespace = ns_.itemsCount();
						opts.disableIdSetCache = 1;
						opts.unbuiltSortOrders = 0;
						opts.indexesNotOptimized = !enableSortOrders;
						opts.inTransaction = inTransaction;
						const auto selIters = index.SelectKey(qe.Values(), qe.Condition(), 0, opts, nullptr, rdxCtx);

						if (auto* selRes = std::get_if<SelectKeyResultsVector>(&selIters); selRes) {
							int res = 0;
							for (const auto& sIt : *selRes) {
								res += sIt.GetMaxIterations();
							}
							return res;
						}
						return maxMaxIters;
					} else {
						return maxMaxIters;
					}
				},
				[maxMaxIters](const BetweenFieldsQueryEntry&) noexcept { return maxMaxIters; },
				[maxMaxIters](const JoinQueryEntry&) noexcept { return maxMaxIters; }, [](const SubQueryEntry&) -> int { throw_as_assert; },
				[](const SubQueryFieldEntry&) -> int { throw_as_assert; },
				[maxMaxIters](const AlwaysTrue&) noexcept { return maxMaxIters; }, [&](const AlwaysFalse&) noexcept { return 0; }));
		switch (GetOperation(cur)) {
			case OpAnd:
				res = std::min(res, current);
				current = maxIterations[cur];
				break;
			case OpNot:
				res = std::min(res, current);
				if (maxIterations[cur] < maxMaxIters) {
					current = maxMaxIters - maxIterations[cur];
				} else {
					current = maxMaxIters;
				}
				break;
			case OpOr:
				current = std::min(maxMaxIters, current + maxIterations[cur]);
				break;
		}
	}
	res = std::min(res, current);
	return res;
}

void QueryPreprocessor::InjectConditionsFromJoins(JoinedSelectors& js, OnConditionInjections& expalainOnInjections, LogLevel logLevel,
												  bool inTransaction, bool enableSortOrders, const RdxContext& rdxCtx) {
	h_vector<int, 256> maxIterations(Size());
	span<int> maxItersSpan(maxIterations.data(), maxIterations.size());
	const int maxIters = calculateMaxIterations(0, Size(), ns_.itemsCount(), maxItersSpan, inTransaction, enableSortOrders, rdxCtx);
	const bool needExplain = query_.NeedExplain() || logLevel >= LogInfo;
	if (needExplain) {
		injectConditionsFromJoins<JoinOnExplainEnabled>(0, Size(), js, expalainOnInjections, maxIters, maxIterations, inTransaction,
														enableSortOrders, rdxCtx);
	} else {
		injectConditionsFromJoins<JoinOnExplainDisabled>(0, Size(), js, expalainOnInjections, maxIters, maxIterations, inTransaction,
														 enableSortOrders, rdxCtx);
	}
	assertrx_dbg(maxIterations.size() == Size());
}

bool QueryPreprocessor::removeAlwaysFalse() {
	const auto [deleted, changed] = removeAlwaysFalse(0, Size());
	return changed || deleted;
}

std::pair<size_t, bool> QueryPreprocessor::removeAlwaysFalse(size_t begin, size_t end) {
	size_t deleted = 0;
	bool changed = false;
	for (size_t i = begin; i < end - deleted;) {
		if (IsSubTree(i)) {
			const auto [d, ch] = removeAlwaysFalse(i + 1, Next(i));
			deleted += d;
			changed = changed || ch;
			i = Next(i);
		} else if (Is<AlwaysFalse>(i)) {
			switch (GetOperation(i)) {
				case OpOr:
					Erase(i, i + 1);
					++deleted;
					break;
				case OpNot:
					SetValue(i, AlwaysTrue{});
					SetOperation(OpAnd, i);
					changed = true;
					++i;
					break;
				case OpAnd:
					if (i + 1 < end - deleted && GetOperation(i + 1) == OpOr) {
						Erase(i, i + 1);
						SetOperation(OpAnd, i);
						++deleted;
						break;
					} else {
						Erase(i + 1, end - deleted);
						Erase(begin, i);
						return {end - begin - 1, false};
					}
			}
		} else {
			i = Next(i);
		}
	}
	return {deleted, changed};
}

bool QueryPreprocessor::removeAlwaysTrue() {
	const auto [deleted, changed] = removeAlwaysTrue(0, Size());
	return changed || deleted;
}

bool QueryPreprocessor::containsJoin(size_t n) noexcept {
	return Visit(
		n, [](const JoinQueryEntry&) noexcept { return true; }, [](const QueryEntry&) noexcept { return false; },
		[](const BetweenFieldsQueryEntry&) noexcept { return false; }, [](const AlwaysTrue&) noexcept { return false; },
		[](const AlwaysFalse&) noexcept { return false; }, [](const SubQueryEntry&) noexcept { return false; },
		[](const SubQueryFieldEntry&) noexcept { return false; },
		[&](const QueryEntriesBracket&) noexcept {
			for (size_t i = n, e = Next(n); i < e; ++i) {
				if (Is<JoinQueryEntry>(i)) {
					return true;
				}
			}
			return false;
		});
}

std::pair<size_t, bool> QueryPreprocessor::removeAlwaysTrue(size_t begin, size_t end) {
	size_t deleted = 0;
	bool changed = false;
	for (size_t i = begin, prev = begin; i < end - deleted;) {
		if (IsSubTree(i)) {
			const auto [d, ch] = removeAlwaysTrue(i + 1, Next(i));
			deleted += d;
			if (Size(i) == 1) {
				SetValue(i, AlwaysTrue{});
				changed = true;
			} else {
				prev = i;
				i = Next(i);
				changed = changed || ch;
			}
		} else if (Is<AlwaysTrue>(i)) {
			switch (GetOperation(i)) {
				case OpAnd:
					if (i + 1 >= end - deleted || GetOperation(i + 1) != OpOr) {
						Erase(i, i + 1);
						++deleted;
					} else {
						size_t n = i + 1;
						const auto savedDeleted = deleted;
						while (n < end - deleted && GetOperation(n) == OpOr) {
							if (containsJoin(n)) {
								n = Next(n);
							} else {
								deleted += Size(n);
								Erase(n, Next(n));
							}
						}
						if (savedDeleted == deleted) {
							i = n;
						}
					}
					break;
				case OpNot:
					SetValue(i, AlwaysFalse{});
					SetOperation(OpAnd, i);
					changed = true;
					prev = i;
					++i;
					break;
				case OpOr: {
					size_t n = i;
					size_t prevN = prev;
					do {
						assertrx_throw(prevN < n);
						bool needMoveI = false;
						if (!containsJoin(prevN)) {
							if (GetOperation(prevN) != OpOr) {
								SetOperation(OpAnd, n);
							}
							deleted += Size(prevN);
							Erase(prevN, n);
							needMoveI = (n == i);
						}
						n = prevN;
						prevN = begin;
						while (Next(prevN) < n) {
							prevN = Next(prevN);
						}
						if (needMoveI) {
							i = n;
							prev = prevN;
						}
					} while (GetOperation(n) == OpOr);
				} break;
			}
		} else {
			prev = i;
			i = Next(i);
		}
	}
	return {deleted, changed};
}

void QueryPreprocessor::Reduce(bool isFt) {
	bool changed;
	do {
		changed = removeBrackets();
		changed = LookupQueryIndexes() || changed;
		changed = removeAlwaysFalse() || changed;
		changed = removeAlwaysTrue() || changed;
		if (!isFt) {
			changed = SubstituteCompositeIndexes() || changed;
		}
	} while (changed);
}

bool QueryPreprocessor::removeBrackets() { return removeBrackets(0, Size()); }

bool QueryPreprocessor::canRemoveBracket(size_t i) const {
	if (Size(i) < 2) {
		throw Error{errQueryExec, "Bracket cannot be empty"};
	}
	const size_t next = Next(i);
	const OpType op = GetOperation(i);
	if (op != OpAnd && GetOperation(i + 1) != OpAnd) {
		return false;
	}
	if (next == Next(i + 1)) {
		return true;
	}
	return op == OpAnd && (next == Size() || GetOperation(next) != OpOr);
}

size_t QueryPreprocessor::removeBrackets(size_t begin, size_t end) {
	if (begin != end && GetOperation(begin) == OpOr) {
		throw Error{errQueryExec, "OR operator in first condition or after left join"};
	}
	size_t deleted = 0;
	for (size_t i = begin; i < end - deleted; i = Next(i)) {
		if (!IsSubTree(i)) {
			continue;
		}
		deleted += removeBrackets(i + 1, Next(i));
		if (canRemoveBracket(i)) {
			if (const OpType op = GetOperation(i); op != OpAnd) {
				SetOperation(op, i + 1);
			}
			Erase(i, i + 1);
			++deleted;
		}
	}
	return deleted;
}

size_t QueryPreprocessor::lookupQueryIndexes(uint16_t dst, uint16_t srcBegin, uint16_t srcEnd) {
	assertrx_throw(dst <= srcBegin);
	h_vector<uint16_t, kMaxIndexes> iidx(kMaxIndexes, uint16_t(0));
	size_t merged = 0;
	for (size_t src = srcBegin, nextSrc; src < srcEnd; src = nextSrc) {
		nextSrc = Next(src);
		const auto mergeResult = container_[src].Visit(
			[](const SubQueryEntry&) -> MergeResult { throw_as_assert; }, [](const SubQueryFieldEntry&) -> MergeResult { throw_as_assert; },
			[&](const QueryEntriesBracket&) {
				if (dst != src) {
					container_[dst] = std::move(container_[src]);
				}
				const size_t mergedInBracket = lookupQueryIndexes(dst + 1, src + 1, nextSrc);
				container_[dst].Value<QueryEntriesBracket>().Erase(mergedInBracket);
				merged += mergedInBracket;
				return MergeResult::NotMerged;
			},
			[&](QueryEntry& entry) {
				if (entry.IsFieldIndexed()) {
					// try to merge entries with AND operator
					if ((GetOperation(src) == OpAnd) && (nextSrc >= srcEnd || GetOperation(nextSrc) != OpOr)) {
						if (size_t(entry.IndexNo()) >= iidx.size()) {
							const auto oldSize = iidx.size();
							iidx.resize(entry.IndexNo() + 1);
							std::fill(iidx.begin() + oldSize, iidx.begin() + iidx.size(), 0);
						}
						auto& iidxRef = iidx[entry.IndexNo()];
						const Index& index = *ns_.indexes_[entry.IndexNo()];
						const auto& indexOpts = index.Opts();
						if (iidxRef > 0 && !indexOpts.IsArray()) {
							const auto orderedFlag = index.IsOrdered() ? MergeOrdered::Yes : MergeOrdered::No;
							const auto mergeRes = IsComposite(index.Type())
													  ? mergeQueryEntries<ValuesType::Composite, PayloadType, FieldsSet>(
															iidxRef - 1, src, orderedFlag, ns_.payloadType_, index.Fields())
													  : mergeQueryEntries<ValuesType::Scalar, CollateOpts>(iidxRef - 1, src, orderedFlag,
																										   indexOpts.collateOpts_);
							switch (mergeRes) {
								case MergeResult::NotMerged:
									break;
								case MergeResult::Merged:
									++merged;
									return MergeResult::Merged;
								case MergeResult::Annihilated:
									iidxRef = 0;
									return MergeResult::Annihilated;
							}
						} else {
							assertrx_throw(dst < std::numeric_limits<uint16_t>::max() - 1);
							iidxRef = dst + 1;
						}
					}
				}
				if (dst != src) {
					container_[dst] = std::move(container_[src]);
				}
				return MergeResult::NotMerged;
			},
			[dst, src, this](const JoinQueryEntry&) {
				if (dst != src) {
					container_[dst] = std::move(container_[src]);
				}
				return MergeResult::NotMerged;
			},
			[dst, src, this](const BetweenFieldsQueryEntry&) {
				if (dst != src) {
					container_[dst] = std::move(container_[src]);
				}
				return MergeResult::NotMerged;
			},
			[dst, src, this](const AlwaysFalse&) {
				if (dst != src) {
					container_[dst] = std::move(container_[src]);
				}
				return MergeResult::NotMerged;
			},
			[dst, src, this](const AlwaysTrue&) {
				if (dst != src) {
					container_[dst] = std::move(container_[src]);
				}
				return MergeResult::NotMerged;
			});
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

void QueryPreprocessor::CheckUniqueFtQuery() const {
	bool found = false;
	VisitForEach(
		Skip<QueryEntriesBracket, JoinQueryEntry, BetweenFieldsQueryEntry, AlwaysFalse, AlwaysTrue>{},
		[](const SubQueryEntry&) { throw_as_assert; }, [](const SubQueryFieldEntry&) { throw_as_assert; },
		[&](const QueryEntry& qe) {
			if (qe.IsFieldIndexed() && IsFullText(ns_.indexes_[qe.IndexNo()]->Type())) {
				if (found) {
					throw Error{errQueryExec, "Query cannot contain more than one full text condition"};
				} else {
					found = true;
				}
			}
		});
}

bool QueryPreprocessor::ContainsFullTextIndexes() const {
	for (auto it = cbegin().PlainIterator(), end = cend().PlainIterator(); it != end; ++it) {
		if (it->Is<QueryEntry>() && it->Value<QueryEntry>().IsFieldIndexed() &&
			IsFullText(ns_.indexes_[it->Value<QueryEntry>().IndexNo()]->Type())) {
			return true;
		}
	}
	return false;
}

const std::vector<int>* QueryPreprocessor::getCompositeIndex(int field) const noexcept {
	if (auto f = ns_.indexesToComposites_.find(field); f != ns_.indexesToComposites_.end()) {
		return &f->second;
	}
	return nullptr;
}

static void createCompositeKeyValues(span<const std::pair<int, VariantArray>> values, Payload& pl, VariantArray& ret,
									 uint32_t resultSetSize, uint32_t n) {
	const auto& v = values[n];
	for (auto it = v.second.cbegin(), end = v.second.cend(); it != end; ++it) {
		pl.Set(v.first, *it);
		if (n + 1 < values.size()) {
			createCompositeKeyValues(values, pl, ret, resultSetSize, n + 1);
		} else if (ret.size() + 1 != resultSetSize) {
			PayloadValue pv(*(pl.Value()));
			pv.Clone();
			ret.emplace_back(std::move(pv));
		} else {
			ret.emplace_back(*(pl.Value()));
		}
	}
}

static VariantArray createCompositeKeyValues(span<const std::pair<int, VariantArray>> values, const PayloadType& plType,
											 uint32_t resultSetSize) {
	PayloadValue d(plType.TotalSize());
	Payload pl(plType, d);
	VariantArray ret;
	ret.reserve(resultSetSize);
	createCompositeKeyValues(values, pl, ret, resultSetSize, 0);
	assertrx_throw(ret.size() == resultSetSize);
	return ret;
}

size_t QueryPreprocessor::substituteCompositeIndexes(const size_t from, const size_t to) {
	using composite_substitution_helpers::CompositeSearcher;
	using composite_substitution_helpers::EntriesRanges;
	using composite_substitution_helpers::CompositeValuesCountLimits;

	size_t deleted = 0;
	CompositeSearcher searcher(ns_);
	for (size_t cur = from, end = to; cur < end; cur = Next(cur), end = to - deleted) {
		if (IsSubTree(cur)) {
			const auto& bracket = Get<QueryEntriesBracket>(cur);
			auto bracketSize = bracket.Size();
			deleted += substituteCompositeIndexes(cur + 1, cur + bracketSize);
			continue;
		}
		if (!Is<QueryEntry>(cur) || GetOperation(cur) != OpAnd) {
			continue;
		}
		const auto next = Next(cur);
		if ((next < end && GetOperation(next) == OpOr)) {
			continue;
		}
		auto& qe = Get<QueryEntry>(cur);
		if ((qe.Condition() != CondEq && qe.Condition() != CondSet) || !qe.IsFieldIndexed() ||
			qe.IndexNo() >= ns_.payloadType_.NumFields()) {
			continue;
		}

		const std::vector<int>* found = getCompositeIndex(qe.IndexNo());
		if (!found || found->empty()) {
			continue;
		}
		searcher.Add(qe.IndexNo(), *found, cur);
	}

	EntriesRanges deleteRanges;
	h_vector<std::pair<int, VariantArray>, 4> values;
	auto resIdx = searcher.GetResult();
	while (resIdx >= 0) {
		auto& res = searcher[resIdx];
		values.clear<false>();
		uint32_t resultSetSize = 1;
		uint32_t maxSetSize = 0;
		for (auto i : res.entries) {
			auto& qe = Get<QueryEntry>(i);
			if rx_unlikely (!res.fields.contains(qe.IndexNo())) {
				throw Error(errLogic, "Error during composite index's fields substitution (this should not happen)");
			}
			maxSetSize = std::max(maxSetSize, qe.Values().size());
			resultSetSize *= qe.Values().size();
		}
		constexpr static CompositeValuesCountLimits kCompositeSetLimits;
		if (resultSetSize != maxSetSize) {
			// Do not perform substitution if result set size becomes larger than initial indexes set size
			// and this size is greater than limit
			// TODO: This is potential customization point for the user's hints system
			if (resultSetSize > kCompositeSetLimits[res.entries.size()]) {
				resIdx = searcher.RemoveUnusedAndGetNext(resIdx);
				continue;
			}
		}
		for (auto i : res.entries) {
			auto& qe = Get<QueryEntry>(i);
			qe.ConvertValuesToFieldType();
			const int idxNo = qe.IndexNo();
			values.emplace_back(idxNo, std::move(qe).Values());
		}
		{
			VariantArray qValues = createCompositeKeyValues(values, ns_.payloadType_, resultSetSize);
			const auto first = res.entries.front();
			SetOperation(OpAnd, first);
			QueryField fld{ns_.indexes_[res.idx]->Name()};
			setQueryIndex(fld, res.idx, ns_);
			container_[first].Emplace<QueryEntry>(std::move(fld), qValues.size() == 1 ? CondEq : CondSet, std::move(qValues));
		}
		deleteRanges.Add(span<const uint16_t>(res.entries.data() + 1, res.entries.size() - 1));
		resIdx = searcher.RemoveUsedAndGetNext(resIdx);
	}
	for (auto rit = deleteRanges.rbegin(); rit != deleteRanges.rend(); ++rit) {
		Erase(rit->From(), rit->To());
		deleted += rit->Size();
	}
	return deleted;
}

void QueryPreprocessor::initIndexedQueries(size_t begin, size_t end) {
	for (auto cur = begin; cur != end; cur = Next(cur)) {
		Visit(
			cur, Skip<JoinQueryEntry, AlwaysFalse, AlwaysTrue>{}, [](const SubQueryEntry&) { throw_as_assert; },
			[](const SubQueryFieldEntry&) { throw_as_assert; },
			[this, cur](const QueryEntriesBracket&) { initIndexedQueries(cur + 1, Next(cur)); },
			[this](BetweenFieldsQueryEntry& entry) {
				if (!entry.FieldsHaveBeenSet()) {
					SetQueryField(entry.LeftFieldData(), ns_);
					SetQueryField(entry.RightFieldData(), ns_);
				}
				checkStrictMode(entry.LeftFieldData());
				checkStrictMode(entry.RightFieldData());
			},
			[this](QueryEntry& qe) {
				if (!qe.FieldsHaveBeenSet()) {
					SetQueryField(qe.FieldData(), ns_);
				}
				checkStrictMode(qe.FieldData());
				qe.ConvertValuesToFieldType(ns_.payloadType_);
			});
	}
}

SortingEntries QueryPreprocessor::detectOptimalSortOrder() const {
	if (!AvailableSelectBySortIndex()) {
		return {};
	}
	if (const Index* maxIdx = findMaxIndex(cbegin(), cend())) {
		SortingEntries sortingEntries;
		sortingEntries.emplace_back(maxIdx->Name(), false);
		return sortingEntries;
	}
	return {};
}

const Index* QueryPreprocessor::findMaxIndex(QueryEntries::const_iterator begin, QueryEntries::const_iterator end) const {
	thread_local h_vector<FoundIndexInfo, 32> foundIndexes;
	foundIndexes.clear<false>();
	findMaxIndex(begin, end, foundIndexes);
	boost::sort::pdqsort(foundIndexes.begin(), foundIndexes.end(), [](const FoundIndexInfo& l, const FoundIndexInfo& r) noexcept {
		if (l.isFitForSortOptimization > r.isFitForSortOptimization) {
			return true;
		}
		if (l.isFitForSortOptimization == r.isFitForSortOptimization) {
			return l.size > r.size;
		}
		return false;
	});
	if (!foundIndexes.empty() && foundIndexes[0].isFitForSortOptimization) {
		return foundIndexes[0].index;
	}
	return nullptr;
}

void QueryPreprocessor::findMaxIndex(QueryEntries::const_iterator begin, QueryEntries::const_iterator end,
									 h_vector<FoundIndexInfo, 32>& foundIndexes) const {
	for (auto it = begin; it != end; ++it) {
		const auto foundIdx = it->Visit([](const SubQueryEntry&) -> FoundIndexInfo { throw_as_assert; },
										[](const SubQueryFieldEntry&) -> FoundIndexInfo { throw_as_assert; },
										[this, &it, &foundIndexes](const QueryEntriesBracket&) {
											findMaxIndex(it.cbegin(), it.cend(), foundIndexes);
											return FoundIndexInfo();
										},
										[this](const QueryEntry& entry) -> FoundIndexInfo {
											if (entry.IsFieldIndexed() && !entry.Distinct()) {
												const auto idxPtr = ns_.indexes_[entry.IndexNo()].get();
												if (idxPtr->IsOrdered() && !idxPtr->Opts().IsArray()) {
													if (IsOrderedCondition(entry.Condition())) {
														return FoundIndexInfo{idxPtr, FoundIndexInfo::ConditionType::Compatible};
													} else if (entry.Condition() == CondAny || entry.Values().size() > 1) {
														return FoundIndexInfo{idxPtr, FoundIndexInfo::ConditionType::Incompatible};
													}
												}
											}
											return {};
										},
										[](const JoinQueryEntry&) noexcept { return FoundIndexInfo(); },
										[](const BetweenFieldsQueryEntry&) noexcept { return FoundIndexInfo(); },
										[](const AlwaysFalse&) noexcept { return FoundIndexInfo(); },
										[](const AlwaysTrue&) noexcept { return FoundIndexInfo(); });
		if (foundIdx.index) {
			auto found = std::find_if(foundIndexes.begin(), foundIndexes.end(),
									  [foundIdx](const FoundIndexInfo& i) { return i.index == foundIdx.index; });
			if (found == foundIndexes.end()) {
				foundIndexes.emplace_back(foundIdx);
			} else {
				found->isFitForSortOptimization &= foundIdx.isFitForSortOptimization;
			}
		}
	}
}

namespace {

class CompositeLess : less_composite_ref {
public:
	CompositeLess(const PayloadType& type, const FieldsSet& fields) noexcept : less_composite_ref(type, fields) {}

	bool operator()(const Variant& lhs, const Variant& rhs) const {
		assertrx_dbg(lhs.Type().Is<KeyValueType::Composite>());
		assertrx_dbg(rhs.Type().Is<KeyValueType::Composite>());
		return less_composite_ref::operator()(static_cast<const PayloadValue&>(lhs), static_cast<const PayloadValue&>(rhs));
	}
};

class CompositeEqual : equal_composite_ref {
public:
	CompositeEqual(const PayloadType& type, const FieldsSet& fields) noexcept : equal_composite_ref(type, fields) {}

	bool operator()(const Variant& lhs, const Variant& rhs) const {
		assertrx_dbg(lhs.Type().Is<KeyValueType::Composite>());
		assertrx_dbg(rhs.Type().Is<KeyValueType::Composite>());
		return equal_composite_ref::operator()(static_cast<const PayloadValue&>(lhs), static_cast<const PayloadValue&>(rhs));
	}
};

template <QueryPreprocessor::ValuesType vt>
using MergeLessT = std::conditional_t<vt == QueryPreprocessor::ValuesType::Scalar, Variant::Less, CompositeLess>;

template <QueryPreprocessor::ValuesType vt>
using MergeEqualT = std::conditional_t<vt == QueryPreprocessor::ValuesType::Scalar, Variant::EqualTo, CompositeEqual>;

}  // namespace

constexpr size_t kMinArraySizeToUseHashSet = 250;
template <QueryPreprocessor::ValuesType vt, typename... CmpArgs>
QueryPreprocessor::MergeResult QueryPreprocessor::mergeQueryEntriesSetSet(QueryEntry& lqe, QueryEntry& rqe, bool distinct, size_t position,
																		  const CmpArgs&... args) {
	// intersect 2 queryentries on the same index
	if rx_unlikely (lqe.Values().empty() || rqe.Values().empty()) {
		SetValue(position, AlwaysFalse{});
		return MergeResult::Annihilated;
	}
	auto&& [first, second] = lqe.Values().size() < rqe.Values().size() ? std::make_pair(std::move(lqe).Values(), std::move(rqe).Values())
																	   : std::make_pair(std::move(rqe).Values(), std::move(lqe).Values());

	if (first.size() == 1) {
		const Variant& firstV = first[0];
		const MergeEqualT<vt> equalTo{args...};
		for (const Variant& secondV : second) {
			if (equalTo(firstV, secondV)) {
				lqe.SetCondAndValues(CondEq, VariantArray{std::move(first[0])});  // NOLINT (bugprone-use-after-move)
				lqe.Distinct(distinct);
				return MergeResult::Merged;
			}
		}
		SetValue(position, AlwaysFalse{});
		return MergeResult::Annihilated;
	} else {
		VariantArray setValues;
		setValues.reserve(first.size());
		if constexpr (vt == ValuesType::Scalar) {
			if (second.size() < kMinArraySizeToUseHashSet) {
				// Intersect via binary search + sort for small vectors
				boost::sort::pdqsort(first.begin(), first.end(), MergeLessT<vt>{args...});
				for (auto&& v : second) {
					if (std::binary_search(first.begin(), first.end(), v, MergeLessT<vt>{args...})) {
						setValues.emplace_back(std::move(v));
					}
				}
			} else {
				// Intersect via hash_set for large vectors
				fast_hash_set_variant set{args...};
				set.reserve(first.size());
				for (auto&& v : first) {
					set.emplace(std::move(v));
				}
				for (auto&& v : second) {
					if (set.erase(v)) {
						setValues.emplace_back(std::move(v));
					}
				}
			}
		} else {
			// Intersect via hash_set for the composite values
			unordered_payload_ref_set set{first.size(), hash_composite_ref{args...}, equal_composite_ref{args...}};
			for (auto& v : first) {
				set.emplace(static_cast<const PayloadValue&>(v));
			}
			for (auto&& v : second) {
				if (set.erase(static_cast<const PayloadValue&>(v))) {
					setValues.emplace_back(std::move(v));
				}
			}
		}
		if rx_unlikely (setValues.empty()) {
			SetValue(position, AlwaysFalse{});
			return MergeResult::Annihilated;
		}
		lqe.SetCondAndValues(CondSet, std::move(setValues));  // NOLINT (bugprone-use-after-move)
		lqe.Distinct(distinct);
		return MergeResult::Merged;
	}
}

template <QueryPreprocessor::ValuesType vt, typename... CmpArgs>
QueryPreprocessor::MergeResult QueryPreprocessor::mergeQueryEntriesAllSetSet(NeedSwitch needSwitch, QueryEntry& allSet, QueryEntry& set,
																			 bool distinct, size_t position, const CmpArgs&... args) {
	if rx_unlikely (allSet.Values().empty() || set.Values().empty()) {
		SetValue(position, AlwaysFalse{});
		return MergeResult::Annihilated;
	}
	const MergeEqualT<vt> equalTo{args...};
	const auto lvIt = allSet.Values().begin();
	const Variant& lv = *lvIt;
	for (auto it = lvIt + 1, endIt = allSet.Values().end(); it != endIt; ++it) {
		if (!equalTo(lv, *it)) {
			SetValue(position, AlwaysFalse{});
			return MergeResult::Annihilated;
		}
	}
	QueryEntry& dst = needSwitch == NeedSwitch::Yes ? set : allSet;
	for (const Variant& rv : set.Values()) {
		if (equalTo(lv, rv)) {
			dst.Distinct(distinct);
			dst.SetCondAndValues(CondEq, VariantArray{std::move(std::move(allSet).Values()[0])});
			return MergeResult::Merged;
		}
	}
	SetValue(position, AlwaysFalse{});
	return MergeResult::Annihilated;
}

template <QueryPreprocessor::ValuesType vt, typename... CmpArgs>
QueryPreprocessor::MergeResult QueryPreprocessor::mergeQueryEntriesAllSetAllSet(QueryEntry& lqe, QueryEntry& rqe, bool distinct,
																				size_t position, const CmpArgs&... args) {
	if rx_unlikely (lqe.Values().empty() || rqe.Values().empty()) {
		SetValue(position, AlwaysFalse{});
		return MergeResult::Annihilated;
	}
	const MergeEqualT<vt> equalTo{args...};
	const auto lvIt = lqe.Values().begin();
	const Variant& lv = *lvIt;
	for (auto it = lvIt + 1, endIt = lqe.Values().end(); it != endIt; ++it) {
		if (!equalTo(lv, *it)) {
			SetValue(position, AlwaysFalse{});
			return MergeResult::Annihilated;
		}
	}
	for (const Variant& rv : rqe.Values()) {
		if (!equalTo(lv, rv)) {
			SetValue(position, AlwaysFalse{});
			return MergeResult::Annihilated;
		}
	}
	lqe.Distinct(distinct);
	lqe.SetCondAndValues(CondEq, VariantArray{std::move(std::move(lqe).Values()[0])});	// NOLINT (bugprone-use-after-move)
	return MergeResult::Merged;
}

QueryPreprocessor::MergeResult QueryPreprocessor::mergeQueryEntriesAny(NeedSwitch needSwitch, QueryEntry& any, QueryEntry& notAny,
																	   bool distinct, size_t position) {
	if (notAny.Condition() == CondEmpty) {
		SetValue(position, AlwaysFalse{});
		return MergeResult::Annihilated;
	}
	notAny.Distinct(distinct);
	if (needSwitch == NeedSwitch::Yes) {
		any = std::move(notAny);
	}
	return MergeResult::Merged;
}

template <QueryPreprocessor::ValuesType vt, typename F>
QueryPreprocessor::MergeResult QueryPreprocessor::mergeQueryEntriesSetNotSet(NeedSwitch needSwitch, QueryEntry& set, QueryEntry& notSet,
																			 F filter, bool distinct, size_t position,
																			 MergeOrdered mergeOrdered) {
	if rx_unlikely (set.Values().empty()) {
		SetValue(position, AlwaysFalse{});
		return MergeResult::Annihilated;
	}
	{
		auto updatableValues = set.UpdatableValues();
		VariantArray& values = updatableValues;
		values.erase(std::remove_if(values.begin(), values.end(), filter), values.end());
	}
	if rx_unlikely (set.Values().empty()) {
		SetValue(position, AlwaysFalse{});
		return MergeResult::Annihilated;
	}
	if (mergeOrdered == MergeOrdered::No || set.Values().size() == 1) {
		set.Distinct(distinct);
		if (needSwitch == NeedSwitch::Yes) {
			notSet = std::move(set);
		}
		return MergeResult::Merged;
	} else {
		return MergeResult::NotMerged;
	}
}

template <QueryPreprocessor::ValuesType vt, typename F, typename... CmpArgs>
QueryPreprocessor::MergeResult QueryPreprocessor::mergeQueryEntriesAllSetNotSet(NeedSwitch needSwitch, QueryEntry& allSet,
																				QueryEntry& notSet, F filter, bool distinct,
																				size_t position, const CmpArgs&... args) {
	if rx_unlikely (allSet.Values().empty()) {
		SetValue(position, AlwaysFalse{});
		return MergeResult::Annihilated;
	}
	const MergeEqualT<vt> equalTo{args...};
	const auto lvIt = allSet.Values().begin();
	const Variant& lv = *lvIt;
	for (auto it = lvIt + 1, endIt = allSet.Values().end(); it != endIt; ++it) {
		if (!equalTo(lv, *it)) {
			SetValue(position, AlwaysFalse{});
			return MergeResult::Annihilated;
		}
	}
	if (filter(lv)) {
		SetValue(position, AlwaysFalse{});
		return MergeResult::Annihilated;
	}
	QueryEntry& dst = needSwitch == NeedSwitch::Yes ? notSet : allSet;
	dst.Distinct(distinct);
	dst.SetCondAndValues(CondEq, VariantArray{std::move(std::move(allSet).Values()[0])});
	return MergeResult::Merged;
}

template <QueryPreprocessor::ValuesType vt, typename... CmpArgs>
QueryPreprocessor::MergeResult QueryPreprocessor::mergeQueryEntriesLt(QueryEntry& lqe, QueryEntry& rqe, bool distinct,
																	  const CmpArgs&... args) {
	const Variant& lv = lqe.Values()[0];
	const Variant& rv = rqe.Values()[0];
	const MergeLessT<vt> less{args...};
	if (less(rv, lv)) {
		lqe.SetCondAndValues(rqe.Condition(), std::move(rqe).Values());	 // NOLINT (bugprone-use-after-move)
	} else if (!less(lv, rv) && (lqe.Condition() != rqe.Condition())) {
		lqe.SetCondAndValues(CondLt, std::move(rqe).Values());
	}
	lqe.Distinct(distinct);
	return MergeResult::Merged;
}

template <QueryPreprocessor::ValuesType vt, typename... CmpArgs>
QueryPreprocessor::MergeResult QueryPreprocessor::mergeQueryEntriesGt(QueryEntry& lqe, QueryEntry& rqe, bool distinct,
																	  const CmpArgs&... args) {
	const Variant& lv = lqe.Values()[0];
	const Variant& rv = rqe.Values()[0];
	if (MergeLessT<vt>{args...}(lv, rv)) {
		lqe.SetCondAndValues(rqe.Condition(), std::move(rqe).Values());	 // NOLINT (bugprone-use-after-move)
	} else if (MergeEqualT<vt>{args...}(lv, rv) && (lqe.Condition() != rqe.Condition())) {
		lqe.SetCondAndValues(CondGt, std::move(rqe).Values());
	}
	lqe.Distinct(distinct);
	return MergeResult::Merged;
}

template <QueryPreprocessor::ValuesType vt, typename... CmpArgs>
QueryPreprocessor::MergeResult QueryPreprocessor::mergeQueryEntriesLtGt(QueryEntry& lt, QueryEntry& gt, size_t position,
																		const CmpArgs&... args) {
	const Variant& ltV = lt.Values()[0];
	const Variant& gtV = gt.Values()[0];
	if (MergeLessT<vt>{args...}(gtV, ltV)) {
		return MergeResult::NotMerged;
	} else {
		SetValue(position, AlwaysFalse{});
		return MergeResult::Annihilated;
	}
}

template <QueryPreprocessor::ValuesType vt, typename... CmpArgs>
QueryPreprocessor::MergeResult QueryPreprocessor::mergeQueryEntriesLeGe(NeedSwitch needSwitch, QueryEntry& le, QueryEntry& ge,
																		bool distinct, size_t position, const CmpArgs&... args) {
	const Variant& leV = le.Values()[0];
	const Variant& geV = ge.Values()[0];
	const MergeLessT<vt> less{args...};
	QueryEntry& target = needSwitch == NeedSwitch::Yes ? ge : le;
	QueryEntry& source = needSwitch == NeedSwitch::Yes ? le : ge;
	if (less(leV, geV)) {
		SetValue(position, AlwaysFalse{});
		return MergeResult::Annihilated;
	} else if (less(geV, leV)) {
		target.SetCondAndValues(CondRange, VariantArray{std::move(ge).Values()[0], std::move(le).Values()[0]});
	} else {
		target.SetCondAndValues(CondEq, std::move(source).Values());
	}
	target.Distinct(distinct);
	return MergeResult::Merged;
}

template <QueryPreprocessor::ValuesType vt, typename... CmpArgs>
QueryPreprocessor::MergeResult QueryPreprocessor::mergeQueryEntriesRangeLt(NeedSwitch needSwitch, QueryEntry& range, QueryEntry& lt,
																		   bool distinct, size_t position, const CmpArgs&... args) {
	const Variant& ltV = lt.Values()[0];
	const Variant& rngL = range.Values()[0];
	const Variant& rngR = range.Values()[1];
	const MergeLessT<vt> less{args...};
	if (!less(rngL, ltV)) {
		SetValue(position, AlwaysFalse{});
		return MergeResult::Annihilated;
	} else if (less(rngR, ltV)) {
		range.Distinct(distinct);
		if (needSwitch == NeedSwitch::Yes) {
			lt = std::move(range);
		}
		return MergeResult::Merged;
	} else {
		return MergeResult::NotMerged;
	}
}

template <QueryPreprocessor::ValuesType vt, typename... CmpArgs>
QueryPreprocessor::MergeResult QueryPreprocessor::mergeQueryEntriesRangeGt(NeedSwitch needSwitch, QueryEntry& range, QueryEntry& gt,
																		   bool distinct, size_t position, const CmpArgs&... args) {
	const Variant& gtV = gt.Values()[0];
	const Variant& rngL = range.Values()[0];
	const Variant& rngR = range.Values()[1];
	const MergeLessT<vt> less{args...};
	if (!less(gtV, rngR)) {
		SetValue(position, AlwaysFalse{});
		return MergeResult::Annihilated;
	} else if (less(gtV, rngL)) {
		range.Distinct(distinct);
		if (needSwitch == NeedSwitch::Yes) {
			gt = std::move(range);
		}
		return MergeResult::Merged;
	} else {
		return MergeResult::NotMerged;
	}
}

template <QueryPreprocessor::ValuesType vt, typename... CmpArgs>
QueryPreprocessor::MergeResult QueryPreprocessor::mergeQueryEntriesRangeLe(NeedSwitch needSwitch, QueryEntry& range, QueryEntry& le,
																		   bool distinct, size_t position, const CmpArgs&... args) {
	const Variant& leV = le.Values()[0];
	const Variant& rngL = range.Values()[0];
	const Variant& rngR = range.Values()[1];
	const MergeLessT<vt> less{args...};
	QueryEntry& target = needSwitch == NeedSwitch::Yes ? le : range;
	if (less(leV, rngL)) {
		SetValue(position, AlwaysFalse{});
		return MergeResult::Annihilated;
	} else if (MergeEqualT<vt>{args...}(leV, rngL)) {
		target.SetCondAndValues(CondEq, std::move(le).Values());
		target.Distinct(distinct);
	} else if (less(leV, rngR)) {
		target.SetCondAndValues(CondRange, VariantArray{std::move(range).Values()[0], std::move(le).Values()[0]});
		target.Distinct(distinct);
	} else {
		range.Distinct(distinct);
		if (needSwitch == NeedSwitch::Yes) {
			le = std::move(range);
		}
	}
	return MergeResult::Merged;
}

template <QueryPreprocessor::ValuesType vt, typename... CmpArgs>
QueryPreprocessor::MergeResult QueryPreprocessor::mergeQueryEntriesRangeGe(NeedSwitch needSwitch, QueryEntry& range, QueryEntry& ge,
																		   bool distinct, size_t position, const CmpArgs&... args) {
	const Variant& geV = ge.Values()[0];
	const Variant& rngL = range.Values()[0];
	const Variant& rngR = range.Values()[1];
	const MergeLessT<vt> less{args...};
	QueryEntry& target = needSwitch == NeedSwitch::Yes ? ge : range;
	if (less(rngR, geV)) {
		SetValue(position, AlwaysFalse{});
		return MergeResult::Annihilated;
	} else if (MergeEqualT<vt>{args...}(geV, rngR)) {
		target.SetCondAndValues(CondEq, std::move(ge).Values());
		target.Distinct(distinct);
	} else if (less(rngL, geV)) {
		target.SetCondAndValues(CondRange, VariantArray{std::move(ge).Values()[0], std::move(range).Values()[1]});
		target.Distinct(distinct);
	} else {
		range.Distinct(distinct);
		if (needSwitch == NeedSwitch::Yes) {
			ge = std::move(range);
		}
	}
	return MergeResult::Merged;
}

template <QueryPreprocessor::ValuesType vt, typename... CmpArgs>
QueryPreprocessor::MergeResult QueryPreprocessor::mergeQueryEntriesRange(QueryEntry& lqe, QueryEntry& rqe, bool distinct, size_t position,
																		 const CmpArgs&... args) {
	const MergeLessT<vt> less{args...};
	QueryEntry& left = less(lqe.Values()[0], rqe.Values()[0]) ? rqe : lqe;
	QueryEntry& right = less(rqe.Values()[1], lqe.Values()[1]) ? rqe : lqe;
	if (less(right.Values()[1], left.Values()[0])) {
		SetValue(position, AlwaysFalse{});
		return MergeResult::Annihilated;
	} else if (MergeEqualT<vt>{args...}(left.Values()[0], right.Values()[1])) {
		lqe.SetCondAndValues(CondEq, VariantArray::Create(std::move(left).Values()[0]));
		lqe.Distinct(distinct);
	} else {
		lqe.SetCondAndValues(CondRange, VariantArray::Create(std::move(left).Values()[0], std::move(right).Values()[1]));
		lqe.Distinct(distinct);
	}
	return MergeResult::Merged;
}

QueryPreprocessor::MergeResult QueryPreprocessor::mergeQueryEntriesDWithin(QueryEntry& lqe, QueryEntry& rqe, bool distinct,
																		   size_t position) {
	Point lp, rp;
	double ld, rd;
	if (lqe.Values()[0].Type().Is<KeyValueType::Tuple>()) {
		lp = lqe.Values()[0].As<Point>();
		ld = lqe.Values()[1].As<double>();
	} else {
		lp = lqe.Values()[1].As<Point>();
		ld = lqe.Values()[0].As<double>();
	}
	if (rqe.Values()[0].Type().Is<KeyValueType::Tuple>()) {
		rp = rqe.Values()[0].As<Point>();
		rd = rqe.Values()[1].As<double>();
	} else {
		rp = rqe.Values()[1].As<Point>();
		rd = rqe.Values()[0].As<double>();
	}
	const auto [minP, minR, maxP, maxR] = ld < rd ? std::make_tuple(lp, ld, rp, rd) : std::make_tuple(rp, rd, lp, ld);
	if (DWithin(maxP, minP, maxR - minR)) {
		lqe.SetCondAndValues(CondDWithin, VariantArray::Create(minP, minR));
		lqe.Distinct(distinct);
		return MergeResult::Merged;
	} else if (DWithin(lp, rp, ld + rd)) {
		return MergeResult::NotMerged;
	} else {
		SetValue(position, AlwaysFalse{});
		return MergeResult::Annihilated;
	}
}

template <QueryPreprocessor::ValuesType vt, typename... CmpArgs>
QueryPreprocessor::MergeResult QueryPreprocessor::mergeQueryEntries(size_t lhs, size_t rhs, MergeOrdered mergeOrdered,
																	const CmpArgs&... args) {
	auto& lqe = Get<QueryEntry>(lhs);
	auto& rqe = Get<QueryEntry>(rhs);
	const bool distinct = lqe.Distinct() || rqe.Distinct();
	const MergeLessT<vt> less{args...};
	switch (lqe.Condition()) {
		case CondEq:
		case CondSet:
			switch (rqe.Condition()) {
				case CondEq:
				case CondSet:
					return mergeQueryEntriesSetSet<vt>(lqe, rqe, distinct, lhs, args...);
				case CondAllSet:
					return mergeQueryEntriesAllSetSet<vt>(NeedSwitch::Yes, rqe, lqe, distinct, lhs, args...);
				case CondLt:
					return mergeQueryEntriesSetNotSet<vt>(
						NeedSwitch::No, lqe, rqe, [&rv = rqe.Values()[0], &less](const Variant& v) { return !less(v, rv); }, distinct, lhs,
						mergeOrdered);
				case CondLe:
					return mergeQueryEntriesSetNotSet<vt>(
						NeedSwitch::No, lqe, rqe, [&rv = rqe.Values()[0], &less](const Variant& v) { return less(rv, v); }, distinct, lhs,
						mergeOrdered);
				case CondGe:
					return mergeQueryEntriesSetNotSet<vt>(
						NeedSwitch::No, lqe, rqe, [&rv = rqe.Values()[0], &less](const Variant& v) { return less(v, rv); }, distinct, lhs,
						mergeOrdered);
				case CondGt:
					return mergeQueryEntriesSetNotSet<vt>(
						NeedSwitch::No, lqe, rqe, [&rv = rqe.Values()[0], &less](const Variant& v) { return !less(rv, v); }, distinct, lhs,
						mergeOrdered);
				case CondRange:
					return mergeQueryEntriesSetNotSet<vt>(
						NeedSwitch::No, lqe, rqe,
						[&rv1 = rqe.Values()[0], &rv2 = rqe.Values()[1], &less](const Variant& v) { return less(v, rv1) || less(rv2, v); },
						distinct, lhs, mergeOrdered);
				case CondAny:
					return mergeQueryEntriesAny(NeedSwitch::No, rqe, lqe, distinct, lhs);
				case CondEmpty:
					SetValue(lhs, AlwaysFalse{});
					return MergeResult::Annihilated;
				case CondDWithin:
				case CondLike:
					return MergeResult::NotMerged;
			}
			break;
		case CondAllSet:
			switch (rqe.Condition()) {
				case CondEq:
				case CondSet:
					return mergeQueryEntriesAllSetSet<vt>(NeedSwitch::No, lqe, rqe, distinct, lhs, args...);
				case CondAllSet:
					return mergeQueryEntriesAllSetAllSet<vt>(lqe, rqe, distinct, lhs, args...);
				case CondLt:
					return mergeQueryEntriesAllSetNotSet<vt>(
						NeedSwitch::No, lqe, rqe, [&rv = rqe.Values()[0], &less](const Variant& v) { return !less(v, rv); }, distinct, lhs,
						args...);
				case CondLe:
					return mergeQueryEntriesAllSetNotSet<vt>(
						NeedSwitch::No, lqe, rqe, [&rv = rqe.Values()[0], &less](const Variant& v) { return less(rv, v); }, distinct, lhs,
						args...);
				case CondGe:
					return mergeQueryEntriesAllSetNotSet<vt>(
						NeedSwitch::No, lqe, rqe, [&rv = rqe.Values()[0], &less](const Variant& v) { return less(v, rv); }, distinct, lhs,
						args...);
				case CondGt:
					return mergeQueryEntriesAllSetNotSet<vt>(
						NeedSwitch::No, lqe, rqe, [&rv = rqe.Values()[0], &less](const Variant& v) { return !less(rv, v); }, distinct, lhs,
						args...);
				case CondRange:
					return mergeQueryEntriesAllSetNotSet<vt>(
						NeedSwitch::No, lqe, rqe,
						[&rv1 = rqe.Values()[0], &rv2 = rqe.Values()[1], &less](const Variant& v) { return less(v, rv1) || less(rv2, v); },
						distinct, lhs, args...);
				case CondAny:
					return mergeQueryEntriesAny(NeedSwitch::No, rqe, lqe, distinct, lhs);
				case CondEmpty:
					SetValue(lhs, AlwaysFalse{});
					return MergeResult::Annihilated;
				case CondDWithin:
				case CondLike:
					return MergeResult::NotMerged;
			}
			break;
		case CondLt:
			switch (rqe.Condition()) {
				case CondEq:
				case CondSet:
					return mergeQueryEntriesSetNotSet<vt>(
						NeedSwitch::Yes, rqe, lqe, [&lv = lqe.Values()[0], &less](const Variant& v) { return !less(v, lv); }, distinct, lhs,
						mergeOrdered);
				case CondAllSet:
					return mergeQueryEntriesAllSetNotSet<vt>(
						NeedSwitch::Yes, rqe, lqe, [&lv = lqe.Values()[0], &less](const Variant& v) { return !less(v, lv); }, distinct, lhs,
						args...);
				case CondAny:
					return mergeQueryEntriesAny(NeedSwitch::No, rqe, lqe, distinct, lhs);
				case CondLt:
				case CondLe:
					return mergeQueryEntriesLt<vt>(lqe, rqe, distinct, args...);
				case CondGt:
				case CondGe:
					return mergeQueryEntriesLtGt<vt>(lqe, rqe, lhs, args...);
				case CondRange:
					return mergeQueryEntriesRangeLt<vt>(NeedSwitch::Yes, rqe, lqe, distinct, lhs, args...);
				case CondEmpty:
					SetValue(lhs, AlwaysFalse{});
					return MergeResult::Annihilated;
				case CondDWithin:
				case CondLike:
					return MergeResult::NotMerged;
			}
			break;
		case CondLe:
			switch (rqe.Condition()) {
				case CondEq:
				case CondSet:
					return mergeQueryEntriesSetNotSet<vt>(
						NeedSwitch::Yes, rqe, lqe, [&lv = lqe.Values()[0], &less](const Variant& v) { return less(lv, v); }, distinct, lhs,
						mergeOrdered);
				case CondAllSet:
					return mergeQueryEntriesAllSetNotSet<vt>(
						NeedSwitch::Yes, rqe, lqe, [&lv = lqe.Values()[0], &less](const Variant& v) { return less(lv, v); }, distinct, lhs,
						args...);
				case CondLt:
				case CondLe:
					return mergeQueryEntriesLt<vt>(lqe, rqe, distinct, args...);
				case CondGt:
					return mergeQueryEntriesLtGt<vt>(lqe, rqe, lhs, args...);
				case CondGe:
					return mergeQueryEntriesLeGe<vt>(NeedSwitch::No, lqe, rqe, distinct, lhs, args...);
				case CondRange:
					return mergeQueryEntriesRangeLe<vt>(NeedSwitch::Yes, rqe, lqe, distinct, lhs, args...);
				case CondAny:
					return mergeQueryEntriesAny(NeedSwitch::No, rqe, lqe, distinct, lhs);
				case CondEmpty:
					SetValue(lhs, AlwaysFalse{});
					return MergeResult::Annihilated;
				case CondDWithin:
				case CondLike:
					return MergeResult::NotMerged;
			}
			break;
		case CondGt:
			switch (rqe.Condition()) {
				case CondEq:
				case CondSet:
					return mergeQueryEntriesSetNotSet<vt>(
						NeedSwitch::Yes, rqe, lqe, [&lv = lqe.Values()[0], &less](const Variant& v) { return !less(lv, v); }, distinct, lhs,
						mergeOrdered);
				case CondAllSet:
					return mergeQueryEntriesAllSetNotSet<vt>(
						NeedSwitch::Yes, rqe, lqe, [&lv = lqe.Values()[0], &less](const Variant& v) { return !less(lv, v); }, distinct, lhs,
						args...);
				case CondGt:
				case CondGe:
					return mergeQueryEntriesGt<vt>(lqe, rqe, distinct, args...);
				case CondLt:
				case CondLe:
					return mergeQueryEntriesLtGt<vt>(rqe, lqe, lhs, args...);
				case CondRange:
					return mergeQueryEntriesRangeGt<vt>(NeedSwitch::Yes, rqe, lqe, distinct, lhs, args...);
				case CondAny:
					return mergeQueryEntriesAny(NeedSwitch::No, rqe, lqe, distinct, lhs);
				case CondEmpty:
					SetValue(lhs, AlwaysFalse{});
					return MergeResult::Annihilated;
				case CondDWithin:
				case CondLike:
					return MergeResult::NotMerged;
			}
			break;
		case CondGe:
			switch (rqe.Condition()) {
				case CondEq:
				case CondSet:
					return mergeQueryEntriesSetNotSet<vt>(
						NeedSwitch::Yes, rqe, lqe, [&lv = lqe.Values()[0], &less](const Variant& v) { return less(v, lv); }, distinct, lhs,
						mergeOrdered);
				case CondAllSet:
					return mergeQueryEntriesAllSetNotSet<vt>(
						NeedSwitch::Yes, rqe, lqe, [&lv = lqe.Values()[0], &less](const Variant& v) { return less(v, lv); }, distinct, lhs,
						args...);
				case CondGt:
				case CondGe:
					return mergeQueryEntriesGt<vt>(lqe, rqe, distinct, args...);
				case CondLt:
					return mergeQueryEntriesLtGt<vt>(rqe, lqe, lhs, args...);
				case CondLe:
					return mergeQueryEntriesLeGe<vt>(NeedSwitch::Yes, rqe, lqe, distinct, lhs, args...);
				case CondRange:
					return mergeQueryEntriesRangeGe<vt>(NeedSwitch::Yes, rqe, lqe, distinct, lhs, args...);
				case CondAny:
					return mergeQueryEntriesAny(NeedSwitch::No, rqe, lqe, distinct, lhs);
				case CondEmpty:
					SetValue(lhs, AlwaysFalse{});
					return MergeResult::Annihilated;
				case CondDWithin:
				case CondLike:
					return MergeResult::NotMerged;
			}
			break;
		case CondRange:
			switch (rqe.Condition()) {
				case CondEq:
				case CondSet:
					return mergeQueryEntriesSetNotSet<vt>(
						NeedSwitch::Yes, rqe, lqe,
						[&lv1 = lqe.Values()[0], &lv2 = lqe.Values()[1], &less](const Variant& v) { return less(v, lv1) || less(lv2, v); },
						distinct, lhs, mergeOrdered);
				case CondAllSet:
					return mergeQueryEntriesAllSetNotSet<vt>(
						NeedSwitch::Yes, rqe, lqe,
						[&lv1 = lqe.Values()[0], &lv2 = lqe.Values()[1], &less](const Variant& v) { return less(v, lv1) || less(lv2, v); },
						distinct, lhs, args...);
				case CondLt:
					return mergeQueryEntriesRangeLt<vt>(NeedSwitch::No, lqe, rqe, distinct, lhs, args...);
				case CondLe:
					return mergeQueryEntriesRangeLe<vt>(NeedSwitch::No, lqe, rqe, distinct, lhs, args...);
				case CondGt:
					return mergeQueryEntriesRangeGt<vt>(NeedSwitch::No, lqe, rqe, distinct, lhs, args...);
				case CondGe:
					return mergeQueryEntriesRangeGe<vt>(NeedSwitch::No, lqe, rqe, distinct, lhs, args...);
				case CondRange:
					return mergeQueryEntriesRange<vt>(lqe, rqe, distinct, lhs, args...);
				case CondAny:
					return mergeQueryEntriesAny(NeedSwitch::No, rqe, lqe, distinct, lhs);
				case CondEmpty:
					SetValue(lhs, AlwaysFalse{});
					return MergeResult::Annihilated;
				case CondDWithin:
				case CondLike:
					return MergeResult::NotMerged;
			}
			break;
		case CondAny:
			return mergeQueryEntriesAny(NeedSwitch::Yes, lqe, rqe, distinct, lhs);
		case CondEmpty:
			switch (rqe.Condition()) {
				case CondEq:
				case CondSet:
				case CondAllSet:
				case CondLt:
				case CondLe:
				case CondGe:
				case CondGt:
				case CondRange:
				case CondDWithin:
				case CondLike:
				case CondAny:
					SetValue(lhs, AlwaysFalse{});
					return MergeResult::Annihilated;
				case CondEmpty:
					lqe.Distinct(distinct);
					return MergeResult::Merged;
			}
			break;
		case CondDWithin:
			switch (rqe.Condition()) {
				case CondDWithin:
					return mergeQueryEntriesDWithin(lqe, rqe, distinct, lhs);
				case CondEq:
				case CondSet:
				case CondAllSet:
				case CondLt:
				case CondLe:
				case CondGe:
				case CondGt:
				case CondRange:
				case CondLike:
				case CondAny:
				case CondEmpty:
					return MergeResult::NotMerged;
			}
			break;
		case CondLike:
			return MergeResult::NotMerged;
	}
	return MergeResult::NotMerged;
}

void QueryPreprocessor::AddDistinctEntries(const h_vector<Aggregator, 4>& aggregators) {
	bool wasAdded = false;
	for (auto& ag : aggregators) {
		if (ag.Type() != AggDistinct) {
			continue;
		}
		assertrx_throw(ag.Names().size() == 1);
		Append<QueryEntry>(wasAdded ? OpOr : OpAnd, ag.Names()[0], QueryEntry::DistinctTag{});
		auto& qe = Get<QueryEntry>(LastAppendedElement());
		SetQueryField(qe.FieldData(), ns_);
		checkStrictMode(qe.FieldData());
		wasAdded = true;
	}
}

std::pair<CondType, VariantArray> QueryPreprocessor::queryValuesFromOnCondition(std::string& explainStr, AggType& oAggType,
																				NamespaceImpl& rightNs, Query joinQuery,
																				JoinPreResult::CPtr joinPreresult,
																				const QueryJoinEntry& joinEntry, CondType condition,
																				int mainQueryMaxIterations, const RdxContext& rdxCtx) {
	size_t limit = 0;
	const auto& rNsCfg = rightNs.config();
	if (rNsCfg.maxPreselectSize == 0) {
		limit = std::max<int64_t>(rNsCfg.minPreselectSize, rightNs.itemsCount() * rNsCfg.maxPreselectPart);
	} else if (rNsCfg.maxPreselectPart == 0.0) {
		limit = rNsCfg.maxPreselectSize;
	} else {
		limit =
			std::min(std::max<int64_t>(rNsCfg.minPreselectSize, rightNs.itemsCount() * rNsCfg.maxPreselectPart), rNsCfg.maxPreselectSize);
	}
	joinQuery.Explain(query_.NeedExplain());
	joinQuery.Limit(limit + 2);
	joinQuery.Offset(QueryEntry::kDefaultOffset);
	joinQuery.sortingEntries_.clear<false>();
	joinQuery.forcedSortOrder_.clear();
	if (joinPreresult->sortOrder.index) {
		joinQuery.sortingEntries_.emplace_back(joinPreresult->sortOrder.sortingEntry);
	}

	joinQuery.aggregations_.clear();
	switch (condition) {
		case CondEq:
		case CondSet:
			joinQuery.Distinct(joinEntry.RightFieldName());
			oAggType = AggType::AggDistinct;
			break;
		case CondLt:
		case CondLe:
			joinQuery.Aggregate(AggMax, {joinEntry.RightFieldName()});
			oAggType = AggType::AggMax;
			break;
		case CondGt:
		case CondGe:
			joinQuery.Aggregate(AggMin, {joinEntry.RightFieldName()});
			oAggType = AggType::AggMin;
			break;
		case CondAny:
		case CondRange:
		case CondAllSet:
		case CondEmpty:
		case CondLike:
		case CondDWithin:
			throw Error(errQueryExec, "Unsupported condition in ON statement: %s", CondTypeToStr(condition));
	}

	LocalQueryResults qr;
	SelectCtxWithJoinPreSelect ctx{joinQuery, nullptr, JoinPreResultExecuteCtx{std::move(joinPreresult), mainQueryMaxIterations}};
	rightNs.Select(qr, ctx, rdxCtx);
	if (ctx.preSelect.Mode() == JoinPreSelectMode::InjectionRejected || qr.Count() > limit) {
		return {CondAny, {}};
	}
	assertrx_throw(qr.aggregationResults.size() == 1);
	auto& aggRes = qr.aggregationResults[0];
	explainStr = qr.explainResults;
	switch (condition) {
		case CondEq:
		case CondSet: {
			assertrx_throw(aggRes.type == AggDistinct);
			VariantArray values;
			values.reserve(aggRes.distincts.size());
			for (Variant& distValue : aggRes.distincts) {
				if (distValue.Type().Is<KeyValueType::Composite>()) {
					ConstPayload pl(aggRes.payloadType, distValue.operator const PayloadValue&());
					values.emplace_back(pl.GetComposite(aggRes.distinctsFields, joinEntry.RightCompositeFieldsTypes()));
				} else {
					values.emplace_back(std::move(distValue));
				}
			}
			return {CondSet, std::move(values)};
		}
		case CondLt:
		case CondLe:
		case CondGt:
		case CondGe:
			if (auto value = aggRes.GetValue()) {
				return {condition, {Variant{*value}}};
			} else {
				return {CondAny, {}};
			}
		case CondAny:
		case CondRange:
		case CondAllSet:
		case CondEmpty:
		case CondLike:
		case CondDWithin:
		default:
			throw Error(errQueryExec, "Unsupported condition in ON statement: %s", CondTypeToStr(condition));
	}
}

std::pair<CondType, VariantArray> QueryPreprocessor::queryValuesFromOnCondition(CondType condition, const QueryJoinEntry& joinEntry,
																				const JoinedSelector& joinedSelector,
																				const CollateOpts& collate) {
	switch (condition) {
		case CondEq:
		case CondSet:
			return {CondSet, joinedSelector.readValuesFromPreResult(joinEntry)};
		case CondLt:
		case CondLe:
		case CondGt:
		case CondGe: {
			const JoinPreResult::Values& values = std::get<JoinPreResult::Values>(joinedSelector.PreResult().payload);
			VariantArray buffer, keyValues;
			for (const ItemRef& item : values) {
				assertrx_throw(!item.Value().IsFree());
				const ConstPayload pl{values.payloadType, item.Value()};
				pl.GetByFieldsSet(joinEntry.RightFields(), buffer, joinEntry.RightFieldType(), joinEntry.RightCompositeFieldsTypes());
				for (Variant& v : buffer) {
					if (keyValues.empty()) {
						keyValues.emplace_back(std::move(v));
					} else {
						const auto cmp = keyValues[0].Compare<NotComparable::Throw>(v, collate);
						if (condition == CondLt || condition == CondLe) {
							if (cmp == ComparationResult::Lt) {
								keyValues[0] = std::move(v);
							}
						} else {
							if (cmp == ComparationResult::Gt) {
								keyValues[0] = std::move(v);
							}
						}
					}
				}
			}
			return {condition, std::move(keyValues)};
		}
		case CondAny:
		case CondRange:
		case CondAllSet:
		case CondEmpty:
		case CondLike:
		case CondDWithin:
		default:
			throw Error(errQueryExec, "Unsupported condition in ON statement: %s", CondTypeToStr(condition));
	}
}

template <typename JS>
void QueryPreprocessor::briefDump(size_t from, size_t to, const std::vector<JS>& joinedSelectors, WrSerializer& ser) const {
	{
		for (auto it = from; it < to; it = Next(it)) {
			if (it != from || container_[it].operation != OpAnd) {
				ser << container_[it].operation << ' ';
			}
			container_[it].Visit([](const SubQueryEntry&) { throw_as_assert; }, [](const SubQueryFieldEntry&) { throw_as_assert; },
								 [&](const QueryEntriesBracket& b) {
									 ser << "(";
									 briefDump(it + 1, Next(it), joinedSelectors, ser);
									 dumpEqualPositions(0, ser, b.equalPositions);
									 ser << ")";
								 },
								 [&ser](const QueryEntry& qe) { ser << qe.DumpBrief() << ' '; },
								 [&joinedSelectors, &ser](const JoinQueryEntry& jqe) { ser << jqe.Dump(joinedSelectors) << ' '; },
								 [&ser](const BetweenFieldsQueryEntry& qe) { ser << qe.Dump() << ' '; },
								 [&ser](const AlwaysFalse&) { ser << "AlwaysFalse" << ' '; },
								 [&ser](const AlwaysTrue&) { ser << "AlwaysTrue" << ' '; });
		}
	}
}

template <typename ExplainPolicy>
size_t QueryPreprocessor::injectConditionsFromJoins(const size_t from, size_t to, JoinedSelectors& js,
													OnConditionInjections& explainOnInjections, int embracedMaxIterations,
													h_vector<int, 256>& maxIterations, bool inTransaction, bool enableSortOrders,
													const RdxContext& rdxCtx) {
	using namespace std::string_view_literals;

	size_t injectedCount = 0;
	for (size_t cur = from; cur < to; cur = Next(cur)) {
		container_[cur].Visit(
			[](const SubQueryEntry&) { throw_as_assert; }, [](const SubQueryFieldEntry&) { throw_as_assert; },
			Skip<QueryEntry, BetweenFieldsQueryEntry, AlwaysFalse, AlwaysTrue>{},
			[&](const QueryEntriesBracket&) {
				const size_t injCount =
					injectConditionsFromJoins<ExplainPolicy>(cur + 1, Next(cur), js, explainOnInjections, maxIterations[cur], maxIterations,
															 inTransaction, enableSortOrders, rdxCtx);
				to += injCount;
				injectedCount += injCount;
				assertrx_throw(to <= container_.size());
			},
			[&](const JoinQueryEntry& jqe) {
				const auto joinIndex = jqe.joinIndex;
				assertrx_throw(js.size() > joinIndex);
				JoinedSelector& joinedSelector = js[joinIndex];
				const JoinPreResult& preResult = joinedSelector.PreResult();
				assertrx_throw(joinedSelector.PreSelectMode() == JoinPreSelectMode::Execute);
				const bool byValues = std::holds_alternative<JoinPreResult::Values>(preResult.payload);

				auto explainJoinOn = ExplainPolicy::AppendJoinOnExplain(explainOnInjections);
				explainJoinOn.Init(jqe, js, byValues);

				// Checking if we are able to preselect something from RightNs, or there are preselected results
				if (!byValues) {
					const auto& rNsCfg = joinedSelector.RightNs()->config();
					if (rNsCfg.maxPreselectSize == 0 && rNsCfg.maxPreselectPart == 0.0) {
						explainJoinOn.Skipped("maxPreselectSize and maxPreselectPart == 0"sv);
						return;
					}
				} else {
					if (!std::get<JoinPreResult::Values>(preResult.payload).IsPreselectAllowed()) {
						explainJoinOn.Skipped("Preselect is not allowed"sv);
						return;
					}
				}
				const auto& joinEntries = joinedSelector.joinQuery_.joinEntries_;
				// LeftJoin-s shall not be in QueryEntries container_ by construction
				assertrx_throw(joinedSelector.Type() == InnerJoin || joinedSelector.Type() == OrInnerJoin);
				// Checking if we have anything to inject into main Where clause
				bool foundANDOrOR = false;
				for (const auto& je : joinEntries) {
					if (je.Operation() != OpNot) {
						foundANDOrOR = true;
						break;
					}
				}
				if (!foundANDOrOR) {
					explainJoinOn.Skipped("And or Or operators not found"sv);
					return;
				}

				OpType op = GetOperation(cur);
				if (joinedSelector.Type() == OrInnerJoin) {
					if (op == OpNot) {
						throw Error(errQueryExec, "OR INNER JOIN with operation NOT");
					}
					op = OpOr;
					joinedSelector.SetType(InnerJoin);
				}

				// inserting Bracket for JoinQuery itself into ExpressionTree
				SetOperation(OpAnd, cur);
				// !!!Warning jqe reference will be invalidated after EncloseInBracket
				EncloseInBracket(cur, cur + 1, op);
				const size_t bracketStart = cur;
				++cur;
				++to;
				++injectedCount;
				size_t count = InjectConditionsFromOnConditions<InjectionDirection::IntoMain>(
					cur, joinEntries, joinedSelector.joinQuery_.Entries(), joinIndex,
					byValues ? nullptr : &joinedSelector.RightNs()->indexes_);
				initIndexedQueries(cur, cur + count);
				cur += count;
				to += count;
				injectedCount += count;
				maxIterations.insert(maxIterations.begin() + bracketStart, count + 1, embracedMaxIterations);
				span<int> maxItersSpan(maxIterations.data(), maxIterations.size());
				maxIterations[bracketStart] = calculateMaxIterations(bracketStart + 1, Next(bracketStart), embracedMaxIterations,
																	 maxItersSpan, inTransaction, enableSortOrders, rdxCtx);

				explainJoinOn.ReserveOnEntries(joinEntries.size());

				count = 0;
				bool prevIsSkipped = false;
				size_t orChainLength = 0;
				for (size_t i = 0, s = joinEntries.size(); i < s; ++i) {
					const QueryJoinEntry& joinEntry = joinEntries[i];
					auto explainEntry = explainJoinOn.AppendOnEntryExplain();
					explainEntry.InitialCondition(joinEntry, joinedSelector);
					CondType condition = joinEntry.Condition();
					OpType operation = joinEntry.Operation();
					switch (operation) {
						case OpNot:
							orChainLength = 0;
							switch (condition) {
								case CondLt:
									condition = CondGe;
									break;
								case CondLe:
									condition = CondGt;
									break;
								case CondGt:
									condition = CondLe;
									break;
								case CondGe:
									condition = CondLt;
									break;
								case CondEq:
								case CondSet:
									prevIsSkipped = true;
									explainEntry.Skipped("Skipped due to condition Eq|Set with operation Not."sv);
									continue;
								case CondAny:
								case CondRange:
								case CondAllSet:
								case CondEmpty:
								case CondLike:
								case CondDWithin:
									throw Error(errQueryExec, "Unsupported condition in ON statement: %s", CondTypeToStr(condition));
							}
							operation = OpAnd;
							break;
						case OpOr:
							explainEntry.OrChainPart(true);
							if (prevIsSkipped) {
								continue;
							}
							++orChainLength;
							break;
						case OpAnd:
							orChainLength = 0;
							break;
					}
					CondType queryCondition{CondAny};
					VariantArray values;
					if (byValues) {
						assertrx_throw(joinedSelector.itemQuery_.Entries().Is<QueryEntry>(i));
						assertrx_throw(joinedSelector.itemQuery_.Entries().Get<QueryEntry>(i).FieldName() == joinEntry.RightFieldName());
						static const CollateOpts collate;
						const CollateOpts* collatePtr = &collate;
						if (joinEntry.IsLeftFieldIndexed()) {
							collatePtr = &ns_.indexes_[joinEntry.LeftIdxNo()]->Opts().collateOpts_;
						}
						std::tie(queryCondition, values) = queryValuesFromOnCondition(condition, joinEntry, joinedSelector, *collatePtr);
					} else {
						bool skip = false;
						switch (condition) {
							case CondAny:
							case CondEmpty:
							case CondLike:
							case CondDWithin:
								explainEntry.Skipped("Skipped due to unsupported ON condition"sv);
								skip = true;
								break;
							case CondRange:
							case CondLt:
							case CondLe:
							case CondGt:
							case CondGe:
								joinedSelector.itemQuery_.Entries().Get<QueryEntry>(i).FieldType().EvaluateOneOf(
									[&skip,
									 &explainEntry](OneOf<KeyValueType::String, KeyValueType::Composite, KeyValueType::Tuple,
														  KeyValueType::Uuid, KeyValueType::Null, KeyValueType::Undefined>) noexcept {
										skip = true;
										explainEntry.Skipped(
											"Skipped due to condition Lt|Le|Gt|Ge|Range with not indexed or not numeric field."sv);
									},
									[](OneOf<KeyValueType::Bool, KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double>) noexcept {
									});
								break;
							case CondEq:
							case CondSet:
							case CondAllSet:
								joinedSelector.itemQuery_.Entries().Get<QueryEntry>(i).FieldType().EvaluateOneOf(
									[&skip, &explainEntry](OneOf<KeyValueType::Composite, KeyValueType::Tuple>) noexcept {
										skip = true;
										explainEntry.Skipped("Skipped due to condition Eq|Set|AllSet with composite index."sv);
									},
									[](OneOf<KeyValueType::Bool, KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double,
											 KeyValueType::String, KeyValueType::Uuid, KeyValueType::Null,
											 KeyValueType::Undefined>) noexcept {});
								break;
						}
						if (!skip) {
							std::string explainSelect;
							AggType selectAggType;
							std::tie(queryCondition, values) =
								(!std::holds_alternative<SelectIteratorContainer>(preResult.payload)
									 ? queryValuesFromOnCondition(explainSelect, selectAggType, *joinedSelector.RightNs(),
																  Query{joinedSelector.RightNsName()}, joinedSelector.PreResultPtr(),
																  joinEntry, condition, embracedMaxIterations, rdxCtx)
									 : queryValuesFromOnCondition(explainSelect, selectAggType, *joinedSelector.RightNs(),
																  joinedSelector.JoinQuery(), joinedSelector.PreResultPtr(), joinEntry,
																  condition, embracedMaxIterations, rdxCtx));

							explainEntry.ExplainSelect(std::move(explainSelect), selectAggType);
						}
					}
					if (!values.empty()) {
						Emplace<QueryEntry>(cur, operation, QueryField(joinEntry.LeftFieldData()), queryCondition, std::move(values));
						explainEntry.Succeed(Get<QueryEntry>(cur));
						maxIterations.insert(maxIterations.begin() + cur, embracedMaxIterations);
						initIndexedQueries(cur, cur + 1);
						++cur;
						++count;
						prevIsSkipped = false;
					} else {
						explainEntry.Skipped("Skipped as cannot obtain values from right namespace."sv);
						if (operation == OpOr) {
							Erase(cur - orChainLength, cur);
							maxIterations.erase(maxIterations.begin() + (cur - orChainLength), maxIterations.begin() + cur);
							cur -= orChainLength;
							count -= orChainLength;
							// Marking On-injections as fail for removed entries.
							explainJoinOn.FailOnEntriesAsOrChain(orChainLength);
						}
						prevIsSkipped = true;
					}
				}  // end of entries processing

				if (count > 0) {
					EncloseInBracket(cur - count, cur, OpAnd);
					maxIterations.insert(maxIterations.begin() + (cur - count), embracedMaxIterations);

					explainJoinOn.Succeed(
						[this, cur, count, &js](WrSerializer& ser) { briefDump(cur - count, Next(cur - count), js, ser); });

					++cur;
					injectedCount += count + 1;
					to += count + 1;
				} else {
					explainJoinOn.Skipped("Skipped as there are no injected conditions");
				}
			});
	}
	return injectedCount;
}

class JoinOnExplainDisabled {
	JoinOnExplainDisabled() noexcept = default;
	struct OnEntryExplain {
		OnEntryExplain() noexcept = default;

		RX_ALWAYS_INLINE void InitialCondition(const QueryJoinEntry&, const JoinedSelector&) const noexcept {}
		RX_ALWAYS_INLINE void Succeed(const QueryEntry&) const noexcept {}
		RX_ALWAYS_INLINE void Skipped(std::string_view) const noexcept {}
		RX_ALWAYS_INLINE void OrChainPart(bool) const noexcept {}
		RX_ALWAYS_INLINE void ExplainSelect(std::string&&, AggType) const noexcept {}
	};

public:
	[[nodiscard]] RX_ALWAYS_INLINE static JoinOnExplainDisabled AppendJoinOnExplain(OnConditionInjections&) noexcept { return {}; }

	RX_ALWAYS_INLINE void Init(const JoinQueryEntry&, const JoinedSelectors&, bool) const noexcept {}
	RX_ALWAYS_INLINE void Succeed(const std::function<void(WrSerializer&)>&) const noexcept {}
	RX_ALWAYS_INLINE void Skipped(std::string_view) const noexcept {}
	RX_ALWAYS_INLINE void ReserveOnEntries(size_t) const noexcept {}
	[[nodiscard]] RX_ALWAYS_INLINE OnEntryExplain AppendOnEntryExplain() const noexcept { return {}; }

	RX_ALWAYS_INLINE void FailOnEntriesAsOrChain(size_t) const noexcept {}
};

class JoinOnExplainEnabled {
	using time_point_t = ExplainCalc::Clock::time_point;
	struct OnEntryExplain {
		OnEntryExplain(ConditionInjection& explainEntry) noexcept : startTime_(ExplainCalc::Clock::now()), explainEntry_(explainEntry) {}
		~OnEntryExplain() noexcept { explainEntry_.totalTime_ = ExplainCalc::Clock::now() - startTime_; }
		OnEntryExplain(const OnEntryExplain&) = delete;
		OnEntryExplain(OnEntryExplain&&) = delete;
		OnEntryExplain& operator=(const OnEntryExplain&) = delete;
		OnEntryExplain& operator=(OnEntryExplain&&) = delete;

		void InitialCondition(const QueryJoinEntry& joinEntry, const JoinedSelector& joinedSelector) {
			explainEntry_.initCond = joinEntry.DumpCondition(joinedSelector);
		}
		void Succeed(const QueryEntry& newEntry) {
			explainEntry_.succeed = true;
			explainEntry_.reason = "";
			explainEntry_.newCond = newEntry.DumpBrief();
			explainEntry_.valuesCount = newEntry.Values().size();
		}

		void Skipped(std::string_view reason) noexcept {
			if (explainEntry_.reason.empty()) {
				explainEntry_.reason = reason;
			}
			explainEntry_.succeed = false;
		}

		void OrChainPart(bool orChainPart) noexcept { explainEntry_.orChainPart_ = orChainPart; }
		void ExplainSelect(std::string&& explain, AggType aggType) noexcept {
			explainEntry_.explain = std::move(explain);
			explainEntry_.aggType = aggType;
		}

	private:
		time_point_t startTime_;
		ConditionInjection& explainEntry_;
	};

	JoinOnExplainEnabled(JoinOnInjection& joinOn) noexcept : explainJoinOn_(joinOn), startTime_(ExplainCalc::Clock::now()) {}

public:
	JoinOnExplainEnabled(const JoinOnExplainEnabled&) = delete;
	JoinOnExplainEnabled(JoinOnExplainEnabled&&) = delete;
	JoinOnExplainEnabled& operator=(const JoinOnExplainEnabled&) = delete;
	JoinOnExplainEnabled& operator=(JoinOnExplainEnabled&&) = delete;

	[[nodiscard]] static JoinOnExplainEnabled AppendJoinOnExplain(OnConditionInjections& explainOnInjections) {
		return {explainOnInjections.emplace_back()};
	}
	~JoinOnExplainEnabled() noexcept { explainJoinOn_.totalTime_ = ExplainCalc::Clock::now() - startTime_; }

	void Init(const JoinQueryEntry& jqe, const JoinedSelectors& js, bool byValues) {
		const JoinedSelector& joinedSelector = js[jqe.joinIndex];
		explainJoinOn_.rightNsName = joinedSelector.RightNsName();
		explainJoinOn_.joinCond = jqe.DumpOnCondition(js);
		explainJoinOn_.type = byValues ? JoinOnInjection::ByValue : JoinOnInjection::Select;
	}
	void Succeed(const std::function<void(WrSerializer&)>& setInjectedCond) {
		explainJoinOn_.succeed = true;
		setInjectedCond(explainJoinOn_.injectedCond);
	}
	void Skipped(std::string_view reason) noexcept {
		if (explainJoinOn_.reason.empty()) {
			explainJoinOn_.reason = reason;
		}
		explainJoinOn_.succeed = false;
	}

	void ReserveOnEntries(size_t count) { explainJoinOn_.conditions.reserve(count); }
	[[nodiscard]] OnEntryExplain AppendOnEntryExplain() { return {explainJoinOn_.conditions.emplace_back()}; }

	void FailOnEntriesAsOrChain(size_t orChainLength) {
		using namespace std::string_view_literals;
		auto& conditions = explainJoinOn_.conditions;
		assertrx_throw(conditions.size() >= orChainLength);
		// Marking On-injections as fail for removed entries.
		for (size_t jsz = conditions.size(), j = jsz - orChainLength; j < jsz; ++j) {
			conditions[j].succeed = false;
			conditions[j].orChainPart_ = true;
		}
	}

private:
	JoinOnInjection& explainJoinOn_;
	time_point_t startTime_;
};

void QueryPreprocessor::setQueryIndex(QueryField& qField, int idxNo, const NamespaceImpl& ns) {
	const auto& idx = *ns.indexes_[idxNo];
	QueryField::CompositeTypesVecT compositeFieldsTypes;
	if (idxNo >= ns.indexes_.firstCompositePos()) {
#ifndef NDEBUG
		const bool ftIdx = IsFullText(idx.Type());
#endif
		const auto& fields = idx.Fields();
		compositeFieldsTypes.reserve(fields.size());
		for (const auto f : fields) {
			if rx_likely (f != IndexValueType::SetByJsonPath) {
				assertrx_throw(f <= ns.indexes_.firstCompositePos());
				compositeFieldsTypes.emplace_back(ns.indexes_[f]->SelectKeyType());
			} else {
				// not indexed fields allowed only in ft composite indexes
				assertrx_throw(ftIdx);
				compositeFieldsTypes.emplace_back(KeyValueType::String{});
			}
		}
	}
	qField.SetIndexData(idxNo, FieldsSet(idx.Fields()), idx.KeyType(), idx.SelectKeyType(), std::move(compositeFieldsTypes));
}

void QueryPreprocessor::SetQueryField(QueryField& qField, const NamespaceImpl& ns) {
	int idxNo = IndexValueType::SetByJsonPath;
	if (ns.getIndexByNameOrJsonPath(qField.FieldName(), idxNo)) {
		setQueryIndex(qField, idxNo, ns);
	} else {
		qField.SetField({ns.tagsMatcher_.path2tag(qField.FieldName())});
	}
}

}  // namespace reindexer
