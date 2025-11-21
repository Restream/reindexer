#include "querypreprocessor.h"

#include "core/index/index.h"
#include "core/keyvalue/fast_hash_set_variant.h"
#include "core/namespace/namespaceimpl.h"
#include "core/nsselecter/joinedselector.h"
#include "core/nsselecter/selectiteratorcontainer.h"
#include "core/payload/fieldsset.h"
#include "core/query/dsl/dslencoder.h"
#include "core/query/queryentry.h"
#include "core/queryresults/localqueryresults.h"
#include "core/sorting/sortexpression.h"
#include "core/type_consts.h"
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
	  forcedSortOrder_(!query_.ForcedSortOrder().empty()),
	  reqMatchedOnce_(ctx.reqMatchedOnceFlag),
	  start_(ctx.offset),
	  count_(ctx.limit),
	  floatVectorsHolder_(ctx.floatVectorsHolder) {
	if (forcedSortOrder_ && (start_ > QueryEntry::kDefaultOffset || count_ < QueryEntry::kDefaultLimit)) {
		assertrx_throw(!query_.GetSortingEntries().empty());
		static const std::vector<JoinedSelector> emptyJoinedSelectors;
		const auto& sEntry = query_.GetSortingEntries()[0];
		if (SortExpression::Parse(sEntry.expression, emptyJoinedSelectors).ByField()) {
			int indexNo = IndexValueType::NotSet;
			std::ignore = ns_.tryGetIndexByNameOrJsonPath(sEntry.expression, indexNo);
			if (indexNo < 0 || !ns_.indexes_[indexNo]->IsFulltext()) {
				VariantArray values;
				values.reserve(query_.ForcedSortOrder().size());
				for (const auto& v : query_.ForcedSortOrder()) {
					assertrx_dbg(!v.IsNullValue());
					values.emplace_back(v);
				}
				desc_ = sEntry.desc;
				QueryField fld{sEntry.expression};
				SetQueryField(fld, ns_);
				[[maybe_unused]] auto appended =
					Append<QueryEntry>(desc_ ? OpNot : OpAnd, std::move(fld), query_.ForcedSortOrder().size() == 1 ? CondEq : CondSet,
									   std::move(values), QueryEntry::ForcedSortOptEntryTag{});
				assertrx_throw(appended == 1);
				hasForcedSortOptimizationEntry_ = true;
			}
		}
	}
}

void QueryPreprocessor::ExcludeFtQuery(const RdxContext& rdxCtx) {
	if (Size() <= 1 || HasForcedSortOptimizationQueryEntry()) {
		return;
	}
	for (auto it = begin(), next = it, endIt = end(); it != endIt; it = next) {
		++next;
		if (it->Is<QueryEntry>()) {
			auto& qe = it->Value<QueryEntry>();
			if (!qe.IsFieldIndexed() || qe.IsDistinctOnly()) {
				continue;
			}
			const auto& index = ns_.indexes_[qe.IndexNo()];
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
			ftEntry_ = std::move(qe);
			const size_t pos = it.PlainIterator() - cbegin().PlainIterator();
			Erase(pos, pos + 1);
			break;
		}
	}
}

bool QueryPreprocessor::NeedNextEvaluation(unsigned start, unsigned count, bool& matchedAtLeastOnce, QresExplainHolder& qresHolder,
										   bool needCalcTotal, const SelectCtx& ctx) {
	if (evaluationStage_ == EvaluationStage::Second) {
		return false;
	}
	evaluationStage_ = EvaluationStage::Second;
	if (HasForcedSortOptimizationQueryEntry()) {
		bool entryWasOptimizedOut = true;
		// Usually forced sort entry is closer to the end of the expression tree
		for (auto rit = container_.rbegin(), rend = container_.rend(); rit != rend; ++rit) {
			if (rit->Is<QueryEntry>()) {
				auto& qe = rit->Value<QueryEntry>();
				if (qe.ForcedSortOptEntry()) {
					rit->operation = desc_ ? OpAnd : OpNot;
					assertrx_throw(start <= start_);
					start_ = start;
					assertrx_throw(count <= count_);
					count_ = count;
					entryWasOptimizedOut = false;
					break;
				}
			}
		}
		return !entryWasOptimizedOut && (count_ || needCalcTotal || (reqMatchedOnce_ && !matchedAtLeastOnce));
	} else if (ftEntry_) {
		if (!matchedAtLeastOnce) {
			return false;
		}
		qresHolder.BackupContainer();
		start_ = ctx.offset;
		count_ = ctx.limit;
		forcedSortOrder_ = !query_.ForcedSortOrder().empty();
		clear();
		assertrx_dbg(ftEntry_.has_value());
		// false-positive
		// NOLINTNEXTLINE (bugprone-unchecked-optional-access)
		std::ignore = Append(OpAnd, std::move(*ftEntry_));
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
						"Current query strict mode allows filtering by indexes only. There are no indexes with name '{}' in namespace '{}'",
						field.FieldName(), ns_.name_);
		case StrictModeNames:
			if (field.HaveEmptyField()) {
				throw Error(errStrictMode,
							"Current query strict mode allows filtering by existing fields only. There are no fields with name '{}' in "
							"namespace '{}'",
							field.FieldName(), ns_.name_);
			}
		case StrictModeNotSet:
		case StrictModeNone:
			return;
	}
}

void QueryPreprocessor::checkAllowedCondition(const QueryField& field, CondType cond) const {
	if (!field.IsFieldIndexed()) {
		return;
	}
	if (ns_.indexes_[field.IndexNo()]->IsFloatVector() && ((cond != CondKnn) && (cond != CondAny) && (cond != CondEmpty))) {
		throw Error{errParams, "Valid conditions for float vector index are KNN, Empty, Any; attempt to use '{}' on field '{}'",
					CondTypeToStrShort(cond), field.FieldName()};
	}
}

class JoinOnExplainEnabled;
class JoinOnExplainDisabled;

int QueryPreprocessor::calculateMaxIterations(size_t from, size_t to, int maxMaxIters, std::span<int>& maxIterations, bool inTransaction,
											  bool enableSortOrders, const RdxContext& rdxCtx) const {
	int res = maxMaxIters;
	int current = maxMaxIters;
	for (size_t cur = from; cur < to; cur = Next(cur)) {
		maxIterations[cur] = std::min(
			maxMaxIters, Visit(
							 cur,
							 [&](const QueryEntriesBracket&) {
								 return calculateMaxIterations(cur + 1, Next(cur), maxMaxIters, maxIterations, inTransaction,
															   enableSortOrders, rdxCtx);
							 },
							 [&](const QueryEntry& qe) {
								 if (qe.IndexNo() >= 0) {
									 Index& index = *ns_.indexes_[qe.IndexNo()];
									 if (IsFullText(index.Type()) || isStore(index.Type())) {
										 return maxMaxIters;
									 }

									 Index::SelectOpts opts;
									 if (qe.Distinct()) {
										 opts.distinct = 1;
									 }
									 opts.itemsCountInNamespace = ns_.itemsCount();
									 opts.disableIdSetCache = 1;
									 opts.unbuiltSortOrders = 0;
									 opts.indexesNotOptimized = !enableSortOrders;
									 opts.inTransaction = inTransaction;
									 opts.strictMode = StrictModeNone;	// Strict mode does not matter here
									 const auto selIters =
										 index.SelectKey(qe.Values(), qe.Condition(), 0, Index::SelectContext{opts, std::nullopt}, rdxCtx);

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
							 [maxMaxIters](const concepts::OneOf<BetweenFieldsQueryEntry, JoinQueryEntry, AlwaysTrue, KnnQueryEntry,
																 MultiDistinctQueryEntry> auto&) noexcept {
								 return maxMaxIters;
							 },	 // TODO maybe change for knn
							 [](const concepts::OneOf<SubQueryEntry, SubQueryFieldEntry> auto&) -> int { throw_as_assert; },
							 [&](const AlwaysFalse&) noexcept { return 0; }));
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

void QueryPreprocessor::InjectConditionsFromJoins(JoinedSelectors& js, OnConditionInjections& explainOnInjections, LogLevel logLevel,
												  bool inTransaction, bool enableSortOrders, const RdxContext& rdxCtx) {
	h_vector<int, 256> maxIterations(Size());
	std::span<int> maxItersSpan(maxIterations.data(), maxIterations.size());
	const int maxIters = calculateMaxIterations(0, Size(), ns_.itemsCount(), maxItersSpan, inTransaction, enableSortOrders, rdxCtx);
	const bool needExplain = query_.NeedExplain() || logLevel >= LogInfo;
	if (needExplain) {
		std::ignore = injectConditionsFromJoins<JoinOnExplainEnabled>(0, Size(), js, explainOnInjections, maxIters, maxIterations,
																	  inTransaction, enableSortOrders, rdxCtx);
	} else {
		std::ignore = injectConditionsFromJoins<JoinOnExplainDisabled>(0, Size(), js, explainOnInjections, maxIters, maxIterations,
																	   inTransaction, enableSortOrders, rdxCtx);
	}
	assertrx_dbg(maxIterations.size() == Size());
}

Changed QueryPreprocessor::removeAlwaysFalse() {
	const auto [deleted, changed] = removeAlwaysFalse(0, Size());
	return changed || Changed{deleted != 0};
}

std::pair<size_t, Changed> QueryPreprocessor::removeAlwaysFalse(size_t begin, size_t end) {
	size_t deleted = 0;
	Changed changed = Changed_False;
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
				case OpNot: {
					std::ignore = SetValue(i, AlwaysTrue{});
					SetOperation(OpAnd, i);
					changed = Changed_True;
					++i;
					break;
				}
				case OpAnd:
					if (i + 1 < end - deleted && GetOperation(i + 1) == OpOr) {
						Erase(i, i + 1);
						SetOperation(OpAnd, i);
						++deleted;
						break;
					} else {
						Erase(i + 1, end - deleted);
						Erase(begin, i);
						return {end - begin - 1, Changed_False};
					}
			}
		} else {
			i = Next(i);
		}
	}
	return {deleted, changed};
}

Changed QueryPreprocessor::removeAlwaysTrue() {
	const auto [deleted, changed] = removeAlwaysTrue(0, Size());
	return changed || Changed{deleted != 0};
}

bool QueryPreprocessor::containsJoin(size_t n) noexcept {
	return Visit(
		n, [](const JoinQueryEntry&) noexcept { return true; },
		[](const concepts::OneOf<QueryEntry, BetweenFieldsQueryEntry, AlwaysTrue, AlwaysFalse, SubQueryEntry, SubQueryFieldEntry,
								 KnnQueryEntry, MultiDistinctQueryEntry> auto&) noexcept { return false; },
		[&](const QueryEntriesBracket&) noexcept {
			for (size_t i = n, e = Next(n); i < e; ++i) {
				if (Is<JoinQueryEntry>(i)) {
					return true;
				}
			}
			return false;
		});
}

std::pair<size_t, Changed> QueryPreprocessor::removeAlwaysTrue(size_t begin, size_t end) {
	size_t deleted = 0;
	Changed changed = Changed_False;
	for (size_t i = begin, prev = begin; i < end - deleted;) {
		if (IsSubTree(i)) {
			const auto [d, ch] = removeAlwaysTrue(i + 1, Next(i));
			deleted += d;
			if (Size(i) == 1) {
				const auto inserted = SetValue(i, AlwaysTrue{});
				changed = Changed_True;
				prev = i;
				i += inserted;
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
				case OpNot: {
					const auto inserted = SetValue(i, AlwaysFalse{});
					SetOperation(OpAnd, i);
					changed = Changed_True;
					prev = i;
					i += inserted;
					break;
				}
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

void QueryPreprocessor::Reduce() {
	Changed changed = Changed_False;
	do {
		changed = removeBrackets();
		changed |= LookupQueryIndexes();
		changed |= removeAlwaysFalse();
		changed |= removeAlwaysTrue();
		changed |= SubstituteCompositeIndexes();
	} while (changed);
}

Changed QueryPreprocessor::removeBrackets() {
	if (!equalPositions.empty()) {
		return Changed_False;
	}
	return Changed{removeBrackets(0, Size()) != 0};
}

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
		if (!IsSubTree(i) || !Get<QueryEntriesBracket>(i).equalPositions.empty()) {
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

[[noreturn]] static void throwRankedWithFPCond() {
	throw Error(errNotValid, "KNN or FullText cannot be mixed with conditions for a floating-point vector field in a query");
}

QueryPreprocessor::Ranked QueryPreprocessor::GetQueryRankType() const {
	auto it = cbegin().PlainIterator();
	const auto end = cend().PlainIterator();
	Ranked result{QueryRankType::No, IndexValueType::NotSet};
	bool floatVectorDetected = false;
	for (; it != end; ++it) {
		if (it->Is<QueryEntry>()) {
			const auto& qe = it->Value<QueryEntry>();
			if (qe.IsFieldIndexed()) {
				const auto indexNo = IndexValueType(qe.IndexNo());
				if (ns_.indexes_[indexNo]->IsFulltext()) {
					if (qe.IsDistinctOnly()) {
						// distinct() condition over fulltext field may be combined with anything
						continue;
					}
					if (floatVectorDetected) {
						throwRankedWithFPCond();
					}
					switch (result.queryRankType) {
						case QueryRankType::FullText:
						case QueryRankType::Hybrid:
							throw Error(errNotValid, "More than one FullText conditions cannot be combined in a query");
						case QueryRankType::KnnIP:
						case QueryRankType::KnnCos:
						case QueryRankType::KnnL2:
							result = {QueryRankType::Hybrid, IndexValueType::NotSet};
							break;
						case QueryRankType::No:
							result = {QueryRankType::FullText, indexNo};
							break;
						case QueryRankType::NotSet:
						default:
							throw_as_assert;
					}
				} else if (ns_.indexes_[indexNo]->IsFloatVector()) {
					floatVectorDetected = true;
					switch (result.queryRankType) {
						case QueryRankType::FullText:
						case QueryRankType::Hybrid:
						case QueryRankType::KnnIP:
						case QueryRankType::KnnCos:
						case QueryRankType::KnnL2:
							throwRankedWithFPCond();
						case QueryRankType::No:
						case QueryRankType::NotSet:
							break;
						default:
							throw_as_assert;
					}
				}
			}
		} else if (it->Is<KnnQueryEntry>()) {
			if (floatVectorDetected) {
				throwRankedWithFPCond();
			}
			const auto& qe = it->Value<KnnQueryEntry>();
			const auto indexNo = IndexValueType(qe.IndexNo());
			switch (result.queryRankType) {
				case QueryRankType::Hybrid:
				case QueryRankType::KnnIP:
				case QueryRankType::KnnCos:
				case QueryRankType::KnnL2:
					throw Error(errNotValid, "More than one KNN conditions cannot be combined in a query");
				case QueryRankType::FullText:
					result = {QueryRankType::Hybrid, IndexValueType::NotSet};
					break;
				case QueryRankType::No:
					result = {ns_.indexes_[indexNo]->RankedType(), indexNo};
					break;
				case QueryRankType::NotSet:
				default:
					throw_as_assert;
			}
		}
	}
	return result;
}

const std::vector<int>* QueryPreprocessor::getCompositeIndex(int field) const noexcept {
	if (auto f = ns_.indexesToComposites_.find(field); f != ns_.indexesToComposites_.end()) {
		return &f->second;
	}
	return nullptr;
}

static void createCompositeKeyValues(std::span<const std::pair<int, VariantArray>> values, Payload& pl, VariantArray& ret,
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

static VariantArray createCompositeKeyValues(std::span<const std::pair<int, VariantArray>> values, const PayloadType& plType,
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
			qe.IndexNo() >= ns_.payloadType_.NumFields() || qe.ForcedSortOptEntry() || qe.Distinct() ||
			ns_.indexes_[qe.IndexNo()]->IsFulltext() || ns_.indexes_[qe.IndexNo()]->IsFloatVector()) {
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
			if (!res.fields.contains(qe.IndexNo())) [[unlikely]] {
				throw Error(errLogic, "Error during composite index's fields substitution (this should not happen)");
			}
			maxSetSize = std::max(maxSetSize, uint32_t(qe.Values().size()));
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
		deleteRanges.Add(std::span<const uint16_t>(res.entries.data() + 1, res.entries.size() - 1));
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
			cur, Skip<JoinQueryEntry, AlwaysFalse, AlwaysTrue, MultiDistinctQueryEntry>{},
			[](const concepts::OneOf<SubQueryEntry, SubQueryFieldEntry> auto&) { throw_as_assert; },
			[this, cur](QueryEntriesBracket& bracket) {
				for (auto& equalPositions : bracket.equalPositions) {
					for (auto& epField : equalPositions) {
						int idxNo = NotSet;
						if (ns_.tryGetIndexByNameOrJsonPath(epField, idxNo)) {
							epField.assign(ns_.indexes_[idxNo]->Name());
						}
					}
				}
				initIndexedQueries(cur + 1, Next(cur));
			},
			[this](BetweenFieldsQueryEntry& entry) {
				if (!entry.FieldsHaveBeenSet()) {
					SetQueryField(entry.LeftFieldData(), ns_);
					SetQueryField(entry.RightFieldData(), ns_);
				}
				auto throwOnFulltext = [this](const QueryField& qfield) {
					if (qfield.IsFieldIndexed() && ns_.indexes_[qfield.IndexNo()]->IsFulltext()) {
						throw Error(errQueryExec, "Can't use fulltext field '{}' in between fields condition", qfield.FieldName());
					}
				};
				throwOnFulltext(entry.LeftFieldData());
				throwOnFulltext(entry.RightFieldData());
				checkStrictMode(entry.LeftFieldData());
				checkStrictMode(entry.RightFieldData());
				checkAllowedCondition(entry.LeftFieldData(), entry.Condition());
				checkAllowedCondition(entry.RightFieldData(), entry.Condition());
			},
			[this](QueryEntry& qe) {
				if (!qe.FieldsHaveBeenSet()) {
					SetQueryField(qe.FieldData(), ns_);
				}
				checkStrictMode(qe.FieldData());
				checkAllowedCondition(qe.FieldData(), qe.Condition());
				qe.ConvertValuesToFieldType(ns_.payloadType_);
			},
			[this](KnnQueryEntry& qe) {
				if (!qe.FieldsHaveBeenSet()) {
					int idxNo = NotSet;
					if (ns_.tryGetIndexByNameOrJsonPath(qe.FieldName(), idxNo)) {
						auto& idx = *ns_.indexes_[idxNo];
						if (!idx.IsFloatVector()) {
							throw Error{errParams, "KNN allowed only for float vector index; {} is not float vector index", qe.FieldName()};
						}
						qe.SetIndexNo(idxNo, idx.Name());
					} else {
						throw Error{errParams, "KNN allowed only for float vector index; {} is not indexed field", qe.FieldName()};
					}
				}
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
		const auto foundIdx =
			it->Visit([](const concepts::OneOf<SubQueryEntry, SubQueryFieldEntry> auto&) -> FoundIndexInfo { throw_as_assert; },
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
					  [](const concepts::OneOf<JoinQueryEntry, BetweenFieldsQueryEntry, AlwaysFalse, AlwaysTrue, KnnQueryEntry,
											   MultiDistinctQueryEntry> auto&) noexcept { return FoundIndexInfo(); });
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

class [[nodiscard]] CompositeLess : less_composite_ref {
public:
	CompositeLess(const PayloadType& type, const FieldsSet& fields) noexcept : less_composite_ref(type, fields) {}

	bool operator()(const Variant& lhs, const Variant& rhs) const {
		assertrx_dbg(lhs.Type().Is<KeyValueType::Composite>());
		assertrx_dbg(rhs.Type().Is<KeyValueType::Composite>());
		return less_composite_ref::operator()(static_cast<const PayloadValue&>(lhs), static_cast<const PayloadValue&>(rhs));
	}
};

class [[nodiscard]] CompositeEqual : equal_composite_ref {
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
QueryPreprocessor::MergeResult QueryPreprocessor::mergeQueryEntriesSetSet(QueryEntry& lqe, QueryEntry& rqe, IsDistinct distinct,
																		  size_t position, const CmpArgs&... args) {
	// intersect 2 queryentries on the same index
	if (lqe.Values().empty() || rqe.Values().empty()) [[unlikely]] {
		std::ignore = SetValue(position, AlwaysFalse{});
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
		std::ignore = SetValue(position, AlwaysFalse{});
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
		if (setValues.empty()) [[unlikely]] {
			std::ignore = SetValue(position, AlwaysFalse{});
			return MergeResult::Annihilated;
		}
		lqe.SetCondAndValues(CondSet, std::move(setValues));  // NOLINT (bugprone-use-after-move)
		lqe.Distinct(distinct);
		return MergeResult::Merged;
	}
}

template <QueryPreprocessor::ValuesType vt, typename... CmpArgs>
QueryPreprocessor::MergeResult QueryPreprocessor::mergeQueryEntriesAllSetSet(NeedSwitch needSwitch, QueryEntry& allSet, QueryEntry& set,
																			 IsDistinct distinct, size_t position, const CmpArgs&... args) {
	if (allSet.Values().empty() || set.Values().empty()) [[unlikely]] {
		std::ignore = SetValue(position, AlwaysFalse{});
		return MergeResult::Annihilated;
	}
	const MergeEqualT<vt> equalTo{args...};
	const auto lvIt = allSet.Values().begin();
	const Variant& lv = *lvIt;
	for (auto it = lvIt + 1, endIt = allSet.Values().end(); it != endIt; ++it) {
		if (!equalTo(lv, *it)) {
			std::ignore = SetValue(position, AlwaysFalse{});
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
	std::ignore = SetValue(position, AlwaysFalse{});
	return MergeResult::Annihilated;
}

template <QueryPreprocessor::ValuesType vt, typename... CmpArgs>
QueryPreprocessor::MergeResult QueryPreprocessor::mergeQueryEntriesAllSetAllSet(QueryEntry& lqe, QueryEntry& rqe, IsDistinct distinct,
																				size_t position, const CmpArgs&... args) {
	if (lqe.Values().empty() || rqe.Values().empty()) [[unlikely]] {
		std::ignore = SetValue(position, AlwaysFalse{});
		return MergeResult::Annihilated;
	}
	const MergeEqualT<vt> equalTo{args...};
	const auto lvIt = lqe.Values().begin();
	const Variant& lv = *lvIt;
	for (auto it = lvIt + 1, endIt = lqe.Values().end(); it != endIt; ++it) {
		if (!equalTo(lv, *it)) {
			std::ignore = SetValue(position, AlwaysFalse{});
			return MergeResult::Annihilated;
		}
	}
	for (const Variant& rv : rqe.Values()) {
		if (!equalTo(lv, rv)) {
			std::ignore = SetValue(position, AlwaysFalse{});
			return MergeResult::Annihilated;
		}
	}
	lqe.Distinct(distinct);
	lqe.SetCondAndValues(CondEq, VariantArray{std::move(std::move(lqe).Values()[0])});	// NOLINT (bugprone-use-after-move)
	return MergeResult::Merged;
}

QueryPreprocessor::MergeResult QueryPreprocessor::mergeQueryEntriesAny(NeedSwitch needSwitch, QueryEntry& any, QueryEntry& notAny,
																	   IsDistinct distinct, size_t position) {
	if (notAny.Condition() == CondEmpty) {
		std::ignore = SetValue(position, AlwaysFalse{});
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
																			 F filter, IsDistinct distinct, size_t position,
																			 MergeOrdered mergeOrdered) {
	if (set.Values().empty()) [[unlikely]] {
		std::ignore = SetValue(position, AlwaysFalse{});
		return MergeResult::Annihilated;
	}
	{
		auto updatableValues = set.UpdatableValues();
		VariantArray& values = updatableValues;
		values.erase(std::remove_if(values.begin(), values.end(), filter), values.end());
	}
	if (set.Values().empty()) [[unlikely]] {
		std::ignore = SetValue(position, AlwaysFalse{});
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
																				QueryEntry& notSet, F filter, IsDistinct distinct,
																				size_t position, const CmpArgs&... args) {
	if (allSet.Values().empty()) [[unlikely]] {
		std::ignore = SetValue(position, AlwaysFalse{});
		return MergeResult::Annihilated;
	}
	const MergeEqualT<vt> equalTo{args...};
	const auto lvIt = allSet.Values().begin();
	const Variant& lv = *lvIt;
	for (auto it = lvIt + 1, endIt = allSet.Values().end(); it != endIt; ++it) {
		if (!equalTo(lv, *it)) {
			std::ignore = SetValue(position, AlwaysFalse{});
			return MergeResult::Annihilated;
		}
	}
	if (filter(lv)) {
		std::ignore = SetValue(position, AlwaysFalse{});
		return MergeResult::Annihilated;
	}
	QueryEntry& dst = needSwitch == NeedSwitch::Yes ? notSet : allSet;
	dst.Distinct(distinct);
	dst.SetCondAndValues(CondEq, VariantArray{std::move(std::move(allSet).Values()[0])});
	return MergeResult::Merged;
}

template <QueryPreprocessor::ValuesType vt, typename... CmpArgs>
QueryPreprocessor::MergeResult QueryPreprocessor::mergeQueryEntriesLt(QueryEntry& lqe, QueryEntry& rqe, IsDistinct distinct,
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
QueryPreprocessor::MergeResult QueryPreprocessor::mergeQueryEntriesGt(QueryEntry& lqe, QueryEntry& rqe, IsDistinct distinct,
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
		std::ignore = SetValue(position, AlwaysFalse{});
		return MergeResult::Annihilated;
	}
}

template <QueryPreprocessor::ValuesType vt, typename... CmpArgs>
QueryPreprocessor::MergeResult QueryPreprocessor::mergeQueryEntriesLeGe(NeedSwitch needSwitch, QueryEntry& le, QueryEntry& ge,
																		IsDistinct distinct, size_t position, const CmpArgs&... args) {
	const Variant& leV = le.Values()[0];
	const Variant& geV = ge.Values()[0];
	const MergeLessT<vt> less{args...};
	QueryEntry& target = needSwitch == NeedSwitch::Yes ? ge : le;
	QueryEntry& source = needSwitch == NeedSwitch::Yes ? le : ge;
	if (less(leV, geV)) {
		std::ignore = SetValue(position, AlwaysFalse{});
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
																		   IsDistinct distinct, size_t position, const CmpArgs&... args) {
	const Variant& ltV = lt.Values()[0];
	const Variant& rngL = range.Values()[0];
	const Variant& rngR = range.Values()[1];
	const MergeLessT<vt> less{args...};
	if (!less(rngL, ltV)) {
		std::ignore = SetValue(position, AlwaysFalse{});
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
																		   IsDistinct distinct, size_t position, const CmpArgs&... args) {
	const Variant& gtV = gt.Values()[0];
	const Variant& rngL = range.Values()[0];
	const Variant& rngR = range.Values()[1];
	const MergeLessT<vt> less{args...};
	if (!less(gtV, rngR)) {
		std::ignore = SetValue(position, AlwaysFalse{});
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
																		   IsDistinct distinct, size_t position, const CmpArgs&... args) {
	const Variant& leV = le.Values()[0];
	const Variant& rngL = range.Values()[0];
	const Variant& rngR = range.Values()[1];
	const MergeLessT<vt> less{args...};
	QueryEntry& target = needSwitch == NeedSwitch::Yes ? le : range;
	if (less(leV, rngL)) {
		std::ignore = SetValue(position, AlwaysFalse{});
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
																		   IsDistinct distinct, size_t position, const CmpArgs&... args) {
	const Variant& geV = ge.Values()[0];
	const Variant& rngL = range.Values()[0];
	const Variant& rngR = range.Values()[1];
	const MergeLessT<vt> less{args...};
	QueryEntry& target = needSwitch == NeedSwitch::Yes ? ge : range;
	if (less(rngR, geV)) {
		std::ignore = SetValue(position, AlwaysFalse{});
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
QueryPreprocessor::MergeResult QueryPreprocessor::mergeQueryEntriesRange(QueryEntry& lqe, QueryEntry& rqe, IsDistinct distinct,
																		 size_t position, const CmpArgs&... args) {
	const MergeLessT<vt> less{args...};
	QueryEntry& left = less(lqe.Values()[0], rqe.Values()[0]) ? rqe : lqe;
	QueryEntry& right = less(rqe.Values()[1], lqe.Values()[1]) ? rqe : lqe;
	if (less(right.Values()[1], left.Values()[0])) {
		std::ignore = SetValue(position, AlwaysFalse{});
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

QueryPreprocessor::MergeResult QueryPreprocessor::mergeQueryEntriesDWithin(QueryEntry& lqe, QueryEntry& rqe, IsDistinct distinct,
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
		std::ignore = SetValue(position, AlwaysFalse{});
		return MergeResult::Annihilated;
	}
}

template <QueryPreprocessor::ValuesType vt, typename... CmpArgs>
QueryPreprocessor::MergeResult QueryPreprocessor::mergeQueryEntries(size_t lhs, size_t rhs, MergeOrdered mergeOrdered,
																	const CmpArgs&... args) {
	auto& lqe = Get<QueryEntry>(lhs);
	auto& rqe = Get<QueryEntry>(rhs);
	const IsDistinct distinct = lqe.Distinct() || rqe.Distinct();
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
				case CondEmpty: {
					std::ignore = SetValue(lhs, AlwaysFalse{});
					return MergeResult::Annihilated;
				}
				case CondDWithin:
				case CondLike:
				case CondKnn:
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
				case CondEmpty: {
					std::ignore = SetValue(lhs, AlwaysFalse{});
					return MergeResult::Annihilated;
				}
				case CondDWithin:
				case CondLike:
				case CondKnn:
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
				case CondEmpty: {
					std::ignore = SetValue(lhs, AlwaysFalse{});
					return MergeResult::Annihilated;
				}
				case CondDWithin:
				case CondLike:
				case CondKnn:
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
				case CondEmpty: {
					std::ignore = SetValue(lhs, AlwaysFalse{});
					return MergeResult::Annihilated;
				}
				case CondDWithin:
				case CondLike:
				case CondKnn:
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
				case CondEmpty: {
					std::ignore = SetValue(lhs, AlwaysFalse{});
					return MergeResult::Annihilated;
				}
				case CondDWithin:
				case CondLike:
				case CondKnn:
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
				case CondEmpty: {
					std::ignore = SetValue(lhs, AlwaysFalse{});
					return MergeResult::Annihilated;
				}
				case CondDWithin:
				case CondLike:
				case CondKnn:
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
				case CondEmpty: {
					std::ignore = SetValue(lhs, AlwaysFalse{});
					return MergeResult::Annihilated;
				}
				case CondDWithin:
				case CondLike:
				case CondKnn:
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
				case CondKnn: {
					std::ignore = SetValue(lhs, AlwaysFalse{});
					return MergeResult::Annihilated;
				}
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
				case CondKnn:
					return MergeResult::NotMerged;
			}
			break;
		case CondLike:
		case CondKnn:
			return MergeResult::NotMerged;
	}
	return MergeResult::NotMerged;
}
template QueryPreprocessor::MergeResult QueryPreprocessor::mergeQueryEntries<QueryPreprocessor::ValuesType::Composite>(size_t, size_t,
																													   MergeOrdered,
																													   const PayloadType&,
																													   const FieldsSet&);
template QueryPreprocessor::MergeResult QueryPreprocessor::mergeQueryEntries<QueryPreprocessor::ValuesType::Scalar>(size_t, size_t,
																													MergeOrdered,
																													const CollateOpts&);

void QueryPreprocessor::AddDistinctEntries(const h_vector<Aggregator, 4>& aggregators) {
	bool wasAdded = false;
	for (auto& ag : aggregators) {
		if (ag.Type() != AggDistinct) {
			continue;
		}
		assertrx_throw(ag.Names().size() >= 1);
		if (ag.Names().size() == 1) {
			QueryEntry qe{ag.Names()[0], QueryEntry::DistinctTag{}};
			SetQueryField(qe.FieldData(), ns_);
			checkStrictMode(qe.FieldData());
			std::ignore = Append(wasAdded ? OpOr : OpAnd, std::move(qe));
		} else {
			FieldsSet fields = ag.GetFieldSet();
			std::ignore = Append<MultiDistinctQueryEntry>(wasAdded ? OpOr : OpAnd, std::move(fields));
		}
		wasAdded = true;
	}
}

std::pair<CondType, VariantArray> QueryPreprocessor::queryValuesFromOnCondition(std::string& explainStr, AggType& oAggType,
																				NamespaceImpl& rightNs, Query joinQuery,
																				JoinPreResult::CPtr joinPreresult,
																				const QueryJoinEntry& joinEntry, CondType condition,
																				int mainQueryMaxIterations, const RdxContext& rdxCtx) {
	int64_t limit = 0;
	const auto& rNsCfg = rightNs.config();
	if (rNsCfg.maxPreselectSize == 0) {
		limit = std::max<int64_t>(rNsCfg.minPreselectSize, rightNs.itemsCount() * rNsCfg.maxPreselectPart);
	} else if (fp::IsZero(rNsCfg.maxPreselectPart)) {
		limit = rNsCfg.maxPreselectSize;
	} else {
		limit =
			std::min(std::max<int64_t>(rNsCfg.minPreselectSize, rightNs.itemsCount() * rNsCfg.maxPreselectPart), rNsCfg.maxPreselectSize);
	}
	constexpr unsigned kExtraLimit = 2;
	if (limit < 0 || limit > (std::numeric_limits<unsigned>::max() - kExtraLimit)) {
		limit = std::numeric_limits<unsigned>::max() - kExtraLimit;
	}
	joinQuery.Explain(query_.NeedExplain());
	joinQuery.Limit(limit + kExtraLimit);
	joinQuery.Offset(QueryEntry::kDefaultOffset);
	joinQuery.ClearSorting();
	if (joinPreresult->sortOrder.index) {
		joinQuery.Sort(joinPreresult->sortOrder.sortingEntry.expression, *joinPreresult->sortOrder.sortingEntry.desc);
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
		case CondKnn:
			throw Error(errQueryExec, "Unsupported condition in ON statement: {}", CondTypeToStr(condition));
	}

	LocalQueryResults qr;
	SelectCtxWithJoinPreSelect ctx{joinQuery, nullptr, JoinPreResultExecuteCtx{std::move(joinPreresult), mainQueryMaxIterations},
								   floatVectorsHolder_};
	rightNs.Select(qr, ctx, rdxCtx);
	if (ctx.preSelect.Mode() == JoinPreSelectMode::InjectionRejected || qr.Count() > size_t(limit)) {
		return {CondAny, {}};
	}
	assertrx_throw(qr.aggregationResults.size() == 1);
	auto& aggRes = qr.aggregationResults[0];
	explainStr = qr.explainResults;
	switch (condition) {
		case CondEq:
		case CondSet: {
			assertrx_throw(aggRes.GetType() == AggDistinct);
			VariantArray values;
			const unsigned rowCount = aggRes.GetDistinctRowCount();
			values.reserve(rowCount);
			for (unsigned i = 0; i < rowCount; i++) {
				const auto& distValue = aggRes.GetDistinctRow(i);
				assertrx_throw(distValue.size() == 1);
				if (distValue[0].Type().Is<KeyValueType::Composite>()) {
					ConstPayload pl(aggRes.GetPayloadType(), distValue[0].operator const PayloadValue&());
					values.emplace_back(pl.GetComposite(aggRes.GetDistinctFields(), joinEntry.RightCompositeFieldsTypes()));
				} else {
					values.emplace_back(distValue[0]);
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
		case CondKnn:
		default:
			throw Error(errQueryExec, "Unsupported condition in ON statement: {}", CondTypeToStr(condition));
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
			for (auto it : values) {
				const PayloadValue& pv = it.GetItemRef().Value();
				assertrx_throw(!pv.IsFree());
				const ConstPayload pl{values.payloadType, pv};
				pl.GetByFieldsSet(joinEntry.RightFields(), buffer, joinEntry.RightFieldType(), joinEntry.RightCompositeFieldsTypes());
				for (Variant& v : buffer) {
					if (v.IsNullValue()) [[unlikely]] {
						continue;
					}
					if (keyValues.empty()) {
						keyValues.emplace_back(std::move(v));
					} else {
						const auto cmp = keyValues[0].Compare<NotComparable::Throw, kWhereCompareNullHandling>(v, collate);
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
		case CondKnn:
		default:
			throw Error(errQueryExec, "Unsupported condition in ON statement: {}", CondTypeToStr(condition));
	}
}

template <typename JS>
size_t QueryPreprocessor::briefDump(size_t from, size_t to, const std::vector<JS>& joinedSelectors, WrSerializer& ser) const {
	size_t totalQeValues = 0;
	for (auto it = from; it < to; it = Next(it)) {
		if (it != from || container_[it].operation != OpAnd) {
			ser << container_[it].operation << ' ';
		}
		container_[it].Visit([](const concepts::OneOf<SubQueryEntry, SubQueryFieldEntry> auto&) { throw_as_assert; },
							 [&](const QueryEntriesBracket& b) {
								 ser << "(";
								 briefDump(it + 1, Next(it), joinedSelectors, ser);
								 dumpEqualPositions(0, ser, b.equalPositions);
								 ser << ")";
							 },
							 [&ser, &totalQeValues](const QueryEntry& qe) {
								 ser << qe.DumpBrief() << ' ';
								 totalQeValues += qe.Values().size();
							 },
							 [&joinedSelectors, &ser](const JoinQueryEntry& jqe) { ser << jqe.Dump(joinedSelectors) << ' '; },
							 [&ser](const BetweenFieldsQueryEntry& qe) { ser << qe.Dump() << ' '; },
							 [&ser](const AlwaysFalse&) { ser << "AlwaysFalse" << ' '; },
							 [&ser](const AlwaysTrue&) { ser << "AlwaysTrue" << ' '; },
							 [&ser](const MultiDistinctQueryEntry& qe) { ser << qe.Dump() << ' '; },
							 [&ser, &totalQeValues](const KnnQueryEntry& qe) {
								 ser << qe.Dump() << ' ';
								 totalQeValues += 1;
							 });
	}
	return totalQeValues;
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
			[](const concepts::OneOf<SubQueryEntry, SubQueryFieldEntry> auto&) { throw_as_assert; },
			Skip<QueryEntry, BetweenFieldsQueryEntry, AlwaysFalse, AlwaysTrue, KnnQueryEntry, MultiDistinctQueryEntry>{},
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
					if (rNsCfg.maxPreselectSize == 0 && fp::IsZero(rNsCfg.maxPreselectPart)) {
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
				if (!joinEntries.empty() && joinEntries.front().Operation() == OpOr) {
					throw Error{errQueryExec, "OR operator in first condition or after left join"};
				}
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
				std::ignore = maxIterations.insert(maxIterations.begin() + bracketStart, count + 1, embracedMaxIterations);
				std::span<int> maxItersSpan(maxIterations.data(), maxIterations.size());
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
									explainEntry.Skipped("Skipped due to condition Eq|Set with operation Not"sv);
									continue;
								case CondAny:
								case CondRange:
								case CondAllSet:
								case CondEmpty:
								case CondLike:
								case CondDWithin:
								case CondKnn:
									throw Error(errQueryExec, "Unsupported condition in ON statement: {}", CondTypeToStr(condition));
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
							case CondKnn:
								explainEntry.Skipped("Skipped due to unsupported ON condition"sv);
								skip = true;
								break;
							case CondRange:
							case CondLt:
							case CondLe:
							case CondGt:
							case CondGe: {
								const auto& qe = joinedSelector.itemQuery_.Entries().Get<QueryEntry>(i);
								if (qe.IsFieldIndexed() && IsFullText(joinedSelector.RightNs()->indexes_[qe.IndexNo()]->Type())) {
									skip = true;
									explainEntry.Skipped("Skipped due to condition Lt|Le|Gt|Ge|Range with fulltext index"sv);
									break;
								}

								qe.FieldType().EvaluateOneOf(
									[&skip,
									 &explainEntry](concepts::OneOf<KeyValueType::String, KeyValueType::Composite, KeyValueType::Tuple,
																	KeyValueType::Uuid, KeyValueType::Null, KeyValueType::Undefined,
																	KeyValueType::FloatVector> auto) noexcept {
										skip = true;
										explainEntry.Skipped(
											"Skipped due to condition Lt|Le|Gt|Ge|Range with not indexed or not numeric field"sv);
									},
									[](concepts::OneOf<KeyValueType::Bool, KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double,
													   KeyValueType::Float> auto) noexcept {});
								break;
							}
							case CondEq:
							case CondSet:
							case CondAllSet: {
								const auto& qe = joinedSelector.itemQuery_.Entries().Get<QueryEntry>(i);
								if (qe.IsFieldIndexed() && IsFullText(joinedSelector.RightNs()->indexes_[qe.IndexNo()]->Type())) {
									skip = true;
									explainEntry.Skipped("Skipped due to condition Eq|Set|AllSet with fulltext index"sv);
									break;
								}

								qe.FieldType().EvaluateOneOf(
									[&skip, &explainEntry](KeyValueType::FloatVector) noexcept {
										skip = true;
										explainEntry.Skipped("Skipped due to condition Eq|Set|AllSet with float vector index"sv);
									},
									[](concepts::OneOf<KeyValueType::Bool, KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double,
													   KeyValueType::Float, KeyValueType::String, KeyValueType::Uuid, KeyValueType::Null,
													   KeyValueType::Composite, KeyValueType::Tuple,
													   KeyValueType::Undefined> auto) noexcept {});
								break;
							}
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
						const auto inserted =
							Emplace<QueryEntry>(cur, operation, QueryField(joinEntry.LeftFieldData()), queryCondition, std::move(values));
						explainEntry.Succeed([&](WrSerializer& ser) { return briefDump(cur, cur + inserted, js, ser); });
						maxIterations.insert(maxIterations.cbegin() + cur, inserted, embracedMaxIterations);
						initIndexedQueries(cur, cur + inserted);
						cur += inserted;
						count += inserted;
						prevIsSkipped = false;
					} else {
						explainEntry.Skipped("Skipped as cannot obtain values from right namespace"sv);
						if (operation == OpOr) {
							Erase(cur - orChainLength, cur);
							maxIterations.erase(maxIterations.cbegin() + (cur - orChainLength), maxIterations.begin() + cur);
							cur -= orChainLength;
							count -= orChainLength;
							// marking On-injections as fail for removed entries
							explainJoinOn.FailOnEntriesAsOrChain(orChainLength);
						}
						prevIsSkipped = true;
					}
				}  // end of entries processing

				if (count > 0) {
					EncloseInBracket(cur - count, cur, OpAnd);
					maxIterations.insert(maxIterations.cbegin() + (cur - count), embracedMaxIterations);

					explainJoinOn.Succeed(
						[this, cur, count, &js](WrSerializer& ser) { std::ignore = briefDump(cur - count, Next(cur - count), js, ser); });

					++cur;
					injectedCount += count + 1;
					to += count + 1;
				} else {
					explainJoinOn.Skipped("Skipped as there are no injected conditions"sv);
				}
			});
	}
	return injectedCount;
}

class [[nodiscard]] JoinOnExplainDisabled {
	JoinOnExplainDisabled() noexcept = default;
	struct [[nodiscard]] OnEntryExplain {
		OnEntryExplain() noexcept = default;

		RX_ALWAYS_INLINE void InitialCondition(const QueryJoinEntry&, const JoinedSelector&) const noexcept {}
		RX_ALWAYS_INLINE void Succeed(const std::function<size_t(WrSerializer&)>&) const noexcept {}
		RX_ALWAYS_INLINE void Skipped(std::string_view) const noexcept {}
		RX_ALWAYS_INLINE void OrChainPart(bool) const noexcept {}
		RX_ALWAYS_INLINE void ExplainSelect(std::string&&, AggType) const noexcept {}
	};

public:
	RX_ALWAYS_INLINE static JoinOnExplainDisabled AppendJoinOnExplain(OnConditionInjections&) noexcept { return {}; }

	RX_ALWAYS_INLINE void Init(const JoinQueryEntry&, const JoinedSelectors&, bool) const noexcept {}
	RX_ALWAYS_INLINE void Succeed(const std::function<void(WrSerializer&)>&) const noexcept {}
	RX_ALWAYS_INLINE void Skipped(std::string_view) const noexcept {}
	RX_ALWAYS_INLINE void ReserveOnEntries(size_t) const noexcept {}
	RX_ALWAYS_INLINE OnEntryExplain AppendOnEntryExplain() const noexcept { return {}; }

	RX_ALWAYS_INLINE void FailOnEntriesAsOrChain(size_t) const noexcept {}
};

class [[nodiscard]] JoinOnExplainEnabled {
	using time_point_t = ExplainCalc::Clock::time_point;
	struct [[nodiscard]] OnEntryExplain {
		OnEntryExplain(ConditionInjection& explainEntry) noexcept : startTime_(ExplainCalc::Clock::now()), explainEntry_(explainEntry) {}
		~OnEntryExplain() noexcept { explainEntry_.totalTime_ = ExplainCalc::Clock::now() - startTime_; }
		OnEntryExplain(const OnEntryExplain&) = delete;
		OnEntryExplain(OnEntryExplain&&) = delete;
		OnEntryExplain& operator=(const OnEntryExplain&) = delete;
		OnEntryExplain& operator=(OnEntryExplain&&) = delete;

		void InitialCondition(const QueryJoinEntry& joinEntry, const JoinedSelector& joinedSelector) {
			explainEntry_.initCond = joinEntry.DumpCondition(joinedSelector);
		}
		void Succeed(const std::function<size_t(WrSerializer&)>& setInjectedCond) {
			explainEntry_.succeed = true;
			explainEntry_.reason = "";
			WrSerializer wser;
			explainEntry_.valuesCount = setInjectedCond(wser);
			explainEntry_.newCond = std::string(trimSpaces(wser.Slice()));
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

	static JoinOnExplainEnabled AppendJoinOnExplain(OnConditionInjections& explainOnInjections) {
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
	OnEntryExplain AppendOnEntryExplain() { return {explainJoinOn_.conditions.emplace_back()}; }

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
			if (f != IndexValueType::SetByJsonPath) [[likely]] {
				assertrx_throw(f <= ns.indexes_.firstCompositePos());
				compositeFieldsTypes.emplace_back(ns.indexes_[f]->SelectKeyType());
			} else {
				// not indexed fields allowed only in ft composite indexes
				assertrx_throw(ftIdx);
				compositeFieldsTypes.emplace_back(KeyValueType::String{});
			}
		}
	}
	qField.SetIndexData(idxNo, idx.Name(), FieldsSet(idx.Fields()), idx.KeyType(), idx.SelectKeyType(), std::move(compositeFieldsTypes));
}

void QueryPreprocessor::SetQueryField(QueryField& qField, const NamespaceImpl& ns) {
	int idxNo = IndexValueType::SetByJsonPath;
	if (ns.tryGetIndexByNameOrJsonPath(qField.FieldName(), idxNo)) {
		setQueryIndex(qField, idxNo, ns);
	} else {
		qField.SetField({ns.tagsMatcher_.path2tag(qField.FieldName())});
	}
}

void QueryPreprocessor::VerifyOnStatementField(const QueryField& qField, const NamespaceImpl& ns, StrictMode strictMode) {
	if (qField.IsFieldIndexed()) {
		if (ns.indexes_[qField.IndexNo()]->IsFloatVector()) {
			throw Error(errLogic, "Float vector indexes is not allowed in ON statement: {}", qField.FieldName());
		}
		return;
	}
	if (strictMode == StrictModeIndexes) {
		throw Error(errStrictMode, "Current query strict mode allows using only indexed fields in ON statement: {}", qField.FieldName());
	} else if (strictMode == StrictModeNames) {
		if (ns.tagsMatcher_.path2tag(qField.FieldName()).empty()) {
			throw Error(errStrictMode, "Current query strict mode allows using only existing fields in ON statement: {}",
						qField.FieldName());
		}
	}
}

}  // namespace reindexer
