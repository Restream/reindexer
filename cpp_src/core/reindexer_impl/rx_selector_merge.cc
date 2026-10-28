#include <algorithm>
#include <memory>
#include <optional>
#include "core/keyvalue/variant.h"
#include "core/nsselecter/joins/iterators.h"
#include "core/nsselecter/joins/results.h"
#include "core/sorting/sortexpression.h"
#include "estl/fast_hash_set.h"
#include "rx_selector.h"

namespace reindexer {
namespace {

const VariantArray& nullIfEmpty(const VariantArray& values) {
	if (!values.empty()) {
		// NOLINTNEXTLINE(bugprone-return-const-ref-from-parameter)
		return values;
	}
	static const VariantArray nullStorage{Variant{}};
	return nullStorage;
}
auto nullIfEmpty(const VariantArray&&) = delete;

bool SameFieldsFilter(const std::shared_ptr<const FieldsFilter>& lhs, const std::shared_ptr<const FieldsFilter>& rhs) noexcept {
	const FieldsSet* lhsRegular = lhs ? lhs->TryRegularFields() : nullptr;
	const FieldsSet* rhsRegular = rhs ? rhs->TryRegularFields() : nullptr;
	if ((lhsRegular == nullptr) != (rhsRegular == nullptr) || (lhsRegular && *lhsRegular != *rhsRegular)) {
		return false;
	}
	const FieldsSet* lhsVectors = lhs ? lhs->TryVectorFields() : nullptr;
	const FieldsSet* rhsVectors = rhs ? rhs->TryVectorFields() : nullptr;
	return (lhsVectors == nullptr) == (rhsVectors == nullptr) && (!lhsVectors || *lhsVectors == *rhsVectors);
}

}  // namespace

// Root sort is executed inside every merged namespace. The concatenated rows are not reordered by it yet.
void RxSelector::copyParentSorting(JoinedQuery& dst, ConstQueryImpl parent) {
	const SortingEntries& entries = parent.GetSortingEntries();
	for (size_t i = 0; i < entries.size(); ++i) {
		const SortOrder order = entries[i].desc ? SortOrder::Desc : SortOrder::Asc;
		if (i == 0) {
			dst.Sort(entries[i].expression, order, parent.ForcedSortOrder());
		} else {
			dst.Sort(entries[i].expression, order);
		}
	}
}

// Queries merged from the same namespace can return one item more than once.
// A different select filter is a different projection, so those rows are not duplicates.
// Called after the merge sort, so the copy that sorts earlier is kept. No-op when every namespace is unique.
void RxSelector::deduplicateMergedItems(ItemRefVector& items, std::span<NamespaceImpl* const> namespaces, std::span<const size_t> rowsPerNs,
										const LocalQueryResults& result) {
	// Items from a namespace that appears once cannot duplicate, so they stay out of the set.
	assertrx_dbg(result.getNamespacesCount() >= namespaces.size());
	assertrx_dbg(rowsPerNs.size() == namespaces.size());
	bool hasRepeatedNs = false;
	h_vector<char, 4> fromRepeatedNs(namespaces.size(), 0);
	// Queries that share a namespace but not a fields filter get different groups.
	h_vector<uint16_t, 4> dedupGroup(namespaces.size());
	for (size_t i = 0; i < namespaces.size(); ++i) {
		dedupGroup[i] = static_cast<uint16_t>(i);
		for (size_t j = 0; j < i; ++j) {
			if (namespaces[i] != namespaces[j] ||
				!SameFieldsFilter(result.getFieldsFilter(static_cast<int>(i)), result.getFieldsFilter(static_cast<int>(j)))) {
				continue;
			}
			dedupGroup[i] = dedupGroup[j];
			fromRepeatedNs[i] = 1;
			fromRepeatedNs[j] = 1;
			hasRepeatedNs = true;
			break;
		}
	}
	if (!hasRepeatedNs || items.Size() < 2) {
		return;
	}

	size_t inserts = 0;
	for (size_t i = 0; i < namespaces.size() && i < rowsPerNs.size(); ++i) {
		inserts += fromRepeatedNs[i] ? rowsPerNs[i] : 0;
	}
	// Group in the high half, item id in the low half.
	fast_hash_set<uint64_t> seen;
	// A small result fits the default table.
	if (float(inserts) > seen.max_load_factor() * float(seen.bucket_count())) {
		seen.reserve(inserts);
	}
	const size_t size = items.Size();
	const auto compact = [&](auto begin, auto itemRef) {
		size_t write = 0;
		for (size_t read = 0; read < size; ++read) {
			const ItemRef& item = itemRef(begin[read]);
			assertrx_throw(item.Nsid() < namespaces.size());
			if (fromRepeatedNs[item.Nsid()] &&
				!seen.insert((uint64_t(dedupGroup[item.Nsid()]) << 32) | uint32_t(item.Id().ToNumber())).second) {
				continue;
			}
			if (write != read) {
				begin[write] = std::move(begin[read]);
			}
			++write;
		}
		return write;
	};
	const size_t kept =
		items.IsRanked()
			? compact(items.begin().Ranked(), [](const ItemRefRanked& item) noexcept -> const ItemRef& { return item.NotRanked(); })
			// NOLINTNEXTLINE(bugprone-return-const-ref-from-parameter)
			: compact(items.begin().NotRanked(), [](const ItemRef& item) noexcept -> const ItemRef& { return item; });
	if (kept != size) {
		items.Erase(items.begin() + static_cast<std::ptrdiff_t>(kept), items.end());
	}
}

std::optional<double> RxSelector::sumMergedField(const ItemRefVector& items, std::span<NamespaceImpl* const> namespaces,
												 std::string_view field) {
	struct [[nodiscard]] Resolved {
		bool byJson = false;
		int index = IndexValueType::NotSet;
		TagsPath path;
	};
	h_vector<Resolved, 4> resolved(namespaces.size());
	h_vector<char, 4> ready(namespaces.size(), 0);
	double sum = 0.0;
	bool hasSum = false;
	for (size_t row = 0; row < items.Size(); ++row) {
		const ItemRef& item = items.GetItemRef(row);
		assertrx_throw(item.Nsid() < namespaces.size());
		Resolved& slot = resolved[item.Nsid()];
		const NamespaceImpl& ns = *namespaces[item.Nsid()];
		if (!ready[item.Nsid()]) {
			int idx = IndexValueType::NotSet;
			if (ns.tryGetIndexByNameOrJsonPath(field, idx)) {
				const Index& index = *ns.indexes()[idx];
				if (index.Opts().IsSparse()) {
					assertrx_throw(index.Fields().getTagsPathsLength() == 1);
					slot.byJson = true;
					slot.path = index.Fields().getTagsPath(0);
				} else {
					slot.index = idx;
				}
			} else {
				slot.byJson = true;
				slot.path = ns.tagsMatcher().path2tag(field);
			}
			ready[item.Nsid()] = 1;
		}
		const PayloadValue& pv = item.ValueInitialized() ? item.Value() : ns.items_[item.Id()];
		ConstPayload pl(ns.payloadType(), pv);
		VariantArray values;
		if (slot.byJson) {
			pl.GetByJsonPath(slot.path, values, KeyValueType::Undefined{});
		} else {
			pl.Get(slot.index, values);
		}
		if (values.IsObjectValue()) [[unlikely]] {
			throw Error(errQueryExec, "Cannot aggregate object field");
		}
		for (const Variant& value : values) {
			sum += value.As<double>();
			hasSum = true;
		}
	}
	return hasSum ? std::optional{sum} : std::nullopt;
}

void RxSelector::adjustAggregationsAfterDedup(LocalQueryResults& result, std::span<NamespaceImpl* const> namespaces,
											  size_t itemsBeforeDedup) {
	if (itemsBeforeDedup <= result.Items().Size()) {
		return;
	}
	const size_t removed = itemsBeforeDedup - result.Items().Size();
	if (result.aggregationResults.empty()) {
		if (result.totalCount > 0) {
			assertrx_dbg(result.totalCount >= static_cast<int>(removed));
			result.totalCount -= static_cast<int>(removed);
		}
		return;
	}

	for (AggregationResult& agg : result.aggregationResults) {
		switch (agg.GetType()) {
			case AggCount:
			case AggCountCached: {
				const double value = agg.GetValueOrZero() - static_cast<double>(removed);
				assertrx_dbg(value >= 0);
				agg.UpdateValue(value);
				break;
			}
			case AggSum: {
				const auto& fields = agg.GetFields();
				if (fields.size() != 1) {
					break;
				}
				if (const std::optional<double> value = sumMergedField(result.Items(), namespaces, fields[0]); value.has_value()) {
					agg.UpdateValue(*value);
				}
				break;
			}
			case AggAvg:
			case AggFacet:
			case AggMin:
			case AggMax:
			case AggDistinct:
			case AggUnknown:
				break;
		}
	}
	if (result.totalCount > 0) {
		assertrx_dbg(result.totalCount >= static_cast<int>(removed));
		result.totalCount -= static_cast<int>(removed);
	}
}

h_vector<VariantArray, 1> RxSelector::mergedFieldValues(const NamespaceImpl& ns, int index, std::string_view column,
														const PayloadValue& pv) {
	h_vector<VariantArray, 1> values;
	ConstPayload cpl{ns.payloadType(), pv};
	if (index == IndexValueType::SetByJsonPath) {
		values.emplace_back();
		cpl.GetByJsonPath(column, ns.tagsMatcher(), values.back(), KeyValueType::Undefined{});
		return values;
	}
	const auto& idx = *ns.indexes()[index];
	if (idx.Opts().IsSparse()) {
		const FieldsSet& fields = idx.Fields();
		assertrx_throw(fields.getTagsPathsLength() > 0);
		values.emplace_back();
		cpl.GetByJsonPath(fields.getTagsPath(0), values.back(), KeyValueType::Undefined{});
		return values;
	}
	if (index >= ns.indexes().firstCompositePos()) {
		const FieldsSet& fields = idx.Fields();
		unsigned jsonPathsIndex = 0;
		values.reserve(fields.size());
		for (size_t i = 0, size = fields.size(); i < size; ++i) {
			values.emplace_back();
			const int field = fields[i];
			if (field != IndexValueType::SetByJsonPath) {
				cpl.Get(field, values.back());
			} else {
				assertrx_dbg(IsFullText(idx.Type()));
				cpl.GetByJsonPath(fields.getTagsPath(jsonPathsIndex++), values.back(), KeyValueType::Undefined{});
			}
		}
		return values;
	}
	values.emplace_back();
	cpl.Get(index, values.back());
	return values;
}

void RxSelector::markMergedForced(MergedSortColumn& key, const NamespaceImpl& ns, int index, const PayloadValue& pv,
								  const VariantArray& forcedSort) {
	// Indexed values are converted to the index type, so the string "10" matches an integer 10.
	// A composite key is converted with the index fields and compared as a whole.
	if (index >= ns.indexes().firstCompositePos()) {
		const Index& idx = *ns.indexes()[index];
		const FieldsSet& fields = idx.Fields();
		const PayloadType& payloadType{ns.payloadType()};
		ConstPayload item{payloadType, pv};
		for (size_t pos = 0; pos < forcedSort.size(); ++pos) {
			Variant value = forcedSort[pos];
			std::ignore = value.convert(idx.KeyType(), &payloadType, &fields);
			if (item.IsEQ(static_cast<const PayloadValue&>(value), fields)) {
				key.inForced = true;
				key.forcedPos = static_cast<ptrdiff_t>(pos);
				break;
			}
		}
		return;
	}
	if (key.values.size() != 1 || key.values[0].empty()) {
		return;
	}
	if (index >= 0) {
		const KeyValueType fieldType{ns.indexes()[index]->KeyType()};
		const Variant& storedValue = key.values[0][0];
		Variant itemValue = storedValue;
		// A sparse value is stored as JSON and is converted to the index type on read, same as the local forced sort.
		if (index >= ns.indexes().firstSparsePos()) {
			std::optional<Variant> converted = storedValue.tryConvert(fieldType);
			if (!converted) {
				return;
			}
			itemValue = std::move(*converted);
		}
		for (size_t pos = 0; pos < forcedSort.size(); ++pos) {
			if (itemValue == forcedSort[pos].convert(fieldType)) {
				key.inForced = true;
				key.forcedPos = static_cast<ptrdiff_t>(pos);
				break;
			}
		}
		return;
	}
	for (size_t pos = 0; pos < forcedSort.size(); ++pos) {
		if (key.values[0][0].RelaxCompare<WithString::No, NotComparable::Return, kDefaultNullsHandling>(forcedSort[pos]) ==
			ComparationResult::Eq) {
			key.inForced = true;
			key.forcedPos = static_cast<ptrdiff_t>(pos);
			break;
		}
	}
}

bool RxSelector::mergedSortLess(std::span<const MergedSortColumn> lhs, std::span<const MergedSortColumn> rhs,
								std::span<const MergedSortColumn> collateRow, bool useForced, uint16_t lhsNs, uint16_t rhsNs) {
	for (size_t col = 0; col < lhs.size(); ++col) {
		if (col == 0 && useForced) {
			if (lhs[col].inForced != rhs[col].inForced) {
				return lhs[col].desc ? !lhs[col].inForced : lhs[col].inForced;
			}
			if (lhs[col].inForced) {
				if (lhs[col].forcedPos == rhs[col].forcedPos) {
					continue;
				}
				return lhs[col].desc ? lhs[col].forcedPos > rhs[col].forcedPos : lhs[col].forcedPos < rhs[col].forcedPos;
			}
		}
		ComparationResult cmp = ComparationResult::Eq;
		if (lhs[col].isExpr) {
			if (!fp::ExactlyEqual(lhs[col].expr, rhs[col].expr)) {
				cmp = lhs[col].expr < rhs[col].expr ? ComparationResult::Lt : ComparationResult::Gt;
			}
		} else {
			const CollateOpts& opts = collateRow[col].opts ? *collateRow[col].opts : CollateOpts{};
			const auto& aFields = lhs[col].values;
			const auto& bFields = rhs[col].values;
			const size_t fieldCount = std::min(aFields.size(), bFields.size());
			for (size_t field = 0; field < fieldCount && cmp == ComparationResult::Eq; ++field) {
				const VariantArray& av = nullIfEmpty(aFields[field]);
				const VariantArray& bv = nullIfEmpty(bFields[field]);
				const size_t n = std::min(av.size(), bv.size());
				for (size_t i = 0; i < n && cmp == ComparationResult::Eq; ++i) {
					cmp = av[i].RelaxCompare<WithString::No, NotComparable::Throw, kDefaultNullsHandling>(bv[i], opts);
				}
				if (cmp == ComparationResult::Eq && av.size() != bv.size()) {
					cmp = av.size() < bv.size() ? ComparationResult::Lt : ComparationResult::Gt;
				}
			}
			if (cmp == ComparationResult::Eq && aFields.size() != bFields.size()) {
				cmp = aFields.size() < bFields.size() ? ComparationResult::Lt : ComparationResult::Gt;
			}
		}
		if (lhs[col].desc) {
			if (cmp == ComparationResult::Lt) {
				cmp = ComparationResult::Gt;
			} else if (cmp == ComparationResult::Gt) {
				cmp = ComparationResult::Lt;
			}
		}
		if (cmp == ComparationResult::Lt) {
			return true;
		}
		if (cmp == ComparationResult::Gt) {
			return false;
		}
	}
	if (lhsNs == rhsNs || lhs.empty()) {
		return false;
	}
	return lhs[0].desc ? lhsNs > rhsNs : lhsNs < rhsNs;
}

void RxSelector::sortMergedItems(ItemRefVector& items, std::span<NamespaceImpl* const> namespaces,
								 std::span<const std::vector<SortExpression>> parsedSort, const SortingEntries& entries,
								 const VariantArray& forcedSort, std::span<const joins::ItemsProcessors> joinProcs,
								 const joins::Results& joined) {
	const auto payloadOf = [](const NamespaceImpl& ns, const ItemRef& item) -> const PayloadValue& {
		return item.ValueInitialized() ? item.Value() : ns.items_[item.Id()];
	};
	const bool useForced = !forcedSort.empty() && !parsedSort.empty() && !parsedSort[0].empty() &&
						   (parsedSort[0][0].ByField() || parsedSort[0][0].ByJoinedField());
	const size_t count = items.Size();
	const size_t columns = entries.size();
	std::vector<MergedSortColumn> keys(count * columns);
	const auto rowKeys = [&](size_t row) noexcept { return std::span<const MergedSortColumn>{keys.data() + row * columns, columns}; };
	std::vector<uint16_t> nsids(count);
	for (size_t row = 0; row < count; ++row) {
		const ItemRef& item = items.GetItemRef(row);
		const size_t nsIdx = item.Nsid();
		nsids[row] = item.Nsid();
		assertrx_throw(nsIdx < namespaces.size() && nsIdx < parsedSort.size());
		const NamespaceImpl& ns = *namespaces[nsIdx];
		const auto& exprs = parsedSort[nsIdx];
		assertrx_throw(exprs.size() == entries.size());
		const PayloadValue& pv = payloadOf(ns, item);
		const RankT rank = items.IsRanked() ? items.GetItemRefRanked(row).Rank() : RankT{};
		for (size_t col = 0; col < entries.size(); ++col) {
			MergedSortColumn& key = keys[row * columns + col];
			key.desc = *entries[col].desc;
			const SortExpression& expr = exprs[col];
			if (expr.ByField()) {
				const auto& index = expr.Get<SortExprFuncs::Index>(0);
				key.values = mergedFieldValues(ns, index.index, index.column, pv);
				if (index.index != IndexValueType::SetByJsonPath) {
					key.opts = &ns.indexes()[index.index]->Opts().collateOpts_;
				}
				if (col == 0 && useForced) {
					markMergedForced(key, ns, index.index, pv, forcedSort);
				}
			} else if (expr.ByJoinedField()) {
				const auto& joinedIndex = expr.Get<SortExprFuncs::JoinedIndex>(0);
				assertrx_throw(nsIdx < joinProcs.size() && nsIdx < joined.size());
				const auto& proc = joinProcs[nsIdx][joinedIndex.nsIdx];
				const NamespaceImpl& rightNs = *proc.RightNs();
				const joins::ItemIterator jIt{&joined[nsIdx], item.Id()};
				const auto jfIt = jIt.At(joinedIndex.nsIdx);
				if (jfIt == jIt.End() || jfIt.ItemsCount() == 0) [[unlikely]] {
					throw Error(errQueryExec, "Not found value joined from ns {}", proc.RightNsName());
				}
				if (jfIt.ItemsCount() > 1) [[unlikely]] {
					throw Error(errQueryExec, "Found more than 1 value joined from ns {}", proc.RightNsName());
				}
				key.values = mergedFieldValues(rightNs, joinedIndex.index, joinedIndex.column, jfIt[0].Value());
				if (joinedIndex.index != IndexValueType::SetByJsonPath) {
					key.opts = &rightNs.indexes()[joinedIndex.index]->Opts().collateOpts_;
				}
				if (col == 0 && useForced) {
					markMergedForced(key, rightNs, joinedIndex.index, jfIt[0].Value(), forcedSort);
				}
			} else {
				key.isExpr = true;
				ConstPayload cpl{ns.payloadType(), pv};
				const joins::NamespaceResults* jr = nsIdx < joined.size() ? &joined[nsIdx] : nullptr;
				const auto procs = nsIdx < joinProcs.size() ? std::span<const joins::ItemsProcessor>{joinProcs[nsIdx]}
															: std::span<const joins::ItemsProcessor>{};
				key.expr = expr.Calculate(item.Id(), cpl, jr, procs, rank, ns.tagsMatcher(), 0);
			}
		}
	}

	std::vector<size_t> order(count);
	for (size_t i = 0; i < count; ++i) {
		order[i] = i;
	}
	const auto less = [&](size_t lhs, size_t rhs) {
		return mergedSortLess(rowKeys(lhs), rowKeys(rhs), rowKeys(0), useForced, nsids[lhs], nsids[rhs]);
	};
	// Each namespace appends one block that it has already sorted by the same entries. When every block is in order,
	// a stable merge of the blocks gives the same order as a stable sort of all rows.
	h_vector<size_t, 4> blockEnds;
	bool blocksSorted = true;
	for (size_t i = 1; i < count && blocksSorted; ++i) {
		if (nsids[i] != nsids[i - 1]) {
			blockEnds.push_back(i);
		} else if (less(i, i - 1)) {
			blocksSorted = false;
		}
	}
	if (blocksSorted) {
		blockEnds.push_back(count);
		for (size_t i = 0; i + 1 < blockEnds.size(); ++i) {
			std::inplace_merge(order.begin(), order.begin() + std::ptrdiff_t(blockEnds[i]),
							   order.begin() + std::ptrdiff_t(blockEnds[i + 1]), less);
		}
	} else {
		std::stable_sort(order.begin(), order.end(), less);
	}

	const auto reorder = [&](auto begin) {
		using Item = std::decay_t<decltype(*begin)>;
		std::vector<Item> sorted;
		sorted.reserve(count);
		for (size_t idx : order) {
			sorted.emplace_back(std::move(begin[idx]));
		}
		for (size_t i = 0; i < count; ++i) {
			begin[i] = std::move(sorted[i]);
		}
	};
	if (items.IsRanked()) {
		reorder(items.begin().Ranked());
	} else {
		reorder(items.begin().NotRanked());
	}
}

}  // namespace reindexer
