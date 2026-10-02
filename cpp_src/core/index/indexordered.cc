#include "indexordered.h"
#include "core/formatters/id_type_fmt.h"
#include "core/id_type.h"
#include "core/nsselecter/btreeindexiterator.h"
#include "core/rdxcontext.h"
#include "tools/errors.h"
#include "tools/logger.h"

namespace reindexer {
namespace {

enum class [[nodiscard]] OrderedRangeStatus { Ok, Empty, Unsupported };

template <typename Map>
struct [[nodiscard]] OrderedKeyRange {
	using iterator = std::conditional_t<std::is_const_v<Map>, typename Map::const_iterator, typename Map::iterator>;
	using ref_type = typename IndexUnordered<std::remove_const_t<Map>>::ref_type;

	iterator start;
	iterator end;
};

template <typename Map>
std::pair<OrderedRangeStatus, OrderedKeyRange<Map>> resolveOrderedRange(Map& idx_map, CondType condition, const VariantArray& keys) {
	using Range = OrderedKeyRange<Map>;
	using ref_type = typename Range::ref_type;

	Range range{idx_map.begin(), idx_map.end()};
	assertrx_dbg(std::none_of(keys.begin(), keys.end(), [](const auto& k) { return k.IsNullValue(); }));

	switch (condition) {
		case CondLt:
			range.end = idx_map.lower_bound(static_cast<ref_type>(keys[0]));
			break;
		case CondLe: {
			const auto& key1 = static_cast<ref_type>(keys[0]);
			range.end = idx_map.lower_bound(key1);
			if (range.end != idx_map.end() && !idx_map.key_comp()(key1, range.end->first)) {
				++range.end;
			}
			break;
		}
		case CondGt:
			range.start = idx_map.upper_bound(static_cast<ref_type>(keys[0]));
			break;
		case CondGe: {
			const auto& key1 = static_cast<ref_type>(keys[0]);
			range.start = idx_map.find(key1);
			if (range.start == idx_map.end()) {
				range.start = idx_map.upper_bound(key1);
			}
			break;
		}
		case CondRange: {
			const auto& key1 = static_cast<ref_type>(keys[0]);
			const auto& key2 = static_cast<ref_type>(keys[1]);

			range.start = idx_map.find(key1);
			if (range.start == idx_map.end()) {
				range.start = idx_map.upper_bound(key1);
			}

			range.end = idx_map.lower_bound(key2);
			if (range.end != idx_map.end() && !idx_map.key_comp()(key2, range.end->first)) {
				++range.end;
			}

			if (range.end != idx_map.end() && idx_map.key_comp()(range.end->first, key1)) {
				return {OrderedRangeStatus::Empty, range};
			}
			break;
		}
		case CondAny:
			break;
		case CondEq:
		case CondSet:
		case CondAllSet:
		case CondEmpty:
		case CondLike:
		case CondDWithin:
		case CondKnn:
			return {OrderedRangeStatus::Unsupported, range};
		default:
			throw Error(errParams, "Unknown query type {}", int(condition));
	}

	if (range.end == range.start || range.start == idx_map.end() || range.end == idx_map.begin()) {
		return {OrderedRangeStatus::Empty, range};
	}
	return {OrderedRangeStatus::Ok, range};
}

}  // namespace

template <typename T>
Variant IndexOrdered<T>::Upsert(const Variant& key, IdType id, bool& clearCache) {
	try {
		if (key.IsNullValue()) {
			if (this->empty_ids_.Unsorted().Add(id, this->sortedIdxCount_)) {
				this->cache_.ResetImpl();
				clearCache = true;
				this->isBuilt_ = false;
			}
			// Return invalid ref
			return Variant();
		}

		const ref_type refKey = static_cast<ref_type>(key);
		auto keyIt = this->idx_map.lower_bound(refKey);
		if (keyIt == this->idx_map.end() || this->idx_map.key_comp()(refKey, keyIt->first)) {
			keyIt = this->idx_map.insert(keyIt, {static_cast<key_type>(key), typename T::mapped_type()});
		} else {
			this->delMemStat(keyIt);
		}

		if (keyIt->second.Unsorted().Add(id, this->sortedIdxCount_)) {
			this->isBuilt_ = false;
			this->cache_.ResetImpl();
			clearCache = true;
		}
		this->tracker_.markUpdated(this->idx_map, keyIt);
		this->addMemStat(keyIt);

		if constexpr (std::is_same_v<StoreIndexKeyType<T>, key_string>) {
			return (IndexStore<StoreIndexKeyType<T>>::shouldHoldOriginalValueInStrMap() && refKey != keyIt->first)
					   ? IndexStore<StoreIndexKeyType<T>>::Upsert(key, id, clearCache)
					   : IndexStore<StoreIndexKeyType<T>>::Upsert(Variant{keyIt->first}, id, clearCache);
		} else {
			return IndexStore<StoreIndexKeyType<T>>::Upsert(Variant{keyIt->first}, id, clearCache);
		}
	} catch (const DuplicatedItemIDError& dupPkErr) {
		IndexUnordered<T>::rethrowDuplicatedPKError(key, dupPkErr);
	}
}

template <typename T>
SelectKeyResults IndexOrdered<T>::SelectKey(const VariantArray& keys, CondType condition, SortType sortId,
											const Index::SelectContext& selectCtx, const RdxContext& rdxCtx) {
	const auto indexWard(rdxCtx.BeforeIndexWork());

	if (selectCtx.opts.forceComparator || (sortId && !this->IsSupportSortedIdsBuild())) {
		return IndexStore<StoreIndexKeyType<T>>::SelectKey(keys, condition, sortId, selectCtx, rdxCtx);
	}

	// Get set of keys or single key
	if (!index::IsOrderedCondition(condition)) {
		const bool isSortIndex = selectCtx.opts.unbuiltSortOrders || (sortId && this->sortId_ == sortId);
		if (condition != CondAny || !isSortIndex) {
			if (selectCtx.opts.unbuiltSortOrders && keys.size() > 1) {
				throw Error(errLogic, "Attempt to use btree index '{}' for sort optimization with unordered multivalued condition ({})",
							this->Name(), CondTypeToStr(condition));
			}
			return IndexUnordered<T>::SelectKey(keys, condition, sortId, selectCtx, rdxCtx);
		}
	}

	SelectKeyResult res;
	const auto [status, range] = resolveOrderedRange(this->idx_map, condition, keys);
	switch (status) {
		case OrderedRangeStatus::Unsupported:
			throw Error(errParams, "Unknown query type {}", int(condition));
		case OrderedRangeStatus::Empty:
			return SelectKeyResults(std::move(res));
		case OrderedRangeStatus::Ok:
			break;
	}

	if (selectCtx.opts.unbuiltSortOrders) {
		IndexIterator::Ptr btreeIt(make_intrusive<BtreeIndexIterator<T>>(range.start, range.end, selectCtx.opts.itemsCountInNamespace));
		res.emplace_back(std::move(btreeIt));
	} else if (sortId && this->sortId_ == sortId && !selectCtx.opts.distinct) {
		assertrx(range.start->second.Sorted(SortedIDsCtx{this->sortId_, this->getExternalSortedIds()}).size());
		IdType idFirst = range.start->second.Sorted(SortedIDsCtx{this->sortId_, this->getExternalSortedIds()}).front();

		auto backIt = range.end;
		--backIt;
		assertrx(backIt->second.Sorted(SortedIDsCtx{this->sortId_, this->getExternalSortedIds()}).size());
		IdType idLast = backIt->second.Sorted(SortedIDsCtx{this->sortId_, this->getExternalSortedIds()}).back();
		// sort by this index. Just give part of sorted ids;
		res.emplace_back(idFirst, idLast.Incr());
	} else {
		// TODO: use count of items in ns to more clever select plan
		const size_t kMaxIdsetsCount = selectCtx.opts.distinct ? kMaxExplicitBtreeKeyCountDistinct : kMaxExplicitBtreeKeyCount;
		size_t count = 0;
		auto it = range.start;

		while (count < kMaxIdsetsCount && it != range.end) {
			++it;
			++count;
		}

		if (it == range.end) {
			struct {
				T* i_map;
				SortType sortId;
				typename T::iterator startIt, endIt;
				const size_t count;
			} selectorCtx = {&this->idx_map, sortId, range.start, range.end, count};

			auto selector = [&selectorCtx, this](SelectKeyResult& res, size_t& idsCount) {
				idsCount = 0;
				res.reserve(selectorCtx.count);
				for (auto it = selectorCtx.startIt; it != selectorCtx.endIt; ++it) {
					assertrx_dbg(it != selectorCtx.i_map->end());
					idsCount += it->second.Unsorted().Size();
					res.emplace_back(it->second, SortedIDsCtx{selectorCtx.sortId, this->getExternalSortedIds()});
				}
				res.deferedExplicitSort = false;
				return false;
			};

			if (count > 1 && !selectCtx.opts.distinct && !selectCtx.opts.disableIdSetCache) {
				// Using btree node pointers instead of the real values from the filter and range instead all the conditions
				// to increase cache hits count
				VariantArray cacheKeys = {Variant{range.start == this->idx_map.end() ? int64_t(0) : int64_t(&(*range.start))},
										  Variant{range.end == this->idx_map.end() ? int64_t(0) : int64_t(&(*range.end))}};
				this->tryIdsetCache(cacheKeys, CondRange, sortId, std::move(selector), res);
			} else {
				size_t idsCount;
				selector(res, idsCount);
			}
		} else {
			return IndexStore<StoreIndexKeyType<T>>::SelectKey(keys, condition, sortId, selectCtx, rdxCtx);
		}
	}
	return SelectKeyResults(std::move(res));
}

template <typename T>
Index::OrderedConditionEstimate IndexOrdered<T>::EstimateOrderedCondition(CondType cond, const VariantArray& keys, size_t cap) const {
	if (!index::IsOrderedCondition(cond)) {
		return Index::OrderedConditionEstimate{.keys = cap, .ids = cap, .indexSize = this->Size(), .complete = false};
	}

	const auto [status, range] = resolveOrderedRange(this->idx_map, cond, keys);
	assertrx_dbg(status != OrderedRangeStatus::Unsupported);
	if (status != OrderedRangeStatus::Ok) {
		return Index::OrderedConditionEstimate{.keys = 0, .ids = 0, .indexSize = this->Size(), .complete = true};
	}

	size_t keysCount = 0;
	size_t idsCount = 0;
	auto it = range.start;
	while (it != range.end && keysCount < cap) {
		idsCount += it->second.Unsorted().Size();
		++it;
		++keysCount;
	}
	return Index::OrderedConditionEstimate{.keys = keysCount, .ids = idsCount, .indexSize = this->Size(), .complete = it == range.end};
}

template <typename T>
WasCanceled IndexOrdered<T>::MakeSortOrders(index::IUpdateSortedContext& ctx, const index::ICancelable& cancelable) {
	logFmt(LogTrace, "IndexOrdered::MakeSortOrders ({})", this->name_);

	RX_RETURN_IF_CANCELED(cancelable);

	auto& ids2Sorts = ctx.Ids2Sorts();
	size_t totalIds = 0;
	for (auto it : ids2Sorts) {
		if (it != SortIdNotExists) {
			totalIds++;
		}
	}

	RX_RETURN_IF_CANCELED(cancelable);

	this->sortId_ = ctx.GetCurSortId();
	this->sortOrders_.resize(totalIds);
	size_t idx = 0;
	auto fill = [&](const auto& keyEntry, const key_type* key) -> WasCanceled {
		const auto idsetRange = keyEntry.Unsorted().idset_range();
		for (auto id : idsetRange) {
			if (idx % index::kCancelCheckFrequency == 0) {
				RX_RETURN_IF_CANCELED(cancelable);
			}
			if (id >= IdType::FromNumber(ids2Sorts.size()) || ids2Sorts[id.ToNumber()] == SortIdNotExists) [[unlikely]] {
				logFmt(
					LogError,
					"Internal error: Index '{}' is broken. Item with key '{}' contains id={}, which is not present in allIds,totalids={}\n",
					this->name_, key ? Variant(*key).As<std::string>() : "null", id, totalIds);
				assertrx(0);
			}
			if (ids2Sorts[id.ToNumber()] == SortIdNotFilled) {
				ids2Sorts[id.ToNumber()] = idx;
				this->sortOrders_[idx++] = id;
			}
		}
		return WasCanceled_False;
	};
	if (fill(this->empty_ids_, nullptr) == WasCanceled_True) {
		return WasCanceled_True;
	}

	RX_RETURN_IF_CANCELED(cancelable);

	for (auto& keyIt : this->idx_map) {
		if (fill(keyIt.second, &keyIt.first) == WasCanceled_True) {
			return WasCanceled_True;
		}
	}
	if (idx != totalIds) {
		// Just in case. This sould never happen
		assertf_dbg(idx == totalIds, "Internal error: Index {} is broken. totalids={}, but indexed={}\n", this->name_, totalIds, idx);
		logFmt(LogError, "Unexpected index error: there are {} missing items in '{}'", totalIds - idx, this->Name());
		bool isFirst = true;
		// fill non-existent indexes
		for (auto it = ids2Sorts.begin(), beg = ids2Sorts.begin(), end = ids2Sorts.end(); it != end; ++it) {
			if (*it == SortIdNotFilled) {
				*it = idx;
				this->sortOrders_[idx++] = IdType::FromNumber(it - beg);
				if (isFirst) {
					isFirst = false;
					logFmt(LogError, "First missing item in '{}' has internal ID {}", this->Name(), IdType::FromNumber(it - beg));
				}
			}
		}
	}

	assertf(idx == totalIds, "Internal error: Index {} is broken. totalids={}, but indexed={}\n", this->name_, totalIds, idx);

	return WasCanceled_False;
}

template <typename T>
IndexIterator::Ptr IndexOrdered<T>::CreateIterator() const {
	return make_intrusive<BtreeIndexIterator<T>>(this->idx_map, this->empty_ids_.Unsorted());
}

template <typename KeyEntryT>
static std::unique_ptr<Index> IndexOrdered_New(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields,
											   const NamespaceCacheConfigData& cacheCfg) {
	switch (idef.IndexType()) {
		case IndexIntBTree:
			return std::make_unique<IndexOrdered<number_map<int, KeyEntryT>>>(idef, std::move(payloadType), std::move(fields), cacheCfg);
		case IndexInt64BTree:
			return std::make_unique<IndexOrdered<number_map<int64_t, KeyEntryT>>>(idef, std::move(payloadType), std::move(fields),
																				  cacheCfg);
		case IndexStrBTree:
			return std::make_unique<IndexOrdered<str_map<KeyEntryT>>>(idef, std::move(payloadType), std::move(fields), cacheCfg);
		case IndexDoubleBTree:
			return std::make_unique<IndexOrdered<number_map<double, KeyEntryT>>>(idef, std::move(payloadType), std::move(fields), cacheCfg);
		case IndexCompositeBTree:
			return std::make_unique<IndexOrdered<payload_map<KeyEntryT>>>(idef, std::move(payloadType), std::move(fields), cacheCfg);
		case IndexStrHash:
		case IndexIntHash:
		case IndexInt64Hash:
		case IndexFastFT:
		case IndexCompositeHash:
		case IndexCompositeFastFT:
		case IndexBool:
		case IndexIntStore:
		case IndexInt64Store:
		case IndexStrStore:
		case IndexDoubleStore:
		case IndexTtl:
		case IndexRTree:
		case IndexUuidHash:
		case IndexUuidStore:
		case IndexHnsw:
		case IndexVectorBruteforce:
		case IndexIvf:
		case IndexDummy:
			break;
	}
	throw_as_assert;
}

// NOLINTBEGIN(*cplusplus.NewDeleteLeaks)
std::unique_ptr<Index> IndexOrdered_New(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields,
										const NamespaceCacheConfigData& cacheCfg) {
	if (idef.Opts().IsPK()) {
		return IndexOrdered_New<Index::KeyEntryPK>(idef, std::move(payloadType), std::move(fields), cacheCfg);
	}

	return idef.Opts().IsDense() ? IndexOrdered_New<Index::KeyEntryPlain>(idef, std::move(payloadType), std::move(fields), cacheCfg)
								 : IndexOrdered_New<Index::KeyEntry>(idef, std::move(payloadType), std::move(fields), cacheCfg);
}
// NOLINTEND(*cplusplus.NewDeleteLeaks)

}  // namespace reindexer
