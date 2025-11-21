#include "indexordered.h"
#include "core/nsselecter/btreeindexiterator.h"
#include "core/rdxcontext.h"
#include "tools/errors.h"
#include "tools/logger.h"

namespace reindexer {

template <typename T>
Variant IndexOrdered<T>::Upsert(const Variant& key, IdType id, bool& clearCache) {
	if (key.IsNullValue()) {
		if (this->empty_ids_.Unsorted().Add(id, IdSet::Auto, this->sortedIdxCount_)) {
			this->cache_.ResetImpl();
			clearCache = true;
			this->isBuilt_ = false;
		}
		// Return invalid ref
		return Variant();
	}

	auto keyIt = this->idx_map.lower_bound(static_cast<ref_type>(key));

	if (keyIt == this->idx_map.end() || this->idx_map.key_comp()(static_cast<ref_type>(key), keyIt->first)) {
		keyIt = this->idx_map.insert(keyIt, {static_cast<key_type>(key), typename T::mapped_type()});
	} else {
		this->delMemStat(keyIt);
	}

	if (keyIt->second.Unsorted().Add(id, this->opts_.IsPK() ? IdSet::Ordered : IdSet::Auto, this->sortedIdxCount_)) {
		this->isBuilt_ = false;
		this->cache_.ResetImpl();
		clearCache = true;
	}
	this->tracker_.markUpdated(this->idx_map, keyIt);
	this->addMemStat(keyIt);

	return IndexStore<StoreIndexKeyType<T>>::Upsert(Variant{keyIt->first}, id, clearCache);
}

template <typename T>
SelectKeyResults IndexOrdered<T>::SelectKey(const VariantArray& keys, CondType condition, SortType sortId,
											const Index::SelectContext& selectCtx, const RdxContext& rdxCtx) {
	const auto indexWard(rdxCtx.BeforeIndexWork());

	if (selectCtx.opts.forceComparator || (sortId && !this->IsSupportSortedIdsBuild())) {
		return IndexStore<StoreIndexKeyType<T>>::SelectKey(keys, condition, sortId, selectCtx, rdxCtx);
	}

	// Get set of keys or single key
	if (!IsOrderedCondition(condition)) {
		if (selectCtx.opts.unbuiltSortOrders && keys.size() > 1) {
			throw Error(errLogic, "Attempt to use btree index '{}' for sort optimization with unordered multivalued condition ({})",
						this->Name(), CondTypeToStr(condition));
		}
		return IndexUnordered<T>::SelectKey(keys, condition, sortId, selectCtx, rdxCtx);
	}

	SelectKeyResult res;
	auto startIt = this->idx_map.begin();
	auto endIt = this->idx_map.end();
	assertrx_dbg(std::none_of(keys.begin(), keys.end(), [](const auto& k) { return k.IsNullValue(); }));
	const auto& key1 = *keys.begin();
	switch (condition) {
		case CondLt:
			endIt = this->idx_map.lower_bound(static_cast<ref_type>(key1));
			break;
		case CondLe:
			endIt = this->idx_map.lower_bound(static_cast<ref_type>(key1));
			if (endIt != this->idx_map.end() && !this->idx_map.key_comp()(static_cast<ref_type>(key1), endIt->first)) {
				++endIt;
			}
			break;
		case CondGt:
			startIt = this->idx_map.upper_bound(static_cast<ref_type>(key1));
			break;
		case CondGe:
			startIt = this->idx_map.find(static_cast<ref_type>(key1));
			if (startIt == this->idx_map.end()) {
				startIt = this->idx_map.upper_bound(static_cast<ref_type>(key1));
			}
			break;
		case CondRange: {
			const auto& key2 = keys[1];

			startIt = this->idx_map.find(static_cast<ref_type>(key1));
			if (startIt == this->idx_map.end()) {
				startIt = this->idx_map.upper_bound(static_cast<ref_type>(key1));
			}

			endIt = this->idx_map.lower_bound(static_cast<ref_type>(key2));
			if (endIt != this->idx_map.end() && !this->idx_map.key_comp()(static_cast<ref_type>(key2), endIt->first)) {
				++endIt;
			}

			if (endIt != this->idx_map.end() && this->idx_map.key_comp()(endIt->first, static_cast<ref_type>(key1))) {
				return SelectKeyResults(std::move(res));
			}
		} break;
		case CondAny:
		case CondEq:
		case CondSet:
		case CondAllSet:
		case CondEmpty:
		case CondLike:
		case CondDWithin:
		case CondKnn:
			throw Error(errParams, "Unknown query type {}", int(condition));
	}

	if (endIt == startIt || startIt == this->idx_map.end() || endIt == this->idx_map.begin()) {
		// Empty result
		return SelectKeyResults(std::move(res));
	}

	if (selectCtx.opts.unbuiltSortOrders) {
		IndexIterator::Ptr btreeIt(make_intrusive<BtreeIndexIterator<T>>(this->idx_map, startIt, endIt));
		res.emplace_back(std::move(btreeIt));
	} else if (sortId && this->sortId_ == sortId && !selectCtx.opts.distinct) {
		assertrx(startIt->second.Sorted(this->sortId_).size());
		IdType idFirst = startIt->second.Sorted(this->sortId_).front();

		auto backIt = endIt;
		--backIt;
		assertrx(backIt->second.Sorted(this->sortId_).size());
		IdType idLast = backIt->second.Sorted(this->sortId_).back();
		// sort by this index. Just give part of sorted ids;
		res.emplace_back(idFirst, idLast + 1);
	} else {
		int count = 0;
		auto it = startIt;

		while (count < 50 && it != endIt) {
			++it;
			++count;
		}
		// TODO: use count of items in ns to more clever select plan
		if (count < 50) {
			struct {
				T* i_map;
				SortType sortId;
				typename T::iterator startIt, endIt;
			} selectorCtx = {&this->idx_map, sortId, startIt, endIt};

			auto selector = [&selectorCtx, count](SelectKeyResult& res, size_t& idsCount) {
				idsCount = 0;
				res.reserve(count);
				for (auto it = selectorCtx.startIt; it != selectorCtx.endIt; ++it) {
					assertrx_dbg(it != selectorCtx.i_map->end());
					idsCount += it->second.Unsorted().Size();
					res.emplace_back(it->second, selectorCtx.sortId);
				}
				res.deferedExplicitSort = false;
				return false;
			};

			if (count > 1 && !selectCtx.opts.distinct && !selectCtx.opts.disableIdSetCache) {
				// Using btree node pointers instead of the real values from the filter and range instead all the conditions
				// to increase cache hits count
				VariantArray cacheKeys = {Variant{startIt == this->idx_map.end() ? int64_t(0) : int64_t(&(*startIt))},
										  Variant{endIt == this->idx_map.end() ? int64_t(0) : int64_t(&(*endIt))}};
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
void IndexOrdered<T>::MakeSortOrders(IUpdateSortedContext& ctx) {
	logFmt(LogTrace, "IndexOrdered::MakeSortOrders ({})", this->name_);
	auto& ids2Sorts = ctx.Ids2Sorts();
	size_t totalIds = 0;
	for (auto it : ids2Sorts) {
		if (it != SortIdNotExists) {
			totalIds++;
		}
	}

	this->sortId_ = ctx.GetCurSortId();
	this->sortOrders_.resize(totalIds);
	size_t idx = 0;
	auto fill = [&](const auto& keyEntry, const key_type* key) {
		for (auto id : keyEntry.Unsorted()) {
			if (id >= int(ids2Sorts.size()) || ids2Sorts[id] == SortIdNotExists) [[unlikely]] {
				logFmt(
					LogError,
					"Internal error: Index '{}' is broken. Item with key '{}' contains id={}, which is not present in allIds,totalids={}\n",
					this->name_, key ? Variant(*key).As<std::string>() : "null", id, totalIds);
				assertrx(0);
			}
			if (ids2Sorts[id] == SortIdNotFilled) {
				ids2Sorts[id] = idx;
				this->sortOrders_[idx++] = id;
			}
		}
	};
	fill(this->empty_ids_, nullptr);
	for (auto& keyIt : this->idx_map) {
		fill(keyIt.second, &keyIt.first);
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
				this->sortOrders_[idx++] = it - beg;
				if (isFirst) {
					isFirst = false;
					logFmt(LogError, "First missing item in '{}' has internal ID {}", this->Name(), IdType(it - beg));
				}
			}
		}
	}

	assertf(idx == totalIds, "Internal error: Index {} is broken. totalids={}, but indexed={}\n", this->name_, totalIds, idx);
}

template <typename T>
IndexIterator::Ptr IndexOrdered<T>::CreateIterator() const {
	return make_intrusive<BtreeIndexIterator<T>>(this->idx_map, this->empty_ids_.Unsorted().BuildBaseIdSet());
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
			return std::make_unique<IndexOrdered<payload_map<KeyEntryT, true>>>(idef, std::move(payloadType), std::move(fields), cacheCfg);
		case IndexStrHash:
		case IndexIntHash:
		case IndexInt64Hash:
		case IndexFastFT:
		case IndexFuzzyFT:
		case IndexCompositeHash:
		case IndexCompositeFastFT:
		case IndexBool:
		case IndexIntStore:
		case IndexInt64Store:
		case IndexStrStore:
		case IndexDoubleStore:
		case IndexCompositeFuzzyFT:
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
	return (idef.Opts().IsPK() || idef.Opts().IsDense())
			   ? IndexOrdered_New<Index::KeyEntryPlain>(idef, std::move(payloadType), std::move(fields), cacheCfg)
			   : IndexOrdered_New<Index::KeyEntry>(idef, std::move(payloadType), std::move(fields), cacheCfg);
}
// NOLINTEND(*cplusplus.NewDeleteLeaks)

}  // namespace reindexer
