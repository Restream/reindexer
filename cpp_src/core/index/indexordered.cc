
#include "indexordered.h"
#include "core/nsselecter/btreeindexiterator.h"
#include "core/rdxcontext.h"
#include "tools/errors.h"
#include "tools/logger.h"

namespace reindexer {

template <typename T>
Variant IndexOrdered<T>::Upsert(const Variant &key, IdType id, bool &clearCache) {
	if (key.Type().Is<KeyValueType::Null>()) {
		if (this->empty_ids_.Unsorted().Add(id, IdSet::Auto, this->sortedIdxCount_)) {
			this->cache_.reset();
			clearCache = true;
			this->isBuilt_ = false;
		}
		// Return invalid ref
		return Variant();
	}

	auto keyIt = this->idx_map.lower_bound(static_cast<ref_type>(key));

	if (keyIt == this->idx_map.end() || this->idx_map.key_comp()(static_cast<ref_type>(key), keyIt->first))
		keyIt = this->idx_map.insert(keyIt, {static_cast<key_type>(key), typename T::mapped_type()});
	else
		this->delMemStat(keyIt);

	if (keyIt->second.Unsorted().Add(id, this->opts_.IsPK() ? IdSet::Ordered : IdSet::Auto, this->sortedIdxCount_)) {
		this->isBuilt_ = false;
		this->cache_.reset();
		clearCache = true;
	}
	this->tracker_.markUpdated(this->idx_map, keyIt);
	this->addMemStat(keyIt);

	if (this->KeyType().template Is<KeyValueType::String>() && this->opts_.GetCollateMode() != CollateNone) {
		return IndexStore<StoreIndexKeyType<T>>::Upsert(key, id, clearCache);
	}

	return Variant(keyIt->first);
}

template <typename T>
SelectKeyResults IndexOrdered<T>::SelectKey(const VariantArray &keys, CondType condition, SortType sortId, Index::SelectOpts opts,
											const BaseFunctionCtx::Ptr &ctx, const RdxContext &rdxCtx) {
	const auto indexWard(rdxCtx.BeforeIndexWork());
	if (opts.forceComparator) return IndexStore<StoreIndexKeyType<T>>::SelectKey(keys, condition, sortId, opts, ctx, rdxCtx);

	// Get set of keys or single key
	if (!IsOrderedCondition(condition)) {
		if (opts.unbuiltSortOrders && keys.size() > 1) {
			throw Error(errLogic, "Attemt to use btree index '%s' for sort optimization with unordered multivalue condition (%s)",
						this->Name(), CondTypeToStr(condition));
		}
		return IndexUnordered<T>::SelectKey(keys, condition, sortId, opts, ctx, rdxCtx);
	}

	SelectKeyResult res;
	auto startIt = this->idx_map.begin();
	auto endIt = this->idx_map.end();
	auto key1 = *keys.begin();
	switch (condition) {
		case CondLt:
			endIt = this->idx_map.lower_bound(static_cast<ref_type>(key1));
			break;
		case CondLe:
			endIt = this->idx_map.lower_bound(static_cast<ref_type>(key1));
			if (endIt != this->idx_map.end() && !this->idx_map.key_comp()(static_cast<ref_type>(key1), endIt->first)) ++endIt;
			break;
		case CondGt:
			startIt = this->idx_map.upper_bound(static_cast<ref_type>(key1));
			break;
		case CondGe:
			startIt = this->idx_map.find(static_cast<ref_type>(key1));
			if (startIt == this->idx_map.end()) startIt = this->idx_map.upper_bound(static_cast<ref_type>(key1));
			break;
		case CondRange: {
			const auto &key2 = keys[1];

			startIt = this->idx_map.find(static_cast<ref_type>(key1));
			if (startIt == this->idx_map.end()) startIt = this->idx_map.upper_bound(static_cast<ref_type>(key1));

			endIt = this->idx_map.lower_bound(static_cast<ref_type>(key2));
			if (endIt != this->idx_map.end() && !this->idx_map.key_comp()(static_cast<ref_type>(key2), endIt->first)) ++endIt;

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
			throw Error(errParams, "Unknown query type %d", condition);
	}

	if (endIt == startIt || startIt == this->idx_map.end() || endIt == this->idx_map.begin()) {
		// Empty result
		return SelectKeyResults(std::move(res));
	}

	if (opts.unbuiltSortOrders) {
		IndexIterator::Ptr btreeIt(make_intrusive<BtreeIndexIterator<T>>(this->idx_map, startIt, endIt));
		res.emplace_back(std::move(btreeIt));
	} else if (sortId && this->sortId_ == sortId && !opts.distinct) {
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
				T *i_map;
				SortType sortId;
				typename T::iterator startIt, endIt;
			} ctx = {&this->idx_map, sortId, startIt, endIt};

			auto selector = [&ctx, count](SelectKeyResult &res, size_t &idsCount) {
				idsCount = 0;
				res.reserve(count);
				for (auto it = ctx.startIt; it != ctx.endIt; ++it) {
					assertrx_dbg(it != ctx.i_map->end());
					idsCount += it->second.Unsorted().Size();
					res.emplace_back(it->second, ctx.sortId);
				}
				res.deferedExplicitSort = false;
				return false;
			};

			if (count > 1 && !opts.distinct && !opts.disableIdSetCache) {
				// Using btree node pointers instead of the real values from the filter and range instead all of the contidions
				// to increase cache hits count
				VariantArray cacheKeys = {Variant{startIt == this->idx_map.end() ? int64_t(0) : int64_t(&(*startIt))},
										  Variant{endIt == this->idx_map.end() ? int64_t(0) : int64_t(&(*endIt))}};
				this->tryIdsetCache(cacheKeys, CondRange, sortId, std::move(selector), res);
			} else {
				size_t idsCount;
				selector(res, idsCount);
			}
		} else {
			return IndexStore<StoreIndexKeyType<T>>::SelectKey(keys, condition, sortId, opts, ctx, rdxCtx);
		}
	}
	return SelectKeyResults(std::move(res));
}

template <typename T>
void IndexOrdered<T>::MakeSortOrders(UpdateSortedContext &ctx) {
	logPrintf(LogTrace, "IndexOrdered::MakeSortOrders (%s)", this->name_);
	auto &ids2Sorts = ctx.ids2Sorts();
	size_t totalIds = 0;
	for (auto it : ids2Sorts)
		if (it != SortIdUnexists) totalIds++;

	this->sortId_ = ctx.getCurSortId();
	this->sortOrders_.resize(totalIds);
	size_t idx = 0;
	for (auto &keyIt : this->idx_map) {
		// assert (keyIt.second.size());
		for (auto id : keyIt.second.Unsorted()) {
			if (id >= int(ids2Sorts.size()) || ids2Sorts[id] == SortIdUnexists) {
				logPrintf(
					LogError,
					"Internal error: Index '%s' is broken. Item with key '%s' contains id=%d, which is not present in allIds,totalids=%d\n",
					this->name_, Variant(keyIt.first).As<std::string>(), id, totalIds);
				assertrx(0);
			}
			if (ids2Sorts[id] == SortIdUnfilled) {
				ids2Sorts[id] = idx;
				this->sortOrders_[idx++] = id;
			}
		}
	}
	// fill unexist indexs

	for (auto it = ids2Sorts.begin(); it != ids2Sorts.end(); ++it) {
		if (*it == SortIdUnfilled) {
			*it = idx;
			this->sortOrders_[idx++] = it - ids2Sorts.begin();
		}
	}

	assertf(idx == totalIds, "Internal error: Index %s is broken. totalids=%d, but indexed=%d\n", this->name_, totalIds, idx);
}

template <typename T>
IndexIterator::Ptr IndexOrdered<T>::CreateIterator() const {
	return make_intrusive<BtreeIndexIterator<T>>(this->idx_map);
}

template <typename KeyEntryT>
static std::unique_ptr<Index> IndexOrdered_New(const IndexDef &idef, PayloadType &&payloadType, FieldsSet &&fields,
											   const NamespaceCacheConfigData &cacheCfg) {
	switch (idef.Type()) {
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
			break;
	}
	std::abort();
}

// NOLINTBEGIN(*cplusplus.NewDeleteLeaks)
std::unique_ptr<Index> IndexOrdered_New(const IndexDef &idef, PayloadType &&payloadType, FieldsSet &&fields,
										const NamespaceCacheConfigData &cacheCfg) {
	return (idef.opts_.IsPK() || idef.opts_.IsDense())
			   ? IndexOrdered_New<Index::KeyEntryPlain>(idef, std::move(payloadType), std::move(fields), cacheCfg)
			   : IndexOrdered_New<Index::KeyEntry>(idef, std::move(payloadType), std::move(fields), cacheCfg);
}
// NOLINTEND(*cplusplus.NewDeleteLeaks)

}  // namespace reindexer
