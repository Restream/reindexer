
#include "indexordered.h"
#include "core/nsselecter/btreeindexiterator.h"
#include "core/rdxcontext.h"
#include "tools/errors.h"
#include "tools/logger.h"

namespace reindexer {

template <typename T>
Variant IndexOrdered<T>::Upsert(const Variant &key, IdType id) {
	if (this->cache_) this->cache_.reset();
	if (key.Type() == KeyValueNull) {
		this->empty_ids_.Unsorted().Add(id, IdSet::Auto, this->sortedIdxCount_);
		// Return invalid ref
		return Variant();
	}

	auto keyIt = this->idx_map.lower_bound(static_cast<ref_type>(key));

	if (keyIt == this->idx_map.end() || this->idx_map.key_comp()(static_cast<ref_type>(key), keyIt->first))
		keyIt = this->idx_map.insert(keyIt, {static_cast<typename T::key_type>(key), typename T::mapped_type()});
	else
		this->delMemStat(keyIt);

	keyIt->second.Unsorted().Add(id, this->opts_.IsPK() ? IdSet::Ordered : IdSet::Auto, this->sortedIdxCount_);
	this->tracker_.markUpdated(this->idx_map, keyIt);
	this->addMemStat(keyIt);

	if (this->KeyType() == KeyValueString && this->opts_.GetCollateMode() != CollateNone) {
		return IndexStore<typename T::key_type>::Upsert(key, id);
	}

	return Variant(keyIt->first);
}

template <typename T>
SelectKeyResults IndexOrdered<T>::SelectKey(const VariantArray &keys, CondType condition, SortType sortId, Index::SelectOpts opts,
											BaseFunctionCtx::Ptr ctx, const RdxContext &rdxCtx) {
	const auto indexWard(rdxCtx.BeforeIndexWork());
	if (opts.forceComparator) return IndexStore<typename T::key_type>::SelectKey(keys, condition, sortId, opts, ctx, rdxCtx);
	SelectKeyResult res;

	// Get set of keys or single key
	if (condition == CondSet || condition == CondEq || condition == CondAny || condition == CondEmpty || condition == CondLike)
		return IndexUnordered<T>::SelectKey(keys, condition, sortId, opts, ctx, rdxCtx);

	if (keys.size() < 1) throw Error(errParams, "For condition required at least 1 argument, but provided 0");

	auto startIt = this->idx_map.begin();
	auto endIt = this->idx_map.end();

	auto key1 = *keys.begin();

	switch (condition) {
		case CondLt:
			endIt = this->idx_map.lower_bound(static_cast<ref_type>(key1));
			break;
		case CondLe:
			endIt = this->idx_map.lower_bound(static_cast<ref_type>(key1));
			if (endIt != this->idx_map.end() && !this->idx_map.key_comp()(static_cast<ref_type>(key1), endIt->first)) endIt++;
			break;
		case CondGt:
			startIt = this->idx_map.upper_bound(static_cast<ref_type>(key1));
			break;
		case CondGe:
			startIt = this->idx_map.find(static_cast<ref_type>(key1));
			if (startIt == this->idx_map.end()) startIt = this->idx_map.upper_bound(static_cast<ref_type>(key1));
			break;
		case CondRange: {
			if (keys.size() != 2) throw Error(errParams, "For ranged query reuqired 2 arguments, but provided %d", keys.size());
			auto key2 = keys[1];

			startIt = this->idx_map.find(static_cast<ref_type>(key1));
			if (startIt == this->idx_map.end()) startIt = this->idx_map.upper_bound(static_cast<ref_type>(key1));

			endIt = this->idx_map.lower_bound(static_cast<ref_type>(key2));
			if (endIt != this->idx_map.end() && !this->idx_map.key_comp()(static_cast<ref_type>(key2), endIt->first)) endIt++;

			if (endIt != this->idx_map.end() && this->idx_map.key_comp()(endIt->first, static_cast<ref_type>(key1))) {
				return SelectKeyResults({res});
			}

		} break;
		default:
			throw Error(errParams, "Unknown query type %d", condition);
	}

	if (endIt == startIt || startIt == this->idx_map.end() || endIt == this->idx_map.begin()) {
		// Empty result
		return SelectKeyResults(res);
	}

	if (opts.unbuiltSortOrders) {
		IndexIterator::Ptr btreeIt(make_intrusive<BtreeIndexIterator<T>>(this->idx_map, startIt, endIt));
		res.push_back(SingleSelectKeyResult(btreeIt));
	} else if (sortId && this->sortId_ == sortId && !opts.distinct) {
		assert(startIt->second.Sorted(this->sortId_).size());
		IdType idFirst = startIt->second.Sorted(this->sortId_).front();

		auto backIt = endIt;
		backIt--;
		assert(backIt->second.Sorted(this->sortId_).size());
		IdType idLast = backIt->second.Sorted(this->sortId_).back();
		// sort by this index. Just give part of sorted ids;
		res.push_back(SingleSelectKeyResult(idFirst, idLast + 1));
	} else {
		int count = 0;
		auto it = startIt;

		while (count < 50 && it != endIt) {
			it++;
			count++;
		}
		if (count < 50) {
			struct {
				T *i_map;
				SortType sortId;
				typename T::iterator startIt, endIt;
			} ctx = {&this->idx_map, sortId, startIt, endIt};

			auto selector = [&ctx](SelectKeyResult &res) {
				for (auto it = ctx.startIt; it != ctx.endIt && it != ctx.i_map->end(); it++) {
					res.push_back(SingleSelectKeyResult(it->second, ctx.sortId));
				}
			};

			if (count > 1 && !opts.distinct && !opts.disableIdSetCache)
				this->tryIdsetCache(keys, condition, sortId, selector, res);
			else
				selector(res);
		} else {
			return IndexStore<typename T::key_type>::SelectKey(keys, condition, sortId, opts, ctx, rdxCtx);
		}
	}
	return SelectKeyResults(res);
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
					this->name_, Variant(keyIt.first).As<string>(), id, totalIds);
				assert(0);
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
Index *IndexOrdered<T>::Clone() {
	return new IndexOrdered<T>(*this);
}

template <typename T>
bool IndexOrdered<T>::IsOrdered() const {
	return true;
}

template <typename T>
IndexIterator::Ptr IndexOrdered<T>::CreateIterator() const {
	return make_intrusive<BtreeIndexIterator<T>>(this->idx_map);
}

template <typename KeyEntryT>
static Index *IndexOrdered_New(const IndexDef &idef, const PayloadType payloadType, const FieldsSet &fields) {
	switch (idef.Type()) {
		case IndexIntBTree:
			return new IndexOrdered<number_map<int, KeyEntryT>>(idef, payloadType, fields);
		case IndexInt64BTree:
			return new IndexOrdered<number_map<int64_t, KeyEntryT>>(idef, payloadType, fields);
		case IndexStrBTree:
			return new IndexOrdered<str_map<KeyEntryT>>(idef, payloadType, fields);
		case IndexDoubleBTree:
			return new IndexOrdered<number_map<double, KeyEntryT>>(idef, payloadType, fields);
		case IndexCompositeBTree:
			return new IndexOrdered<payload_map<KeyEntryT>>(idef, payloadType, fields);
		default:
			abort();
	}
}

Index *IndexOrdered_New(const IndexDef &idef, const PayloadType payloadType, const FieldsSet &fields) {
	return (idef.opts_.IsPK() || idef.opts_.IsDense()) ? IndexOrdered_New<Index::KeyEntryPlain>(idef, payloadType, fields)
													   : IndexOrdered_New<Index::KeyEntry>(idef, payloadType, fields);
}

}  // namespace reindexer
