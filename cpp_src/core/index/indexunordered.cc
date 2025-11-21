#include "indexunordered.h"
#include "core/dbconfig.h"
#include "core/index/indextext/ftkeyentry.h"
#include "core/index/payload_map.h"
#include "core/index/string_map.h"
#include "core/indexdef.h"
#include "core/rdxcontext.h"
#include "rtree/greenesplitter.h"
#include "rtree/linearsplitter.h"
#include "rtree/quadraticsplitter.h"
#include "rtree/rstarsplitter.h"
#include "rtree/rtree.h"
#include "tools/errors.h"
#include "tools/logger.h"

namespace reindexer {

constexpr int kMaxIdsForDistinct = 500;

template <typename T>
IndexUnordered<T>::IndexUnordered(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields,
								  const NamespaceCacheConfigData& cacheCfg)
	: Base(idef, std::move(payloadType), std::move(fields)),
	  idx_map(),
	  cacheMaxSize_(cacheCfg.idxIdsetCacheSize),
	  hitsToCache_(cacheCfg.idxIdsetHitsToCache) {
	static_assert(!(is_str_map_v<T> || is_payload_map_v<T>));
}

template <>
IndexUnordered<str_map<Index::KeyEntryPlain>>::IndexUnordered(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields,
															  const NamespaceCacheConfigData& cacheCfg)
	: Base(idef, std::move(payloadType), std::move(fields)),
	  idx_map(idef.Opts().collateOpts_),
	  cacheMaxSize_(cacheCfg.idxIdsetCacheSize),
	  hitsToCache_(cacheCfg.idxIdsetHitsToCache) {}

template <>
IndexUnordered<str_map<Index::KeyEntry>>::IndexUnordered(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields,
														 const NamespaceCacheConfigData& cacheCfg)
	: Base(idef, std::move(payloadType), std::move(fields)),
	  idx_map(idef.Opts().collateOpts_),
	  cacheMaxSize_(cacheCfg.idxIdsetCacheSize),
	  hitsToCache_(cacheCfg.idxIdsetHitsToCache) {}

template <>
IndexUnordered<unordered_str_map<Index::KeyEntry>>::IndexUnordered(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields,
																   const NamespaceCacheConfigData& cacheCfg)
	: Base(idef, std::move(payloadType), std::move(fields)),
	  idx_map(idef.Opts().collateOpts_),
	  cacheMaxSize_(cacheCfg.idxIdsetCacheSize),
	  hitsToCache_(cacheCfg.idxIdsetHitsToCache) {}

template <>
IndexUnordered<unordered_str_map<Index::KeyEntryPlain>>::IndexUnordered(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields,
																		const NamespaceCacheConfigData& cacheCfg)
	: Base(idef, std::move(payloadType), std::move(fields)),
	  idx_map(idef.Opts().collateOpts_),
	  cacheMaxSize_(cacheCfg.idxIdsetCacheSize),
	  hitsToCache_(cacheCfg.idxIdsetHitsToCache) {}

template <>
IndexUnordered<unordered_str_map<FtKeyEntry>>::IndexUnordered(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields,
															  const NamespaceCacheConfigData& cacheCfg)
	: Base(idef, std::move(payloadType), std::move(fields)),
	  idx_map(idef.Opts().collateOpts_),
	  cacheMaxSize_(cacheCfg.idxIdsetCacheSize),
	  hitsToCache_(cacheCfg.idxIdsetHitsToCache) {}

template <>
IndexUnordered<unordered_payload_map<FtKeyEntry, true>>::IndexUnordered(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields,
																		const NamespaceCacheConfigData& cacheCfg)
	: Base(idef, std::move(payloadType), std::move(fields)),
	  idx_map(PayloadType{Base::GetPayloadType()}, FieldsSet{Base::Fields()}),
	  cacheMaxSize_(cacheCfg.idxIdsetCacheSize),
	  hitsToCache_(cacheCfg.idxIdsetHitsToCache) {}

template <>
IndexUnordered<unordered_payload_map<Index::KeyEntry, true>>::IndexUnordered(const IndexDef& idef, PayloadType&& payloadType,
																			 FieldsSet&& fields, const NamespaceCacheConfigData& cacheCfg)
	: Base(idef, std::move(payloadType), std::move(fields)),
	  idx_map(PayloadType{Base::GetPayloadType()}, FieldsSet{Base::Fields()}),
	  cacheMaxSize_(cacheCfg.idxIdsetCacheSize),
	  hitsToCache_(cacheCfg.idxIdsetHitsToCache) {}

template <>
IndexUnordered<unordered_payload_map<Index::KeyEntryPlain, true>>::IndexUnordered(const IndexDef& idef, PayloadType&& payloadType,
																				  FieldsSet&& fields,
																				  const NamespaceCacheConfigData& cacheCfg)
	: Base(idef, std::move(payloadType), std::move(fields)),
	  idx_map(PayloadType{Base::GetPayloadType()}, FieldsSet{Base::Fields()}),
	  cacheMaxSize_(cacheCfg.idxIdsetCacheSize),
	  hitsToCache_(cacheCfg.idxIdsetHitsToCache) {}

template <>
IndexUnordered<payload_map<Index::KeyEntry, true>>::IndexUnordered(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields,
																   const NamespaceCacheConfigData& cacheCfg)
	: Base(idef, std::move(payloadType), std::move(fields)),
	  idx_map(PayloadType{Base::GetPayloadType()}, FieldsSet{Base::Fields()}),
	  cacheMaxSize_(cacheCfg.idxIdsetCacheSize),
	  hitsToCache_(cacheCfg.idxIdsetHitsToCache) {}

template <>
IndexUnordered<payload_map<Index::KeyEntryPlain, true>>::IndexUnordered(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields,
																		const NamespaceCacheConfigData& cacheCfg)
	: Base(idef, std::move(payloadType), std::move(fields)),
	  idx_map(PayloadType{Base::GetPayloadType()}, FieldsSet{Base::Fields()}),
	  cacheMaxSize_(cacheCfg.idxIdsetCacheSize),
	  hitsToCache_(cacheCfg.idxIdsetHitsToCache) {}

template <typename T>
bool IndexUnordered<T>::HoldsStrings() const noexcept {
	if constexpr (is_payload_map_v<T>) {
		return idx_map.have_str_fields();
	} else {
		return is_str_map_v<T>;
	}
}

template <typename T>
IndexUnordered<T>::IndexUnordered(const IndexUnordered& other)
	: Base(other),
	  idx_map(other.idx_map),
	  cacheMaxSize_(other.cacheMaxSize_),
	  hitsToCache_(other.hitsToCache_),
	  empty_ids_(other.empty_ids_),
	  tracker_(other.tracker_) {
	cache_.CopyInternalPerfStatsFrom(other.cache_);
}

template <typename key_type>
size_t heap_size(const key_type& /*kt*/) {
	return 0;
}

template <>
size_t heap_size<key_string>(const key_string& kt) {
	return kt.heap_size();
}

template <>
size_t heap_size<key_string_with_hash>(const key_string_with_hash& kt) {
	return kt.heap_size();
}

struct [[nodiscard]] DeepClean {
	template <typename T>
	void operator()(T& v) const {
		free_node(v.first);
		free_node(v.second);
	}

	template <typename T>
	void free_node(T& v) const {
		if constexpr (!std::is_const_v<T>) {
			v = T{};
		}
	}
};

template <typename T>
void IndexUnordered<T>::addMemStat(typename T::iterator it) {
	this->memStat_.idsetPlainSize += sizeof(typename T::value_type) + it->second.Unsorted().heap_size();
	this->memStat_.idsetBTreeSize += it->second.Unsorted().BTreeSize();
	this->memStat_.dataSize += heap_size(it->first);
}

template <typename T>
void IndexUnordered<T>::delMemStat(typename T::iterator it) {
	this->memStat_.idsetPlainSize -= sizeof(typename T::value_type) + it->second.Unsorted().heap_size();
	this->memStat_.idsetBTreeSize -= it->second.Unsorted().BTreeSize();
	this->memStat_.dataSize -= heap_size(it->first);
}

template <typename T>
Variant IndexUnordered<T>::Upsert(const Variant& key, IdType id, bool& clearCache) {
	// reset cache
	if (key.IsNullValue()) {
		assertrx_dbg(this->Opts().IsSparse() || this->Opts().IsArray());
		if (this->empty_ids_.Unsorted().Add(id, IdSet::Auto, this->sortedIdxCount_)) {
			cache_.ResetImpl();
			clearCache = true;
			this->isBuilt_ = false;
		}
		// Return invalid ref
		return Variant();
	}

	typename T::iterator keyIt = this->idx_map.find(static_cast<ref_type>(key));
	if (keyIt == this->idx_map.end()) {
		keyIt = this->idx_map.insert({static_cast<key_type>(key), typename T::mapped_type()}).first;
	} else {
		delMemStat(keyIt);
		if (this->opts_.IsPK()) {
			WrSerializer wrser;
			wrser << "Can't insert item in PK index, duplicate key - ";
			key.Dump(wrser, this->GetPayloadType(), this->Fields(), CheckIsStringPrintable::No);
			throw Error(errLogic, wrser.Slice());
		}
	}

	if (keyIt->second.Unsorted().Add(id, this->opts_.IsPK() ? IdSet::Ordered : IdSet::Auto, this->sortedIdxCount_)) {
		cache_.ResetImpl();
		clearCache = true;
		this->isBuilt_ = false;
	}
	this->tracker_.markUpdated(this->idx_map, keyIt);

	addMemStat(keyIt);

	return this->IsFulltext() ? Variant{keyIt->first} : IndexStore<StoreIndexKeyType<T>>::Upsert(Variant{keyIt->first}, id, clearCache);
}

template <typename T>
void IndexUnordered<T>::Delete(const Variant& key, IdType id, MustExist mustExist, StringsHolder& strHolder, bool& clearCache) {
	if (key.IsNullValue()) {
		std::ignore = this->empty_ids_.Unsorted().Erase(id);
		this->isBuilt_ = false;
		cache_.ResetImpl();
		clearCache = true;
		return;
	}

	typename T::iterator keyIt = this->idx_map.find(static_cast<ref_type>(key));
	[[maybe_unused]] int delcnt = 0;
	if (keyIt != idx_map.end()) {
		delMemStat(keyIt);
		delcnt = keyIt->second.Unsorted().Erase(id);
		this->isBuilt_ = false;
		cache_.ResetImpl();
		clearCache = true;
	}
	assertf(!mustExist || delcnt || this->opts_.IsArray() || this->Opts().IsSparse(),
			"Delete non-existing id from index '{}' id={}, key={} ({})", this->name_, id,
			key.As<std::string>(this->payloadType_, this->Fields()),
			Variant(keyIt->first).As<std::string>(this->payloadType_, this->Fields()));
	if (keyIt == idx_map.end()) {
		return;
	}

	if (keyIt->second.Unsorted().IsEmpty()) {
		this->tracker_.markDeleted(keyIt);
		if constexpr (is_str_map_v<T>) {
			idx_map.template erase<StringMapEntryCleaner<true>>(
				keyIt, {strHolder, this->KeyType().template Is<KeyValueType::String>() && this->opts_.GetCollateMode() == CollateNone});
		} else if constexpr (is_payload_map_v<T>) {
			idx_map.template erase<DeepClean>(keyIt, strHolder);
		} else {
			idx_map.template erase<DeepClean>(keyIt);
		}
	} else {
		addMemStat(keyIt);
		this->tracker_.markUpdated(this->idx_map, keyIt);
	}

	if (!this->IsFulltext()) {
		IndexStore<StoreIndexKeyType<T>>::Delete(key, id, mustExist, strHolder, clearCache);
	}
}

// WARNING: 'keys' is a key for LRUCache and in some cases (for ordered indexes, for example) can contain values,
// which are not correspond to the initial values from queries conditions
template <typename T>
bool IndexUnordered<T>::tryIdsetCache(const VariantArray& keys, CondType condition, SortType sortId,
									  const std::function<bool(SelectKeyResult&, size_t&)>& selector, SelectKeyResult& res) {
	size_t idsCount;
	if (!cache_.IsActive() || IsComposite(this->Type())) {
		selector(res, idsCount);
		return false;
	}

	bool scanWin = false;
	IdSetCacheKey ckey{keys, condition, sortId};
	auto cached = cache_.Get(ckey);
	if (cached.valid) {
		if (!cached.val.IsInitialized()) {
			scanWin = selector(res, idsCount);
			if (!scanWin) {
				// Do not use generic sort, when expecting duplicates in the id sets
				const bool useGenericSort =
					res.deferedExplicitSort && !(this->opts_.IsArray() && (condition == CondEq || condition == CondSet));
				cache_.Put(ckey,
						   res.MergeIdsets(SelectKeyResult::MergeOptions{.genericSort = useGenericSort, .shrinkResult = true}, idsCount));
			}
		} else {
			res.emplace_back(std::move(cached.val.ids));
		}
	} else {
		scanWin = selector(res, idsCount);
	}
	return scanWin;
}

template <typename T>
SelectKeyResults IndexUnordered<T>::SelectKey(const VariantArray& keys, CondType condition, SortType sortId,
											  const Index::SelectContext& selectCtx, const RdxContext& rdxCtx) {
	const auto indexWard(rdxCtx.BeforeIndexWork());

	if (selectCtx.opts.forceComparator || (sortId && !this->IsSupportSortedIdsBuild())) {
		return Base::SelectKey(keys, condition, sortId, selectCtx, rdxCtx);
	}

	SelectKeyResult res;

	switch (condition) {
		case CondEmpty:
			if (!this->opts_.IsArray() && !this->opts_.IsSparse()) {
				throw Error(errParams, "The 'is NULL' condition is supported only by 'sparse' or 'array' indexes");
			}
			res.emplace_back(this->empty_ids_, sortId);
			break;
		// Get set of keys or single key
		case CondEq:
		case CondSet: {
			assertrx_throw(std::none_of(keys.cbegin(), keys.cend(), [](const auto& key) noexcept { return key.IsNullValue(); }));

			struct {
				T* i_map;
				const VariantArray& keys;
				SortType sortId;
				Index::SelectOpts opts;
				IsSparse isSparse;
			} ctx = {&this->idx_map, keys, sortId, selectCtx.opts, this->opts_.IsSparse()};
			bool selectorWasSkipped = false;
			// should return true, if fallback to comparator required
			auto selector = [&ctx, &selectorWasSkipped](SelectKeyResult& res, size_t& idsCount) -> bool {
				idsCount = 0;
				// Skip this index if there are some other indexes with potentially higher selectivity
				if (!ctx.opts.distinct && ctx.keys.size() > 1 && 8 * ctx.keys.size() > size_t(ctx.opts.maxIterations) &&
					ctx.opts.itemsCountInNamespace) {
					selectorWasSkipped = true;
					return true;
				}
				std::optional<fast_hash_set<uintptr_t>> dedupSet;
				if (ctx.opts.distinct && ctx.keys.size() > 1) {
					dedupSet.emplace(ctx.keys.size());
				}
				res.reserve(ctx.keys.size());
				for (const auto& key : ctx.keys) {
					auto keyIt = ctx.i_map->find(static_cast<ref_type>(key));
					if (keyIt != ctx.i_map->end()) {
						if (dedupSet.has_value()) {
							auto res = dedupSet->emplace(uintptr_t(&(keyIt->second)));
							if (!res.second) [[unlikely]] {
								continue;
							}
						}
						res.emplace_back(keyIt->second, ctx.sortId);
						idsCount += keyIt->second.Unsorted().Size();
					}
				}

				res.deferedExplicitSort = SelectKeyResult::IsGenericSortRecommended(res.size(), idsCount, idsCount);

				// avoid comparator for sparse index
				if (ctx.isSparse || !ctx.opts.itemsCountInNamespace) {
					return false;
				}
				// Check selectivity:
				// if ids count too much (more than maxSelectivityPercentForIdset() of namespace),
				// and index not optimized, or we have >4 other conditions
				return res.size() > 1u && ((2 * idsCount > size_t(ctx.opts.maxIterations)) ||
										   (100u * idsCount / ctx.opts.itemsCountInNamespace > maxSelectivityPercentForIdset()));
			};

			bool scanWin = false;
			// Get from cache
			if (!selectCtx.opts.distinct && !selectCtx.opts.disableIdSetCache && keys.size() > 1) {
				scanWin = tryIdsetCache(keys, condition, sortId, std::move(selector), res);
			} else {
				size_t idsCount;
				scanWin = selector(res, idsCount);
			}

			if ((scanWin || selectorWasSkipped) && !selectCtx.opts.distinct) {
				// fallback to comparator, due to expensive idset
				return Base::SelectKey(keys, condition, sortId, selectCtx, rdxCtx);
			}
		} break;
		case CondAllSet: {
			// Get set of key, where all request keys are present
			SelectKeyResults rslts;
			for (const auto& key : keys) {
				assertrx_dbg(!key.IsNullValue());
				SelectKeyResult res1;
				auto keyIt = this->idx_map.find(static_cast<ref_type>(key.convert(this->KeyType())));
				if (keyIt == this->idx_map.end()) {
					rslts.Clear();
					rslts.EmplaceBack(std::move(res1));
					return rslts;
				}
				res1.emplace_back(keyIt->second, sortId);
				rslts.EmplaceBack(std::move(res1));
			}
			return rslts;
		}

		case CondAny:
			if (selectCtx.opts.distinct && this->idx_map.size() < kMaxIdsForDistinct) {	 // TODO change to more clever condition
				// Get set of any keys
				res.reserve(this->idx_map.size());
				for (auto& keyIt : this->idx_map) {
					res.emplace_back(keyIt.second, sortId);
				}
				break;
			}  // else fallthrough
		case CondGe:
		case CondLe:
		case CondRange:
		case CondGt:
		case CondLt:
		case CondLike:
			return Base::SelectKey(keys, condition, sortId, selectCtx, rdxCtx);
		case CondDWithin:
		case CondKnn:
			throw Error(errQueryExec, "{} query on index '{}'", CondTypeToStrShort(condition), this->name_);
	}

	return SelectKeyResults(std::move(res));
}

template <typename T>
void IndexUnordered<T>::Commit() {
	this->empty_ids_.Unsorted().Commit();

	if (!cache_.IsActive()) {
		cache_.Reinitialize(cacheMaxSize_, hitsToCache_);
	}

	if (!tracker_.isUpdated()) {
		return;
	}

	logFmt(LogTrace, "IndexUnordered::Commit ({}) {} uniq keys, {} empty, {}", this->name_, this->idx_map.size(),
		   this->empty_ids_.Unsorted().size(), tracker_.isCompleteUpdated() ? "complete" : "partial");

	if (tracker_.isCompleteUpdated()) {
		for (auto& keyIt : this->idx_map) {
			keyIt.second.Unsorted().Commit();
			assertrx(keyIt.second.Unsorted().size());
		}
	} else {
		tracker_.commitUpdated(idx_map);
	}
	tracker_.clear();
}

template <typename T>
void IndexUnordered<T>::UpdateSortedIds(const IUpdateSortedContext& ctx) {
	assertrx_dbg(IsSupportSortedIdsBuild());

	logFmt(LogTrace, "IndexUnordered::UpdateSortedIds ({}) {} uniq keys, {} empty", this->name_, this->idx_map.size(),
		   this->empty_ids_.Unsorted().size());
	// For all keys in index
	for (auto& keyIt : this->idx_map) {
		keyIt.second.UpdateSortedIds(ctx);
	}

	this->empty_ids_.UpdateSortedIds(ctx);
}

template <typename T>
void IndexUnordered<T>::SetSortedIdxCount(int sortedIdxCount) {
	if (this->sortedIdxCount_ != sortedIdxCount) {
		this->sortedIdxCount_ = sortedIdxCount;
		for (auto& keyIt : idx_map) {
			keyIt.second.Unsorted().ReserveForSorted(sortedIdxCount);
		}
		empty_ids_.Unsorted().ReserveForSorted(sortedIdxCount);
	}
}

template <typename T>
IndexPerfStat IndexUnordered<T>::GetIndexPerfStat() {
	auto stats = Base::GetIndexPerfStat();
	stats.cache = cache_.GetPerfStat();
	return stats;
}

template <typename T>
void IndexUnordered<T>::ResetIndexPerfStat() {
	Base::ResetIndexPerfStat();
	cache_.ResetPerfStat();
}

template <typename T>
IndexMemStat IndexUnordered<T>::GetMemStat(const RdxContext& ctx) {
	IndexMemStat ret = Base::GetMemStat(ctx);
	ret.uniqKeysCount = idx_map.size() + int(!empty_ids_.Unsorted().Empty());
	ret.idsetCache = cache_.GetMemStat();
	ret.trackedUpdatesCount = tracker_.updatesSize();
	ret.trackedUpdatesBuckets = tracker_.updatesBuckets();
	ret.trackedUpdatesSize = tracker_.allocated();
	ret.trackedUpdatesOverflow = tracker_.overflow();
	return ret;
}

template <typename T>
template <typename S>
void IndexUnordered<T>::dump(S& os, std::string_view step, std::string_view offset) const {
	std::string newOffset{offset};
	newOffset += step;
	os << "{\n" << newOffset << "<IndexStore>: ";
	Base::Dump(os, step, newOffset);
	os << ",\n" << newOffset << "idx_map: {";
	if (!idx_map.empty()) {
		std::string secondOffset{newOffset};
		secondOffset += step;
		for (auto b = idx_map.begin(), it = b, e = idx_map.end(); it != e; ++it) {
			if (it != b) {
				os << ',';
			}
			os << '\n' << secondOffset << '{' << it->first << ": ";
			it->second.Dump(os, step, secondOffset);
			os << '}';
		}
		os << '\n' << newOffset;
	}
	os << "},\n" << newOffset << "cache: ";
	cache_.Dump(os, step, newOffset);
	os << ",\n" << newOffset << "empty_ids: ";
	empty_ids_.Dump(os, step, newOffset);
	os << "\n" << offset << '}';
}

template <typename T>
void IndexUnordered<T>::AddDestroyTask(tsl::detail_sparse_hash::ThreadTaskQueue& q) {
	if constexpr (Base::template HasAddTask<decltype(idx_map)>::value) {
		idx_map.add_destroy_task(&q);
	}
	(void)q;
}

template <typename T>
void IndexUnordered<T>::ReconfigureCache(const NamespaceCacheConfigData& cacheCfg) {
	if (cacheMaxSize_ != cacheCfg.idxIdsetCacheSize || hitsToCache_ != cacheCfg.idxIdsetHitsToCache) {
		cacheMaxSize_ = cacheCfg.idxIdsetCacheSize;
		hitsToCache_ = cacheCfg.idxIdsetHitsToCache;
		if (cache_.IsActive()) {
			cache_.Reinitialize(cacheMaxSize_, hitsToCache_);
		}
	}
}

template <typename KeyEntryT>
static std::unique_ptr<Index> IndexUnordered_New(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields,
												 const NamespaceCacheConfigData& cacheCfg) {
	switch (idef.IndexType()) {
		case IndexIntHash:
			return std::make_unique<IndexUnordered<unordered_number_map<int, KeyEntryT>>>(idef, std::move(payloadType), std::move(fields),
																						  cacheCfg);
		case IndexInt64Hash:
			return std::make_unique<IndexUnordered<unordered_number_map<int64_t, KeyEntryT>>>(idef, std::move(payloadType),
																							  std::move(fields), cacheCfg);
		case IndexStrHash:
			return std::make_unique<IndexUnordered<unordered_str_map<KeyEntryT>>>(idef, std::move(payloadType), std::move(fields),
																				  cacheCfg);
		case IndexCompositeHash:
			return std::make_unique<IndexUnordered<unordered_payload_map<KeyEntryT, true>>>(idef, std::move(payloadType), std::move(fields),
																							cacheCfg);
		case IndexStrBTree:
		case IndexIntBTree:
		case IndexInt64BTree:
		case IndexDoubleBTree:
		case IndexFastFT:
		case IndexFuzzyFT:
		case IndexCompositeBTree:
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
std::unique_ptr<Index> IndexUnordered_New(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields,
										  const NamespaceCacheConfigData& cacheCfg) {
	return (idef.Opts().IsPK() || idef.Opts().IsDense())
			   ? IndexUnordered_New<Index::KeyEntryPlain>(idef, std::move(payloadType), std::move(fields), cacheCfg)
			   : IndexUnordered_New<Index::KeyEntry>(idef, std::move(payloadType), std::move(fields), cacheCfg);
}
// NOLINTEND(*cplusplus.NewDeleteLeaks)

template class IndexUnordered<number_map<int, Index::KeyEntryPlain>>;
template class IndexUnordered<number_map<int64_t, Index::KeyEntryPlain>>;
template class IndexUnordered<number_map<double, Index::KeyEntryPlain>>;
template class IndexUnordered<str_map<Index::KeyEntryPlain>>;
template class IndexUnordered<payload_map<Index::KeyEntryPlain, true>>;
template class IndexUnordered<number_map<int, Index::KeyEntry>>;
template class IndexUnordered<number_map<int64_t, Index::KeyEntry>>;
template class IndexUnordered<number_map<double, Index::KeyEntry>>;
template class IndexUnordered<str_map<Index::KeyEntry>>;
template class IndexUnordered<payload_map<Index::KeyEntry, true>>;
template class IndexUnordered<unordered_str_map<FtKeyEntry>>;
template class IndexUnordered<unordered_payload_map<FtKeyEntry, true>>;
template class IndexUnordered<GeometryMap<Index::KeyEntry, LinearSplitter, 32, 4>>;
template class IndexUnordered<GeometryMap<Index::KeyEntryPlain, LinearSplitter, 32, 4>>;
template class IndexUnordered<GeometryMap<Index::KeyEntry, QuadraticSplitter, 32, 4>>;
template class IndexUnordered<GeometryMap<Index::KeyEntryPlain, QuadraticSplitter, 32, 4>>;
template class IndexUnordered<GeometryMap<Index::KeyEntry, GreeneSplitter, 16, 4>>;
template class IndexUnordered<GeometryMap<Index::KeyEntryPlain, GreeneSplitter, 16, 4>>;
template class IndexUnordered<GeometryMap<Index::KeyEntry, RStarSplitter, 32, 4>>;
template class IndexUnordered<GeometryMap<Index::KeyEntryPlain, RStarSplitter, 32, 4>>;
template class IndexUnordered<unordered_uuid_map<Index::KeyEntryPlain>>;

}  // namespace reindexer
