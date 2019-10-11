#include "indexunordered.h"
#include "core/index/indextext/ftkeyentry.h"
#include "core/index/payload_map.h"
#include "core/index/string_map.h"
#include "core/indexdef.h"
#include "core/rdxcontext.h"
#include "tools/errors.h"
#include "tools/logger.h"

namespace reindexer {

template <typename T>
IndexUnordered<T>::IndexUnordered(const IndexDef &idef, const PayloadType payloadType, const FieldsSet &fields)
	: IndexStore<typename T::key_type>(idef, payloadType, fields), idx_map(payloadType, fields, idef.opts_.collateOpts_) {}

template <typename T>
IndexUnordered<T>::IndexUnordered(const IndexUnordered &other)
	: IndexStore<typename T::key_type>(other),
	  idx_map(other.idx_map),
	  cache_(nullptr),
	  empty_ids_(other.empty_ids_),
	  tracker_(other.tracker_) {}

template <typename key_type>
size_t heap_size(const key_type & /*kt*/) {
	return 0;
}

template <>
size_t heap_size<key_string>(const key_string &kt) {
	return kt->heap_size() + sizeof(*kt.get());
}

template <typename T, typename std::enable_if<std::is_const<T>::value>::type * = nullptr>
void free_node(T &) {}

template <typename T, typename std::enable_if<!std::is_const<T>::value>::type * = nullptr>
void free_node(T &v) {
	v = T();
}

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
Variant IndexUnordered<T>::Upsert(const Variant &key, IdType id) {
	// reset cache
	if (cache_) cache_.reset();
	if (key.Type() == KeyValueNull) {
		this->empty_ids_.Unsorted().Add(id, IdSet::Auto, this->sortedIdxCount_);
		// Return invalid ref
		return Variant();
	}

	typename T::iterator keyIt = this->idx_map.find(static_cast<ref_type>(key));
	if (keyIt == this->idx_map.end()) {
		keyIt = this->idx_map.insert({static_cast<typename T::key_type>(key), typename T::mapped_type()}).first;
	} else {
		delMemStat(keyIt);
	}

	keyIt->second.Unsorted().Add(id, this->opts_.IsPK() ? IdSet::Ordered : IdSet::Auto, this->sortedIdxCount_);
	this->tracker_.markUpdated(this->idx_map, keyIt);

	addMemStat(keyIt);

	if (this->KeyType() == KeyValueString && this->opts_.GetCollateMode() != CollateNone) {
		return IndexStore<typename T::key_type>::Upsert(key, id);
	}

	return Variant(keyIt->first);
}

template <typename T>
void IndexUnordered<T>::Delete(const Variant &key, IdType id) {
	if (cache_) cache_.reset();
	int delcnt = 0;
	if (key.Type() == KeyValueNull) {
		delcnt = this->empty_ids_.Unsorted().Erase(id);
		assert(delcnt);
		return;
	}

	typename T::iterator keyIt = this->idx_map.find(static_cast<ref_type>(key));
	if (keyIt == idx_map.end()) return;

	delMemStat(keyIt);
	delcnt = keyIt->second.Unsorted().Erase(id);
	(void)delcnt;
	// TODO: we have to implement removal of composite indexes (doesn't work right now)
	assertf(this->opts_.IsArray() || this->Opts().IsSparse() || delcnt, "Delete unexists id from index '%s' id=%d,key=%s", this->name_, id,
			key.As<string>());

	if (keyIt->second.Unsorted().IsEmpty()) {
		this->tracker_.markDeleted(keyIt);
		free_node(keyIt->first);
		free_node(keyIt->second);
		idx_map.erase(keyIt);
	} else {
		addMemStat(keyIt);
		this->tracker_.markUpdated(this->idx_map, keyIt);
	}

	if (this->KeyType() == KeyValueString && this->opts_.GetCollateMode() != CollateNone) {
		IndexStore<typename T::key_type>::Delete(key, id);
	}
}

template <typename T>
void IndexUnordered<T>::tryIdsetCache(const VariantArray &keys, CondType condition, SortType sortId,
									  std::function<void(SelectKeyResult &)> selector, SelectKeyResult &res) {
	if (!cache_ || isComposite(this->Type())) {
		selector(res);
		return;
	}

	IdSetCacheKey ckey{keys, condition, sortId};
	auto cached = cache_->Get(ckey);
	if (cached.valid) {
		if (!cached.val.ids) {
			selector(res);
			cache_->Put(ckey, res.mergeIdsets());
		} else {
			res.push_back(SingleSelectKeyResult(cached.val.ids));
		}
	} else {
		selector(res);
	}
}

template <typename T>
SelectKeyResults IndexUnordered<T>::SelectKey(const VariantArray &keys, CondType condition, SortType sortId, Index::SelectOpts opts,
											  BaseFunctionCtx::Ptr ctx, const RdxContext &rdxCtx) {
	const auto indexWard(rdxCtx.BeforeIndexWork());
	if (opts.forceComparator) return IndexStore<typename T::key_type>::SelectKey(keys, condition, sortId, opts, ctx, rdxCtx);

	SelectKeyResult res;

	switch (condition) {
		case CondEmpty:
			if (!this->opts_.IsArray() && !this->opts_.IsSparse())
				throw Error(errParams, "The 'is NULL' condition is suported only by 'sparse' or 'array' indexes");
			res.push_back(SingleSelectKeyResult(this->empty_ids_, sortId));
			break;
		// Get set of keys or single key
		case CondEq:
		case CondSet:
			if (condition == CondEq && keys.size() < 1)
				throw Error(errParams, "For condition required at least 1 argument, but provided 0");
			{
				struct {
					T *i_map;
					const VariantArray &keys;
					SortType sortId;
				} ctx = {&this->idx_map, keys, sortId};
				auto selector = [&ctx](SelectKeyResult &res) {
					res.reserve(ctx.keys.size());
					for (auto key : ctx.keys) {
						auto keyIt = ctx.i_map->find(static_cast<ref_type>(key));
						if (keyIt != ctx.i_map->end()) {
							res.push_back(SingleSelectKeyResult(keyIt->second, ctx.sortId));
						}
					}
				};

				// Get from cache
				if (!opts.distinct && !opts.disableIdSetCache && keys.size() > 1) {
					tryIdsetCache(keys, condition, sortId, selector, res);
				} else
					selector(res);
			}
			break;
		case CondAllSet: {
			// Get set of key, where all request keys are present
			SelectKeyResults rslts;
			for (auto key : keys) {
				SelectKeyResult res1;
				key.convert(this->KeyType());
				auto keyIt = this->idx_map.find(static_cast<ref_type>(key));
				if (keyIt == this->idx_map.end()) {
					rslts.clear();
					rslts.push_back(res1);
					return rslts;
				}
				res1.push_back(SingleSelectKeyResult(keyIt->second, sortId));
				rslts.push_back(res1);
			}
			return rslts;
		}

		case CondAny:  // Get set of any keys
		case CondGe:
		case CondLe:
		case CondRange:
		case CondGt:
		case CondLt:
		case CondLike:
			return IndexStore<typename T::key_type>::SelectKey(keys, condition, sortId, opts, ctx, rdxCtx);
		default:
			throw Error(errQueryExec, "Unknown query on index '%s'", this->name_);
	}

	return SelectKeyResults(res);
}

template <typename T>
void IndexUnordered<T>::Commit() {
	this->empty_ids_.Unsorted().Commit();

	if (!cache_) cache_.reset(new IdSetCache());

	if (!tracker_.isUpdated()) return;

	logPrintf(LogTrace, "IndexUnordered::Commit (%s) %d uniq keys, %d empty, %s", this->name_, this->idx_map.size(),
			  this->empty_ids_.Unsorted().size(), tracker_.isCompleteUpdated() ? "complete" : "partial");

	if (tracker_.isCompleteUpdated()) {
		for (auto &keyIt : this->idx_map) {
			keyIt.second.Unsorted().Commit();
			assert(keyIt.second.Unsorted().size());
		}
	} else {
		tracker_.commitUpdated(idx_map);
	}
	tracker_.clear();
}

template <typename T>
void IndexUnordered<T>::UpdateSortedIds(const UpdateSortedContext &ctx) {
	logPrintf(LogTrace, "IndexUnordered::UpdateSortedIds (%s) %d uniq keys, %d empty", this->name_, this->idx_map.size(),
			  this->empty_ids_.Unsorted().size());
	// For all keys in index
	for (auto &keyIt : this->idx_map) {
		keyIt.second.UpdateSortedIds(ctx);
	}

	this->empty_ids_.UpdateSortedIds(ctx);
}

template <typename T>
Index *IndexUnordered<T>::Clone() {
	return new IndexUnordered<T>(*this);
}

template <typename T>
void IndexUnordered<T>::SetSortedIdxCount(int sortedIdxCount) {
	if (this->sortedIdxCount_ != sortedIdxCount) {
		this->sortedIdxCount_ = sortedIdxCount;
		for (auto &keyIt : idx_map) keyIt.second.Unsorted().ReserveForSorted(this->sortedIdxCount_);
	}
}

template <typename T>
IndexMemStat IndexUnordered<T>::GetMemStat() {
	IndexMemStat ret = IndexStore<typename T::key_type>::GetMemStat();
	ret.uniqKeysCount = idx_map.size();
	if (cache_) ret.idsetCache = cache_->GetMemStat();
	return ret;
}

template <typename KeyEntryT>
static Index *IndexUnordered_New(const IndexDef &idef, const PayloadType payloadType, const FieldsSet &fields) {
	switch (idef.Type()) {
		case IndexIntHash:
			return new IndexUnordered<unordered_number_map<int, KeyEntryT>>(idef, payloadType, fields);
		case IndexInt64Hash:
			return new IndexUnordered<unordered_number_map<int64_t, KeyEntryT>>(idef, payloadType, fields);
		case IndexStrHash:
			return new IndexUnordered<unordered_str_map<KeyEntryT>>(idef, payloadType, fields);
		case IndexCompositeHash:
			return new IndexUnordered<unordered_payload_map<KeyEntryT>>(idef, payloadType, fields);
		default:
			abort();
	}
}

Index *IndexUnordered_New(const IndexDef &idef, const PayloadType payloadType, const FieldsSet &fields) {
	return (idef.opts_.IsPK() || idef.opts_.IsDense()) ? IndexUnordered_New<Index::KeyEntryPlain>(idef, payloadType, fields)
													   : IndexUnordered_New<Index::KeyEntry>(idef, payloadType, fields);
}

template class IndexUnordered<number_map<int, Index::KeyEntryPlain>>;
template class IndexUnordered<number_map<int64_t, Index::KeyEntryPlain>>;
template class IndexUnordered<number_map<double, Index::KeyEntryPlain>>;
template class IndexUnordered<str_map<Index::KeyEntryPlain>>;
template class IndexUnordered<payload_map<Index::KeyEntryPlain>>;
template class IndexUnordered<number_map<int, Index::KeyEntry>>;
template class IndexUnordered<number_map<int64_t, Index::KeyEntry>>;
template class IndexUnordered<number_map<double, Index::KeyEntry>>;
template class IndexUnordered<str_map<Index::KeyEntry>>;
template class IndexUnordered<payload_map<Index::KeyEntry>>;
template class IndexUnordered<unordered_str_map<FtKeyEntry>>;
template class IndexUnordered<unordered_payload_map<FtKeyEntry>>;

}  // namespace reindexer
