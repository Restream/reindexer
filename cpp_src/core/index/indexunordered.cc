#include "indexunordered.h"
#include "core/ft/ft_fast/ftfastkeyentry.h"
#include "core/indexdef.h"
#include "tools/errors.h"
#include "tools/logger.h"
namespace reindexer {

template <typename T>
Variant IndexUnordered<T>::Upsert(const Variant &key, IdType id) {
	// reset cache
	if (key.Type() == KeyValueNull) {
		this->empty_ids_.Unsorted().Add(id, IdSet::Auto, this->sortedIdxCount_);
		// Return invalid ref
		return Variant();
	}

	auto keyIt = find(key);
	if (keyIt == this->idx_map.end()) {
		keyIt = this->idx_map.insert({static_cast<typename T::key_type>(key), typename T::mapped_type()}).first;
	}
	keyIt->second.Unsorted().Add(id, this->opts_.IsPK() ? IdSet::Ordered : IdSet::Auto, this->sortedIdxCount_);
	markUpdated(&*keyIt);

	if (this->KeyType() == KeyValueString && this->opts_.GetCollateMode() != CollateNone) {
		return IndexStore<typename T::key_type>::Upsert(key, id);
	}

	return Variant(keyIt->first);
}

template <typename T>
void IndexUnordered<T>::Delete(const Variant &key, IdType id) {
	int delcnt = 0;
	if (key.Type() == KeyValueNull) {
		delcnt = this->empty_ids_.Unsorted().Erase(id);
		assert(delcnt);
		return;
	}

	auto keyIt = find(key);
	if (keyIt == idx_map.end()) return;

	delcnt = keyIt->second.Unsorted().Erase(id);
	(void)delcnt;
	// TODO: we have to implement removal of composite indexes (doesn't work right now)
	assertf(this->opts_.IsArray() || this->Opts().IsSparse() || delcnt, "Delete unexists id from index '%s' id=%d,key=%s",
			this->name_.c_str(), id, Variant(key).As<string>().c_str());

	if (keyIt->second.Unsorted().IsEmpty()) {
		this->tracker_.markDeleted(&*keyIt);
		idx_map.erase(keyIt);
	} else {
		markUpdated(&*keyIt);
	}
	if (this->KeyType() == KeyValueString && this->opts_.GetCollateMode() != CollateNone) {
		IndexStore<typename T::key_type>::Delete(key, id);
	}
}

// special implementation for string: avoid allocation string for *_map::find
// !!!! Not thread safe. Do not use this in Select
template <typename T>
template <typename U, typename std::enable_if<is_string_map_key<U>::value || is_string_unord_map_key<T>::value>::type *>
typename T::iterator IndexUnordered<T>::find(const Variant &key) {
	p_string skey(key);
	this->tmpKeyVal_->assign(skey.data(), skey.length());
	return this->idx_map.find(this->tmpKeyVal_);
}

template <typename T>
template <typename U, typename std::enable_if<!is_string_map_key<U>::value && !is_string_unord_map_key<T>::value>::type *>
typename T::iterator IndexUnordered<T>::find(const Variant &key) {
	return this->idx_map.find(static_cast<typename T::key_type>(key));
}

template <typename T>
IdSetRef IndexUnordered<T>::Find(const Variant &key) {
	auto res = this->find(key);
	return (res != idx_map.end()) ? res->second.Sorted(0) : IdSetRef();
}

template <typename T>
void IndexUnordered<T>::tryIdsetCache(const VariantArray &keys, CondType condition, SortType sortId,
									  std::function<void(SelectKeyResult &)> selector, SelectKeyResult &res) {
	if (isComposite(this->Type())) {
		selector(res);
		return;
	}

	auto cached = cache_->Get(IdSetCacheKey{keys, condition, sortId});
	if (cached.key) {
		if (!cached.val.ids) {
			selector(res);
			cache_->Put(*cached.key, res.mergeIdsets());
		} else {
			res.push_back(SingleSelectKeyResult(cached.val.ids));
		}
	} else {
		selector(res);
	}
}

template <typename T>
SelectKeyResults IndexUnordered<T>::SelectKey(const VariantArray &keys, CondType condition, SortType sortId, Index::ResultType res_type,
											  BaseFunctionCtx::Ptr ctx) {
	if (res_type == Index::ForceComparator) return IndexStore<typename T::key_type>::SelectKey(keys, condition, sortId, res_type, ctx);

	SelectKeyResult res;

	switch (condition) {
		case CondEmpty:
			res.push_back(SingleSelectKeyResult(this->empty_ids_, sortId));
			break;
		case CondAny:
			// Get set of any keys
			if (res_type == Index::ForceIdset) {
				res.reserve(this->idx_map.size());
				for (auto &keyIt : this->idx_map) res.push_back(SingleSelectKeyResult(keyIt.second, sortId));
			} else {
				return IndexStore<typename T::key_type>::SelectKey(keys, condition, sortId, res_type, ctx);
			}
			break;
		// Get set of keys or single key
		case CondEq:
		case CondSet:
			if (condition == CondEq && keys.size() < 1)
				throw Error(errParams, "For condition required at least 1 argument, but provided 0");
			if (keys.size() > 1000 && res_type != Index::ForceIdset) {
				return IndexStore<typename T::key_type>::SelectKey(keys, condition, sortId, res_type, ctx);
			} else {
				struct {
					T *i_map;
					const VariantArray &keys;
					SortType sortId;
				} ctx = {&this->idx_map, keys, sortId};
				auto selector = [&ctx](SelectKeyResult &res) {
					res.reserve(ctx.keys.size());
					for (auto key : ctx.keys) {
						auto keyIt = ctx.i_map->find(static_cast<typename T::key_type>(key));
						if (keyIt != ctx.i_map->end()) {
							res.push_back(SingleSelectKeyResult(keyIt->second, ctx.sortId));
						}
					}
				};

				// Get from cache
				if (res_type != Index::ForceIdset && res_type != Index::DisableIdSetCache && keys.size() > 1) {
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
				auto keyIt = this->idx_map.find(static_cast<typename T::key_type>(key));
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

		case CondGe:
		case CondLe:
		case CondRange:
		case CondGt:
		case CondLt:
			return IndexStore<typename T::key_type>::SelectKey(keys, condition, sortId, res_type, ctx);
		default:
			throw Error(errQueryExec, "Unknown query on index '%s'", this->name_.c_str());
	}

	return SelectKeyResults(res);
}  // namespace reindexer

template <typename T>
void IndexUnordered<T>::DumpKeys() {
	fprintf(stderr, "Dumping index: %s,keys=%d\n", this->name_.c_str(), int(this->idx_map.size()));
	for (auto &k : this->idx_map) {
		string buf;
		buf += Variant(k.first).As<string>() + ":";
		if (!k.second.Unsorted().size()) {
			buf += "<no ids>";
		} else {
			buf += k.second.Unsorted().Dump();
		}
		fprintf(stderr, "%s\n", buf.c_str());
	}
}

template <typename T>
void IndexUnordered<T>::Commit() {
	this->empty_ids_.Unsorted().Commit();
	if (!tracker_.isUpdated()) return;

	logPrintf(LogTrace, "IndexUnordered::Commit (%s) %d uniq keys, %d empty, %s", this->name_.c_str(), int(this->idx_map.size()),
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
	logPrintf(LogTrace, "IndexUnordered::UpdateSortedIds (%s) %d uniq keys, %d empty", this->name_.c_str(), int(this->idx_map.size()),
			  this->empty_ids_.Unsorted().size());
	// For all keys in index
	for (auto &keyIt : this->idx_map) {
		keyIt.second.UpdateSortedIds(ctx);
	}

	this->empty_ids_.UpdateSortedIds(ctx);
}

template <typename T>
void IndexUnordered<T>::markUpdated(typename T::value_type *key) {
	this->tracker_.markUpdated(this->idx_map, key);
	cache_->Clear();
}

template <typename T>
Index *IndexUnordered<T>::Clone() {
	return new IndexUnordered<T>(*this);
}

template <typename T>
IndexMemStat IndexUnordered<T>::GetMemStat() {
	IndexMemStat ret = IndexStore<typename T::key_type>::GetMemStat();
	ret.uniqKeysCount = idx_map.size();
	ret.sortOrdersSize = this->sortOrders_.capacity();
	if (cache_) ret.idsetCache = cache_->GetMemStat();
	getMemStat(ret);
	for (auto &it : idx_map) {
		ret.idsetPlainSize += sizeof(it.second) + it.second.ids_.heap_size();
		ret.idsetBTreeSize += it.second.ids_.BTreeSize();
	}
	return ret;
}

template <typename T>
template <typename U, typename std::enable_if<is_string_map_key<U>::value || is_string_unord_map_key<T>::value>::type *>
void IndexUnordered<T>::getMemStat(IndexMemStat &ret) const {
	for (auto &it : idx_map) {
		ret.dataSize += it.first->heap_size() + sizeof(*it.first.get());
	}
}

template <typename T>
template <typename U, typename std::enable_if<!is_string_map_key<U>::value && !is_string_unord_map_key<T>::value>::type *>
void IndexUnordered<T>::getMemStat(IndexMemStat & /*ret*/) const {}

template <typename KeyEntryT>
static Index *IndexUnordered_New(const IndexDef &idef, const PayloadType payloadType, const FieldsSet &fields) {
	switch (idef.Type()) {
		case IndexIntHash:
			return new IndexUnordered<unordered_map<int, KeyEntryT>>(idef, payloadType, fields);
		case IndexInt64Hash:
			return new IndexUnordered<unordered_map<int64_t, KeyEntryT>>(idef, payloadType, fields);
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

typedef unordered_str_map<FtFastKeyEntry> FtStrMap;
typedef unordered_payload_map<FtFastKeyEntry> FtPlMap;

template class IndexUnordered<btree_map<int, Index::KeyEntryPlain>>;
template class IndexUnordered<btree_map<int64_t, Index::KeyEntryPlain>>;
template class IndexUnordered<btree_map<double, Index::KeyEntryPlain>>;
template class IndexUnordered<str_map<Index::KeyEntryPlain>>;
template class IndexUnordered<payload_map<Index::KeyEntryPlain>>;
template class IndexUnordered<btree_map<int, Index::KeyEntry>>;
template class IndexUnordered<btree_map<int64_t, Index::KeyEntry>>;
template class IndexUnordered<btree_map<double, Index::KeyEntry>>;
template class IndexUnordered<str_map<Index::KeyEntry>>;
template class IndexUnordered<payload_map<Index::KeyEntry>>;
template class IndexUnordered<FtStrMap>;
template class IndexUnordered<FtPlMap>;

template typename FtStrMap::iterator IndexUnordered<FtStrMap>::find(const Variant &key);
template typename FtPlMap::iterator IndexUnordered<FtPlMap>::find(const Variant &key);
}  // namespace reindexer
