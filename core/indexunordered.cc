#include <stdint.h>
#include <stdio.h>
#include <algorithm>
#include <map>

#include "core/index.h"
#include "core/indexunordered.h"
#include "tools/errors.h"
#include "tools/logger.h"
#include "tools/strings.h"

using namespace std;

namespace reindexer {
template <typename T>
IndexUnordered<T>::~IndexUnordered() {
	logPrintf(LogTrace, "IndexUnordered::~IndexUnordered (%s) %d uniq keys", this->name.c_str(), this->idx_map.size());
}

template <typename T>
KeyRef IndexUnordered<T>::Upsert(const KeyRef &key, IdType id) {
	if (key.Type() == KeyValueEmpty) {
		this->empty_ids_.Unsorted().push_back(id, false);
		// Return invalid ref
		return KeyRef();
	}

	auto keyIt = find(key);
	if (keyIt == this->idx_map.end()) keyIt = this->idx_map.insert({(typename T::key_type)(KeyValue)key, Index::KeyEntry()}).first;
	keyIt->second.Unsorted().push_back(id, this->opts_.IsPK);

	return KeyRef(keyIt->first);
}

template <typename T>
void IndexUnordered<T>::Delete(const KeyRef &key, IdType id) {
	if (key.Type() == KeyValueEmpty) {
		this->empty_ids_.Unsorted().erase(id, false);
		return;
	}

	auto keyIt = find(key);
	assertf(keyIt != this->idx_map.end(), "Delete unexists key from index '%s' id=%d", this->name.c_str(), id);
	keyIt->second.Unsorted().erase(id, this->opts_.IsPK);
	//	if (this->opts_.IsPK && !keyIt->second.Unsorted().size()) this->idx_map.erase(keyIt);
}

// special implementation for string: avoid allocation string for *_map::find
// !!!! Not thread safe. Do not use this in Select
template <>
unordered_str_map<Index::KeyEntry>::iterator IndexUnordered<unordered_str_map<Index::KeyEntry>>::find(const KeyRef &key) {
	p_string skey = (p_string)key;
	tmpKeyVal_->assign(skey.data(), skey.length());

	auto res = this->idx_map.find(tmpKeyVal_);
	return res;
}

template <>
str_map<Index::KeyEntry>::iterator IndexUnordered<str_map<Index::KeyEntry>>::find(const KeyRef &key) {
	p_string skey = (p_string)key;
	tmpKeyVal_->assign(skey.data(), skey.length());
	return this->idx_map.find(tmpKeyVal_);
}

template <typename T>
typename T::iterator IndexUnordered<T>::find(const KeyRef &key) {
	return this->idx_map.find((typename T::key_type)key);
}

template <typename T>
void IndexUnordered<T>::tryIdsetCache(const KeyValues &keys, CondType condition, SortType sortId,
									  std::function<void(SelectKeyResult &)> selector, SelectKeyResult &res) {
	auto cached = cache_->Get(IdSetCacheKey{keys, condition, sortId});

	if (cached.key) {
		if (!cached.val.ids->size()) {
			selector(res);
			cache_->Put(*cached.key, res.mergeIdsets());
		} else
			res.push_back(cached.val.ids);
	} else
		selector(res);
}

template <typename T>
SelectKeyResults IndexUnordered<T>::SelectKey(const KeyValues &keys, CondType condition, SortType sortId, Index::ResultType res_type) {
	if (res_type == Index::ForceComparator) return IndexStore<typename T::key_type>::SelectKey(keys, condition, sortId, res_type);

	SelectKeyResult res;

	switch (condition) {
		case CondEmpty:
			res.push_back(this->empty_ids_.Sorted(sortId));
			break;
		case CondAny:
			// Get set of any keys
			res.reserve(this->idx_map.size());
			for (auto &keyIt : this->idx_map) res.push_back(keyIt.second.Sorted(sortId));
			break;
		case CondEq:
			// Get set of keys or single key
			if (keys.size() < 1) throw Error(errParams, "For condition reuqired at least 1 argument, but provided 0");
		case CondSet:
			if (keys.size() > 1000 && res_type != Index::ForceIdset) {
				return IndexStore<typename T::key_type>::SelectKey(keys, condition, sortId, res_type);
			} else {
				T *i_map = &this->idx_map;
				auto selector = [&keys, sortId, i_map](SelectKeyResult &res) {
					res.reserve(keys.size());
					for (auto key : keys) {
						auto keyIt = i_map->find((typename T::key_type)key);
						if (keyIt != i_map->end()) res.push_back(keyIt->second.Sorted(sortId));
					}
				};

				// Get from cache
				if (res_type != Index::ForceIdset && keys.size() > 1) {
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
				auto keyIt = this->idx_map.find((typename T::key_type)key);
				if (keyIt == this->idx_map.end()) {
					rslts.clear();
					rslts.push_back(res1);
					return rslts;
				}
				res1.push_back(keyIt->second.Sorted(sortId));
				rslts.push_back(res1);
			}
			return rslts;
		}

		case CondGe:
		case CondLe:
		case CondRange:
		case CondGt:
		case CondLt:
			return IndexStore<typename T::key_type>::SelectKey(keys, condition, sortId, res_type);
		default:
			throw Error(errQueryExec, "Unknown query on index '%s'", this->name.c_str());
	}

	return SelectKeyResults(res);
}  // namespace reindexer

template <typename T>
void IndexUnordered<T>::DumpKeys() {
	fprintf(stderr, "Dumping index: %s,keys=%d\n", this->name.c_str(), (int)this->idx_map.size());
	for (auto &k : this->idx_map) {
		string buf;
		buf += KeyValue(k.first).toString() + ":";
		if (!k.second.Unsorted().size()) {
			buf += "<no ids>";
		} else {
			buf += k.second.Unsorted().dump();
		}
		fprintf(stderr, "%s\n", buf.c_str());
	}
}

template <typename T>
void IndexUnordered<T>::Commit(const CommitContext &ctx) {
	if (ctx.phases() & CommitContext::MakeIdsets) {
		// reset cache
		cache_.reset(new IdSetCache());

		logPrintf(LogTrace, "IndexUnordered::Commit (%s) %d uniq keys, %d empty", this->name.c_str(), this->idx_map.size(),
				  this->empty_ids_.Unsorted().size());
		for (auto &keyIt : this->idx_map) keyIt.second.Unsorted().commit(ctx);
		this->empty_ids_.Unsorted().commit(ctx);

		for (auto keyIt = this->idx_map.begin(); keyIt != this->idx_map.end();) {
			if (!keyIt->second.Unsorted().size())
				keyIt = this->idx_map.erase(keyIt);
			else
				keyIt++;
		}
		if (!this->idx_map.size()) this->idx_map.clear();
	}
}

template <typename T>
void IndexUnordered<T>::UpdateSortedIds(const UpdateSortedContext &ctx) {
	logPrintf(LogTrace, "IndexUnordered::UpdateSortedIds (%s) %d uniq keys, %d empty", this->name.c_str(), this->idx_map.size(),
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

template class IndexUnordered<map<int64_t, Index::KeyEntry>>;
template class IndexUnordered<map<int, Index::KeyEntry>>;
template class IndexUnordered<map<double, Index::KeyEntry>>;
template class IndexUnordered<str_map<Index::KeyEntry>>;
template class IndexUnordered<unordered_map<int64_t, Index::KeyEntry>>;
template class IndexUnordered<unordered_str_map<Index::KeyEntry>>;
template class IndexUnordered<unordered_map<int, Index::KeyEntry>>;
template class IndexUnordered<unordered_map<PayloadData, Index::KeyEntry, hash_composite, equal_composite>>;
template class IndexUnordered<map<PayloadData, Index::KeyEntry, less_composite>>;

}  // namespace reindexer
