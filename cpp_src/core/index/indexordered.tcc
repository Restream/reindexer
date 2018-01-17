//#include <stdint.h>

#include "indexordered.h"
#include "tools/errors.h"
#include "tools/logger.h"

namespace reindexer {

template <typename T>
KeyRef IndexOrdered<T>::Upsert(const KeyRef &key, IdType id) {
	if (key.Type() == KeyValueEmpty) {
		this->empty_ids_.Unsorted().Add(id, IdSet::Auto);
		// Return invalid ref
		return KeyRef();
	}

	bool found = false;
	auto keyIt = lower_bound(key, found);

	if (keyIt == this->idx_map.end() || !found)
		keyIt = this->idx_map.insert(keyIt, {static_cast<typename T::key_type>(key), typename T::mapped_type()});
	keyIt->second.Unsorted().Add(id, this->opts_.IsPK ? IdSet::Ordered : IdSet::Auto);
	this->tracker_.markUpdated(this->idx_map, &*keyIt);

	if (this->KeyType() == KeyValueString && this->opts_.CollateMode != CollateNone) {
		return IndexStore<typename T::key_type>::Upsert(key, id);
	}

	return KeyRef(keyIt->first);
}

// special implementation for string: avoid allocation string for *_map::lower_bound
// !!!! Not thread safe. Do not use this in Select
template <typename T>
template <typename U, typename std::enable_if<is_string_map_key<U>::value>::type *>
typename T::iterator IndexOrdered<T>::lower_bound(const KeyRef &key, bool &found) {
	p_string skey = static_cast<p_string>(key);
	this->tmpKeyVal_->assign(skey.data(), skey.length());
	auto it = this->idx_map.lower_bound(this->tmpKeyVal_);
	found = (it != this->idx_map.end() && this->tmpKeyVal_ == it->first);
	return it;
}

template <typename T>
template <typename U, typename std::enable_if<!is_string_map_key<U>::value>::type *>
typename T::iterator IndexOrdered<T>::lower_bound(const KeyRef &key, bool &found) {
	auto it = this->idx_map.lower_bound(static_cast<typename T::key_type>(key));
	found = (it != this->idx_map.end() && !this->idx_map.key_comp()(static_cast<typename T::key_type>(key), it->first));
	return it;
}

template <typename T>
SelectKeyResults IndexOrdered<T>::SelectKey(const KeyValues &keys, CondType condition, SortType sortId, Index::ResultType res_type) {
	if (res_type == Index::ForceComparator) return IndexStore<typename T::key_type>::SelectKey(keys, condition, sortId, res_type);
	SelectKeyResult res;

	// Get set of keys or single key
	if (condition == CondSet || condition == CondEq || condition == CondAny || condition == CondEmpty)
		return IndexUnordered<T>::SelectKey(keys, condition, sortId, res_type);

	assert(!this->tracker_.updated_.size() && !this->tracker_.completeUpdated_);

	if (keys.size() < 1) throw Error(errParams, "For condition reuqired at least 1 argument, but provided 0");

	auto startIt = this->idx_map.begin();
	auto endIt = this->idx_map.end();

	auto key1 = *keys.begin();

	switch (condition) {
		case CondLt:
			endIt = this->idx_map.lower_bound(static_cast<typename T::key_type>(key1));
			break;
		case CondLe:
			endIt = this->idx_map.lower_bound(static_cast<typename T::key_type>(key1));
			if (endIt != this->idx_map.end() && !this->idx_map.key_comp()(static_cast<typename T::key_type>(key1), endIt->first)) endIt++;
			break;
		case CondGt:
			startIt = this->idx_map.upper_bound(static_cast<typename T::key_type>(key1));
			break;
		case CondGe:
			startIt = this->idx_map.find(static_cast<typename T::key_type>(key1));
			if (startIt == this->idx_map.end()) startIt = this->idx_map.upper_bound(static_cast<typename T::key_type>(key1));
			break;
		case CondRange: {
			if (keys.size() != 2) throw Error(errParams, "For ranged query reuqired 2 arguments, but provided %d", int(keys.size()));
			auto key2 = keys[1];

			if (this->idx_map.key_comp()(static_cast<typename T::key_type>(key2), static_cast<typename T::key_type>(key1))) {
				return SelectKeyResults({res});
			}

			startIt = this->idx_map.find(static_cast<typename T::key_type>(key1));
			if (startIt == this->idx_map.end()) startIt = this->idx_map.upper_bound(static_cast<typename T::key_type>(key1));

			endIt = this->idx_map.lower_bound(static_cast<typename T::key_type>(key2));
			if (endIt != this->idx_map.end() && !this->idx_map.key_comp()(static_cast<typename T::key_type>(key2), endIt->first)) endIt++;
		} break;
		default:
			throw Error(errParams, "Unknown query type %d", condition);
	}

	if (endIt == startIt || startIt == this->idx_map.end() || endIt == this->idx_map.begin())
		// Empty result
		return SelectKeyResults(res);

	if (this->sort_id == sortId && sortId && res_type != Index::ForceIdset) {
		assert(startIt->second.Sorted(this->sort_id).size());
		IdType idFirst = startIt->second.Sorted(this->sort_id).front();

		auto backIt = endIt;
		backIt--;
		assert(backIt->second.Sorted(this->sort_id).size());
		IdType idLast = backIt->second.Sorted(this->sort_id).back();
		// sort by this index. Just give part of sorted ids;
		res.push_back(SingleSelectKeyResult(idFirst, idLast + 1));
	} else {
		int count = 0;
		auto it = startIt;

		while (count < 50 && it != endIt) {
			it++;
			count++;
		}
		if (count < 50 || res_type == Index::ForceIdset) {
			T *i_map = &this->idx_map;
			auto selector = [&keys, sortId, i_map, startIt, endIt](SelectKeyResult &res) {
				for (auto it = startIt; it != endIt && it != i_map->end(); it++) res.push_back(it->second.Sorted(sortId));
			};

			if (count > 1 && res_type != Index::ForceIdset)
				this->tryIdsetCache(keys, condition, sortId, selector, res);
			else
				selector(res);
		} else {
			return IndexStore<typename T::key_type>::SelectKey(keys, condition, sortId, res_type);
		}
	}
	return SelectKeyResults(res);
}

template <typename T>
void IndexOrdered<T>::MakeSortOrders(UpdateSortedContext &ctx) {
	logPrintf(LogTrace, "IndexOrdered::MakeSortOrders (%s)", this->name.c_str());
	auto &ids2Sorts = ctx.ids2Sorts();
	size_t totalIds = 0;
	for (auto it : ids2Sorts)
		if (it != SortIdUnexists) totalIds++;

	this->sort_orders.resize(totalIds);
	size_t idx = 0;
	for (auto &keyIt : this->idx_map) {
		// assert (keyIt.second.size());
		for (auto id : keyIt.second.Unsorted()) {
			if (id >= int(ids2Sorts.size()) || ids2Sorts[id] == SortIdUnexists) {
				logPrintf(
					LogError,
					"Internal error: Index '%s' is broken. Item with key '%s' contains id=%d, which is not present in allIds,totalids=%d\n",
					this->name.c_str(), KeyValue(keyIt.first).toString().c_str(), id, int(totalIds));
				this->DumpKeys();
				assert(0);
			}
			if (ids2Sorts[id] == SortIdUnfilled) {
				ids2Sorts[id] = idx;
				this->sort_orders[idx++] = id;
			}
		}
	}
	// fill unexist indexs

	for (auto it = ids2Sorts.begin(); it != ids2Sorts.end(); ++it) {
		if (*it == SortIdUnfilled) {
			*it = idx;
			this->sort_orders[idx++] = it - ids2Sorts.begin();
		}
	}

	if (idx != totalIds) {
		fprintf(stderr, "Internal error: Index %s is broken. totalids=%d, but indexed=%d\n", this->name.c_str(), int(totalIds), int(idx));
		this->DumpKeys();
		assert(0);
	}
}

template <typename T>
Index *IndexOrdered<T>::Clone() {
	return new IndexOrdered<T>(*this);
}

template <typename T>
bool IndexOrdered<T>::IsOrdered() const {
	return true;
}

}  // namespace reindexer
