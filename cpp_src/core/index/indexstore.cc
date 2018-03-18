
#include "indexstore.h"
#include "tools/errors.h"
#include "tools/logger.h"

namespace reindexer {

// special implementation for string: avoid allocation string for *_map::find
// !!!! Not thread safe. Do not use this in Select
template <>
unordered_str_map<int>::iterator IndexStore<key_string>::find(const KeyRef &key) {
	p_string skey = static_cast<p_string>(key);
	tmpKeyVal_->assign(skey.data(), skey.length());
	return str_map.find(tmpKeyVal_);
}

template <typename T>
unordered_str_map<int>::iterator IndexStore<T>::find(const KeyRef & /*key*/) {
	return str_map.end();
}

template <>
void IndexStore<key_string>::Delete(const KeyRef &key, IdType id) {
	if (key.Type() == KeyValueEmpty) return;
	auto keyIt = find(key);
	assertf(keyIt != str_map.end(), "Delete unexists key from index '%s' id=%d", name_.c_str(), id);
	if (keyIt->second) keyIt->second--;
	(void)id;
}
template <typename T>
void IndexStore<T>::Delete(const KeyRef & /*key*/, IdType /* id */) {}

template <>
KeyRef IndexStore<key_string>::Upsert(const KeyRef &key, IdType /*id*/) {
	if (key.Type() == KeyValueEmpty) return KeyRef();

	auto keyIt = find(key);
	if (keyIt == str_map.end()) keyIt = str_map.emplace(static_cast<key_string>(key), 0).first;

	keyIt->second++;
	return KeyRef(keyIt->first);
}

template <>
KeyRef IndexStore<PayloadValue>::Upsert(const KeyRef &key, IdType /*id*/) {
	return KeyRef(key);
}

template <typename T>
KeyRef IndexStore<T>::Upsert(const KeyRef &key, IdType id) {
	if (!opts_.IsArray() && !opts_.IsDense()) {
		idx_data.resize(std::max(id + 1, int(idx_data.size())));
		idx_data[id] = static_cast<T>(key);
	}
	return KeyRef(key);
}

template <typename T>
void IndexStore<T>::Commit(const CommitContext &ctx) {
	if (ctx.phases() & CommitContext::MakeIdsets) {
		logPrintf(LogTrace, "IndexStore::Commit (%s) %d uniq strings", name_.c_str(), str_map.size());

		for (auto keyIt = str_map.begin(); keyIt != str_map.end();) {
			if (!keyIt->second)
				keyIt = str_map.erase(keyIt);
			else
				keyIt++;
		}
		if (!str_map.size()) str_map.clear();
	}
}

template <typename T>
SelectKeyResults IndexStore<T>::SelectKey(const KeyValues &keys, CondType condition, SortType /*sortId*/, ResultType res_type,
										  BaseFunctionCtx::Ptr /*ctx*/) {
	if (res_type == Index::ForceIdset) {
		throw Error(errLogic, "Can't return idset from '%d'. DISTINCT is allowed only on indexed fields", name_.c_str());
	}
	SelectKeyResult res;
	res.comparators_.push_back(Comparator(condition, KeyType(), keys, opts_.IsArray(), payloadType_, fields_,
										  idx_data.size() ? idx_data.data() : nullptr, opts_.GetCollateMode()));
	return SelectKeyResults(res);
}

template <typename T>
Index *IndexStore<T>::Clone() {
	return new IndexStore<T>(*this);
}

Index *IndexStore_New(IndexType type, const string &name, const IndexOpts &opts, const PayloadType /*payloadType*/,
					  const FieldsSet & /*fields*/) {
	switch (type) {
		case IndexBool:
		case IndexIntStore:
			return new IndexStore<int>(type, name, opts);
		case IndexInt64Store:
			return new IndexStore<int64_t>(type, name, opts);
		case IndexDoubleStore:
			return new IndexStore<double>(type, name, opts);
		case IndexStrStore:
			return new IndexStore<key_string>(type, name, opts);
		default:
			abort();
	}
}

template class IndexStore<PayloadValue>;

}  // namespace reindexer
