
#include "indexstore.h"
#include "core/rdxcontext.h"
#include "tools/errors.h"
#include "tools/logger.h"

namespace reindexer {

template <>
void IndexStore<key_string>::Delete(const Variant &key, IdType id) {
	if (key.Type() == KeyValueNull) return;
	auto keyIt = str_map.find(string_view(key));
	// assertf(keyIt != str_map.end(), "Delete unexists key from index '%s' id=%d", name_, id);
	if (keyIt == str_map.end()) return;
	if (keyIt->second) keyIt->second--;
	if (!keyIt->second) {
		memStat_.dataSize -= sizeof(unordered_str_map<int>::value_type) + sizeof(*keyIt->first.get()) + keyIt->first->heap_size();
		keyIt->first = key_string();
		str_map.erase(keyIt);
	}

	(void)id;
}
template <typename T>
void IndexStore<T>::Delete(const Variant & /*key*/, IdType /* id */) {}

template <>
Variant IndexStore<key_string>::Upsert(const Variant &key, IdType /*id*/) {
	if (key.Type() == KeyValueNull) return Variant();

	auto keyIt = str_map.find(string_view(key));
	if (keyIt == str_map.end()) {
		keyIt = str_map.emplace(static_cast<key_string>(key), 0).first;
		memStat_.dataSize += sizeof(unordered_str_map<int>::value_type) + sizeof(*keyIt->first.get()) + keyIt->first->heap_size();
	}
	keyIt->second++;

	return Variant(keyIt->first);
}

template <>
Variant IndexStore<PayloadValue>::Upsert(const Variant &key, IdType /*id*/) {
	return Variant(key);
}

template <typename T>
Variant IndexStore<T>::Upsert(const Variant &key, IdType id) {
	if (!opts_.IsArray() && !opts_.IsDense() && !opts_.IsSparse() && key.Type() != KeyValueNull) {
		idx_data.resize(std::max(id + 1, int(idx_data.size())));
		idx_data[id] = static_cast<T>(key);
	}
	return Variant(key);
}

template <typename T>
void IndexStore<T>::Commit() {
	logPrintf(LogTrace, "IndexStore::Commit (%s) %d uniq strings", name_, str_map.size());
}

template <typename T>
SelectKeyResults IndexStore<T>::SelectKey(const VariantArray &keys, CondType condition, SortType /*sortId*/, Index::SelectOpts sopts,
										  BaseFunctionCtx::Ptr /*ctx*/, const RdxContext &rdxCtx) {
	const auto indexWard(rdxCtx.BeforeIndexWork());
	SelectKeyResult res;
	if (condition == CondEmpty && !this->opts_.IsArray() && !this->opts_.IsSparse())
		throw Error(errParams, "The 'is NULL' condition is suported only by 'sparse' or 'array' indexes");

	if (condition == CondAny && !this->opts_.IsArray() && !this->opts_.IsSparse() && !sopts.distinct)
		throw Error(errParams, "The 'NOT NULL' condition is suported only by 'sparse' or 'array' indexes");

	res.comparators_.push_back(Comparator(condition, KeyType(), keys, opts_.IsArray(), sopts.distinct, payloadType_, fields_,
										  idx_data.size() ? idx_data.data() : nullptr, opts_.collateOpts_));
	return SelectKeyResults(res);
}

template <typename T>
Index *IndexStore<T>::Clone() {
	return new IndexStore<T>(*this);
}

template <typename T>
IndexMemStat IndexStore<T>::GetMemStat() {
	IndexMemStat ret = memStat_;
	ret.name = name_;
	ret.uniqKeysCount = str_map.size();
	ret.columnSize = idx_data.size() * sizeof(T);
	return ret;
}

Index *IndexStore_New(const IndexDef &idef, const PayloadType payloadType, const FieldsSet &fields) {
	switch (idef.Type()) {
		case IndexBool:
			return new IndexStore<bool>(idef, payloadType, fields);
		case IndexIntStore:
			return new IndexStore<int>(idef, payloadType, fields);
		case IndexInt64Store:
			return new IndexStore<int64_t>(idef, payloadType, fields);
		case IndexDoubleStore:
			return new IndexStore<double>(idef, payloadType, fields);
		case IndexStrStore:
			return new IndexStore<key_string>(idef, payloadType, fields);
		default:
			abort();
	}
}

template class IndexStore<PayloadValue>;

}  // namespace reindexer
