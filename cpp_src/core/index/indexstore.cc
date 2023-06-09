#include "indexstore.h"
#include "core/keyvalue/uuid.h"
#include "core/rdxcontext.h"
#include "tools/errors.h"
#include "tools/logger.h"

namespace reindexer {

template <>
IndexStore<Point>::IndexStore(const IndexDef &idef, PayloadType payloadType, const FieldsSet &fields)
	: Index(idef, std::move(payloadType), fields) {
	keyType_ = selectKeyType_ = KeyValueType::Double{};
	opts_.Array(true);
}

template <>
void IndexStore<key_string>::Delete(const Variant &key, IdType id, StringsHolder &strHolder, bool & /*clearCache*/) {
	if (key.Type().Is<KeyValueType::Null>()) return;
	auto keyIt = str_map.find(std::string_view(key));
	// assertf(keyIt != str_map.end(), "Delete unexists key from index '%s' id=%d", name_, id);
	if (keyIt == str_map.end()) return;
	if (keyIt->second) --(keyIt->second);
	if (!keyIt->second) {
		const auto strSize = sizeof(*keyIt->first.get()) + keyIt->first->heap_size();
		memStat_.dataSize -= sizeof(unordered_str_map<int>::value_type) + strSize;
		strHolder.Add(std::move(keyIt->first), strSize);
		str_map.template erase<no_deep_clean>(keyIt);
	}

	(void)id;
}
template <typename T>
void IndexStore<T>::Delete(const Variant & /*key*/, IdType /* id */, StringsHolder &, bool & /*clearCache*/) {}

template <typename T>
void IndexStore<T>::Delete(const VariantArray &keys, IdType id, StringsHolder &strHolder, bool &clearCache) {
	if (keys.empty()) {
		Delete(Variant{}, id, strHolder, clearCache);
	} else {
		for (const auto &key : keys) Delete(key, id, strHolder, clearCache);
	}
}

template <>
void IndexStore<Point>::Delete(const VariantArray & /*keys*/, IdType /*id*/, StringsHolder &, bool & /*clearCache*/) {
	assertrx(0);
}

template <>
Variant IndexStore<key_string>::Upsert(const Variant &key, IdType /*id*/, bool & /*clearCache*/) {
	if (key.Type().Is<KeyValueType::Null>()) return Variant();

	auto keyIt = str_map.find(std::string_view(key));
	if (keyIt == str_map.end()) {
		keyIt = str_map.emplace(static_cast<key_string>(key), 0).first;
		// sizeof(key_string) + heap of string
		memStat_.dataSize += sizeof(unordered_str_map<int>::value_type) + sizeof(*keyIt->first.get()) + keyIt->first->heap_size();
	}
	++(keyIt->second);

	return Variant(keyIt->first);
}

template <>
Variant IndexStore<PayloadValue>::Upsert(const Variant &key, IdType /*id*/, bool & /*clearCache*/) {
	return Variant(key);
}

template <typename T>
Variant IndexStore<T>::Upsert(const Variant &key, IdType id, bool & /*clearCache*/) {
	if (!opts_.IsArray() && !opts_.IsDense() && !opts_.IsSparse() && key.Type().Is<KeyValueType::Null>()) {
		idx_data.resize(std::max(id + 1, int(idx_data.size())));
		idx_data[id] = static_cast<T>(key);
	}
	return Variant(key);
}

template <typename T>
void IndexStore<T>::Upsert(VariantArray &result, const VariantArray &keys, IdType id, bool &clearCache) {
	if (keys.empty()) {
		Upsert(Variant{}, id, clearCache);
	} else {
		result.reserve(keys.size());
		for (const auto &key : keys) result.emplace_back(Upsert(key, id, clearCache));
	}
}

template <>
void IndexStore<Point>::Upsert(VariantArray & /*result*/, const VariantArray & /*keys*/, IdType /*id*/, bool & /*clearCache*/) {
	assertrx(0);
}

template <typename T>
void IndexStore<T>::Commit() {
	logPrintf(LogTrace, "IndexStore::Commit (%s) %d uniq strings", name_, str_map.size());
}

template <typename T>
SelectKeyResults IndexStore<T>::SelectKey(const VariantArray &keys, CondType condition, SortType /*sortId*/, Index::SelectOpts sopts,
										  const BaseFunctionCtx::Ptr & /*ctx*/, const RdxContext &rdxCtx) {
	const auto indexWard(rdxCtx.BeforeIndexWork());
	SelectKeyResult res;
	if (condition == CondEmpty && !this->opts_.IsArray() && !this->opts_.IsSparse())
		throw Error(errParams, "The 'is NULL' condition is suported only by 'sparse' or 'array' indexes");

	if (condition == CondAny && !this->opts_.IsArray() && !this->opts_.IsSparse() && !sopts.distinct)
		throw Error(errParams, "The 'NOT NULL' condition is suported only by 'sparse' or 'array' indexes");

	res.comparators_.push_back(Comparator(condition, KeyType(), keys, opts_.IsArray(), sopts.distinct, payloadType_, fields_,
										  idx_data.size() ? idx_data.data() : nullptr, opts_.collateOpts_));
	return SelectKeyResults(std::move(res));
}

template <typename T>
IndexMemStat IndexStore<T>::GetMemStat(const RdxContext &) {
	IndexMemStat ret = memStat_;
	ret.name = name_;
	ret.uniqKeysCount = str_map.size();
	ret.columnSize = idx_data.capacity() * sizeof(T);
	return ret;
}

template <typename T>
template <typename S>
void IndexStore<T>::dump(S &os, std::string_view step, std::string_view offset) const {
	std::string newOffset{offset};
	newOffset += step;
	os << "{\n" << newOffset << "<Index>: ";
	Index::Dump(os, step, newOffset);
	os << ",\n" << newOffset << "str_map: {";
	for (auto b = str_map.begin(), it = b, e = str_map.end(); it != e; ++it) {
		if (it != b) os << ", ";
		os << '{' << (*it).first << ": " << (*it).second << '}';
	}
	os << "},\n" << newOffset << "idx_data: [";
	for (auto b = idx_data.cbegin(), it = b, e = idx_data.cend(); it != e; ++it) {
		if (it != b) os << ", ";
		os << *it;
	}
	os << "]\n" << offset << '}';
}

template <typename T>
void IndexStore<T>::AddDestroyTask(tsl::detail_sparse_hash::ThreadTaskQueue &q) {
	if constexpr (HasAddTask<decltype(str_map)>::value) {
		str_map.add_destroy_task(&q);
	}
	(void)q;
}

std::unique_ptr<Index> IndexStore_New(const IndexDef &idef, PayloadType payloadType, const FieldsSet &fields) {
	switch (idef.Type()) {
		case IndexBool:
			return std::unique_ptr<Index>{new IndexStore<bool>(idef, std::move(payloadType), fields)};
		case IndexIntStore:
			return std::unique_ptr<Index>{new IndexStore<int>(idef, std::move(payloadType), fields)};
		case IndexInt64Store:
			return std::unique_ptr<Index>{new IndexStore<int64_t>(idef, std::move(payloadType), fields)};
		case IndexDoubleStore:
			return std::unique_ptr<Index>{new IndexStore<double>(idef, std::move(payloadType), fields)};
		case IndexStrStore:
			return std::unique_ptr<Index>{new IndexStore<key_string>(idef, std::move(payloadType), fields)};
		case IndexStrHash:
		case IndexStrBTree:
		case IndexIntBTree:
		case IndexIntHash:
		case IndexInt64BTree:
		case IndexInt64Hash:
		case IndexDoubleBTree:
		case IndexFastFT:
		case IndexFuzzyFT:
		case IndexCompositeBTree:
		case IndexCompositeHash:
		case IndexCompositeFastFT:
		case IndexCompositeFuzzyFT:
		case IndexTtl:
		case IndexRTree:
		case IndexUuidHash:
		default:
			abort();
	}
}

template class IndexStore<bool>;
template class IndexStore<int>;
template class IndexStore<int64_t>;
template class IndexStore<double>;
template class IndexStore<key_string>;
template class IndexStore<PayloadValue>;
template class IndexStore<Point>;
template class IndexStore<Uuid>;

}  // namespace reindexer
