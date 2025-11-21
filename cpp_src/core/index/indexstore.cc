#include "indexstore.h"
#include "core/keyvalue/uuid.h"
#include "core/rdxcontext.h"
#include "tools/errors.h"
#include "tools/logger.h"

namespace reindexer {

template <>
IndexStore<Point>::IndexStore(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields)
	: Index(idef, std::move(payloadType), std::move(fields)) {
	keyType_ = selectKeyType_ = KeyValueType::Double{};
	opts_.Array(true);
}

template <>
void IndexStore<key_string>::Delete(const Variant& key, [[maybe_unused]] IdType id, [[maybe_unused]] MustExist mustExist,
									StringsHolder& strHolder, bool& /*clearCache*/) {
	assertrx_dbg(!IsFulltext());
	if (key.Type().Is<KeyValueType::Null>()) {
		return;
	}
	if (!shouldHoldValueInStrMap()) {
		return;
	}
	auto keyIt = str_map.find(std::string_view(key));
	assertf(!mustExist || keyIt != str_map.end(), "Delete non-existent key from index '{}' id={}", name_, id);
	if (keyIt == str_map.end()) {
		return;
	}
	assertrx_dbg(keyIt->second > 0);
	if ((keyIt->second--) == 1) {
		const auto strSize = keyIt->first.heap_size();
		const auto staticSizeApproximate = size_t(float(sizeof(unordered_str_map<int>::value_type)) / str_map.max_load_factor());
		memStat_.dataSize -= staticSizeApproximate + strSize;
		strHolder.Add(std::move(keyIt->first), strSize);
		std::ignore = str_map.template erase<no_deep_clean>(keyIt);
	}
}
template <typename T>
void IndexStore<T>::Delete(const Variant& /*key*/, IdType /* id */, MustExist, StringsHolder&, bool& /*clearCache*/) {
	assertrx_dbg(!IsFulltext());
}

template <typename T>
void IndexStore<T>::Delete(const VariantArray& keys, IdType id, MustExist mustExist, StringsHolder& strHolder, bool& clearCache) {
	if (keys.empty()) {
		Delete(Variant{}, id, mustExist, strHolder, clearCache);
	} else {
		for (const auto& key : keys) {
			Delete(key, id, mustExist, strHolder, clearCache);
		}
	}
}

template <>
void IndexStore<Point>::Delete(const VariantArray& /*keys*/, IdType /*id*/, MustExist, StringsHolder&, bool& /*clearCache*/) {
	assertrx(0);
}

template <>
Variant IndexStore<key_string>::Upsert(const Variant& key, IdType id, bool& /*clearCache*/) {
	assertrx_dbg(!IsFulltext());
	if (key.Type().Is<KeyValueType::Null>()) {
		return Variant();
	}

	std::string_view val;
	const bool holdValueInStrMap = shouldHoldValueInStrMap();
	auto keyIt = str_map.end();
	if (holdValueInStrMap) {
		keyIt = str_map.find(std::string_view(key));
		if (keyIt == str_map.end()) {
			keyIt = str_map.emplace(static_cast<key_string>(key), 0).first;
			const auto staticSizeApproximate = size_t(float(sizeof(unordered_str_map<int>::value_type)) / str_map.max_load_factor());
			memStat_.dataSize += staticSizeApproximate + keyIt->first.heap_size();
		}
		++(keyIt->second);
		val = keyIt->first;
	} else {
		val = std::string_view(key);
	}
	if (!IsColumnIndexDisabled() && !IsFulltext()) {
		idx_data.resize(std::max(id + 1, IdType(idx_data.size())));
		idx_data[id] = val;
	}

	return holdValueInStrMap ? Variant(keyIt->first) : key;
}

template <>
Variant IndexStore<PayloadValue>::Upsert(const Variant& key, IdType /*id*/, bool& /*clearCache*/) {
	return key;
}

template <typename T>
Variant IndexStore<T>::Upsert(const Variant& key, IdType id, bool& /*clearCache*/) {
	if (!IsColumnIndexDisabled() && !key.Type().Is<KeyValueType::Null>()) {
		idx_data.resize(std::max(id + 1, IdType(idx_data.size())));
		idx_data[id] = static_cast<T>(key);
	}
	return Variant(key);
}

template <typename T>
void IndexStore<T>::Upsert(VariantArray& result, const VariantArray& keys, IdType id, bool& clearCache) {
	if (keys.IsObjectValue()) {
		return;
	}
	if (keys.empty()) {
		std::ignore = Upsert(Variant{}, id, clearCache);
	} else {
		result.reserve(keys.size());
		for (const auto& key : keys) {
			result.emplace_back(Upsert(key, id, clearCache));
		}
	}
}

template <>
void IndexStore<Point>::Upsert(VariantArray& /*result*/, const VariantArray& /*keys*/, IdType /*id*/, bool& /*clearCache*/) {
	assertrx(0);
}

template <typename T>
void IndexStore<T>::Commit() {
	logFmt(LogTrace, "IndexStore::Commit ({}) {} uniq strings", name_, str_map.size());
}

template <typename T>
SelectKeyResults IndexStore<T>::SelectKey(const VariantArray& keys, CondType condition, SortType /*sortId*/,
										  const Index::SelectContext& selectCtx, const RdxContext& rdxCtx) {
	const auto indexWard(rdxCtx.BeforeIndexWork());
	if (condition == CondEmpty && !this->opts_.IsArray() && !this->opts_.IsSparse()) {
		throw Error(errParams, "The 'is NULL' condition is supported only by 'sparse' or 'array' indexes");
	}

	if (condition == CondAny && !this->opts_.IsArray() && !this->opts_.IsSparse() && !selectCtx.opts.distinct) {
		throw Error(errParams, "The 'NOT NULL' condition is supported only by 'sparse' or 'array' indexes");
	}

	return ComparatorIndexed<T>{Name(),
								condition,
								keys,
								idx_data.size() ? idx_data.data() : nullptr,
								opts_.IsArray(),
								IsDistinct(selectCtx.opts.distinct),
								payloadType_,
								Fields(),
								opts_.collateOpts_};
}

template <typename T>
IndexMemStat IndexStore<T>::GetMemStat(const RdxContext&) {
	IndexMemStat ret = memStat_;
	ret.name = name_;
	ret.uniqKeysCount = str_map.size();
	ret.columnSize = idx_data.capacity() * sizeof(T);
	return ret;
}

template <typename T>
template <typename S>
void IndexStore<T>::dump(S& os, std::string_view step, std::string_view offset) const {
	std::string newOffset{offset};
	newOffset += step;
	os << "{\n" << newOffset << "<Index>: ";
	Index::Dump(os, step, newOffset);
	os << ",\n" << newOffset << "str_map: {";
	for (auto b = str_map.begin(), it = b, e = str_map.end(); it != e; ++it) {
		if (it != b) {
			os << ", ";
		}
		os << '{' << (*it).first << ": " << (*it).second << '}';
	}
	os << "},\n" << newOffset << "idx_data: [";
	for (auto b = idx_data.cbegin(), it = b, e = idx_data.cend(); it != e; ++it) {
		if (it != b) {
			os << ", ";
		}
		if constexpr (std::is_same_v<T, bool>) {
			os << int(*it);
		} else {
			os << *it;
		}
	}
	os << "]\n" << offset << '}';
}

template <typename T>
void IndexStore<T>::AddDestroyTask(tsl::detail_sparse_hash::ThreadTaskQueue& q) {
	if constexpr (HasAddTask<decltype(str_map)>::value) {
		str_map.add_destroy_task(&q);
	}
	(void)q;
}

template <typename T>
bool IndexStore<T>::shouldHoldValueInStrMap() const noexcept {
	return this->opts_.GetCollateMode() != CollateNone || Type() == IndexStrStore;
}

std::unique_ptr<Index> IndexStore_New(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields) {
	switch (idef.IndexType()) {
		case IndexBool:
			return std::make_unique<IndexStore<bool>>(idef, std::move(payloadType), std::move(fields));
		case IndexIntStore:
			return std::make_unique<IndexStore<int>>(idef, std::move(payloadType), std::move(fields));
		case IndexInt64Store:
			return std::make_unique<IndexStore<int64_t>>(idef, std::move(payloadType), std::move(fields));
		case IndexDoubleStore:
			return std::make_unique<IndexStore<double>>(idef, std::move(payloadType), std::move(fields));
		case IndexStrStore:
			return std::make_unique<IndexStore<key_string>>(idef, std::move(payloadType), std::move(fields));
		case IndexUuidStore:
			return std::make_unique<IndexStore<Uuid>>(idef, std::move(payloadType), std::move(fields));
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
		case IndexHnsw:
		case IndexVectorBruteforce:
		case IndexIvf:
		case IndexDummy:
			break;
	}
	throw_as_assert;
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
