#pragma once

#include "core/cjson/tagsmatcher.h"
#include "core/id_type.h"
#include "core/itemimpl.h"
#include "core/namespace/float_vectors_indexes.h"
#include "core/payload/fieldsset.h"

namespace reindexer {
class Index;
class AsyncStorage;
class FloatVectorIndex;
class NamespaceImpl;

class [[nodiscard]] FloatVectorExtractor {
public:
	explicit FloatVectorExtractor(const AsyncStorage& asyncStorage, const FloatVectorIndex& index, FieldsSet pkFields,
								  const PayloadType& payloadType, const TagsMatcher& tm)
		: asyncStorage_(asyncStorage),
		  index_(index),
		  pkFields_(std::move(pkFields)),
		  payloadType_(payloadType),
		  tm_(tm),
		  itemImpl_(payloadType_, tm_) {}

	std::vector<FloatVector> LoadVectorDataFromStorage(const std::vector<PayloadValue>& items);
	Variant GetVector(IdType id, const PayloadValue& item);

private:
	FloatVector loadVectorDataFromStorage(auto&& item);

	const AsyncStorage& asyncStorage_;
	const FloatVectorIndex& index_;
	const FieldsSet pkFields_;
	const PayloadType& payloadType_;
	const TagsMatcher& tm_;
	ItemImpl itemImpl_;

	WrSerializer pkSer_;
	std::string storageItem_;
};

class [[nodiscard]] FloatVectorIndexRawDataInserter {
public:
	explicit FloatVectorIndexRawDataInserter(std::span<const FloatVector> vectorData, Index* pkIndex, size_t vecSizeBytes)
		: vectorData_(vectorData), pkIndex_(pkIndex), vecSizeBytes_(vecSizeBytes) {}

	explicit FloatVectorIndexRawDataInserter(std::span<const FloatVector> vectorData, size_t vecSizeBytes)
		: vectorData_(vectorData), vecSizeBytes_(vecSizeBytes) {}

	IdType operator()(const VariantArray& keys, void* targetVec) const;

private:
	std::span<const FloatVector> vectorData_;
	Index* pkIndex_ = nullptr;
	const size_t vecSizeBytes_;
};

class [[nodiscard]] FloatVectorsGetter {
public:
	explicit FloatVectorsGetter(const NamespaceImpl& ns, IdType rowId, const FieldsSet* oldPkFields = nullptr) noexcept
		: ns_(ns), oldPkFields_(oldPkFields), rowId_(rowId) {}
	FloatVectorsIndexesValues operator()(const PayloadType& pt, const TagsMatcher& tm) const;

private:
	const NamespaceImpl& ns_;
	const FieldsSet* oldPkFields_;
	IdType rowId_;
};
}  // namespace reindexer
