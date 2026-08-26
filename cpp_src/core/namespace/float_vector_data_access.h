#pragma once

#include "core/cjson/tagsmatcher.h"
#include "core/id_type.h"
#include "core/itemimpl.h"
#include "core/keyvalue/float_vector.h"
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
								  const PayloadType& payloadType, const TagsMatcher& tm);

	std::vector<h_vector<FloatVector, 1>> LoadVectorDataFromStorage(const std::vector<PayloadValue>& items);
	Variant GetVector(FloatVectorId id, const PayloadValue& item);
	h_vector<Variant, 1> GetVectorArray(IdType rowId, size_t arrSize, const PayloadValue& item);

private:
	FloatVector loadVectorFromStorage(const PayloadValue&, size_t arrayIndex);
	void loadItem(const PayloadValue&);

	const AsyncStorage& asyncStorage_;
	const PayloadType& payloadType_;
	const FloatVectorIndex& index_;
	const int indexField_;
	const FieldsSet pkFields_;
	const TagsMatcher& tm_;
	ItemImpl itemImpl_;

	WrSerializer pkSer_;
	std::string storageItem_;
};

class [[nodiscard]] FloatVectorIndexRawDataInserter {
public:
	explicit FloatVectorIndexRawDataInserter(std::span<const h_vector<FloatVector, 1>> vectorData, Index& pkIndex,
											 size_t vecSizeBytes) noexcept
		: vectorsData_(vectorData), pkIndex_(&pkIndex), vecSizeBytes_(vecSizeBytes) {}

	explicit FloatVectorIndexRawDataInserter(std::span<const h_vector<FloatVector, 1>> vectorData, size_t vecSizeBytes) noexcept
		: vectorsData_(vectorData), vecSizeBytes_(vecSizeBytes) {}

	IdType operator()(Variant&& keys, uint32_t arrIdx, void* targetVec) const;

private:
	std::span<const h_vector<FloatVector, 1>> vectorsData_{};
	Index* pkIndex_ = nullptr;
	const size_t vecSizeBytes_;
};

class [[nodiscard]] FloatVectorsGetter {
public:
	explicit FloatVectorsGetter(const NamespaceImpl& ns, IdType rowId, const FieldsSet* oldPkFields = nullptr) noexcept
		: ns_(ns), oldPkFields_(oldPkFields), rowId_(rowId) {}
	FloatVectorsIndexesValues operator()(const PayloadType&, const PayloadValue&, const TagsMatcher&) const;

private:
	const NamespaceImpl& ns_;
	const FieldsSet* oldPkFields_;
	IdType rowId_;
};

}  // namespace reindexer
