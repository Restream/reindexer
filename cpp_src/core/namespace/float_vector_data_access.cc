#include "float_vector_data_access.h"
#include "core/index/float_vector/float_vector_index.h"
#include "core/keyvalue/float_vector.h"
#include "core/namespace/asyncstorage.h"
#include "core/namespace/namespaceimpl.h"
#include "core/payload/payloadiface.h"
#include "core/rdxcontext.h"
#include "core/storage/storage_prefixes.h"

namespace reindexer {

FloatVectorExtractor::FloatVectorExtractor(const AsyncStorage& asyncStorage, const FloatVectorIndex& index, FieldsSet pkFields,
										   const PayloadType& payloadType, const TagsMatcher& tm)
	: asyncStorage_(asyncStorage),
	  payloadType_(payloadType),
	  index_(index),
	  indexField_(payloadType_.FieldByName(index_.Name())),
	  pkFields_(std::move(pkFields)),
	  tm_(tm),
	  itemImpl_(payloadType_, tm_) {}

std::vector<h_vector<FloatVector, 1>> FloatVectorExtractor::LoadVectorDataFromStorage(const std::vector<PayloadValue>& items) {
	std::vector<h_vector<FloatVector, 1>> res;
	res.reserve(items.size());
	for (const auto& item : items) {
		auto& addedVect = res.emplace_back();
		if (item.IsFree()) {
			continue;
		}
		loadItem(item);
		const auto arraySize = itemImpl_.GetFieldLen(indexField_);
		addedVect.reserve(arraySize);
		for (size_t i = 0; i < arraySize; ++i) {
			Variant vectorField = itemImpl_.GetField(indexField_, i);
			assertrx(vectorField.Type().Is<KeyValueType::FloatVector>());
			const auto vecView = ConstFloatVectorView(vectorField);
			if (!vecView.IsEmpty()) {
				assertrx(vecView.Dimension() == index_.Dimension());
				addedVect.emplace_back(vecView);
			} else {
				addedVect.emplace_back();
			}
		}
	}
	return res;
}

Variant FloatVectorExtractor::GetVector(FloatVectorId id, const PayloadValue& item) {
	if (auto fvView = index_.getFloatVectorView(id); !fvView.IsStripped()) {
		return Variant(fvView);
	} else if (item.IsFree()) {
		return Variant{ConstFloatVectorView{}};
	} else {
		return Variant{loadVectorFromStorage(item, id.ArrayIndex())};
	}
}

h_vector<Variant, 1> FloatVectorExtractor::GetVectorArray(IdType rowId, size_t arraySize, const PayloadValue& item) {
	h_vector<Variant, 1> res;
	if (item.IsFree() || !arraySize) {
		return res;
	}

	ConstPayload pl(payloadType_, item);
	res.reserve(arraySize);
	unsigned i = 0;
	for (; i < arraySize; ++i) {
		auto fvView = index_.getFloatVectorView({rowId, i});
		if (fvView.IsStripped()) {
			break;
		} else if (!fvView.IsEmpty()) {
			assertrx(fvView.Dimension() == index_.Dimension());
			res.emplace_back(FloatVector{fvView});
		} else {
			res.emplace_back(ConstFloatVectorView{});
		}
	}
	if (i != arraySize) {
		loadItem(item);
		for (; i < arraySize; ++i) {
			Variant vectorField = itemImpl_.GetField(indexField_, i);
			assertrx(vectorField.Type().Is<KeyValueType::FloatVector>());
			ConstFloatVectorView fvView = ConstFloatVectorView(vectorField);
			assertrx_dbg(!fvView.IsStripped());
			if (!fvView.IsEmpty()) {
				assertrx(fvView.Dimension() == index_.Dimension());
				res.emplace_back(FloatVector{fvView});
			} else {
				res.emplace_back(ConstFloatVectorView{});
			}
		}
	}
	return res;
}

void FloatVectorExtractor::loadItem(const PayloadValue& item) {
	assertrx_throw(asyncStorage_.IsValid());
	ConstPayload pl(payloadType_, item);

	pkSer_.Reset();
	pkSer_ << kRxStorageItemPrefix;
	pl.SerializeFields(pkSer_, pkFields_);
	if (auto err = asyncStorage_.Read(StorageOpts{}, pkSer_.Slice(), storageItem_); !err.ok()) {
		throw Error(err.code(), "Error while searching for an item with pk='{}' in the storage: {}", pkSer_.Slice(), err.what());
	}

	itemImpl_.Unsafe(true);
	itemImpl_.FromCJSON(storageItem_.substr(sizeof(int64_t)));	// skip lsn (int64_t)
}

FloatVector FloatVectorExtractor::loadVectorFromStorage(const PayloadValue& item, size_t arrayIndex) {
	loadItem(item);
	Variant vectorField = itemImpl_.GetField(indexField_, arrayIndex);
	assertrx(vectorField.Type().Is<KeyValueType::FloatVector>());
	ConstFloatVectorView vecView = ConstFloatVectorView(vectorField);
	if (!vecView.IsEmpty()) {
		assertrx(vecView.Dimension() == index_.Dimension());
		return FloatVector{vecView};
	} else {
		return FloatVector{};
	}
}

IdType FloatVectorIndexRawDataInserter::operator()(Variant&& key, uint32_t arrIdx, void* targetVec) const {
	static const Index::SelectContext kDummySelectContext;
	static const RdxContext kDummyRdxContext;
	auto select = [this](Variant&& key) {
		auto res = pkIndex_
					   ->SelectKey({key.convert(pkIndex_->SelectKeyType(), &pkIndex_->GetPayloadType(), &pkIndex_->Fields())}, CondEq, 0,
								   kDummySelectContext, kDummyRdxContext)
					   .Front();
		if (res.empty() || res[0].ids_.empty()) {
			throw Error(errLogic, "Requested PK does not exist");
		}
		return res[0].ids_[0];
	};

	const auto rowId = pkIndex_ ? select(std::move(key)) : IdType::FromNumber(key.As<int>());
	assertrx_dbg(vectorsData_.size() > size_t(rowId.ToNumber()));
	const auto& vectors = vectorsData_[rowId.ToNumber()];
	assertrx_dbg(arrIdx < vectors.size());
	std::memcpy(targetVec, vectors[arrIdx].RawData(), vecSizeBytes_);
	return rowId;
}

FloatVectorsIndexesValues FloatVectorsGetter::operator()(const PayloadType& pt, const PayloadValue& pv, const TagsMatcher& tm) const {
	auto vectorIndexes = ns_.getVectorIndexes(pt);
	FloatVectorsIndexesValues res;
	res.reserve(vectorIndexes.size());

	ConstPayload pl{pt, pv};
	for (auto& data : vectorIndexes) {
		res.emplace_back(std::move(data), ns_.getFloatVectorArray(rowId_, pl.GetFieldLen(data.ptField), *data.ptr, &pt, &tm, oldPkFields_));
	}

	return res;
}

}  // namespace reindexer
