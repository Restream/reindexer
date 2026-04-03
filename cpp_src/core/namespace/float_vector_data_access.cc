#include "float_vector_data_access.h"
#include "core/index/float_vector/float_vector_index.h"
#include "core/namespace/asyncstorage.h"
#include "core/namespace/namespaceimpl.h"
#include "core/rdxcontext.h"
#include "core/storage/storage_prefixes.h"

namespace reindexer {

std::vector<FloatVector> FloatVectorExtractor::LoadVectorDataFromStorage(const std::vector<PayloadValue>& items) {
	std::vector<FloatVector> res;
	res.reserve(items.size());
	for (const auto& item : items) {
		res.emplace_back(loadVectorDataFromStorage(item));
	}
	return res;
}

Variant FloatVectorExtractor::GetVector(IdType id, const PayloadValue& item) {
	if (auto fvView = index_.getFloatVectorView(id); !fvView.IsStripped()) {
		return Variant(fvView);
	} else {
		return Variant{loadVectorDataFromStorage(item)};
	}
}

FloatVector FloatVectorExtractor::loadVectorDataFromStorage(auto&& item) {
	assertrx_throw(asyncStorage_.IsValid());

	if (item.IsFree()) {
		return {};
	}

	ConstPayload pl(payloadType_, item);

	pkSer_.Reset();
	pkSer_ << kRxStorageItemPrefix;
	pl.SerializeFields(pkSer_, pkFields_);
	if (auto err = asyncStorage_.Read(StorageOpts{}, pkSer_.Slice(), storageItem_); !err.ok()) {
		throw Error(err.code(), "Error while searching for an item with pk='{}' in the storage: {}", pkSer_.Slice(), err.what());
	}

	itemImpl_.Unsafe(true);
	itemImpl_.FromCJSON(storageItem_.substr(sizeof(int64_t)));	// skip lsn (int64_t)

	Variant vectorField = itemImpl_.GetField(payloadType_->FieldByName(index_.Name()));
	assertrx(vectorField.Type().Is<KeyValueType::FloatVector>());
	ConstFloatVectorView vecView = ConstFloatVectorView(vectorField);
	assertrx(vecView.Dimension() == index_.Dimension());

	return FloatVector{vecView};
}

IdType FloatVectorIndexRawDataInserter::operator()(Variant&& key, void* targetVec) const {
	static const Index::SelectContext kDummySelectContext;
	static const RdxContext kDummyRdxContext;
	auto select = [this](Variant&& key) {
		auto res = pkIndex_
					   ->SelectKey({key.convert(pkIndex_->SelectKeyType(), &pkIndex_->GetPayloadType(), &pkIndex_->Fields())}, CondEq, 0,
								   kDummySelectContext, kDummyRdxContext)
					   .Front();
		if (res[0].ids_.empty()) {
			throw Error(errLogic, "Requested PK does not exist");
		}
		return res[0].ids_[0];
	};

	const auto id = pkIndex_ ? select(std::move(key)) : IdType::FromNumber(key.As<int>());
	std::memcpy(targetVec, vectorData_[id.ToNumber()].RawData(), vecSizeBytes_);
	return id;
}

FloatVectorsIndexesValues FloatVectorsGetter::operator()(const PayloadType& pt, const TagsMatcher& tm) const {
	auto vectorIndexes = ns_.getVectorIndexes(pt);
	FloatVectorsIndexesValues res;
	res.reserve(vectorIndexes.size());

	for (auto& data : vectorIndexes) {
		res.emplace_back(std::move(data), ns_.getFloatVector(rowId_, *data.ptr, &pt, &tm, oldPkFields_));
	}

	return res;
}

}  // namespace reindexer