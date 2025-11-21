#include "comporator_distinct_multi.h"

namespace reindexer {

ComparatorDistinctMulti::ComparatorDistinctMulti(
	const PayloadType& payloadType, std::string&& comparatorName, const FieldsSet& fieldNames,
	std::vector<std::variant<std::pair<const void*, KeyValueType>, int, const TagsPath>>&& rawData)
	: name_(std::move(comparatorName)), fieldNames_(fieldNames), payloadType_(payloadType), dataSource_(std::move(rawData)) {}

bool ComparatorDistinctMulti::Compare(const PayloadValue& item, IdType rowId) {
	++totalCalls_;
	ConstPayload pv{payloadType_, item};
	lastData_.rowId = rowId;
	getData(item, lastData_.data, lastData_.maxIndex, rowId);
	bool res = false;
	for (unsigned int i = 0; i < lastData_.maxIndex; i++) {
		const bool isNullValue = DistinctHelpers::GetMultiFieldValue(lastData_.data, i, fieldNames_.size(), rowValues_);
		if (isNullValue) {
			break;
		}
		if (values_.find(rowValues_) == values_.end()) {
			res = true;
			break;
		}
	}
	matchedCount_ += int(res);
	return res;
}

void ComparatorDistinctMulti::ExcludeDistinctValues(const PayloadValue& item, IdType rowId) {
	ConstPayload pv{payloadType_, item};
	if (rowId != lastData_.rowId) {
		getData(item, lastData_.data, lastData_.maxIndex, rowId);
		lastData_.rowId = rowId;
	}
	for (unsigned int i = 0; i < lastData_.maxIndex; i++) {
		DistinctHelpers::FieldsValue rowValues;
		rowValues.reserve(fieldNames_.size());
		const bool isNullValue = DistinctHelpers::GetMultiFieldValue(lastData_.data, i, fieldNames_.size(), rowValues);
		if (isNullValue) {
			continue;
		}
		values_.emplace(std::move(rowValues));
	}
}

void ComparatorDistinctMulti::getData(const PayloadValue& item, std::vector<DistinctHelpers::DataType>& data, size_t& maxArraySize,
									  IdType rowId) {
	data.resize(0);
	data.reserve(fieldNames_.size());
	ConstPayload pv{payloadType_, item};
	maxArraySize = 0;
	for (const auto& d : dataSource_) {
		std::visit(
			overloaded{[&](std::pair<const void*, KeyValueType> raw) {
						   raw.second.EvaluateOneOf(
							   [&](concepts::OneOf<KeyValueType::Bool, KeyValueType::Int64, KeyValueType::Double, KeyValueType::Float,
												   KeyValueType::String, KeyValueType::Int, KeyValueType::Uuid> auto keyValueType) {
								   using ViewType = decltype(keyValueType)::ViewType;
								   const auto* bv = reinterpret_cast<const ViewType*>(raw.first);
								   data.emplace_back(std::span<const ViewType>{bv + rowId, 1}, IsArray_False);
							   },
							   [&](concepts::OneOf<KeyValueType::Null, KeyValueType::Undefined, KeyValueType::Composite,
												   KeyValueType::Tuple, KeyValueType::FloatVector> auto) { assertrx_throw(false); });
						   maxArraySize = std::max(maxArraySize, size_t(1));
					   },
					   [&](int i) {
						   VariantArray b;

						   PayloadFieldValue pfv = pv.Field(i);

						   pfv.t_.Type().EvaluateOneOf(
							   [&](concepts::OneOf<KeyValueType::Bool, KeyValueType::Int64, KeyValueType::Int, KeyValueType::Float,
												   KeyValueType::Double, KeyValueType::String, KeyValueType::Uuid> auto keyValueType) {
								   using PayloadFieldValueType = decltype(keyValueType)::PayloadFieldValueType;
								   auto sp = pv.GetSpan<PayloadFieldValueType>(i);
								   maxArraySize = std::max(maxArraySize, sp.size());
								   data.emplace_back(sp, pfv.t_.IsArray());
							   },
							   [&](concepts::OneOf<KeyValueType::Null, KeyValueType::Undefined, KeyValueType::Composite,
												   KeyValueType::Tuple, KeyValueType::FloatVector> auto) { assertrx_throw(false); });
					   },
					   [&](const TagsPath& t) {
						   VariantArray v;
						   pv.GetByJsonPath(t, v, KeyValueType::Undefined{});
						   maxArraySize = std::max(maxArraySize, size_t(v.size()));
						   IsArray isArray(v.IsArrayValue());
						   data.emplace_back(std::move(v), isArray);
					   }},
			d);
	}
}

ComparatorDistinctMultiArray::ComparatorDistinctMultiArray(const PayloadType& payloadType, std::string&& comparatorName,
														   std::vector<int>&& fieldsIndex)
	: name_(std::move(comparatorName)), payloadType_(payloadType), dataSource_(std::move(fieldsIndex)) {}

void ComparatorDistinctMultiArray::getData(const PayloadValue& item, std::vector<DistinctHelpers::DataType>& data, size_t& maxArraySize) {
	data.resize(0);
	data.reserve(dataSource_.size());
	ConstPayload pv{payloadType_, item};
	maxArraySize = 0;
	for (const auto& d : dataSource_) {
		PayloadFieldValue pfv = pv.Field(d);
		pfv.t_.Type().EvaluateOneOf(
			[&](concepts::OneOf<KeyValueType::Bool, KeyValueType::Int64, KeyValueType::Int, KeyValueType::Float, KeyValueType::Double,
								KeyValueType::String, KeyValueType::Uuid> auto keyValueType) {
				using PayloadFieldValueType = decltype(keyValueType)::PayloadFieldValueType;
				auto sp = pv.GetArray<PayloadFieldValueType>(d);
				maxArraySize = std::max(maxArraySize, sp.size());
				data.emplace_back(sp, IsArray_True);
			},
			[&](concepts::OneOf<KeyValueType::Null, KeyValueType::Undefined, KeyValueType::Composite, KeyValueType::Tuple,
								KeyValueType::FloatVector> auto) { assertrx_throw(false); });
	}
}

bool ComparatorDistinctMultiArray::Compare(const PayloadValue& item, IdType rowId) {
	++totalCalls_;
	ConstPayload pv{payloadType_, item};
	lastData_.rowId = rowId;
	getData(item, lastData_.data, lastData_.maxIndex);
	bool res = false;
	for (unsigned int i = 0; i < lastData_.maxIndex; i++) {
		const bool isNullValue = DistinctHelpers::GetMultiFieldValue(lastData_.data, i, dataSource_.size(), rowValues_);
		if (isNullValue) {
			break;
		}
		if (values_.find(rowValues_) == values_.end()) {
			res = true;
			break;
		}
	}
	matchedCount_ += int(res);
	return res;
}

void ComparatorDistinctMultiArray::ExcludeDistinctValues(const PayloadValue& item, IdType rowId) {
	if (rowId != lastData_.rowId) {
		getData(item, lastData_.data, lastData_.maxIndex);
		lastData_.rowId = rowId;
	}
	for (unsigned int i = 0; i < lastData_.maxIndex; i++) {
		DistinctHelpers::FieldsValue rowValues;
		rowValues.reserve(dataSource_.size());
		const bool isNullValue = DistinctHelpers::GetMultiFieldValue(lastData_.data, i, dataSource_.size(), rowValues);
		if (isNullValue) {
			continue;
		}
		values_.emplace(std::move(rowValues));
	}
}

}  // namespace reindexer
