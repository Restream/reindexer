#include "comporator_distinct_multi.h"

namespace reindexer {

ComparatorDistinctMulti::ComparatorDistinctMulti(
	const PayloadType& payloadType, const FieldsSet& fN,
	std::vector<std::variant<std::pair<const void*, KeyValueType>, int, const TagsPath> >&& rawData)
	: fNames_(fN), values_{std::make_unique<SetType>()}, payloadType_(payloadType), dataSource_(std::move(rawData)) {
	unsigned fieldIndex = 0;
	for (const auto& f : fNames_) {
		if (f != IndexValueType::SetByJsonPath) {
			checkFieldArray(fieldIndex, bool(payloadType.Field(f).IsArray()));
		}
		++fieldIndex;
	}
}

ComparatorDistinctMulti::ComparatorDistinctMulti(const ComparatorDistinctMulti& v)
	: lastData_(v.lastData_),
	  name_(v.name_),
	  fNames_(v.fNames_),
	  values_{v.values_ ? std::make_unique<SetType>(*v.values_) : std::make_unique<SetType>()},
	  payloadType_(v.payloadType_) {}

bool ComparatorDistinctMulti::Compare(const PayloadValue& item, IdType rowId) {
	ConstPayload pv{payloadType_, item};
	lastData_.rowId = rowId;
	getData(item, lastData_.data, lastData_.maxIndex, rowId);
	for (unsigned int i = 0; i < lastData_.maxIndex; i++) {
		const bool isNullValue = DistinctHelpers::GetMultiFieldValue(lastData_.data, i, fNames_.size(), rowValues_);
		if (isNullValue) {
			return false;
		}
		if (values_->find(rowValues_) == values_->end()) {
			return true;
		}
	}
	return false;
}

void ComparatorDistinctMulti::ClearDistinctValues() noexcept { values_->clear(); }

void ComparatorDistinctMulti::ExcludeDistinctValues(const PayloadValue& item, IdType rowId) {
	ConstPayload pv{payloadType_, item};
	if (rowId != lastData_.rowId) {
		getData(item, lastData_.data, lastData_.maxIndex, rowId);
	}
	for (unsigned int i = 0; i < lastData_.maxIndex; i++) {
		DistinctHelpers::FieldsValue rowValues;
		rowValues.reserve(fNames_.size());
		const bool isNullValue = DistinctHelpers::GetMultiFieldValue(lastData_.data, i, fNames_.size(), rowValues);
		if (isNullValue) {
			continue;
		}
		values_->emplace(std::move(rowValues));
	}
}

void ComparatorDistinctMulti::getData(const PayloadValue& item, std::vector<DistinctHelpers::DataType>& data, size_t& maxArraySize,
									  IdType rowId) {
	data.resize(0);
	data.reserve(fNames_.size());
	ConstPayload pv{payloadType_, item};
	maxArraySize = 0;
	const bool resetIsArray = isArray_ == IsArray::NotSet ? true : false;
	unsigned fieldIndex = 0;
	for (const auto& d : dataSource_) {
		DistinctHelpers::DataType dt;

		std::visit(overloaded{[&](std::pair<const void*, KeyValueType> raw) {
								  raw.second.EvaluateOneOf(
									  [&](KeyValueType::Bool) {
										  const bool* bv = reinterpret_cast<const bool*>(raw.first);
										  dt = std::span<const bool>(bv + rowId, 1);
									  },
									  [&](KeyValueType::Int64) {
										  const int64_t* bv = reinterpret_cast<const int64_t*>(raw.first);
										  dt = std::span<const int64_t>(bv + rowId, 1);
									  },
									  [&](KeyValueType::Double) {
										  const double* bv = reinterpret_cast<const double*>(raw.first);
										  dt = std::span<const double>(bv + rowId, 1);
									  },
									  [&](KeyValueType::Float) {
										  const float* bv = reinterpret_cast<const float*>(raw.first);
										  dt = std::span<const float>(bv + rowId, 1);
									  },
									  [&](KeyValueType::String) {
										  const std::string_view* bv = reinterpret_cast<const std::string_view*>(raw.first);
										  dt = std::span<const std::string_view>(bv + rowId, 1);
									  },
									  [&](KeyValueType::Int) {
										  const int32_t* bv = reinterpret_cast<const int32_t*>(raw.first);
										  dt = std::span<const int32_t>(bv + rowId, 1);
									  },
									  [&](KeyValueType::Uuid) {
										  const Uuid* bv = reinterpret_cast<const Uuid*>(raw.first);
										  dt = std::span<const Uuid>(bv + rowId, 1);
									  },
									  [&](OneOf<KeyValueType::Null, KeyValueType::Undefined, KeyValueType::Composite, KeyValueType::Tuple,
												KeyValueType::FloatVector>) { assertrx_throw(false); });
							  },
							  [&](int i) {
								  VariantArray b;
								  PayloadFieldValue pfv = pv.Field(i);
								  pfv.t_.Type().EvaluateOneOf(
									  [&](KeyValueType::Bool) { dt = pv.GetSpan<bool>(i); },
									  [&](KeyValueType::Int64) { dt = pv.GetSpan<int64_t>(i); },
									  [&](KeyValueType::Double) { dt = pv.GetSpan<double>(i); },
									  [&](KeyValueType::Float) { dt = pv.GetSpan<float>(i); },
									  [&](KeyValueType::String) { dt = pv.GetSpan<p_string>(i); },
									  [&](KeyValueType::Int) { dt = pv.GetSpan<int32_t>(i); },
									  [&](KeyValueType::Uuid) { dt = pv.GetSpan<Uuid>(i); },
									  [&](OneOf<KeyValueType::Null, KeyValueType::Undefined, KeyValueType::Composite, KeyValueType::Tuple,
												KeyValueType::FloatVector>) { assertrx_throw(false); });
							  },
							  [&](const TagsPath& t) {
								  VariantArray v;
								  pv.GetByJsonPath(t, v, KeyValueType::Undefined{});
								  if (!v.empty()) {
									  checkFieldArray(fieldIndex, v.IsArrayValue());
								  }

								  dt = std::move(v);
							  }},
				   d);
		std::visit([&](const auto& a) { maxArraySize = std::max(maxArraySize, size_t(a.size())); }, dt);
		data.emplace_back(std::move(dt));
		++fieldIndex;
	}
	if rx_unlikely (resetIsArray) {
		isArray_ = IsArray::NotSet;
	}
}

void ComparatorDistinctMulti::checkFieldArray(unsigned int i, bool isArrayVariant) {
	const IsArray isArrayCur = isArrayVariant ? IsArray::True : IsArray::False;
	if (isArray_ == IsArray::NotSet) {
		isArray_ = isArrayCur;
	} else if (isArray_ != isArrayCur) {
		throw Error(errQueryExec, "Fields in Distinct can be either arrays or not. Field number {}", i);
	}
}

}  // namespace reindexer
