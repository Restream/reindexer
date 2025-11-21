#pragma once

#include "core/nsselecter/distincthelpers.h"
#include "core/type_consts.h"
#include "estl/fast_hash_set.h"

namespace reindexer {

using SetType = fast_hash_set<DistinctHelpers::FieldsValue, DistinctHelpers::DistinctHasher<DistinctHelpers::IsCompositeSupported::No>,
							  DistinctHelpers::CompareVariantVector<DistinctHelpers::IsCompositeSupported::No>,
							  DistinctHelpers::LessDistinctVector<DistinctHelpers::IsCompositeSupported::No>>;

class [[nodiscard]] ComparatorDistinctMulti {
public:
	ComparatorDistinctMulti(const PayloadType& payloadType, std::string&& comparatorName, const FieldsSet& fieldNames,
							std::vector<std::variant<std::pair<const void*, KeyValueType>, int, const TagsPath>>&& rawData);
	const std::string& Name() const& noexcept { return name_; }
	auto Name() const&& = delete;
	std::string Dump() const { return Name(); }
	int GetMatchedCount(bool invert) const noexcept {
		assertrx_dbg(totalCalls_ >= matchedCount_);
		return invert ? (totalCalls_ - matchedCount_) : matchedCount_;
	}
	double Cost(double expectedIterations) const noexcept { return expectedIterations + 2.0; }
	bool Compare(const PayloadValue& item, IdType rowId);
	void ExcludeDistinctValues(const PayloadValue& item, IdType rowId);
	reindexer::IsDistinct IsDistinct() const noexcept { return IsDistinct_True; }
	bool IsIndexed() const noexcept {
		for (const auto& f : fieldNames_) {
			if (f == IndexValueType::SetByJsonPath) {
				return false;
			}
		}
		return true;
	}

private:
	void getData(const PayloadValue& item, std::vector<DistinctHelpers::DataType>& data, size_t& maxArraySize, IdType rowId);

	struct [[nodiscard]] ItemData {
		std::vector<DistinctHelpers::DataType> data;
		IdType rowId = -1;
		size_t maxIndex = 0;
	};

	ItemData lastData_;
	std::string name_;
	FieldsSet fieldNames_;
	int totalCalls_{0};
	int matchedCount_{0};
	SetType values_;
	PayloadType payloadType_;
	DistinctHelpers::FieldsValue rowValues_;
	std::vector<std::variant<std::pair<const void*, KeyValueType>, int, const TagsPath>> dataSource_;
};

template <typename GetterT>
class [[nodiscard]] ComparatorDistinctMultiScalarBase {
public:
	ComparatorDistinctMultiScalarBase(std::string&& comparatorName, GetterT&& getter)
		: name_(std::move(comparatorName)), getter_(std::move(getter)) {}
	const std::string& Name() const& noexcept { return name_; }
	auto Name() const&& = delete;
	std::string Dump() const { return Name(); }
	int GetMatchedCount(bool invert) const noexcept {
		assertrx_dbg(totalCalls_ >= matchedCount_);
		return invert ? (totalCalls_ - matchedCount_) : matchedCount_;
	}
	double Cost(double expectedIterations) const noexcept { return expectedIterations + 2.0; }
	bool Compare(const PayloadValue& item, IdType rowId) {
		++totalCalls_;
		lastData_.rowId = rowId;
		getter_.GetData(item, rowId, lastData_.data);
		const bool res = values_.find(lastData_.data) == values_.end();
		matchedCount_ += int(res);
		return res;
	}
	void ExcludeDistinctValues(const PayloadValue& item, IdType rowId) {
		if (rowId != lastData_.rowId) {
			getter_.GetData(item, rowId, lastData_.data);
			lastData_.rowId = rowId;
		}
		values_.emplace(lastData_.data);
	}
	reindexer::IsDistinct IsDistinct() const noexcept { return IsDistinct_True; }
	bool IsIndexed() const noexcept { return true; }

private:
	std::string name_;
	int totalCalls_{0};
	int matchedCount_{0};
	SetType values_;
	struct [[nodiscard]] ItemData {
		DistinctHelpers::FieldsValue data;
		IdType rowId = -1;
	};

	ItemData lastData_;
	GetterT getter_;
};

class [[nodiscard]] ComparatorDistinctMultiIndexedGetter {
public:
	ComparatorDistinctMultiIndexedGetter(const PayloadType& payloadType, std::vector<int>&& fieldIndex)
		: payloadType_(payloadType), dataSource_(std::move(fieldIndex)) {}
	void RX_ALWAYS_INLINE GetData(const PayloadValue& item, [[maybe_unused]] IdType rowId, h_vector<Variant, 2>& data) {
		data.resize(0);
		data.reserve(dataSource_.size());
		ConstPayload pv{payloadType_, item};
		for (const int d : dataSource_) {
			PayloadFieldValue pfv = pv.Field(d);
			data.emplace_back(pfv.Get());
		}
	}

private:
	PayloadType payloadType_;
	std::vector<int> dataSource_;
};

class [[nodiscard]] ComparatorDistinctMultiColumnGetter {
public:
	ComparatorDistinctMultiColumnGetter(std::vector<std::pair<const void*, KeyValueType>>&& ds) : dataSource_(std::move(ds)) {}
	void RX_ALWAYS_INLINE GetData([[maybe_unused]] const PayloadValue& item, IdType rowId, h_vector<Variant, 2>& data) {
		data.resize(0);
		data.reserve(dataSource_.size());
		for (const auto& d : dataSource_) {
			d.second.EvaluateOneOf(
				[&](concepts::OneOf<KeyValueType::Bool, KeyValueType::Int64, KeyValueType::Double, KeyValueType::Float, KeyValueType::Int,
									KeyValueType::Uuid> auto keyValueType) {
					using ViewType = decltype(keyValueType)::ViewType;
					const auto* bv = reinterpret_cast<const ViewType*>(d.first);
					data.emplace_back(*(bv + rowId));
				},
				[&](concepts::OneOf<KeyValueType::String> auto keyValueType) {
					using ViewType = decltype(keyValueType)::ViewType;
					const auto* bv = reinterpret_cast<const ViewType*>(d.first);
					data.emplace_back(p_string(bv + rowId));
				},
				[&](concepts::OneOf<KeyValueType::Null, KeyValueType::Undefined, KeyValueType::Composite, KeyValueType::Tuple,
									KeyValueType::FloatVector> auto) { assertrx_throw(false); });
		}
	}

private:
	std::vector<std::pair<const void*, KeyValueType>> dataSource_;
};

class [[nodiscard]] ComparatorDistinctMultiScalarGetter {
public:
	ComparatorDistinctMultiScalarGetter(const PayloadType& payloadType,
										std::vector<std::variant<std::pair<const void*, KeyValueType>, int, const TagsPath>>&& ds)
		: payloadType_(payloadType), dataSource_(std::move(ds)) {}
	void RX_ALWAYS_INLINE GetData(const PayloadValue& item, IdType rowId, h_vector<Variant, 2>& data) {
		data.resize(0);
		data.reserve(dataSource_.size());
		ConstPayload pv{payloadType_, item};
		for (const auto& d : dataSource_) {
			std::visit(
				overloaded{[&](std::pair<const void*, KeyValueType> raw) {
							   raw.second.EvaluateOneOf(
								   [&](concepts::OneOf<KeyValueType::Bool, KeyValueType::Int64, KeyValueType::Double, KeyValueType::Float,
													   KeyValueType::Int, KeyValueType::Uuid> auto keyValueType) {
									   using ViewType = decltype(keyValueType)::ViewType;
									   const auto* bv = reinterpret_cast<const ViewType*>(raw.first);
									   data.emplace_back(*(bv + rowId));
								   },
								   [&](concepts::OneOf<KeyValueType::String> auto keyValueType) {
									   using ViewType = decltype(keyValueType)::ViewType;
									   const auto* bv = reinterpret_cast<const ViewType*>(raw.first);
									   data.emplace_back(p_string(bv + rowId));
								   },

								   [&](concepts::OneOf<KeyValueType::Null, KeyValueType::Undefined, KeyValueType::Composite,
													   KeyValueType::Tuple, KeyValueType::FloatVector> auto) { assertrx_throw(false); });
						   },
						   [&](int i) {
							   PayloadFieldValue pfv = pv.Field(i);
							   data.emplace_back(pfv.Get());
						   },
						   [&](const TagsPath&) { assertrx_throw(false); }},
				d);
		}
	}

private:
	PayloadType payloadType_;
	std::vector<std::variant<std::pair<const void*, KeyValueType>, int, const TagsPath>> dataSource_;
};

class [[nodiscard]] ComparatorDistinctMultiArray {
public:
	ComparatorDistinctMultiArray(const PayloadType& payloadType, std::string&& comparatorName, std::vector<int>&& fieldsIndex);
	const std::string& Name() const& noexcept { return name_; }
	auto Name() const&& = delete;
	std::string Dump() const { return Name(); }
	int GetMatchedCount(bool invert) const noexcept {
		assertrx_dbg(totalCalls_ >= matchedCount_);
		return invert ? (totalCalls_ - matchedCount_) : matchedCount_;
	}
	double Cost(double expectedIterations) const noexcept { return expectedIterations + 2.0; }
	bool Compare(const PayloadValue& item, IdType rowId);
	void ExcludeDistinctValues(const PayloadValue& item, IdType rowId);
	reindexer::IsDistinct IsDistinct() const noexcept { return IsDistinct_True; }
	bool IsIndexed() const noexcept { return true; }

private:
	void getData(const PayloadValue& item, std::vector<DistinctHelpers::DataType>& data, size_t& maxArraySize);

	std::string name_;
	int totalCalls_{0};
	int matchedCount_{0};
	struct [[nodiscard]] ItemData {
		std::vector<DistinctHelpers::DataType> data;
		IdType rowId = -1;
		size_t maxIndex = 0;
	};

	ItemData lastData_;
	SetType values_;
	PayloadType payloadType_;
	DistinctHelpers::FieldsValue rowValues_;
	std::vector<int> dataSource_;
};

}  // namespace reindexer
