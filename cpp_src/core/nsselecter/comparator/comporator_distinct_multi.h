#pragma once

#include "core/nsselecter/distincthelpers.h"
#include "core/type_consts.h"
#include "estl/fast_hash_set.h"

namespace reindexer {

class [[nodiscard]] ComparatorDistinctMulti {
public:
	ComparatorDistinctMulti(const PayloadType& payloadType, const FieldsSet& fieldNames,
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

	bool IsDistinct() const noexcept { return true; }

private:
	void getData(const PayloadValue& item, std::vector<DistinctHelpers::DataType>& data, size_t& maxArraySize, IdType rowId);

	struct [[nodiscard]] ItemData {
		std::vector<DistinctHelpers::DataType> data;
		IdType rowId = -1;
		size_t maxIndex = 0;
	};

	ItemData lastData_;
	std::string name_;
	FieldsSet fNames_;

	using SetType = fast_hash_set<DistinctHelpers::FieldsValue, DistinctHelpers::DistinctHasher<DistinctHelpers::IsCompositeSupported::No>,
								  DistinctHelpers::CompareVariantVector<DistinctHelpers::IsCompositeSupported::No>,
								  DistinctHelpers::LessDistinctVector<DistinctHelpers::IsCompositeSupported::No>>;
	int totalCalls_{0};
	int matchedCount_{0};
	SetType values_;
	PayloadType payloadType_;
	DistinctHelpers::FieldsValue rowValues_;
	std::vector<std::variant<std::pair<const void*, KeyValueType>, int, const TagsPath>> dataSource_;
};

}  // namespace reindexer
