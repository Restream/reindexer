#pragma once

#include "core/nsselecter/distincthelpers.h"
#include "core/type_consts.h"
#include "estl/fast_hash_set.h"

namespace reindexer {

class [[nodiscard]] ComparatorDistinctMulti {
public:
	ComparatorDistinctMulti(const PayloadType& payloadType, const FieldsSet& fN,
							std::vector<std::variant<std::pair<const void*, KeyValueType>, int, const TagsPath>>&& rawData);
	ComparatorDistinctMulti(ComparatorDistinctMulti&& v) = default;
	ComparatorDistinctMulti& operator=(ComparatorDistinctMulti&&) = default;
	ComparatorDistinctMulti(const ComparatorDistinctMulti& v);
	[[nodiscard]] const std::string& Name() const& noexcept { return name_; }
	auto Name() const&& = delete;
	[[nodiscard]] std::string ConditionStr() const;
	[[nodiscard]] std::string Dump() const { return Name(); }
	[[nodiscard]] int GetMatchedCount() const noexcept { return 0; }
	[[nodiscard]] double Cost(double expectedIterations) const noexcept { return expectedIterations + 1.0; }
	void SetNotOperationFlag(bool isNotOperation) noexcept { (void)isNotOperation; }

	[[nodiscard]] bool Compare(const PayloadValue& item, IdType rowId);

	void ClearDistinctValues() noexcept;

	void ExcludeDistinctValues(const PayloadValue& item, IdType rowId);

	[[nodiscard]] bool IsDistinct() const noexcept { return true; }

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
	std::unique_ptr<SetType> values_;
	PayloadType payloadType_;
	DistinctHelpers::FieldsValue rowValues_;
	std::vector<std::variant<std::pair<const void*, KeyValueType>, int, const TagsPath>> dataSource_;

	enum class IsArray { NotSet, True, False } isArray_ = IsArray::NotSet;
	void checkFieldArray(unsigned i, bool isArrayVariant);
};

}  // namespace reindexer
