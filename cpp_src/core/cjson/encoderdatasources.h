#pragma once

#include "baseencoder.h"

namespace reindexer {

template <typename Builder>
class [[nodiscard]] IJoinsDatasource {
public:
	IJoinsDatasource() = default;
	virtual ~IJoinsDatasource() = default;

	virtual size_t GetFieldsCount() const noexcept = 0;
	virtual size_t GetRowItemsCount(size_t rowId) const = 0;
	virtual ConstPayload GetItemPayload(size_t rowid, size_t plIndex) = 0;
	virtual const std::string& GetItemNamespace(size_t rowid) & noexcept = 0;
	virtual const TagsMatcher& GetItemTagsMatcher(size_t rowid) & noexcept = 0;
	virtual const FieldsFilter& GetItemFieldsFilter(size_t rowid) & noexcept = 0;
	virtual EncoderContext<Builder> BuildFieldJoinsDatasourceContext(size_t /*field*/, size_t /*rowId*/) = 0;

	auto GetItemNamespace(size_t) && = delete;
	auto GetItemTagsMatcher(size_t) && = delete;
	auto GetItemFieldsFilter(size_t) && = delete;
};

}  // namespace reindexer
