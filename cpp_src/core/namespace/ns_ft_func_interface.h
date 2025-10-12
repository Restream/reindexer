#pragma once

#include <core/cjson/tagspath.h>
#include <core/type_consts.h>

namespace reindexer {

class NamespaceImpl;
class Index;
class FieldsSet;

class [[nodiscard]] NsFtFuncInterface {
public:
	explicit NsFtFuncInterface(const NamespaceImpl& nm) noexcept : nm_(nm) {}
	bool getIndexByName(std::string_view name, int& index) const noexcept;
	int getIndexesCount() const noexcept;
	const std::string& getIndexName(int id) const noexcept;
	IndexType getIndexType(int id) const noexcept;
	const FieldsSet& getIndexFields(int id) const noexcept;
	TagsPath getTagsPathForField(std::string_view jsonPath) const noexcept;

private:
	const NamespaceImpl& nm_;
};

}  // namespace reindexer
