#pragma once
#include <core/cjson/tagsmatcher.h>
#include <core/type_consts.h>

namespace reindexer {
class NamespaceImpl;
class Index;
class FieldsSet;

class NsSelectFuncInterface {
public:
	explicit NsSelectFuncInterface(const NamespaceImpl& nm) noexcept : nm_(nm) {}
	const std::string& GetName() const noexcept;
	int getIndexByName(std::string_view index) const noexcept;
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
