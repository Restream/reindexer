#pragma once
#include <core/cjson/tagsmatcher.h>
#include <core/type_consts.h>
#include <string>

namespace reindexer {
class NamespaceImpl;
class Index;
class FieldsSet;

class NsSelectFuncInterface {
public:
	NsSelectFuncInterface(const NamespaceImpl& nm) : nm_(nm) {}
	const std::string& GetName() const;
	int getIndexByName(const std::string& index) const;
	bool getIndexByName(const std::string& name, int& index) const;
	int getIndexesCount() const;
	const std::string& getIndexName(int id) const;
	IndexType getIndexType(int id) const;
	const FieldsSet& getIndexFields(int id) const;
	TagsPath getTagsPathForField(const std::string& jsonPath) const;

private:
	const NamespaceImpl& nm_;
};
}  // namespace reindexer
