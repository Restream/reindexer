#pragma once
#include <core/cjson/tagsmatcher.h>
#include <core/type_consts.h>
#include <string>

namespace reindexer {
class NamespaceImpl;
class Index;
using std::string;
class FieldsSet;

class NsSelectFuncInterface {
public:
	NsSelectFuncInterface(const NamespaceImpl& nm) : nm_(nm) {}
	const string& GetName() const;
	int getIndexByName(const string& index) const;
	bool getIndexByName(const string& name, int& index) const;
	int getIndexesCount() const;
	const string& getIndexName(int id) const;
	IndexType getIndexType(int id) const;
	const FieldsSet& getIndexFields(int id) const;
	TagsPath getTagsPathForField(const string& jsonPath) const;

private:
	const NamespaceImpl& nm_;
};
}  // namespace reindexer
