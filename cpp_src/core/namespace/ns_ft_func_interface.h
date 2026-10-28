#pragma once

#include <core/cjson/tagspath.h>
#include <core/type_consts.h>

namespace reindexer {

namespace ns_indexes {
class Registry;
}
class FieldsSet;

class [[nodiscard]] NsFtFuncInterface {
public:
	explicit NsFtFuncInterface(const ns_indexes::Registry& registry) noexcept : registry_(registry) {}
	bool getIndexByName(std::string_view name, int& index) const noexcept;
	int getIndexesCount() const noexcept;
	const std::string& getIndexName(int id) const noexcept;
	IndexType getIndexType(int id) const noexcept;
	const FieldsSet& getIndexFields(int id) const noexcept;
	TagsPath getTagsPathForField(std::string_view jsonPath) const noexcept;

private:
	const ns_indexes::Registry& registry_;
};

}  // namespace reindexer
