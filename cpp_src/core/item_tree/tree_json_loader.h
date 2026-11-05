#pragma once

#include <memory>
#include <string_view>
#include "core/cjson/tagsmatcher.h"
#include "core/cjson/tagspath.h"
#include "core/item_tree/item_tree.h"
#include "core/tag_name_index.h"
#include "gason/gason.h"

namespace reindexer {

class [[nodiscard]] JsonTreeLoader {
public:
	JsonTreeLoader(TreeNode& node, TreeNode& root, ConstPayload& oldPayload, TagsMatcher& tm, TagsPath&& tagsPath,
				   IntermediateIndexesData& indexData)
		: tm_(tm),
		  treeNode_(node),
		  root_(root),
		  oldPayload_(oldPayload),
		  pathForIndexedField_(std::move(tagsPath)),
		  indexData_(indexData) {}
	void Load(std::string_view json);

private:
	void loadJsonInternal(const gason::JsonNode& node, TreeNode& nodeTree);
	void addValue(TreeNode& node, TagName tag, Variant&& v, int index);
	void processJsonTag(const gason::JsonNode& node, TreeNode& nodeTree, const FieldProperties& field, TagName tag);
	void processFloatVectorTag(const gason::JsonNode& node, TreeNode& nodeTree, const FieldProperties& field, TagName tag);
	void loadFloatVectorArray(const gason::JsonValue& value, TreeNode& nodeTree, const FieldProperties& field, std::string_view fieldName);
	Variant makeFloatVector(const gason::JsonValue& value, const FieldProperties& field, std::string_view fieldName) const;

	class [[nodiscard]] TagsPathLevel {
	public:
		TagsPathLevel(TagsPath& t, TagName n) : path_(t) { path_.emplace_back(n); }
		~TagsPathLevel() { path_.pop_back(); }

	private:
		TagsPath& path_;
	};

	TagsMatcher& tm_;
	TreeNode& treeNode_;
	TreeNode& root_;
	ConstPayload& oldPayload_;
	TagsPath pathForIndexedField_;
	IntermediateIndexesData& indexData_;
};
}  // namespace reindexer
