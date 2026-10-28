#pragma once

#include "core/cjson/tagsmatcher.h"
#include "core/definitions/indexdef.h"
#include "core/payload/fieldsset.h"

namespace reindexer::ns_indexes {

/**
 * @brief Registers the JSON-paths of the composite index definition in the tags matcher and builds the
 * corresponding fields set. Modifies the tags matcher, so callers pass either the tags matcher copy of the prepared
 * state, or their own throwaway copy: registering a JSON-path is irreversible, a tag is never removed from the tags
 * matcher.
 * @param tm - tags matcher to register the new JSON-paths in.
 * @param lookupNonComposite - resolves a component by non-composite index name into its field number.
 * Components not resolved by name are treated as JSON-paths.
 * @return the fields set describing the composite index.
 */
template <typename NonCompositeIndexLookup>
[[nodiscard]] FieldsSet CreateFieldsSetFromJsonPaths(const IndexDef& indexDef, TagsMatcher& tm,
													 NonCompositeIndexLookup&& lookupNonComposite) {
	FieldsSet fields;
	for (const auto& jsonPath : indexDef.JsonPaths()) {
		int idx = IndexValueType::SetByJsonPath;
		if (!lookupNonComposite(jsonPath, idx)) {
			TagsPath tagsPath = tm.path2tag(jsonPath, CanAddField_True);
			if (tagsPath.empty()) {
				throw Error(errLogic, "Unable to get or create json-path '{}' for composite index '{}'", jsonPath, indexDef.Name());
			}
			fields.push_back(std::move(tagsPath));
			fields.push_back(jsonPath);
		}
		fields.push_back(idx);
	}
	assertrx_throw(fields.getJsonPathsLength() == fields.getTagsPathsLength());
	return fields;
}

}  // namespace reindexer::ns_indexes
