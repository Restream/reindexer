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
 * @param tryGetScalarIndexByName - resolves a JSON-path already covered by a non-composite index into that field's
 * number, so the composite index refers to the field instead of the JSON-path.
 * @return the fields set describing the composite index.
 */
template <typename ScalarIndexLookup>
[[nodiscard]] FieldsSet CreateFieldsSetFromJsonPaths(const IndexDef& indexDef, TagsMatcher& tm,
													 ScalarIndexLookup&& tryGetScalarIndexByName) {
	FieldsSet fields;
	for (const auto& jsonPath : indexDef.JsonPaths()) {
		int idx = IndexValueType::SetByJsonPath;
		if (!tryGetScalarIndexByName(jsonPath, idx)) {
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
