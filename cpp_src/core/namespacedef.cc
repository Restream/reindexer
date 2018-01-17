#include "namespacedef.h"
#include "gason/gason.h"
#include "string.h"
#include "tools/errors.h"

namespace reindexer {

template <typename T>
static void parseJsonField(const char *name, T &ref, JsonNode *elem) {
	if (strcmp(name, elem->key)) return;
	if (elem->value.getTag() == JSON_TRUE) {
		ref = true;
	} else if (elem->value.getTag() == JSON_FALSE) {
		ref = false;
	} else
		throw Error(errParseJson, "Expected value `true` of `false` for setting '%s' of ft config", name);
}

static void parseJsonField(const char *name, string &ref, JsonNode *elem) {
	if (strcmp(name, elem->key)) return;
	if (elem->value.getTag() == JSON_STRING) {
		ref = elem->value.toString();
	} else
		throw Error(errParseJson, "Expected string setting '%s' of ft config", name);
}

IndexType IndexDef::Type() const {
	if (fieldType == "int" && (indexType == "hash" || indexType == "")) return IndexIntHash;
	if (fieldType == "int" && indexType == "tree") return IndexIntBTree;
	if (fieldType == "int" && indexType == "-") return IndexIntStore;
	if (fieldType == "int64" && (indexType == "hash" || indexType == "")) return IndexInt64Hash;
	if (fieldType == "int64" && indexType == "tree") return IndexInt64BTree;
	if (fieldType == "int64" && indexType == "-") return IndexInt64Store;
	if (fieldType == "string" && (indexType == "hash" || indexType == "")) return IndexStrHash;
	if (fieldType == "string" && indexType == "tree") return IndexStrBTree;
	if (fieldType == "string" && indexType == "text") return IndexFullText;
	if (fieldType == "string" && indexType == "fulltext") return IndexNewFullText;
	if (fieldType == "string" && indexType == "-") return IndexStrStore;
	if (fieldType == "bool" && (indexType == "hash" || indexType == "" || indexType == "-")) return IndexBool;
	if (fieldType == "double" && (indexType == "tree" || indexType == "")) return IndexDoubleBTree;
	if (fieldType == "double" && indexType == "-") return IndexDoubleStore;
	if (fieldType == "composite" && (indexType == "hash" || indexType == "")) return IndexCompositeHash;
	if (fieldType == "composite" && indexType == "tree") return IndexCompositeBTree;
	if (fieldType == "composite" && indexType == "text") return IndexCompositeText;
	if (fieldType == "composite" && indexType == "fulltext") return IndexCompositeNewText;

	throw Error(errParams, "Unsupported combination of field '%s' type '%s' and index type '%s'", name.c_str(), fieldType.c_str(),
				indexType.c_str());
}

Error NamespaceDef::Parse(char *json) {
	JsonAllocator jalloc;
	JsonValue jvalue;
	char *endp;

	int status = jsonParse(json, &endp, &jvalue, jalloc);

	if (status != JSON_OK) {
		return Error(errParseJson, "Malformed JSON with namespace indexes");
	}
	try {
		for (auto elem : jvalue) {
			if (elem->value.getTag() == JSON_NULL) continue;
			parseJsonField("name", name, elem);

			if (!strcmp("storage", elem->key)) {
				if (elem->value.getTag() != JSON_OBJECT) {
					return Error(errParseJson, "Expected object in 'storage' field, but found %d", elem->value.getTag());
				}
				for (auto selem : elem->value) {
					parseJsonField("enabled", storage.IsEnabled, selem);
					parseJsonField("drop_on_file_format_error", storage.IsDropOnFileFormatError, selem);
					parseJsonField("drop_on_indexes_conflict", storage.IsDropOnIndexesConflict, selem);
					parseJsonField("create_if_missing", storage.IsCreateIfMissing, selem);
				}

			} else if (!strcmp("indexes", elem->key)) {
				if (elem->value.getTag() != JSON_ARRAY) {
					return Error(errParseJson, "Expected array in 'indexes' field, but found %d", elem->value.getTag());
				}
				for (auto arrelem : elem->value) {
					IndexDef idx;

					for (auto idxelem : arrelem->value) {
						if (idxelem->value.getTag() == JSON_NULL) continue;

						parseJsonField("name", idx.name, idxelem);
						parseJsonField("json_path", idx.jsonPath, idxelem);
						parseJsonField("field_type", idx.fieldType, idxelem);
						parseJsonField("index_type", idx.indexType, idxelem);
						parseJsonField("is_pk", idx.opts.IsPK, idxelem);
						parseJsonField("is_array", idx.opts.IsArray, idxelem);
						parseJsonField("is_dense", idx.opts.IsDense, idxelem);

						string collate;
						parseJsonField("collate_mode", collate, idxelem);
						if (collate == "none" || collate == "") {
							idx.opts.CollateMode = CollateNone;
						} else if (collate == "ascii") {
							idx.opts.CollateMode = CollateASCII;
						} else if (collate == "utf8") {
							idx.opts.CollateMode = CollateUTF8;
						} else if (collate == "numeric") {
							idx.opts.CollateMode = CollateNumeric;
						} else {
							return Error(errParams, "Unknown collate mode %s", collate.c_str());
						}
					}
					indexes.push_back(idx);
				}
			}
		}
	} catch (const Error &err) {
		return err;
	}
	return 0;
}
}  // namespace reindexer
