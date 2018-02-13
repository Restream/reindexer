#include <sstream>

#include "estl/fast_hash_map.h"
#include "gason/gason.h"
#include "namespacedef.h"
#include "string.h"
#include "tools/errors.h"
#include "tools/serializer.h"
#include "tools/slice.h"

using std::stringstream;

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

void IndexDef::FromType(IndexType type) {
	switch (type) {
		case IndexIntHash:
		case IndexIntBTree:
		case IndexIntStore:
			fieldType = "int";
			break;
		case IndexInt64Hash:
		case IndexInt64BTree:
		case IndexInt64Store:
			fieldType = "int64";
			break;
		case IndexStrHash:
		case IndexStrBTree:
		case IndexStrStore:
		case IndexFullText:
		case IndexNewFullText:
			fieldType = "string";
			break;
		case IndexBool:
			fieldType = "bool";
			break;
		case IndexDoubleBTree:
		case IndexDoubleStore:
			fieldType = "double";
			break;
		case IndexCompositeBTree:
		case IndexCompositeHash:
		case IndexCompositeNewText:
		case IndexCompositeText:
			fieldType = "composite";
			break;
		default:
			throw Error(errLogic, "Unknown index type.");
	}

	switch (type) {
		case IndexIntHash:
		case IndexInt64Hash:
		case IndexStrHash:
		case IndexCompositeHash:
		case IndexBool:
			indexType = "hash";
			break;
		case IndexIntBTree:
		case IndexInt64BTree:
		case IndexStrBTree:
		case IndexDoubleBTree:
		case IndexCompositeBTree:
			indexType = "tree";
			break;
		case IndexFullText:
		case IndexCompositeText:
			indexType = "text";
			break;
		case IndexNewFullText:
		case IndexCompositeNewText:
			indexType = "fulltext";
			break;
		case IndexIntStore:
		case IndexInt64Store:
		case IndexDoubleStore:
		case IndexStrStore:
			indexType = "-";
			break;
		default:
			throw Error(errLogic, "Unknown index type.");
	}
}

Error IndexDef::Parse(char *json) {
	JsonAllocator jalloc;
	JsonValue jvalue;
	char *endp;

	int status = jsonParse(json, &endp, &jvalue, jalloc);
	if (status != JSON_OK) {
		return Error(errParseJson, "Malformed JSON with namespace indexes");
	}

	return IndexDef::Parse(jvalue);
}

Error IndexDef::Parse(JsonValue &jvalue) {
	try {
		for (auto elem : jvalue) {
			if (elem->value.getTag() == JSON_NULL) continue;
			bool isPk = false, isArray = false, isDense = false, isAppendable = false;

			parseJsonField("name", name, elem);
			parseJsonField("json_path", jsonPath, elem);
			parseJsonField("field_type", fieldType, elem);
			parseJsonField("index_type", indexType, elem);
			parseJsonField("is_pk", isPk, elem);
			parseJsonField("is_array", isArray, elem);
			parseJsonField("is_dense", isDense, elem);
			parseJsonField("is_appendable", isAppendable, elem);

			if (isPk) opts.PK();
			if (isArray) opts.Array();
			if (isDense) opts.Dense();
			if (isAppendable) opts.Appendable();

			string collate;
			parseJsonField("collate_mode", collate, elem);
			if (collate == "none" || collate == "") {
				opts.SetCollateMode(CollateNone);
			} else if (collate == "ascii") {
				opts.SetCollateMode(CollateASCII);
			} else if (collate == "utf8") {
				opts.SetCollateMode(CollateUTF8);
			} else if (collate == "numeric") {
				opts.SetCollateMode(CollateNumeric);
			} else {
				return Error(errParams, "Unknown collate mode %s", collate.c_str());
			}
		}
	} catch (const Error &err) {
		return err;
	}

	return 0;
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
					bool isEnabled = true, isDropOnFileFormatError = false, isCreateIfMissing = false;
					parseJsonField("enabled", isEnabled, selem);
					parseJsonField("drop_on_file_format_error", isDropOnFileFormatError, selem);
					parseJsonField("create_if_missing", isCreateIfMissing, selem);
					storage.Enabled(isEnabled).DropOnFileFormatError(isDropOnFileFormatError).CreateIfMissing(isCreateIfMissing);
				}

			} else if (!strcmp("indexes", elem->key)) {
				if (elem->value.getTag() != JSON_ARRAY) {
					return Error(errParseJson, "Expected array in 'indexes' field, but found %d", elem->value.getTag());
				}
				for (auto arrelem : elem->value) {
					IndexDef idx;
					idx.Parse(arrelem->value);
					indexes.push_back(idx);
				}
			}
		}
	} catch (const Error &err) {
		return err;
	}
	return 0;
}

void NamespaceDef::Print(WrSerializer &ser) {
	ser.PutChar('{');
	ser.Printf("\"name\":\"%s\",", name.c_str());
	ser.PutChars("\"storage\":{");
	ser.Printf("\"enabled\":%s,", storage.IsEnabled() ? "true" : "false");
	ser.Printf("\"drop_on_file_format_error\":%s,", storage.IsDropOnFileFormatError() ? "true" : "false");
	ser.Printf("\"create_if_missing\":%s", storage.IsCreateIfMissing() ? "true" : "false");
	ser.PutChars("},");

	PrintIndexes(ser);

	ser.PutChar('}');
}

void NamespaceDef::PrintIndexes(WrSerializer &ser, const char *jsonIndexFieldName) {
	static const fast_hash_map<int, string> collates = {
		{CollateNone, "none"}, {CollateASCII, "ascii"}, {CollateUTF8, "utf8"}, {CollateNumeric, "numeric"}};

	size_t lastIdx = indexes.size() - 1;

	ser.PutChars("\"");
	ser.PutChars(jsonIndexFieldName);
	ser.PutChars("\":[");

	for (size_t i = 0; i < indexes.size(); i++) {
		auto &idx = indexes[i];
		auto it = collates.find(idx.opts.GetCollateMode());
		auto collateMode = it == collates.end() ? "none" : it->second;

		ser.PutChar('{');
		ser.Printf("\"name\":\"%s\",", idx.name.c_str());
		ser.Printf("\"json_path\":\"%s\",", idx.jsonPath.c_str());
		ser.Printf("\"field_type\":\"%s\",", idx.fieldType.c_str());
		ser.Printf("\"index_type\":\"%s\",", idx.indexType.c_str());
		ser.Printf("\"is_pk\":%s,", idx.opts.IsPK() ? "true" : "false");
		ser.Printf("\"is_array\":%s,", idx.opts.IsArray() ? "true" : "false");
		ser.Printf("\"is_dense\":%s,", idx.opts.IsDense() ? "true" : "false");
		ser.Printf("\"is_appendable\":%s,", idx.opts.IsAppendable() ? "true" : "false");
		ser.Printf("\"collate_mode\":\"%s\"", collateMode.c_str());
		ser.PutChars(i == lastIdx ? "}" : "},");
	}

	ser.PutChar(']');
}

}  // namespace reindexer
