#include "namespacedef.h"
#include <unordered_map>
#include "string.h"
#include "tools/errors.h"
#include "tools/jsontools.h"
#include "tools/serializer.h"

namespace reindexer {

Error NamespaceDef::FromJSON(char *json) {
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
			int cacheModeInt = -1;
			parseJsonField("cached_mode", cacheModeInt, elem, 0, 2);
			if (cacheModeInt != -1) cacheMode = static_cast<CacheMode>(cacheModeInt);

			if (!strcmp("storage", elem->key)) {
				if (elem->value.getTag() != JSON_OBJECT) {
					return Error(errParseJson, "Expected object in 'storage' field, but found %d", elem->value.getTag());
				}
				bool isEnabled = true, isDropOnFileFormatError = false, isCreateIfMissing = true;
				for (auto selem : elem->value) {
					parseJsonField("enabled", isEnabled, selem);
					parseJsonField("drop_on_file_format_error", isDropOnFileFormatError, selem);
					parseJsonField("create_if_missing", isCreateIfMissing, selem);
				}
				storage.Enabled(isEnabled).DropOnFileFormatError(isDropOnFileFormatError).CreateIfMissing(isCreateIfMissing);

			} else if (!strcmp("indexes", elem->key)) {
				if (elem->value.getTag() != JSON_ARRAY) {
					return Error(errParseJson, "Expected array in 'indexes' field, but found %d", elem->value.getTag());
				}
				for (auto arrelem : elem->value) {
					IndexDef idx;
					idx.FromJSON(arrelem->value);
					indexes.push_back(idx);
				}
			}
		}
	} catch (const Error &err) {
		return err;
	}
	return 0;
}

void NamespaceDef::GetJSON(WrSerializer &ser) const {
	ser.PutChar('{');
	ser.Printf("\"name\":\"%s\",", name.c_str());
	ser.Printf("\"cached_mode\":%d,", static_cast<int>(cacheMode));

	ser.PutChars("\"storage\":{");
	ser.Printf("\"enabled\":%s", storage.IsEnabled() ? "true" : "false");
	ser.PutChars("},");

	ser.PutChars("\"indexes\":[");
	for (size_t i = 0; i < indexes.size(); i++) {
		if (i != 0) ser.PutChar(',');
		indexes[i].GetJSON(ser);
	}

	ser.PutChars("]}");
}

}  // namespace reindexer
