#include "namespacedef.h"
#include <unordered_map>
#include "cjson/jsonbuilder.h"
#include "string.h"
#include "tools/errors.h"
#include "tools/jsontools.h"

namespace reindexer {

Error NamespaceDef::FromJSON(span<char> json) {
	try {
		FromJSON(gason::JsonParser().Parse(json));
	} catch (const gason::Exception &ex) {
		return Error(errParseJson, "NamespaceDef: %s", ex.what());
	} catch (const Error &err) {
		return err;
	}
	return errOK;
}

void NamespaceDef::FromJSON(const gason::JsonNode &root) {
	name = root["name"].As<string>();
	storage.Enabled(root["storage"]["enabled"].As<bool>(true));
	storage.DropOnFileFormatError(root["storage"]["drop_on_file_format_error"].As<bool>());
	storage.CreateIfMissing(root["storage"]["create_if_missing"].As<bool>(true));

	for (auto &arrelem : root["indexes"]) {
		IndexDef idx;
		idx.FromJSON(arrelem);
		indexes.push_back(idx);
	}
}

void NamespaceDef::GetJSON(WrSerializer &ser, int formatFlags) const {
	JsonBuilder json(ser);
	json.Put("name", name);
	json.Object("storage").Put("enabled", storage.IsEnabled());
	auto arr = json.Array("indexes");

	for (auto &idx : indexes) {
		arr.Raw(nullptr, "");
		idx.GetJSON(ser, formatFlags);
	}
}

}  // namespace reindexer
