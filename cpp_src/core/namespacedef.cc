#include "namespacedef.h"
#include "cjson/jsonbuilder.h"
#include "core/namespace/namespace.h"
#include "vendor/gason/gason.h"

namespace reindexer {

using namespace std::string_view_literals;

Error NamespaceDef::FromJSON(std::span<char> json) {
	try {
		gason::JsonParser parser;
		FromJSON(parser.Parse(json));
	} catch (const gason::Exception& ex) {
		return Error(errParseJson, "NamespaceDef: {}", ex.what());
	} catch (const Error& err) {
		return err;
	}
	return {};
}

void NamespaceDef::FromJSON(const gason::JsonNode& root) {
	name = root["name"].As<std::string>();
	auto storageNode = root["storage"];
	auto indexesNode = root["indexes"];
	auto schemaNode = root["schema"];
	auto temporaryNode = root["temporary"];
	isNameOnly = storageNode.empty() && indexesNode.empty() && schemaNode.empty() && temporaryNode.empty();
	if (!isNameOnly) {
		storage.Enabled(storageNode["enabled"].As<bool>(true));
		storage.DropOnFileFormatError(storageNode["drop_on_file_format_error"].As<bool>());
		storage.CreateIfMissing(storageNode["create_if_missing"].As<bool>(true));

		for (auto& arrelem : indexesNode) {
			indexes.emplace_back(IndexDef::FromJSON(arrelem));
		}
		schemaJson = schemaNode.As<std::string>(schemaJson);
	}
}

void NamespaceDef::GetJSON(WrSerializer& ser, ExtraIndexDescription withIndexExtras) const {
	JsonBuilder json(ser);
	json.Put("name", name);
	if (!isNameOnly) {
		json.Object("storage").Put("enabled", storage.IsEnabled());
		{
			auto arr = json.Array("indexes");
			for (auto& idx : indexes) {
				arr.Raw("");
				idx.GetJSON(ser, withIndexExtras);
			}
		}
		if (!schemaJson.empty()) {
			json.Put("schema", schemaJson);
		}
	}
}

bool EnumNamespacesOpts::MatchFilter(std::string_view nsName, const Namespace& ns, const RdxContext& ctx) const {
	return MatchNameFilter(nsName) && (!IsHideTemporary() || !ns.IsTemporary(ctx));
}

Error NsReplicationOpts::FromJSON(std::span<char> json) {
	try {
		gason::JsonParser parser;
		FromJSON(parser.Parse(json));
	} catch (const gason::Exception& ex) {
		return Error(errParseJson, "NamespaceDef: {}", ex.what());
	} catch (const Error& err) {
		return err;
	}
	return {};
}

void NsReplicationOpts::FromJSON(const gason::JsonNode& root) {
	auto& tmNode = root["state_token"sv];
	if (!tmNode.empty()) {
		tmStateToken.emplace(tmNode.As<int32_t>());
	}
	nsVersion = lsn_t(root["ns_version"sv].As<int64_t>());
}

void NsReplicationOpts::GetJSON(WrSerializer& ser) const {
	JsonBuilder json(ser);
	if (tmStateToken.has_value()) {
		json.Put("state_token"sv, tmStateToken.value());
	}
	json.Put("ns_version"sv, int64_t(nsVersion));
}

}  // namespace reindexer
