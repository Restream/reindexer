#include "dumpoptions.h"
#include "core/cjson/jsonbuilder.h"
#include "gason/gason.h"

namespace reindexer_tool {

using reindexer::Error;
using namespace std::string_view_literals;

DumpOptions::Mode DumpOptions::ModeFromStr(std::string_view mode) {
	if (mode == "full_node"sv) {
		return Mode::FullNode;
	} else if (mode == "sharded_only"sv) {
		return Mode::ShardedOnly;
	} else if (mode == "local_only"sv) {
		return Mode::LocalOnly;
	}
	throw Error(errParams, "Unknown dump mode: '{}'", mode);
}

std::string_view DumpOptions::StrFromMode(Mode mode) {
	switch (mode) {
		case Mode::FullNode:
			return "full_node"sv;
		case Mode::ShardedOnly:
			return "sharded_only"sv;
		case Mode::LocalOnly:
			return "local_only"sv;
		default:
			throw Error(errParams, "Unknown dump mode: '{}'", int(mode));
	}
}

Error DumpOptions::FromJSON(std::span<char> json) {
	try {
		gason::JsonParser parser;
		auto root = parser.Parse(json);
		mode = ModeFromStr(root["mode"].As<std::string_view>());
	} catch (const gason::Exception& ex) {
		return Error(errParseJson, "DumpOptions: {}", ex.what());
	} catch (const Error& err) {
		return err;
	}
	return Error();
}

void DumpOptions::GetJSON(reindexer::WrSerializer& ser) const {
	reindexer::JsonBuilder jb(ser);
	jb.Put("mode", StrFromMode(mode));
}

}  // namespace reindexer_tool
