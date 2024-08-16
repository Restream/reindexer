#include "shardedmeta.h"
#include "core/cjson/jsonbuilder.h"

namespace reindexer {

using namespace std::string_view_literals;

Error ShardedMeta::FromJSON(span<char> json) {
	try {
		gason::JsonParser parser;
		auto root = parser.Parse(json);
		shardId = root["shard_id"sv].As<int>(shardId);
		data = root["data"sv].As<std::string>();
	} catch (const gason::Exception& ex) {
		return Error(errParseJson, "ShardedMeta: %s", ex.what());
	} catch (const Error& err) {
		return err;
	}
	return Error();
}

void ShardedMeta::GetJSON(WrSerializer& ser) const {
	JsonBuilder jb(ser);
	jb.Put("shard_id"sv, shardId);
	jb.Put("data"sv, data);
}

}  // namespace reindexer
