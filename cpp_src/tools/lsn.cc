#include "lsn.h"

#include "core/cjson/jsonbuilder.h"

namespace reindexer {

void lsn_t::GetJSON(JsonBuilder &builder) const {
	builder.Put("server_id", Server());
	builder.Put("counter", Counter());
}

void lsn_t::validateServerId(int16_t server) {
	if (server < kMinServerIDValue) {
		throw Error(errLogic, "Server id < %d", kMinServerIDValue);
	}
	if (server > kMaxServerIDValue) {
		throw Error(errLogic, "Server id > %d", kMaxServerIDValue);
	}
}
}  // namespace reindexer
