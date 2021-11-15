#include "lsn.h"
#include "core/cjson/jsonbuilder.h"

namespace reindexer {

void lsn_t::GetJSON(JsonBuilder &builder) const {
	builder.Put("server_id", Server());
	builder.Put("counter", Counter());
}

}  // namespace reindexer
