#include "lsn.h"
#include "cjson/jsonbuilder.h"

namespace reindexer {

void lsn_t::GetJSON(JsonBuilder& builder) const {
	builder.Put("server_id", Server());
	builder.Put("counter", Counter());
}

[[noreturn]] void lsn_t::throwValidation(ErrorCode code, const char* fmt, int64_t value) { throw Error(code, fmt, value); }

}  // namespace reindexer