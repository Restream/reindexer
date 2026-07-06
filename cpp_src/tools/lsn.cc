#include "lsn.h"

#include <iostream>
#include "core/cjson/jsonbuilder.h"
#include "vendor/gason/gason.h"

namespace reindexer {

void lsn_t::GetJSON(JsonBuilder& builder) const {
	builder.Put("server_id", Server());
	builder.Put("counter", Counter());
}

void lsn_t::FromJSON(const gason::JsonNode& root) {
	const int server = root["server_id"].As<int>(0);
	const int64_t counter = root["counter"].As<int64_t>(kDefaultCounter);
	payload_ = int64_t(lsn_t(counter, server));
}

void lsn_t::throwValidation(ErrorCode code, std::string_view fmt, int64_t v1, int64_t v2) {
	throw Error(code, fmt::vformat(fmt, fmt::make_format_args(v1, v2)));
}

std::ostream& operator<<(std::ostream& o, const reindexer::lsn_t& sv) {
	o << sv.Server() << ":" << sv.Counter();
	return o;
}

}  // namespace reindexer
