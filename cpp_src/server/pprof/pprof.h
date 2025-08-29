#pragma once

#include <string>
#include "net/http/router.h"
#include "tools/serializer.h"

namespace reindexer_server {

using std::string;
using namespace reindexer::net;
using reindexer::WrSerializer;

class [[nodiscard]] Pprof {
public:
	void Attach(http::Router& router);

	int Profile(http::Context& ctx);
	int ProfileHeap(http::Context& ctx);
	int Growth(http::Context& ctx);
	int CmdLine(http::Context& ctx);
	int Symbol(http::Context& ctx);

protected:
	void resolveSymbol(uintptr_t ptr, WrSerializer& out);
};

}  // namespace reindexer_server
