#pragma once

#include <string>
#include "net/http/router.h"

namespace reindexer_server {

using std::string;
using namespace reindexer::net;

class Pprof {
public:
	void Attach(http::Router &router);

	int ProfileHeap(http::Context &ctx);
	int CmdLine(http::Context &ctx);
	int Profile(http::Context &ctx);
	int Symbol(http::Context &ctx);

protected:
	string resolveSymbol(uintptr_t ptr);
};

}  // namespace reindexer_server
