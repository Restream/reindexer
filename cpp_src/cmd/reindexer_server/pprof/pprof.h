#pragma once

#include <string>
#include "http/router.h"

namespace reindexer_server {

using std::string;

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
