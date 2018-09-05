#pragma once

#include <algorithm>
#include <string>

#include "net/http/router.h"
#include "tools/fsops.h"

using std::string;

namespace reindexer {

using net::http::Context;
using net::http::HttpStatusCode;

struct web {
	static fs::FileStatus stat(const string& targer);
	static int file(Context& ctx, HttpStatusCode code, const string& target);
};

}  // namespace reindexer
