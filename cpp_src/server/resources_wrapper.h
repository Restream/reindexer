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
	web(const string& webRoot) : webRoot_(webRoot) {}
	fs::FileStatus stat(const string& targer);
	int file(Context& ctx, HttpStatusCode code, const string& target);
	const string& webRoot_;
};

}  // namespace reindexer
