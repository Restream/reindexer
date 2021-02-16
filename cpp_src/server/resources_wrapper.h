#pragma once

#include <algorithm>
#include <string>

#include "net/http/router.h"
#include "tools/fsops.h"

using std::string;

namespace reindexer {

using net::http::Context;
using net::http::HttpStatusCode;

struct DocumentStatus {
	DocumentStatus() {}
	DocumentStatus(fs::FileStatus s, bool gzip) : fstatus(s), isGzip(gzip) {}
	DocumentStatus(fs::FileStatus s) : fstatus(s) {}
	fs::FileStatus fstatus = fs::StatError;
	bool isGzip = false;
};

struct web {
	web(const string& webRoot) : webRoot_(webRoot) {}
	DocumentStatus stat(const string& targer);
	int file(Context& ctx, HttpStatusCode code, const string& target, bool isGzip);

private:
	const string& webRoot_;
	DocumentStatus fsStatus(const std::string& target);
};

}  // namespace reindexer
