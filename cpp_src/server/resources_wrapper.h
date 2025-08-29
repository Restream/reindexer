#pragma once

#include <algorithm>
#include <string>

#include "net/http/router.h"
#include "tools/fsops.h"

namespace reindexer_server {

struct [[nodiscard]] DocumentStatus {
	DocumentStatus() {}
	DocumentStatus(reindexer::fs::FileStatus s, bool gzip) : fstatus(s), isGzip(gzip) {}
	DocumentStatus(reindexer::fs::FileStatus s) : fstatus(s) {}
	reindexer::fs::FileStatus fstatus = reindexer::fs::StatError;
	bool isGzip = false;
};

struct [[nodiscard]] web {
	using Context = reindexer::net::http::Context;
	using HttpStatusCode = reindexer::net::http::HttpStatusCode;

	web(const std::string& webRoot) : webRoot_(webRoot) {}
	DocumentStatus stat(const std::string& targer);
	int file(Context& ctx, HttpStatusCode code, const std::string& target, bool isGzip, bool withCache);

private:
	const std::string& webRoot_;
	DocumentStatus fsStatus(const std::string& target);
};

}  // namespace reindexer_server
