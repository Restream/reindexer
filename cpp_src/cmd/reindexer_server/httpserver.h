#pragma once

#include "core/reindexer.h"
#include "net/http/router.h"
#include "pprof/pprof.h"

namespace reindexer_server {

using std::string;
using namespace reindexer::net;

class HTTPServer {
public:
	HTTPServer(shared_ptr<reindexer::Reindexer> db, const string &webRoot);
	~HTTPServer();

	bool Start(int port);

	int NotFoundHandler(http::Context &ctx);
	int DocHandler(http::Context &ctx);
	int Check(http::Context &ctx);
	int GetQuery(http::Context &ctx);
	int PostQuery(http::Context &ctx);
	int GetNamespaces(http::Context &ctx);
	int GetNamespace(http::Context &ctx);
	int PostNamespace(http::Context &ctx);
	int DeleteNamespace(http::Context &ctx);
	int GetItems(http::Context &ctx);
	int PostItems(http::Context &ctx);
	int PutItems(http::Context &ctx);
	int DeleteItems(http::Context &ctx);
	int GetIndexes(http::Context &ctx);
	int PostIndex(http::Context &ctx);

protected:
	int modifyItem(http::Context &ctx, int mode);
	int queryResults(http::Context &ctx, reindexer::QueryResults &res, const char *name);
	int jsonStatus(http::Context &ctx, bool isSuccess = true, int respcode = http::StatusOK, const string &description = "");

	shared_ptr<reindexer::Reindexer> db_;
	Pprof pprof;

	string webRoot_;

	static const int limit_default = 10;
	static const int limit_max = 100;
	static const int offset_default = 0;
};

}  // namespace reindexer_server
