#pragma once

#include "core/reindexer.h"
#include "http/router.h"
#include "pprof/pprof.h"

namespace reindexer_server {

using std::string;

class Server {
public:
	Server(shared_ptr<reindexer::Reindexer> db, const string &webRoot);
	~Server();

	bool Start(int port);

	int GetQuery(http::Context &ctx);
	int PostQuery(http::Context &ctx);
	int PostItems(http::Context &ctx);
	int PutItems(http::Context &ctx);
	int DeleteItems(http::Context &ctx);
	int PostNamespaces(http::Context &ctx);
	int DeleteNamespaces(http::Context &ctx);
	int GetNamespaces(http::Context &ctx);
	int Check(http::Context &ctx);
	int DocHandler(http::Context &ctx);

protected:
	int modifyItem(http::Context &ctx, int mode);
	int queryResults(http::Context &ctx, reindexer::QueryResults &res, const char *name);

	shared_ptr<reindexer::Reindexer> db_;
	Pprof pprof;

	string webRoot_;
};

}  // namespace reindexer_server
