#pragma once

#include <memory>
#include "core/reindexer.h"
#include "dbmanager.h"
#include "loggerwrapper.h"
#include "net/http/router.h"
#include "net/listener.h"
#include "pprof/pprof.h"
#include "tools/logger.h"

namespace reindexer_server {

using std::string;
using namespace reindexer::net;

struct HTTPClientData : public http::ClientData {
	AuthContext auth;
};

class HTTPServer {
public:
	HTTPServer(DBManager &dbMgr, const string &webRoot);
	HTTPServer(DBManager &dbMgr, const string &webRoot, LoggerWrapper logger, bool allocDebug = false);
	~HTTPServer();

	bool Start(const string &addr, ev::dynamic_loop &loop);
	void Stop() { listener_->Stop(); }

	int NotFoundHandler(http::Context &ctx);
	int DocHandler(http::Context &ctx);
	int Check(http::Context &ctx);
	int GetQuery(http::Context &ctx);
	int PostQuery(http::Context &ctx);
	int GetDatabases(http::Context &ctx);
	int PostDatabase(http::Context &ctx);
	int DeleteDatabase(http::Context &ctx);
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
	int DeleteIndex(http::Context &ctx);
	int CheckAuth(http::Context &ctx);
	void Logger(http::Context &ctx);

protected:
	int modifyItem(http::Context &ctx, int mode);
	int queryResults(http::Context &ctx, reindexer::QueryResults &res, const char *name);
	int jsonStatus(http::Context &ctx, bool isSuccess = true, int respcode = http::StatusOK, const string &description = "");
	shared_ptr<Reindexer> getDB(http::Context &ctx, UserRole role);
	string getNameFromJson(string json);

	DBManager &dbMgr_;
	Pprof pprof_;

	string webRoot_;

	http::Router router_;
	std::unique_ptr<Listener> listener_;

	LoggerWrapper logger_;
	bool allocDebug_;

	static const int limit_default = 10;
	static const int limit_max = 100;
	static const int offset_default = 0;
};

}  // namespace reindexer_server
