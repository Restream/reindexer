#pragma once

#include <memory>
#include "core/cbinding/resultserializer.h"
#include "core/keyvalue/keyref.h"
#include "core/reindexer.h"
#include "dbmanager.h"
#include "loggerwrapper.h"
#include "net/cproto/dispatcher.h"
#include "net/listener.h"

namespace reindexer_server {

using std::string;
using std::pair;
using namespace reindexer::net;
using namespace reindexer;

struct RPCClientData : public cproto::ClientData {
	h_vector<pair<QueryResults, bool>, 1> results;
	AuthContext auth;
	int connID;
};

class RPCServer {
public:
	RPCServer(DBManager &dbMgr);
	RPCServer(DBManager &dbMgr, LoggerWrapper logger, bool allocDebug = false);
	~RPCServer();

	bool Start(const string &addr, ev::dynamic_loop &loop);
	void Stop() { listener_->Stop(); }

	Error Ping(cproto::Context &ctx);
	Error Login(cproto::Context &ctx, p_string login, p_string password, p_string db);
	Error OpenDatabase(cproto::Context &ctx, p_string db);
	Error CloseDatabase(cproto::Context &ctx);
	Error DropDatabase(cproto::Context &ctx);

	Error OpenNamespace(cproto::Context &ctx, p_string ns, p_string def);
	Error DropNamespace(cproto::Context &ctx, p_string ns);
	Error CloseNamespace(cproto::Context &ctx, p_string ns);
	Error EnumNamespaces(cproto::Context &ctx);

	Error AddIndex(cproto::Context &ctx, p_string ns, p_string indexDef);
	Error ConfigureIndex(cproto::Context &ctx, p_string ns, p_string index, p_string config);

	Error Commit(cproto::Context &ctx, p_string ns);

	Error ModifyItem(cproto::Context &ctx, p_string itemPack, int mode);
	Error DeleteQuery(cproto::Context &ctx, p_string query);

	Error Select(cproto::Context &ctx, p_string query, int flags, int limit, int64_t fetchDataMask, p_string ptVersions);
	Error SelectSQL(cproto::Context &ctx, p_string query, int flags, int limit, int64_t fetchDataMask, p_string ptVersions);
	Error FetchResults(cproto::Context &ctx, int reqId, int flags, int offset, int limit, int64_t fetchDataMask);
	Error CloseResults(cproto::Context &ctx, int reqId);

	Error GetMeta(cproto::Context &ctx, p_string ns, p_string key);
	Error PutMeta(cproto::Context &ctx, p_string ns, p_string key, p_string data);
	Error EnumMeta(cproto::Context &ctx, p_string ns);

	Error CheckAuth(cproto::Context &ctx);
	void Logger(cproto::Context &ctx, const Error &err, const cproto::Args &ret);
	void OnClose(cproto::Context &ctx, const Error &err);

protected:
	Error sendResults(cproto::Context &ctx, QueryResults &qr, int reqId, const ResultFetchOpts &opts);
	Error fetchResults(cproto::Context &ctx, int reqId, const ResultFetchOpts &opts);
	void freeQueryResults(cproto::Context &ctx, int id);
	QueryResults &getQueryResults(cproto::Context &ctx, int &id);

	shared_ptr<Reindexer> getDB(cproto::Context &ctx, UserRole role);

	DBManager &dbMgr_;
	cproto::Dispatcher dispatcher;
	std::unique_ptr<Listener> listener_;

	LoggerWrapper logger_;
	bool allocDebug_;
};

}  // namespace reindexer_server
