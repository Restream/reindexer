#pragma once

#include <memory>
#include <unordered_map>
#include "core/cbinding/resultserializer.h"
#include "core/keyvalue/variant.h"
#include "core/reindexer.h"
#include "dbmanager.h"
#include "loggerwrapper.h"
#include "net/cproto/dispatcher.h"
#include "net/listener.h"
#include "rpcupdatespusher.h"
#include "statscollect/istatswatcher.h"

namespace reindexer_server {

using std::string;
using std::pair;
using namespace reindexer::net;
using namespace reindexer;

struct RPCClientData : public cproto::ClientData {
	~RPCClientData();
	h_vector<pair<QueryResults, bool>, 1> results;
	vector<Transaction> txs;

	AuthContext auth;
	cproto::RPCUpdatesPusher pusher;
	int connID;
	bool subscribed;
};

class RPCServer {
public:
	RPCServer(DBManager &dbMgr, LoggerWrapper logger, bool allocDebug = false, IStatsWatcher *statsCollector = nullptr);
	~RPCServer();

	bool Start(const string &addr, ev::dynamic_loop &loop);
	void Stop() { listener_->Stop(); }

	Error Ping(cproto::Context &ctx);
	Error Login(cproto::Context &ctx, p_string login, p_string password, p_string db);
	Error OpenDatabase(cproto::Context &ctx, p_string db);
	Error CloseDatabase(cproto::Context &ctx);
	Error DropDatabase(cproto::Context &ctx);

	Error OpenNamespace(cproto::Context &ctx, p_string ns);
	Error DropNamespace(cproto::Context &ctx, p_string ns);
	Error TruncateNamespace(cproto::Context &ctx, p_string ns);
	Error CloseNamespace(cproto::Context &ctx, p_string ns);
	Error EnumNamespaces(cproto::Context &ctx);

	Error AddIndex(cproto::Context &ctx, p_string ns, p_string indexDef);
	Error UpdateIndex(cproto::Context &ctx, p_string ns, p_string indexDef);
	Error DropIndex(cproto::Context &ctx, p_string ns, p_string index);

	Error Commit(cproto::Context &ctx, p_string ns);

	Error ModifyItem(cproto::Context &ctx, p_string nsName, int format, p_string itemData, int mode, p_string percepsPack, int stateToken,
					 int txID);

	Error StartTransaction(cproto::Context &ctx, p_string nsName);

	Error AddTxItem(cproto::Context &ctx, int format, p_string itemData, int mode, p_string percepsPack, int stateToken, int64_t txID);
	Error CommitTx(cproto::Context &ctx, int64_t txId);

	Error RollbackTx(cproto::Context &ctx, int64_t txId);

	Error DeleteQuery(cproto::Context &ctx, p_string query);
	Error UpdateQuery(cproto::Context &ctx, p_string query);

	Error Select(cproto::Context &ctx, p_string query, int flags, int limit, p_string ptVersions);
	Error SelectSQL(cproto::Context &ctx, p_string query, int flags, int limit, p_string ptVersions);
	Error FetchResults(cproto::Context &ctx, int reqId, int flags, int offset, int limit);
	Error CloseResults(cproto::Context &ctx, int reqId);
	Error GetSQLSuggestions(cproto::Context &ctx, p_string query, int pos);

	Error GetMeta(cproto::Context &ctx, p_string ns, p_string key);
	Error PutMeta(cproto::Context &ctx, p_string ns, p_string key, p_string data);
	Error EnumMeta(cproto::Context &ctx, p_string ns);
	Error SubscribeUpdates(cproto::Context &ctx, int subscribe);

	Error CheckAuth(cproto::Context &ctx);
	void Logger(cproto::Context &ctx, const Error &err, const cproto::Args &ret);
	void OnClose(cproto::Context &ctx, const Error &err);
	void OnResponse(cproto::Context &ctx);

protected:
	Error sendResults(cproto::Context &ctx, QueryResults &qr, int reqId, const ResultFetchOpts &opts);

	Error fetchResults(cproto::Context &ctx, int reqId, const ResultFetchOpts &opts);
	void freeQueryResults(cproto::Context &ctx, int id);
	QueryResults &getQueryResults(cproto::Context &ctx, int &id);
	Transaction &getTx(cproto::Context &ctx, int64_t id);
	int64_t addTx(cproto::Context &ctx, Transaction &&tr);
	void clearTx(cproto::Context &ctx, uint64_t txId);

	Reindexer getDB(cproto::Context &ctx, UserRole role);
	constexpr static string_view statsSourceName() { return "rpc"_sv; }

	DBManager &dbMgr_;
	cproto::Dispatcher dispatcher_;
	std::unique_ptr<Listener> listener_;

	LoggerWrapper logger_;
	bool allocDebug_;
	IStatsWatcher *statsWatcher_;

	std::chrono::system_clock::time_point startTs_;
};

}  // namespace reindexer_server
