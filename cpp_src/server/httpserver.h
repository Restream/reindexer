#pragma once

#include <memory>
#include "config.h"
#include "core/reindexer.h"
#include "dbmanager.h"
#include "estl/fast_hash_map.h"
#include "loggerwrapper.h"
#include "net/http/router.h"
#include "net/listener.h"
#include "pprof/pprof.h"
#include "tools/logger.h"

namespace reindexer_server {

class Prometheus;
struct IStatsWatcher;

using std::string;
using namespace reindexer::net;

struct HTTPClientData : public http::ClientData {
	AuthContext auth;
};

class HTTPServer {
public:
	class OptionalConfig {
	public:
		OptionalConfig(const ServerConfig &serverConfig, Prometheus *prometheusI = nullptr, IStatsWatcher *statsWatcherI = nullptr)
			: allocDebug(serverConfig.DebugAllocs),
			  enablePprof(serverConfig.DebugPprof),
			  prometheus(prometheusI),
			  statsWatcher(statsWatcherI),
			  txIdleTimeout(serverConfig.TxIdleTimeout),
			  rpcAddress(serverConfig.RPCAddr),
			  httpAddress(serverConfig.HTTPAddr),
			  storagePath(serverConfig.StoragePath),
			  rpcLog(serverConfig.RpcLog),
			  httpLog(serverConfig.HttpLog),
			  logLevel(serverConfig.LogLevel),
			  coreLog(serverConfig.CoreLog),
			  serverLog(serverConfig.ServerLog) {}
		bool allocDebug;
		bool enablePprof;
		Prometheus *prometheus;
		IStatsWatcher *statsWatcher;
		std::chrono::seconds txIdleTimeout;
		string rpcAddress;
		string httpAddress;
		string storagePath;
		string rpcLog;
		string httpLog;
		string logLevel;
		string coreLog;
		string serverLog;
	};

	HTTPServer(DBManager &dbMgr, const string &webRoot, LoggerWrapper &logger, OptionalConfig config);

	bool Start(const string &addr, ev::dynamic_loop &loop);
	void Stop() { listener_->Stop(); }

	int NotFoundHandler(http::Context &ctx);
	int DocHandler(http::Context &ctx);
	int Check(http::Context &ctx);
	int PostQuery(http::Context &ctx);
	int DeleteQuery(http::Context &ctx);
	int UpdateQuery(http::Context &ctx);
	int GetSQLQuery(http::Context &ctx);
	int PostSQLQuery(http::Context &ctx);
	int GetSQLSuggest(http::Context &ctx);
	int GetDatabases(http::Context &ctx);
	int PostDatabase(http::Context &ctx);
	int DeleteDatabase(http::Context &ctx);
	int GetNamespaces(http::Context &ctx);
	int GetNamespace(http::Context &ctx);
	int PostNamespace(http::Context &ctx);
	int DeleteNamespace(http::Context &ctx);
	int TruncateNamespace(http::Context &ctx);
	int RenameNamespace(http::Context &ctx);
	int GetItems(http::Context &ctx);
	int PostItems(http::Context &ctx);
	int PatchItems(http::Context &ctx);
	int PutItems(http::Context &ctx);
	int DeleteItems(http::Context &ctx);
	int GetIndexes(http::Context &ctx);
	int PostIndex(http::Context &ctx);
	int PutIndex(http::Context &ctx);
	int PutSchema(http::Context &ctx);
	int GetSchema(http::Context &ctx);
	int GetProtobufSchema(http::Context &ctx);
	int GetMetaList(http::Context &ctx);
	int GetMetaByKey(http::Context &ctx);
	int PutMetaByKey(http::Context &ctx);
	int DeleteIndex(http::Context &ctx);
	int CheckAuth(http::Context &ctx);
	int BeginTx(http::Context &ctx);
	int CommitTx(http::Context &ctx);
	int RollbackTx(http::Context &ctx);
	int PostItemsTx(http::Context &ctx);
	int PutItemsTx(http::Context &ctx);
	int PatchItemsTx(http::Context &ctx);
	int DeleteItemsTx(http::Context &ctx);
	int GetSQLQueryTx(http::Context &ctx);
	int DeleteQueryTx(http::Context &ctx);
	void Logger(http::Context &ctx);
	void OnResponse(http::Context &ctx);

protected:
	Error modifyItem(Reindexer &db, string &nsName, Item &item, ItemModifyMode mode);
	int modifyItems(http::Context &ctx, ItemModifyMode mode);
	int modifyItemsTx(http::Context &ctx, ItemModifyMode mode);
	int modifyItemsProtobuf(http::Context &ctx, string &nsName, const vector<string> &precepts, ItemModifyMode mode);
	int modifyItemsMsgPack(http::Context &ctx, string &nsName, const vector<string> &precepts, ItemModifyMode mode);
	int modifyItemsJSON(http::Context &ctx, string &nsName, const vector<string> &precepts, ItemModifyMode mode);
	int modifyItemsTxMsgPack(http::Context &ctx, Transaction &tx, const vector<string> &precepts, ItemModifyMode mode);
	int modifyItemsTxJSON(http::Context &ctx, Transaction &tx, const vector<string> &precepts, ItemModifyMode mode);
	int queryResults(http::Context &ctx, reindexer::QueryResults &res, bool isQueryResults = false, unsigned limit = kDefaultLimit,
					 unsigned offset = kDefaultOffset);
	int queryResultsMsgPack(http::Context &ctx, reindexer::QueryResults &res, bool isQueryResults, unsigned limit, unsigned offset,
							bool withColumns, int width = 0);
	int queryResultsProtobuf(http::Context &ctx, reindexer::QueryResults &res, bool isQueryResults, unsigned limit, unsigned offset,
							 bool withColumns, int width = 0);
	int queryResultsJSON(http::Context &ctx, reindexer::QueryResults &res, bool isQueryResults, unsigned limit, unsigned offset,
						 bool withColumns, int width = 0);
	template <typename Builder>
	void queryResultParams(Builder &builder, reindexer::QueryResults &res, bool isQueryResults, unsigned limit, bool withColumns,
						   int width);
	int status(http::Context &ctx, const http::HttpStatus &status = http::HttpStatus());
	int jsonStatus(http::Context &ctx, const http::HttpStatus &status = http::HttpStatus());
	int msgpackStatus(http::Context &ctx, const http::HttpStatus &status = http::HttpStatus());
	int protobufStatus(http::Context &ctx, const http::HttpStatus &status = http::HttpStatus());
	unsigned prepareLimit(const string_view &limitParam, int limitDefault = kDefaultLimit);
	unsigned prepareOffset(const string_view &offsetParam, int offsetDefault = kDefaultOffset);
	int modifyQueryTxImpl(http::Context &ctx, const std::string &dbName, string_view txId, Query &q);

	Reindexer getDB(http::Context &ctx, UserRole role, std::string *dbNameOut = nullptr);
	string getNameFromJson(string_view json);
	constexpr static string_view statsSourceName() { return "http"_sv; }

	std::shared_ptr<Transaction> getTx(const string &dbName, string_view txId);
	string addTx(string dbName, Transaction &&tx);
	void removeTx(const string &dbName, string_view txId);
	void removeExpiredTx();
	void deadlineTimerCb(ev::periodic &, int) { removeExpiredTx(); }

	DBManager &dbMgr_;
	Pprof pprof_;
	Prometheus *prometheus_;
	IStatsWatcher *statsWatcher_;

	string webRoot_;

	http::Router router_;
	std::unique_ptr<Listener> listener_;

	LoggerWrapper logger_;
	bool allocDebug_;
	bool enablePprof_;

	std::chrono::system_clock::time_point startTs_;

	using TxDeadlineClock = std::chrono::steady_clock;
	struct TxInfo {
		std::shared_ptr<Transaction> tx;
		std::chrono::time_point<TxDeadlineClock> txDeadline;
		string dbName;
	};
	fast_hash_map<string, TxInfo, nocase_hash_str, nocase_equal_str> txMap_;
	std::mutex txMtx_;
	std::chrono::seconds txIdleTimeout_;
	ev::timer deadlineChecker_;

	static const int kDefaultLimit = INT_MAX;
	static const int kDefaultOffset = 0;

	const string rpcAddress_;
	const string httpAddress_;
	const string storagePath_;
	const string rpcLog_;
	const string httpLog_;
	const string logLevel_;
	const string coreLog_;
	const string serverLog_;
};

}  // namespace reindexer_server
