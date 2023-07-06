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

using namespace reindexer::net;

struct HTTPClientData : public http::ClientData {
	AuthContext auth;
};

class HTTPServer {
public:
	HTTPServer(DBManager &dbMgr, LoggerWrapper &logger, const ServerConfig &serverConfig, Prometheus *prometheusI = nullptr,
			   IStatsWatcher *statsWatcherI = nullptr);

	bool Start(const std::string &addr, ev::dynamic_loop &loop);
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
	int PostMemReset(http::Context &ctx);
	int GetMemInfo(http::Context &ctx);
	void Logger(http::Context &ctx);
	void OnResponse(http::Context &ctx);

protected:
	Error modifyItem(Reindexer &db, std::string &nsName, Item &item, ItemModifyMode mode);
	Error modifyItem(Reindexer &db, std::string &nsName, Item &item, QueryResults &, ItemModifyMode mode);
	int modifyItems(http::Context &ctx, ItemModifyMode mode);
	int modifyItemsTx(http::Context &ctx, ItemModifyMode mode);
	int modifyItemsProtobuf(http::Context &ctx, std::string &nsName, const std::vector<std::string> &precepts, ItemModifyMode mode);
	int modifyItemsMsgPack(http::Context &ctx, std::string &nsName, const std::vector<std::string> &precepts, ItemModifyMode mode);
	int modifyItemsJSON(http::Context &ctx, std::string &nsName, const std::vector<std::string> &precepts, ItemModifyMode mode);
	int modifyItemsTxMsgPack(http::Context &ctx, Transaction &tx, const std::vector<std::string> &precepts, ItemModifyMode mode);
	int modifyItemsTxJSON(http::Context &ctx, Transaction &tx, const std::vector<std::string> &precepts, ItemModifyMode mode);
	int queryResults(http::Context &ctx, reindexer::QueryResults &res, bool isQueryResults = false, unsigned limit = kDefaultLimit,
					 unsigned offset = kDefaultOffset);
	int queryResultsMsgPack(http::Context &ctx, const reindexer::QueryResults &res, bool isQueryResults, unsigned limit, unsigned offset,
							bool withColumns, int width = 0);
	int queryResultsProtobuf(http::Context &ctx, const reindexer::QueryResults &res, bool isQueryResults, unsigned limit, unsigned offset,
							 bool withColumns, int width = 0);
	int queryResultsJSON(http::Context &ctx, const reindexer::QueryResults &res, bool isQueryResults, unsigned limit, unsigned offset,
						 bool withColumns, int width = 0);
	int queryResultsCSV(http::Context &ctx, reindexer::QueryResults &res, unsigned limit, unsigned offset);
	template <typename Builder>
	void queryResultParams(Builder &builder, const reindexer::QueryResults &res, bool isQueryResults, unsigned limit, bool withColumns,
						   int width);
	int status(http::Context &ctx, const http::HttpStatus &status = http::HttpStatus());
	int jsonStatus(http::Context &ctx, const http::HttpStatus &status = http::HttpStatus());
	int msgpackStatus(http::Context &ctx, const http::HttpStatus &status = http::HttpStatus());
	int protobufStatus(http::Context &ctx, const http::HttpStatus &status = http::HttpStatus());
	unsigned prepareLimit(std::string_view limitParam, int limitDefault = kDefaultLimit);
	unsigned prepareOffset(std::string_view offsetParam, int offsetDefault = kDefaultOffset);
	int modifyQueryTxImpl(http::Context &ctx, const std::string &dbName, std::string_view txId, Query &q);

	Reindexer getDB(http::Context &ctx, UserRole role, std::string *dbNameOut = nullptr);
	std::string getNameFromJson(std::string_view json);
	constexpr static std::string_view statsSourceName() { return std::string_view{"http"}; }

	std::shared_ptr<Transaction> getTx(const std::string &dbName, std::string_view txId);
	std::string addTx(std::string dbName, Transaction &&tx);
	void removeTx(const std::string &dbName, std::string_view txId);
	void removeExpiredTx();
	void deadlineTimerCb(ev::periodic &, int) { removeExpiredTx(); }

	DBManager &dbMgr_;
	Pprof pprof_;
	const ServerConfig &serverConfig_;
	Prometheus *prometheus_;
	IStatsWatcher *statsWatcher_;
	const std::string webRoot_;

	http::Router router_;
	std::unique_ptr<IListener> listener_;

	LoggerWrapper logger_;

	std::chrono::system_clock::time_point startTs_;

	using TxDeadlineClock = std::chrono::steady_clock;
	struct TxInfo {
		std::shared_ptr<Transaction> tx;
		std::chrono::time_point<TxDeadlineClock> txDeadline;
		std::string dbName;
	};
	fast_hash_map<std::string, TxInfo, nocase_hash_str, nocase_equal_str, nocase_less_str> txMap_;
	std::mutex txMtx_;
	ev::timer deadlineChecker_;

	static const int kDefaultLimit = INT_MAX;
	static const int kDefaultOffset = 0;

	constexpr static int32_t kMaxConcurrentCsvDownloads = 2;
	std::atomic<int32_t> currentCsvDownloads_ = {0};

private:
	Error execSqlQueryByType(std::string_view sqlQuery, reindexer::QueryResults &res, http::Context &ctx);
};

}  // namespace reindexer_server
