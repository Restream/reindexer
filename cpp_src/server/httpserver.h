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

namespace reindexer_server {

class Prometheus;
struct IStatsWatcher;

using namespace reindexer::net;

struct HTTPClientData final : public http::ClientData {
	AuthContext auth;
};

class [[nodiscard]] HTTPServer final {
public:
	HTTPServer(DBManager& dbMgr, LoggerWrapper& logger, const ServerConfig& serverConfig, Prometheus* prometheusI = nullptr,
			   IStatsWatcher* statsWatcherI = nullptr);

	void Start(const std::string& addr, ev::dynamic_loop& loop);
	void Stop() { listener_->Stop(); }

	[[nodiscard]] int NotFoundHandler(http::Context& ctx);
	[[nodiscard]] int DocHandler(http::Context& ctx);
	[[nodiscard]] int Check(http::Context& ctx);
	[[nodiscard]] int PostQuery(http::Context& ctx);
	[[nodiscard]] int DeleteQuery(http::Context& ctx);
	[[nodiscard]] int UpdateQuery(http::Context& ctx);
	[[nodiscard]] int GetSQLQuery(http::Context& ctx);
	[[nodiscard]] int PostSQLQuery(http::Context& ctx);
	[[nodiscard]] int GetSQLSuggest(http::Context& ctx);
	[[nodiscard]] int GetDatabases(http::Context& ctx);
	[[nodiscard]] int PostDatabase(http::Context& ctx);
	[[nodiscard]] int DeleteDatabase(http::Context& ctx);
	[[nodiscard]] int GetNamespaces(http::Context& ctx);
	[[nodiscard]] int GetNamespace(http::Context& ctx);
	[[nodiscard]] int PostNamespace(http::Context& ctx);
	[[nodiscard]] int DeleteNamespace(http::Context& ctx);
	[[nodiscard]] int TruncateNamespace(http::Context& ctx);
	[[nodiscard]] int RenameNamespace(http::Context& ctx);
	[[nodiscard]] int GetItems(http::Context& ctx);
	[[nodiscard]] int PostItems(http::Context& ctx);
	[[nodiscard]] int PatchItems(http::Context& ctx);
	[[nodiscard]] int PutItems(http::Context& ctx);
	[[nodiscard]] int DeleteItems(http::Context& ctx);
	[[nodiscard]] int GetIndexes(http::Context& ctx);
	[[nodiscard]] int PostIndex(http::Context& ctx);
	[[nodiscard]] int PutIndex(http::Context& ctx);
	[[nodiscard]] int PutSchema(http::Context& ctx);
	[[nodiscard]] int GetSchema(http::Context& ctx);
	[[nodiscard]] int GetProtobufSchema(http::Context& ctx);
	[[nodiscard]] int GetMetaList(http::Context& ctx);
	[[nodiscard]] int GetMetaByKey(http::Context& ctx);
	[[nodiscard]] int PutMetaByKey(http::Context& ctx);
	[[nodiscard]] int DeleteMetaByKey(http::Context& ctx);
	[[nodiscard]] int DeleteIndex(http::Context& ctx);
	[[nodiscard]] int CheckAuth(http::Context& ctx);
	[[nodiscard]] int BeginTx(http::Context& ctx);
	[[nodiscard]] int CommitTx(http::Context& ctx);
	[[nodiscard]] int RollbackTx(http::Context& ctx);
	[[nodiscard]] int PostItemsTx(http::Context& ctx);
	[[nodiscard]] int PutItemsTx(http::Context& ctx);
	[[nodiscard]] int PatchItemsTx(http::Context& ctx);
	[[nodiscard]] int DeleteItemsTx(http::Context& ctx);
	[[nodiscard]] int GetSQLQueryTx(http::Context& ctx);
	[[nodiscard]] int DeleteQueryTx(http::Context& ctx);
	[[nodiscard]] int PostMemReset(http::Context& ctx);
	[[nodiscard]] int GetMemInfo(http::Context& ctx);
	void Logger(http::Context& ctx);
	void OnResponse(http::Context& ctx);
	[[nodiscard]] int GetRole(http::Context& ctx);
	[[nodiscard]] int GetDefaultConfigs(http::Context& ctx);

private:
	[[nodiscard]] Error modifyItem(Reindexer& db, std::string_view nsName, Item& item, ItemModifyMode mode);
	[[nodiscard]] Error modifyItem(Reindexer& db, std::string_view nsName, Item& item, QueryResults&, ItemModifyMode mode);
	[[nodiscard]] int modifyItems(http::Context& ctx, ItemModifyMode mode);
	[[nodiscard]] int modifyItemsTx(http::Context& ctx, ItemModifyMode mode);
	[[nodiscard]] int modifyItemsProtobuf(http::Context& ctx, std::string_view nsName, std::vector<std::string>&& precepts,
										  ItemModifyMode mode);
	[[nodiscard]] int modifyItemsMsgPack(http::Context& ctx, std::string_view nsName, std::vector<std::string>&& precepts,
										 ItemModifyMode mode);
	[[nodiscard]] int modifyItemsJSON(http::Context& ctx, std::string_view nsName, std::vector<std::string>&& precepts,
									  ItemModifyMode mode);
	[[nodiscard]] int modifyItemsTxMsgPack(http::Context& ctx, Transaction& tx, std::vector<std::string>&& precepts, ItemModifyMode mode);
	[[nodiscard]] int modifyItemsTxJSON(http::Context& ctx, Transaction& tx, std::vector<std::string>&& precepts, ItemModifyMode mode);
	[[nodiscard]] int queryResults(http::Context& ctx, reindexer::QueryResults& res, bool isQueryResults = false,
								   unsigned limit = kDefaultLimit, unsigned offset = kDefaultOffset);
	[[nodiscard]] int queryResultsMsgPack(http::Context& ctx, reindexer::QueryResults& res, bool isQueryResults, unsigned limit, unsigned offset,
							bool withColumns, int width = 0);
	[[nodiscard]] int queryResultsProtobuf(http::Context& ctx, reindexer::QueryResults& res, bool isQueryResults, unsigned limit, unsigned offset,
							 bool withColumns, int width = 0);
	[[nodiscard]] int queryResultsJSON(http::Context& ctx, reindexer::QueryResults& res, bool isQueryResults, unsigned limit, unsigned offset,
						 bool withColumns, int width = 0);
	[[nodiscard]] int queryResultsCSV(http::Context& ctx, reindexer::QueryResults& res, unsigned limit, unsigned offset);
	template <typename Builder>
	void queryResultParams(Builder& builder, reindexer::QueryResults& res, std::vector<std::string>&& jsonData, bool isQueryResults,
						   unsigned limit, bool withColumns, int width);
	[[nodiscard]] int statusOK(http::Context& ctx, chunk&& chunk);
	[[nodiscard]] int status(http::Context& ctx, const http::HttpStatus& status = http::HttpStatus());
	[[nodiscard]] int jsonStatus(http::Context& ctx, const http::HttpStatus& status = http::HttpStatus());
	[[nodiscard]] int msgpackStatus(http::Context& ctx, const http::HttpStatus& status = http::HttpStatus());
	[[nodiscard]] int protobufStatus(http::Context& ctx, const http::HttpStatus& status = http::HttpStatus());
	[[nodiscard]] unsigned prepareLimit(std::string_view limitParam, int limitDefault = kDefaultLimit);
	[[nodiscard]] unsigned prepareOffset(std::string_view offsetParam, int offsetDefault = kDefaultOffset);
	[[nodiscard]] int modifyQueryTxImpl(http::Context& ctx, const std::string& dbName, std::string_view txId, Query& q);

	template <UserRole role>
	[[nodiscard]] Reindexer getDB(http::Context& ctx, std::string* dbNameOut = nullptr);
	[[nodiscard]] std::string getNameFromJson(std::string_view json);
	[[nodiscard]] constexpr static std::string_view statsSourceName() { return std::string_view{"http"}; }

	[[nodiscard]] std::shared_ptr<Transaction> getTx(const std::string& dbName, std::string_view txId);
	[[nodiscard]] std::string addTx(std::string dbName, Transaction&& tx);
	void removeTx(const std::string& dbName, std::string_view txId);
	void removeExpiredTx();
	void deadlineTimerCb(ev::periodic&, int) { removeExpiredTx(); }

	[[nodiscard]] Error execSqlQueryByType(std::string_view sqlQuery, reindexer::QueryResults& res, http::Context& ctx);
	[[nodiscard]] bool isParameterSetOn(std::string_view val) const noexcept;
	[[nodiscard]] int getAuth(http::Context& ctx, AuthContext& auth, const std::string& dbName) const;

	DBManager& dbMgr_;
	Pprof pprof_;
	const ServerConfig& serverConfig_;
	Prometheus* prometheus_;
	IStatsWatcher* statsWatcher_;
	const std::string webRoot_;

	http::Router router_;
	std::unique_ptr<IListener> listener_;

	LoggerWrapper logger_;

	system_clock_w::time_point startTs_;

	using TxDeadlineClock = steady_clock_w;
	struct TxInfo {
		std::shared_ptr<Transaction> tx;
		TxDeadlineClock::time_point txDeadline;
		std::string dbName;
	};
	fast_hash_map<std::string, TxInfo, nocase_hash_str, nocase_equal_str, nocase_less_str> txMap_;
	std::mutex txMtx_;
	ev::timer deadlineChecker_;

	constexpr static int kDefaultLimit = std::numeric_limits<int>::max();
	constexpr static int kDefaultOffset = 0;

	constexpr static int32_t kMaxConcurrentCsvDownloads = 2;
	std::atomic<int32_t> currentCsvDownloads_ = {0};
};

}  // namespace reindexer_server
