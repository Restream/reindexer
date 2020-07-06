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
		OptionalConfig(bool allocDebugI = false, bool enablePprofI = false, Prometheus *prometheusI = nullptr,
					   IStatsWatcher *statsWatcherI = nullptr)
			: allocDebug(allocDebugI), enablePprof(enablePprofI), prometheus(prometheusI), statsWatcher(statsWatcherI) {}
		bool allocDebug;
		bool enablePprof;
		Prometheus *prometheus;
		IStatsWatcher *statsWatcher;
	};

	HTTPServer(DBManager &dbMgr, const string &webRoot, LoggerWrapper logger, OptionalConfig config = OptionalConfig());

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
	int PutItems(http::Context &ctx);
	int DeleteItems(http::Context &ctx);
	int GetIndexes(http::Context &ctx);
	int PostIndex(http::Context &ctx);
	int PutIndex(http::Context &ctx);
	int PutSchema(http::Context &ctx);
	int GetSchema(http::Context &ctx);
	int GetMetaList(http::Context &ctx);
	int GetMetaByKey(http::Context &ctx);
	int PutMetaByKey(http::Context &ctx);
	int DeleteIndex(http::Context &ctx);
	int CheckAuth(http::Context &ctx);
	void Logger(http::Context &ctx);
	void OnResponse(http::Context &ctx);

protected:
	Error modifyItem(Reindexer &db, string &nsName, Item &item, int mode);
	int modifyItems(http::Context &ctx, int mode);
	int modifyItemsMsgPack(http::Context &ctx, string &nsName, const vector<string> &precepts, int mode);
	int modifyItemsJSON(http::Context &ctx, string &nsName, const vector<string> &precepts, int mode);
	int queryResults(http::Context &ctx, reindexer::QueryResults &res, bool isQueryResults = false, unsigned limit = kDefaultLimit,
					 unsigned offset = kDefaultOffset);
	int queryResultsMsgPack(http::Context &ctx, reindexer::QueryResults &res, bool isQueryResults, unsigned limit, unsigned offset,
							bool withColumns, int width = 0);
	int queryResultsJSON(http::Context &ctx, reindexer::QueryResults &res, bool isQueryResults, unsigned limit, unsigned offset,
						 bool withColumns, int width = 0);
	template <typename Builder>
	void queryResultParams(Builder &builder, reindexer::QueryResults &res, bool isQueryResults, unsigned limit, bool withColumns,
						   int width);
	int status(http::Context &ctx, Error status);
	int jsonStatus(http::Context &ctx, http::HttpStatus status = http::HttpStatus());
	int msgpackStatus(http::Context &ctx, Error status);
	unsigned prepareLimit(const string_view &limitParam, int limitDefault = kDefaultLimit);
	unsigned prepareOffset(const string_view &offsetParam, int offsetDefault = kDefaultOffset);

	Reindexer getDB(http::Context &ctx, UserRole role);
	string getNameFromJson(string_view json);
	constexpr static string_view statsSourceName() { return "http"_sv; }

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

	static const int kDefaultLimit = INT_MAX;
	static const int kDefaultOffset = 0;
	static const int kDefaultItemsLimit = 10;
};

}  // namespace reindexer_server
