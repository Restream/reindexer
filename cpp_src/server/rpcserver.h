#pragma once

#include <memory>
#include <unordered_map>
#include "config.h"
#include "core/cbinding/resultserializer.h"
#include "core/keyvalue/variant.h"
#include "core/reindexer.h"
#include "dbmanager.h"
#include "net/cproto/dispatcher.h"
#include "net/listener.h"
#include "rpcqrwatcher.h"
#include "rpcupdatespusher.h"
#include "statscollect/istatswatcher.h"
#include "tools/semversion.h"

namespace reindexer {
struct TxStats;
}

namespace reindexer_server {

using namespace reindexer::net;
using namespace reindexer;

struct RPCClientData : public cproto::ClientData {
	~RPCClientData();
	h_vector<RPCQrId, 8> results;
	std::vector<Transaction> txs;
	std::shared_ptr<TxStats> txStats;

	AuthContext auth;
	cproto::RPCUpdatesPusher pusher;
	int connID;
	bool subscribed;
	SemVersion rxVersion;
};

class RPCServer {
public:
	RPCServer(DBManager &dbMgr, LoggerWrapper &logger, IClientsStats *clientsStats, const ServerConfig &serverConfig,
			  IStatsWatcher *statsCollector = nullptr);
	~RPCServer();

	bool Start(const std::string &addr, ev::dynamic_loop &loop);
	void Stop() {
		terminate_ = true;
		if (qrWatcherThread_.joinable()) {
			qrWatcherTerminateAsync_.send();
			qrWatcherThread_.join();
		}
		listener_->Stop();
		terminate_ = false;
	}

	Error Ping(cproto::Context &ctx);
	Error Login(cproto::Context &ctx, p_string login, p_string password, p_string db, std::optional<bool> createDBIfMissing,
				std::optional<bool> checkClusterID, std::optional<int> expectedClusterID, std::optional<p_string> clientRxVersion,
				std::optional<p_string> appName);
	Error OpenDatabase(cproto::Context &ctx, p_string db, std::optional<bool> createDBIfMissing);
	Error CloseDatabase(cproto::Context &ctx);
	Error DropDatabase(cproto::Context &ctx);

	Error OpenNamespace(cproto::Context &ctx, p_string ns);
	Error DropNamespace(cproto::Context &ctx, p_string ns);
	Error TruncateNamespace(cproto::Context &ctx, p_string ns);
	Error RenameNamespace(cproto::Context &ctx, p_string srcNsName, p_string dstNsName);

	Error CloseNamespace(cproto::Context &ctx, p_string ns);
	Error EnumNamespaces(cproto::Context &ctx, std::optional<int> opts, std::optional<p_string> filter);
	Error EnumDatabases(cproto::Context &ctx);

	Error AddIndex(cproto::Context &ctx, p_string ns, p_string indexDef);
	Error UpdateIndex(cproto::Context &ctx, p_string ns, p_string indexDef);
	Error DropIndex(cproto::Context &ctx, p_string ns, p_string index);

	Error SetSchema(cproto::Context &ctx, p_string ns, p_string schema);

	Error Commit(cproto::Context &ctx, p_string ns);

	Error ModifyItem(cproto::Context &ctx, p_string nsName, int format, p_string itemData, int mode, p_string percepsPack, int stateToken,
					 int txID);

	Error StartTransaction(cproto::Context &ctx, p_string nsName);

	Error AddTxItem(cproto::Context &ctx, int format, p_string itemData, int mode, p_string percepsPack, int stateToken, int64_t txID);
	Error DeleteQueryTx(cproto::Context &ctx, p_string query, int64_t txID);
	Error UpdateQueryTx(cproto::Context &ctx, p_string query, int64_t txID);

	Error CommitTx(cproto::Context &ctx, int64_t txId, std::optional<int> flags);
	Error RollbackTx(cproto::Context &ctx, int64_t txId);

	Error DeleteQuery(cproto::Context &ctx, p_string query, std::optional<int> flags);
	Error UpdateQuery(cproto::Context &ctx, p_string query, std::optional<int> flags);

	Error Select(cproto::Context &ctx, p_string query, int flags, int limit, p_string ptVersions);
	Error SelectSQL(cproto::Context &ctx, p_string query, int flags, int limit, p_string ptVersions);
	Error FetchResults(cproto::Context &ctx, int reqId, int flags, int offset, int limit, std::optional<int64_t> qrUID);
	Error CloseResults(cproto::Context &ctx, int reqId, std::optional<int64_t> qrUID, std::optional<bool> doNotReply);
	Error GetSQLSuggestions(cproto::Context &ctx, p_string query, int pos);

	Error GetMeta(cproto::Context &ctx, p_string ns, p_string key);
	Error PutMeta(cproto::Context &ctx, p_string ns, p_string key, p_string data);
	Error EnumMeta(cproto::Context &ctx, p_string ns);
	Error SubscribeUpdates(cproto::Context &ctx, int subscribe, std::optional<p_string> filterJson, std::optional<int> options);

	Error CheckAuth(cproto::Context &ctx);
	void Logger(cproto::Context &ctx, const Error &err, const cproto::Args &ret);
	void OnClose(cproto::Context &ctx, const Error &err);
	void OnResponse(cproto::Context &ctx);

protected:
	Error execSqlQueryByType(std::string_view sqlQuery, reindexer::QueryResults &res, cproto::Context &ctx);
	Error sendResults(cproto::Context &ctx, QueryResults &qr, RPCQrId id, const ResultFetchOpts &opts);
	Error processTxItem(DataFormat format, std::string_view itemData, Item &item, ItemModifyMode mode, int stateToken) const noexcept;

	RPCQrWatcher::Ref createQueryResults(cproto::Context &ctx, RPCQrId &id);
	void freeQueryResults(cproto::Context &ctx, RPCQrId id);
	Transaction &getTx(cproto::Context &ctx, int64_t id);
	int64_t addTx(cproto::Context &ctx, std::string_view nsName);
	void clearTx(cproto::Context &ctx, uint64_t txId);

	Reindexer getDB(cproto::Context &ctx, UserRole role);
	constexpr static std::string_view statsSourceName() { return std::string_view{"rpc"}; }

	DBManager &dbMgr_;
	cproto::Dispatcher dispatcher_;
	std::unique_ptr<IListener> listener_;
	const ServerConfig &serverConfig_;

	LoggerWrapper logger_;
	IStatsWatcher *statsWatcher_;

	IClientsStats *clientsStats_;

	std::chrono::system_clock::time_point startTs_;
	std::thread qrWatcherThread_;
	RPCQrWatcher qrWatcher_;
	std::atomic<bool> terminate_ = {false};
	ev::async qrWatcherTerminateAsync_;
};

}  // namespace reindexer_server
