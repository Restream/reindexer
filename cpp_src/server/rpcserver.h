#pragma once

#include <estl/fast_hash_set.h>
#include <memory>
#include <unordered_map>
#include "config.h"
#include "core/cbinding/resultserializer.h"
#include "core/keyvalue/variant.h"
#include "core/namespace/snapshot/snapshot.h"
#include "core/reindexer.h"
#include "dbmanager.h"
#include "net/cproto/dispatcher.h"
#include "net/listener.h"
#include "rpcqrwatcher.h"
#include "statscollect/istatswatcher.h"
#include "tools/semversion.h"

#include "replv3/rpcupdatespusher.h"

namespace reindexer {
struct TxStats;
}

namespace reindexer_server {

using namespace reindexer::net;
using namespace reindexer;

enum class RPCSocketT : bool { Unx, TCP };

struct RPCClientData final : public cproto::ClientData {
	~RPCClientData();
	h_vector<RPCQrId, 8> results;
	h_vector<std::pair<Snapshot, bool>, 1> snapshots;
	std::vector<Transaction> txs;
	fast_hash_set<std::string, nocase_hash_str, nocase_equal_str, nocase_less_str> tmpNss;
	std::shared_ptr<TxStats> txStats;

	AuthContext auth;
	int connID;
	SemVersion rxVersion;
	BindingCapabilities caps;

#ifdef REINDEX_WITH_V3_FOLLOWERS
	cproto::RPCUpdatesPusher pusher;
	bool subscribed;
#endif	// REINDEX_WITH_V3_FOLLOWERS
};

class RPCServer {
public:
	RPCServer(DBManager &dbMgr, LoggerWrapper &logger, IClientsStats *clientsStats, const ServerConfig &serverConfig,
			  IStatsWatcher *statsCollector = nullptr);
	~RPCServer();

	bool Start(const std::string &addr, ev::dynamic_loop &loop, RPCSocketT sockDomain, std::string_view threadingMode);
	void Stop() {
		terminate_ = true;
		if (qrWatcherThread_.joinable()) {
			qrWatcherTerminateAsync_.send();
			qrWatcherThread_.join();
		}
		if (listener_) listener_->Stop();
		terminate_ = false;
	}

	Error Ping(cproto::Context &ctx);
	Error Login(cproto::Context &ctx, p_string login, p_string password, p_string db, std::optional<bool> createDBIfMissing,
				std::optional<bool> checkClusterID, std::optional<int> expectedClusterID, std::optional<p_string> clientRxVersion,
				std::optional<p_string> appName, std::optional<int64_t> bindingCaps);
	Error OpenDatabase(cproto::Context &ctx, p_string db, std::optional<bool> createDBIfMissing);
	Error CloseDatabase(cproto::Context &ctx);
	Error DropDatabase(cproto::Context &ctx);

	Error OpenNamespace(cproto::Context &ctx, p_string ns, std::optional<p_string> version);
	Error DropNamespace(cproto::Context &ctx, p_string ns);
	Error TruncateNamespace(cproto::Context &ctx, p_string ns);
	Error RenameNamespace(cproto::Context &ctx, p_string srcNsName, p_string dstNsName);
	Error CreateTemporaryNamespace(cproto::Context &ctx, p_string ns, int64_t v);

	Error CloseNamespace(cproto::Context &ctx, p_string ns);
	Error EnumNamespaces(cproto::Context &ctx, std::optional<int> opts, std::optional<p_string> filter);
	Error EnumDatabases(cproto::Context &ctx);

	Error AddIndex(cproto::Context &ctx, p_string ns, p_string indexDef);
	Error UpdateIndex(cproto::Context &ctx, p_string ns, p_string indexDef);
	Error DropIndex(cproto::Context &ctx, p_string ns, p_string index);

	Error SetSchema(cproto::Context &ctx, p_string ns, p_string schema);
	Error GetSchema(cproto::Context &ctx, p_string ns, int format);

	Error Commit(cproto::Context &ctx, p_string ns);

	Error ModifyItem(cproto::Context &ctx, p_string nsName, int format, p_string itemData, int mode, p_string percepsPack, int stateToken,
					 int txID);

	Error StartTransaction(cproto::Context &ctx, p_string nsName);

	Error AddTxItem(cproto::Context &ctx, int format, p_string itemData, int mode, p_string percepsPack, int stateToken, int64_t txID);
	Error DeleteQueryTx(cproto::Context &ctx, p_string query, int64_t txID) noexcept;
	Error UpdateQueryTx(cproto::Context &ctx, p_string query, int64_t txID) noexcept;
	Error PutMetaTx(cproto::Context &ctx, p_string key, p_string data, int64_t txID) noexcept;
	Error SetTagsMatcherTx(cproto::Context &ctx, int64_t statetoken, int64_t version, p_string data, int64_t txID) noexcept;

	Error CommitTx(cproto::Context &ctx, int64_t txId, std::optional<int> flags);
	Error RollbackTx(cproto::Context &ctx, int64_t txId);

	Error DeleteQuery(cproto::Context &ctx, p_string query, std::optional<int> flags) noexcept;
	Error UpdateQuery(cproto::Context &ctx, p_string query, std::optional<int> flags) noexcept;

	Error Select(cproto::Context &ctx, p_string query, int flags, int limit, p_string ptVersions);
	Error SelectSQL(cproto::Context &ctx, p_string query, int flags, int limit, p_string ptVersions);
	Error FetchResults(cproto::Context &ctx, int reqId, int flags, int offset, int limit, std::optional<int64_t> qrUID);
	Error CloseResults(cproto::Context &ctx, int reqId, std::optional<int64_t> qrUID, std::optional<bool> doNotReply);
	Error GetSQLSuggestions(cproto::Context &ctx, p_string query, int pos);
	Error GetReplState(cproto::Context &ctx, p_string ns);
	Error SetClusterizationStatus(cproto::Context &ctx, p_string ns, p_string serStatus);
	Error GetSnapshot(cproto::Context &ctx, p_string ns, p_string optsJson);
	Error FetchSnapshot(cproto::Context &ctx, int id, int64_t offset);
	Error ApplySnapshotChunk(cproto::Context &ctx, p_string ns, p_string rec);

	[[nodiscard]] Error ShardingControlRequest(cproto::Context &ctx, p_string data) noexcept;

	Error GetMeta(cproto::Context &ctx, p_string ns, p_string key, std::optional<int> options);
	Error PutMeta(cproto::Context &ctx, p_string ns, p_string key, p_string data);
	Error EnumMeta(cproto::Context &ctx, p_string ns);
	Error SubscribeUpdates(cproto::Context &ctx, int subscribe, std::optional<p_string> filterJson, std::optional<int> options);

	Error SuggestLeader(cproto::Context &ctx, p_string suggestion);
	Error LeadersPing(cproto::Context &ctx, p_string leader);
	Error GetRaftInfo(cproto::Context &ctx);
	Error ClusterControlRequest(cproto::Context &ctx, p_string data);
	Error SetTagsMatcher(cproto::Context &ctx, p_string ns, int64_t statetoken, int64_t version, p_string data);

	Error CheckAuth(cproto::Context &ctx);
	void Logger(cproto::Context &ctx, const Error &err, const cproto::Args &ret);
	void OnClose(cproto::Context &ctx, const Error &err);
	void OnResponse(cproto::Context &ctx);

protected:
	Error execSqlQueryByType(std::string_view sqlQuery, reindexer::QueryResults &res, int fetchLimit, cproto::Context &ctx) noexcept;
	Error sendResults(cproto::Context &ctx, QueryResults &qr, RPCQrId id, const ResultFetchOpts &opts);
	Error processTxItem(DataFormat format, std::string_view itemData, Item &item, ItemModifyMode mode, int stateToken) const noexcept;

	RPCQrWatcher::Ref createQueryResults(cproto::Context &ctx, RPCQrId &id, int flags = 0);
	void freeQueryResults(cproto::Context &ctx, RPCQrId id);
	Transaction &getTx(cproto::Context &ctx, int64_t id);
	int64_t addTx(cproto::Context &ctx, std::string_view nsName);
	void clearTx(cproto::Context &ctx, uint64_t txId);
	Snapshot &getSnapshot(cproto::Context &ctx, int &id);
	Error fetchSnapshotRecords(cproto::Context &ctx, int id, int64_t offset, bool putHeaders);
	void freeSnapshot(cproto::Context &ctx, int id);

	Reindexer getDB(cproto::Context &ctx, UserRole role);
	void cleanupTmpNamespaces(RPCClientData &clientData, std::string_view activity = "tmp_ns_cleanup_on_close");
	constexpr static std::string_view statsSourceName() noexcept { return std::string_view{"rpc"}; }

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
	std::string_view protocolName_;
};

}  // namespace reindexer_server
