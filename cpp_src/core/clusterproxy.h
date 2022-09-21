#pragma once
#include <condition_variable>
#include "client/synccororeindexer.h"
#include "core/item.h"
#include "core/reindexerimpl.h"
#include "core/shardedmeta.h"
#include "estl/shared_mutex.h"

namespace reindexer {

constexpr uint32_t kMaxClusterProxyConnCount = 64;
constexpr uint32_t kMaxClusterProxyConnConcurrency = 1024;

namespace sharding {
class LocatorService;
}

class ClusterProxy {
public:
	ClusterProxy(ReindexerConfig cfg);
	~ClusterProxy();
	Error Connect(const std::string &dsn, ConnectOpts opts = ConnectOpts());
	Error OpenNamespace(std::string_view nsName, const StorageOpts &opts, const NsReplicationOpts &replOpts, const InternalRdxContext &ctx);
	Error AddNamespace(const NamespaceDef &nsDef, const NsReplicationOpts &replOpts, const InternalRdxContext &ctx);
	Error CloseNamespace(std::string_view nsName, const InternalRdxContext &ctx);  // deleted metod
	Error DropNamespace(std::string_view nsName, const InternalRdxContext &ctx);
	Error TruncateNamespace(std::string_view nsName, const InternalRdxContext &ctx);
	Error RenameNamespace(std::string_view srcNsName, const std::string &dstNsName, const InternalRdxContext &ctx);
	Error AddIndex(std::string_view nsName, const IndexDef &index, const InternalRdxContext &ctx);
	Error UpdateIndex(std::string_view nsName, const IndexDef &index, const InternalRdxContext &ctx);
	Error DropIndex(std::string_view nsName, const IndexDef &index, const InternalRdxContext &ctx);
	Error SetSchema(std::string_view nsName, std::string_view schema, const InternalRdxContext &ctx);
	Error GetSchema(std::string_view nsName, int format, std::string &schema, const InternalRdxContext &ctx);
	Error EnumNamespaces(vector<NamespaceDef> &defs, EnumNamespacesOpts opts, const InternalRdxContext &ctx);
	Error Insert(std::string_view nsName, Item &item, const InternalRdxContext &ctx);
	Error Insert(std::string_view nsName, Item &item, QueryResults &result, const InternalRdxContext &ctx);
	Error Update(std::string_view nsName, Item &item, const InternalRdxContext &ctx);
	Error Update(std::string_view nsName, Item &item, QueryResults &result, const InternalRdxContext &ctx);
	Error Update(const Query &query, QueryResults &result, const InternalRdxContext &ctx);
	Error Upsert(std::string_view nsName, Item &item, const InternalRdxContext &ctx);
	Error Upsert(std::string_view nsName, Item &item, QueryResults &result, const InternalRdxContext &ctx);
	Error Delete(std::string_view nsName, Item &item, const InternalRdxContext &ctx);
	Error Delete(std::string_view nsName, Item &item, QueryResults &result, const InternalRdxContext &ctx);
	Error Delete(const Query &query, QueryResults &result, const InternalRdxContext &ctx);
	Error Select(std::string_view sql, QueryResults &result, unsigned proxyFetchLimit, const InternalRdxContext &ctx);
	Error Select(const Query &query, QueryResults &result, unsigned proxyFetchLimit, const InternalRdxContext &ctx);
	Error Commit(std::string_view nsName, const InternalRdxContext &ctx);
	Item NewItem(std::string_view nsName, const InternalRdxContext &ctx);

	Transaction NewTransaction(std::string_view nsName, const InternalRdxContext &ctx);
	Error CommitTransaction(Transaction &tr, QueryResults &result, const InternalRdxContext &ctx);
	Error RollBackTransaction(Transaction &tr, const InternalRdxContext &ctx);

	Error GetMeta(std::string_view nsName, const string &key, string &data, const InternalRdxContext &ctx);
	Error GetMeta(std::string_view nsName, const string &key, vector<ShardedMeta> &data, const InternalRdxContext &ctx);
	Error PutMeta(std::string_view nsName, const string &key, std::string_view data, const InternalRdxContext &ctx);
	Error EnumMeta(std::string_view nsName, vector<string> &keys, const InternalRdxContext &ctx);

	Error GetSqlSuggestions(const std::string_view sqlQuery, int pos, vector<string> &suggestions, const InternalRdxContext &ctx) {
		using namespace std::string_view_literals;
		const auto rdxCtx = ctx.CreateRdxContext("SQL SUGGESTIONS"sv, impl_.activities_);
		return impl_.GetSqlSuggestions(sqlQuery, pos, suggestions, rdxCtx);
	}
	Error Status() { return impl_.Status(); }
	Error GetProtobufSchema(WrSerializer &ser, vector<string> &namespaces) { return impl_.GetProtobufSchema(ser, namespaces); }
	Error GetReplState(std::string_view nsName, ReplicationStateV2 &state, const InternalRdxContext &ctx) {
		using namespace std::string_view_literals;
		WrSerializer ser;
		const auto rdxCtx =
			ctx.CreateRdxContext(ctx.NeedTraceActivity() ? (ser << "GET repl_state FROM " << nsName).Slice() : ""sv, impl_.activities_);
		return impl_.GetReplState(nsName, state, rdxCtx);
	}
	Error SetClusterizationStatus(std::string_view nsName, const ClusterizationStatus &status, const InternalRdxContext &ctx) {
		using namespace std::string_view_literals;
		WrSerializer ser;
		const auto rdxCtx = ctx.CreateRdxContext(
			ctx.NeedTraceActivity() ? (ser << "SET clusterization_status ON " << nsName).Slice() : ""sv, impl_.activities_);

		return impl_.SetClusterizationStatus(nsName, status, rdxCtx);
	}
	bool NeedTraceActivity() { return impl_.NeedTraceActivity(); }
	Error EnableStorage(const string &storagePath, bool skipPlaceholderCheck, const InternalRdxContext &ctx);
	Error InitSystemNamespaces() { return impl_.InitSystemNamespaces(); }
	Error ApplySnapshotChunk(std::string_view nsName, const SnapshotChunk &ch, const InternalRdxContext &ctx);

	Error SuggestLeader(const cluster::NodeData &suggestion, cluster::NodeData &response) {
		return impl_.SuggestLeader(suggestion, response);
	}
	Error LeadersPing(const cluster::NodeData &leader) {
		Error err = impl_.LeadersPing(leader);
		if (err.ok()) {
			std::unique_lock<std::mutex> lck(processPingEventMutex_);
			lastPingLeaderId_ = leader.serverId;
			lck.unlock();
			processPingEvent_.notify_all();
		}
		return err;
	}
	Error GetRaftInfo(cluster::RaftInfo &info, const InternalRdxContext &ctx);

	Error CreateTemporaryNamespace(std::string_view baseName, std::string &resultName, const StorageOpts &opts, lsn_t nsVersion,
								   const InternalRdxContext &ctx);
	Error GetSnapshot(std::string_view nsName, const SnapshotOpts &opts, Snapshot &snapshot, const InternalRdxContext &ctx);
	Error ClusterControlRequest(const ClusterControlRequestData &request) { return impl_.ClusterControlRequest(request); }
	Error SetTagsMatcher(std::string_view nsName, TagsMatcher &&tm, const InternalRdxContext &ctx) {
		using namespace std::string_view_literals;
		WrSerializer ser;
		const auto rdxCtx =
			ctx.CreateRdxContext(ctx.NeedTraceActivity() ? (ser << "SET TAGSMATCHER " << nsName).Slice() : ""sv, impl_.activities_);
		return impl_.SetTagsMatcher(nsName, std::move(tm), rdxCtx);
	}
	Error DumpIndex(std::ostream &os, std::string_view nsName, std::string_view index, const InternalRdxContext &ctx) {
		using namespace std::string_view_literals;
		WrSerializer ser;
		const auto rdxCtx = ctx.CreateRdxContext(
			ctx.NeedTraceActivity() ? (ser << "DUMP INDEX " << index << " ON " << nsName).Slice() : ""sv, impl_.activities_);
		return impl_.DumpIndex(os, nsName, index, rdxCtx);
	}
	void ShutdownCluster();

private:
	class ConnectionsMap {
	public:
		void SetParams(int clientThreads, int clientConns, int clientConnConcurrency) {
			std::lock_guard lck(mtx_);
			clientThreads_ = clientThreads > 0 ? clientThreads : cluster::kDefaultClusterProxyConnThreads;
			clientConns_ =
				clientConns > 0 ? (std::min(uint32_t(clientConns), kMaxClusterProxyConnCount)) : cluster::kDefaultClusterProxyConnCount;
			clientConnConcurrency_ = clientConnConcurrency > 0
										 ? (std::min(uint32_t(clientConnConcurrency), kMaxClusterProxyConnConcurrency))
										 : cluster::kDefaultClusterProxyCoroPerConn;
		}
		std::shared_ptr<client::SyncCoroReindexer> Get(const std::string &dsn) {
			std::shared_ptr<client::SyncCoroReindexer> res;
			{
				shared_lock lck(mtx_);
				if (shutdown_) {
					throw Error(errTerminated, "Proxy is already shut down");
				}
				auto found = conns_.find(dsn);
				if (found != conns_.end()) {
					res = found->second;
				}
			}

			client::CoroReindexerConfig cfg;
			cfg.AppName = "cluster_proxy";
			cfg.SyncRxCoroCount = clientConnConcurrency_;
			cfg.EnableCompression = true;

			std::lock_guard lck(mtx_);
			if (shutdown_) {
				throw Error(errTerminated, "Proxy is already shut down");
			}
			auto found = conns_.find(dsn);
			if (found != conns_.end()) {
				return res;
			}
			res = std::make_shared<client::SyncCoroReindexer>(cfg, clientConns_, clientThreads_);
			auto err = res->Connect(dsn);
			if (!err.ok()) {
				throw err;
			}
			conns_[dsn] = res;
			return res;
		}
		void Shutdown() {
			std::lock_guard lck(mtx_);
			shutdown_ = true;
			for (auto &conn : conns_) {
				conn.second->Stop();
			}
		}

	private:
		shared_timed_mutex mtx_;
		fast_hash_map<std::string, std::shared_ptr<client::SyncCoroReindexer>> conns_;
		bool shutdown_ = false;
		uint32_t clientThreads_ = cluster::kDefaultClusterProxyConnThreads;
		uint32_t clientConns_ = cluster::kDefaultClusterProxyConnCount;
		uint32_t clientConnConcurrency_ = cluster::kDefaultClusterProxyCoroPerConn;
	};

	ReindexerImpl impl_;
	shared_timed_mutex mtx_;
	int32_t leaderId_ = -1;
	std::shared_ptr<client::SyncCoroReindexer> leader_;
	ConnectionsMap clusterConns_;
	std::atomic<unsigned short> sId_;
	int configHandlerId_;
	std::shared_ptr<sharding::LocatorService> shardingRouter_;
	std::atomic_bool shardingInitialized_ = {false};

	std::condition_variable processPingEvent_;
	std::mutex processPingEventMutex_;
	int lastPingLeaderId_ = -1;

	std::shared_ptr<client::SyncCoroReindexer> getLeader(const cluster::RaftInfo &info);
	void resetLeader();

	template <typename R>
	ErrorCode getErrCode(const Error &err, R &r);

	template <typename R>
	void setErrorCode(R &r, Error &&err);

	template <typename Fn, Fn fn, typename R, typename... Args>
	R localCall(const RdxContext &ctx, Args &&...args);

	template <typename Fn, Fn fn, typename FnL, FnL fnl, typename R, typename FnA, typename... Args>
	R proxyCall(const RdxContext &ctx, std::string_view nsName, FnA &action, Args &&...args);

	template <typename FnL, FnL fnl, typename... Args>
	Error baseFollowerAction(const RdxContext &ctx, std::shared_ptr<client::SyncCoroReindexer> clientToLeader, Args &&...args);

	template <typename FnL, FnL fnl>
	Error itemFollowerAction(const RdxContext &ctx, std::shared_ptr<client::SyncCoroReindexer> clientToLeader, std::string_view nsName,
							 Item &item);
	template <typename FnL, FnL fnl>
	Error resultFollowerAction(const RdxContext &ctx, std::shared_ptr<client::SyncCoroReindexer> clientToLeader, const Query &query,
							   LocalQueryResults &result);

	template <typename FnL, FnL fnl>
	Error resultItemFollowerAction(const RdxContext &ctx, std::shared_ptr<client::SyncCoroReindexer> clientToLeader,
								   std::string_view nsName, Item &item, LocalQueryResults &result);

	Transaction newTxFollowerAction(bool isWithSharding, const RdxContext &ctx, std::shared_ptr<client::SyncCoroReindexer> clientToLeader,
									std::string_view nsName);

	reindexer::client::Item toClientItem(std::string_view ns, client::SyncCoroReindexer *connection, reindexer::Item &clientItem);
	void clientToCoreQueryResults(client::SyncCoroQueryResults &, LocalQueryResults &);

	bool isWithSharding(const Query &q, const InternalRdxContext &ctx) const;
	bool isWithSharding(std::string_view nsName, const InternalRdxContext &ctx) const noexcept;
	bool isWithSharding(const InternalRdxContext &ctx) const noexcept;
	bool isSharderQuery(const Query &q) const;
	bool shouldProxyQuery(const Query &q);

	template <typename Func, typename FLocal, typename... Args>
	Error delegateToShards(const RdxContext &rdxCtx, Func f, const FLocal &&local, Args &&...args);

	template <typename Func, typename FLocal, typename... Args>
	Error delegateToShardsByNs(const RdxContext &rdxCtx, Func f, const FLocal &&local, std::string_view nsName, Args &&...args);

	typedef Error (client::SyncCoroReindexer::*ItemModifyFun)(std::string_view, client::Item &);

	template <ItemModifyFun fn>
	Error modifyItemOnShard(const InternalRdxContext &ctx, const RdxContext &rdxCtx, std::string_view nsName, Item &item,
							const std::function<Error(std::string_view, Item &)> &&localFn);

	template <typename Func, typename FLocal, typename T, typename P = std::function<bool(const T &)>, typename... Args>
	Error collectFromShardsByNs(const RdxContext &rdxCtx, Func f, const FLocal &&local, std::vector<T> &result, P predicate,
								std::string_view nsName, [[maybe_unused]] Args &&...args);

	typedef Error (client::SyncCoroReindexer::*ItemModifyFunQr)(std::string_view, client::Item &, client::SyncCoroQueryResults &);

	template <ItemModifyFunQr fn>
	Error modifyItemOnShard(const InternalRdxContext &ctx, const RdxContext &rdxCtx, std::string_view nsName, Item &item,
							QueryResults &result, const std::function<Error(std::string_view, Item &, LocalQueryResults &)> &&localFn);

	Error executeQueryOnShard(const Query &query, QueryResults &result, unsigned proxyFetchLimit, const InternalRdxContext &,
							  const RdxContext &,
							  const std::function<Error(const Query &, LocalQueryResults &, const RdxContext &)> && = {}) noexcept;
	Error executeQueryOnClient(client::SyncCoroReindexer &connection, const Query &q, client::SyncCoroQueryResults &qrClient,
							   const std::function<void(size_t count, size_t totalCount)> &limitOffsetCalc);

	void calculateNewLimitOfsset(size_t count, size_t totalCount, unsigned &limit, unsigned &offset);
	void addEmptyLocalQrWithShardID(const Query &query, QueryResults &result);
};
}  // namespace reindexer
