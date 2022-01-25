#pragma once
#include <condition_variable>
#include "client/synccororeindexer.h"
#include "core/item.h"
#include "core/reindexerimpl.h"
#include "estl/shared_mutex.h"

namespace reindexer {

namespace sharding {
class LocatorService;
}

constexpr size_t kClusterProxyConnCount = 8;
constexpr size_t kClusterProxyCoroPerConn = 2;

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
	Error Select(std::string_view sql, QueryResults &result, const InternalRdxContext &ctx);
	Error Select(const Query &query, QueryResults &result, const InternalRdxContext &ctx);
	Error Commit(std::string_view nsName, const InternalRdxContext &ctx);
	Item NewItem(std::string_view nsName, const InternalRdxContext &ctx);

	Transaction NewTransaction(std::string_view nsName, const InternalRdxContext &ctx);
	Error CommitTransaction(Transaction &tr, QueryResults &result, const InternalRdxContext &ctx);
	Error RollBackTransaction(Transaction &tr, const InternalRdxContext &ctx);

	Error GetMeta(std::string_view nsName, const string &key, string &data, const InternalRdxContext &ctx);
	Error PutMeta(std::string_view nsName, const string &key, std::string_view data, const InternalRdxContext &ctx);
	Error EnumMeta(std::string_view nsName, vector<string> &keys, const InternalRdxContext &ctx);

	Error GetSqlSuggestions(const std::string_view sqlQuery, int pos, vector<string> &suggestions, const InternalRdxContext &ctx) {
		return impl_.GetSqlSuggestions(sqlQuery, pos, suggestions, ctx);
	}
	Error Status() { return impl_.Status(); }
	Error GetProtobufSchema(WrSerializer &ser, vector<string> &namespaces) { return impl_.GetProtobufSchema(ser, namespaces); }
	Error GetReplState(std::string_view nsName, ReplicationStateV2 &state, const InternalRdxContext &ctx) {
		return impl_.GetReplState(nsName, state, ctx);
	}
	Error SetClusterizationStatus(std::string_view nsName, const ClusterizationStatus &status, const InternalRdxContext &ctx) {
		return impl_.SetClusterizationStatus(nsName, status, ctx);
	}
	bool NeedTraceActivity() { return impl_.NeedTraceActivity(); }
	Error EnableStorage(const string &storagePath, bool skipPlaceholderCheck, const InternalRdxContext &ctx) {
		return impl_.EnableStorage(storagePath, skipPlaceholderCheck, ctx);
	}
	Error InitSystemNamespaces() { return impl_.InitSystemNamespaces(); }
	Error ApplySnapshotChunk(std::string_view nsName, const SnapshotChunk &ch, const InternalRdxContext &ctx) {
		return impl_.ApplySnapshotChunk(nsName, ch, ctx);
	}
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
	Error GetRaftInfo(cluster::RaftInfo &info, const InternalRdxContext &ctx) { return impl_.GetRaftInfo(true, info, ctx); }
	Error CreateTemporaryNamespace(std::string_view baseName, std::string &resultName, const StorageOpts &opts, lsn_t nsVersion,
								   const InternalRdxContext &ctx) {
		return impl_.CreateTemporaryNamespace(baseName, resultName, opts, nsVersion, ctx);
	}
	Error GetSnapshot(std::string_view nsName, const SnapshotOpts &opts, Snapshot &snapshot, const InternalRdxContext &ctx) {
		return impl_.GetSnapshot(nsName, opts, snapshot, ctx);
	}
	Error ClusterControlRequest(const ClusterControlRequestData &request) { return impl_.ClusterControlRequest(request); }
	Error SetTagsMatcher(std::string_view nsName, TagsMatcher &&tm, const InternalRdxContext &ctx) {
		return impl_.SetTagsMatcher(nsName, std::move(tm), ctx);
	}
	Error DumpIndex(std::ostream &os, std::string_view nsName, std::string_view index, const InternalRdxContext &ctx) {
		return impl_.DumpIndex(os, nsName, index, ctx);
	}
	void ShutdownCluster();

private:
	class ConnectionsMap {
	public:
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
			cfg.SyncRxCoroCount = kClusterProxyCoroPerConn;

			std::lock_guard lck(mtx_);
			if (shutdown_) {
				throw Error(errTerminated, "Proxy is already shut down");
			}
			auto found = conns_.find(dsn);
			if (found != conns_.end()) {
				return res;
			}
			res = std::make_shared<client::SyncCoroReindexer>(cfg, kClusterProxyConnCount);
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
	};

	ReindexerImpl impl_;
	shared_timed_mutex mtx_;
	int32_t leaderId_ = -1;
	std::shared_ptr<client::SyncCoroReindexer> leader_;
	ConnectionsMap clusterConns_;
	std::atomic<unsigned short> sId_;
	int configHandlerId_;
#ifdef WITH_SHARDING
	std::shared_ptr<sharding::LocatorService> shardingRouter_;
	std::atomic_bool shardingInitialized_ = {false};
#endif	// WITH_SHARDING

	std::condition_variable processPingEvent_;
	std::mutex processPingEventMutex_;
	int lastPingLeaderId_ = -1;

	std::shared_ptr<client::SyncCoroReindexer> getLeader(const cluster::RaftInfo &info);
	void resetLeader();

	template <typename Fn, Fn fn, typename FnL, FnL fnl, typename R, typename FnA, typename... Args>
	R proxyCall(const InternalRdxContext &ctx, std::string_view nsName, FnA &action, Args &&...args);

	template <typename FnL, FnL fnl, typename... Args>
	Error baseFollowerAction(const InternalRdxContext &_ctx, std::shared_ptr<client::SyncCoroReindexer> clientToLeader, Args &&...args);
	template <typename FnL, FnL fnl>
	Error itemFollowerAction(const InternalRdxContext &ctx, std::shared_ptr<client::SyncCoroReindexer> clientToLeader,
							 std::string_view nsName, Item &item);
	template <typename FnL, FnL fnl>
	Error resultFollowerAction(const InternalRdxContext &ctx, std::shared_ptr<client::SyncCoroReindexer> clientToLeader, const Query &query,
							   QueryResults &result);

	template <typename FnL, FnL fnl>
	Error resultItemFollowerAction(const InternalRdxContext &ctx, std::shared_ptr<client::SyncCoroReindexer> clientToLeader,
								   std::string_view nsName, Item &item, QueryResults &result);

	Transaction newTxFollowerAction(const InternalRdxContext &ctx, std::shared_ptr<client::SyncCoroReindexer> clientToLeader,
									std::string_view nsName);

	Error transactionRollBackAction(const InternalRdxContext &ctx, std::shared_ptr<client::SyncCoroReindexer> clientToLeader,
									Transaction &tx);

	reindexer::client::Item toClientItem(std::string_view ns, client::SyncCoroReindexer *connection, reindexer::Item &clientItem);

	void clientToCoreQueryResults(client::SyncCoroQueryResults &, QueryResults &, bool, const std::unordered_set<int32_t> &stateTokenList);
	void clientToCoreQueryResults(const Query &query, client::SyncCoroQueryResults &, QueryResults &, bool,
								  const std::unordered_set<int32_t> &stateTokenList);

	typedef std::function<void(const client::SyncCoroQueryResults::Iterator &, const client::SyncCoroQueryResults &, QueryResults &)>
		CopyJoinResultsType;

	void clientToCoreQueryResultsInternal(client::SyncCoroQueryResults &, QueryResults &, bool createTm,
										  const std::unordered_set<int32_t> &stateTokenList,
										  CopyJoinResultsType copyJoinResults = CopyJoinResultsType());

	void copyJoinedData(const Query &query, const client::SyncCoroQueryResults::Iterator &it,
						const client::SyncCoroQueryResults &clientResults, QueryResults &result);

	bool isWithSharding(std::string_view nsName, const InternalRdxContext &ctx) const noexcept;
	bool isWithSharding(const InternalRdxContext &ctx) const noexcept;

	bool shouldProxyQuery(const Query &q);

	template <typename Func, typename... Args>
	Error delegateToShards(Func f, Args &&...args);
	template <typename Func, typename... Args>
	Error delegateToShardsByNs(bool toAll, Func f, std::string_view nsName, Args &&...args);

	Error modifyItemOnShard(std::string_view nsName, Item &item, ItemModifyMode mode);
	Error executeQueryOnShard(const Query &query, QueryResults &result, InternalRdxContext &,
							  const std::function<Error(const Query &, QueryResults &)> & = {}) noexcept;
};
}  // namespace reindexer
