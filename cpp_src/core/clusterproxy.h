#pragma once

#include "client/reindexer.h"
#include "cluster/config.h"
#include "core/reindexer_impl/reindexerimpl.h"
#include "estl/condition_variable.h"
#include "estl/mutex.h"

namespace reindexer {

namespace sharding {
struct ShardingControlRequestData;
struct ShardingControlResponseData;
}  // namespace sharding

class [[nodiscard]] ClusterProxy {
public:
	ClusterProxy(ReindexerConfig cfg, ActivityContainer& activities, ReindexerImpl::CallbackMap&& proxyCallbacks);
	~ClusterProxy();
	Error Connect(const std::string& dsn, ConnectOpts opts = ConnectOpts());
	Error OpenNamespace(std::string_view nsName, const StorageOpts& opts, const NsReplicationOpts& replOpts, const RdxContext& ctx)
		RX_REQUIRES(!mtx_);
	Error AddNamespace(const NamespaceDef& nsDef, const NsReplicationOpts& replOpts, const RdxContext& ctx) RX_REQUIRES(!mtx_);
	Error CloseNamespace(std::string_view nsName, const RdxContext& ctx) RX_REQUIRES(!mtx_);
	Error DropNamespace(std::string_view nsName, const RdxContext& ctx) RX_REQUIRES(!mtx_);
	Error TruncateNamespace(std::string_view nsName, const RdxContext& ctx) RX_REQUIRES(!mtx_);
	Error RenameNamespace(std::string_view srcNsName, const std::string& dstNsName, const RdxContext& ctx) RX_REQUIRES(!mtx_);
	Error AddIndex(std::string_view nsName, const IndexDef& index, const RdxContext& ctx) RX_REQUIRES(!mtx_);
	Error UpdateIndex(std::string_view nsName, const IndexDef& index, const RdxContext& ctx) RX_REQUIRES(!mtx_);
	Error DropIndex(std::string_view nsName, const IndexDef& index, const RdxContext& ctx) RX_REQUIRES(!mtx_);
	Error SetSchema(std::string_view nsName, std::string_view schema, const RdxContext& ctx) RX_REQUIRES(!mtx_);
	Error GetSchema(std::string_view nsName, int format, std::string& schema, const RdxContext& ctx);
	Error EnumNamespaces(std::vector<NamespaceDef>& defs, EnumNamespacesOpts opts, const RdxContext& ctx);
	Error Insert(std::string_view nsName, Item& item, const RdxContext& ctx) RX_REQUIRES(!mtx_);
	Error Insert(std::string_view nsName, Item& item, LocalQueryResults& qr, const RdxContext& ctx) RX_REQUIRES(!mtx_);
	Error Update(std::string_view nsName, Item& item, const RdxContext& ctx) RX_REQUIRES(!mtx_);
	Error Update(std::string_view nsName, Item& item, LocalQueryResults& qr, const RdxContext& ctx) RX_REQUIRES(!mtx_);
	Error Update(const Query& q, LocalQueryResults& qr, const RdxContext& ctx) RX_REQUIRES(!mtx_);
	Error Upsert(std::string_view nsName, Item& item, const RdxContext& ctx) RX_REQUIRES(!mtx_);
	Error Upsert(std::string_view nsName, Item& item, LocalQueryResults& qr, const RdxContext& ctx) RX_REQUIRES(!mtx_);
	Error Delete(std::string_view nsName, Item& item, const RdxContext& ctx) RX_REQUIRES(!mtx_);
	Error Delete(std::string_view nsName, Item& item, LocalQueryResults& qr, const RdxContext& ctx) RX_REQUIRES(!mtx_);
	Error Delete(const Query& q, LocalQueryResults& qr, const RdxContext& ctx) RX_REQUIRES(!mtx_);
	Error Select(const Query& q, LocalQueryResults& qr, const RdxContext& ctx) RX_REQUIRES(!mtx_);
	Item NewItem(std::string_view nsName, const RdxContext& ctx) { return impl_.NewItem(nsName, ctx); }

	Transaction NewTransaction(std::string_view nsName, const RdxContext& ctx) RX_REQUIRES(!mtx_);
	Error CommitTransaction(Transaction& tr, QueryResults& qr, bool txExpectsSharding, const RdxContext& ctx);
	Error RollBackTransaction(Transaction& tr, const RdxContext& ctx);

	Error GetMeta(std::string_view nsName, const std::string& key, std::string& data, const RdxContext& ctx);
	Error PutMeta(std::string_view nsName, const std::string& key, std::string_view data, const RdxContext& ctx) RX_REQUIRES(!mtx_);
	Error EnumMeta(std::string_view nsName, std::vector<std::string>& keys, const RdxContext& ctx);
	Error DeleteMeta(std::string_view nsName, const std::string& key, const RdxContext& ctx) RX_REQUIRES(!mtx_);

	Error GetSqlSuggestions(std::string_view sqlQuery, int pos, std::vector<std::string>& suggestions, const RdxContext& ctx);
	Error Status() noexcept;
	Error GetProtobufSchema(WrSerializer& ser, std::vector<std::string>& namespaces);
	Error GetReplState(std::string_view nsName, ReplicationStateV2& state, const RdxContext& ctx);
	Error SetClusterOperationStatus(std::string_view nsName, const ClusterOperationStatus& status, const RdxContext& ctx);
	bool NeedTraceActivity() const noexcept { return impl_.NeedTraceActivity(); }
	Error ApplySnapshotChunk(std::string_view nsName, const SnapshotChunk& ch, const RdxContext& ctx);

	Error SuggestLeader(const cluster::NodeData& suggestion, cluster::NodeData& response);
	Error LeadersPing(const cluster::NodeData& leader);
	Error GetRaftInfo(cluster::RaftInfo& info, const RdxContext& ctx);
	Error CreateTemporaryNamespace(std::string_view baseName, std::string& resultName, const StorageOpts& opts, lsn_t nsVersion,
								   const RdxContext& ctx);
	Error GetSnapshot(std::string_view nsName, const SnapshotOpts& opts, Snapshot& snapshot, const RdxContext& ctx);
	Error ClusterControlRequest(const ClusterControlRequestData& request) { return impl_.ClusterControlRequest(request); }
	Error SetTagsMatcher(std::string_view nsName, TagsMatcher&& tm, const RdxContext& ctx);
	Error DumpIndex(std::ostream& os, std::string_view nsName, std::string_view index, const RdxContext& ctx);
	void ShutdownCluster();

	intrusive_ptr<const intrusive_atomic_rc_wrapper<cluster::ShardingConfig>> GetShardingConfig() const noexcept {
		return impl_.shardingConfig_.Get();
	}
	Namespace::Ptr GetNamespacePtr(std::string_view nsName, const RdxContext& ctx);
	Namespace::Ptr GetNamespacePtrNoThrow(std::string_view nsName, const RdxContext& ctx);

	PayloadType GetPayloadType(std::string_view nsName);
	bool IsFulltextOrVector(std::string_view nsName, std::string_view indexName) const;

	Error ResetShardingConfig(std::optional<cluster::ShardingConfig> config = std::nullopt) noexcept;
	void SaveNewShardingConfigFile(const cluster::ShardingConfig& config) const;

	Error ShardingControlRequest(const sharding::ShardingControlRequestData& request, sharding::ShardingControlResponseData& response,
								 const RdxContext& ctx) RX_REQUIRES(!mtx_);

	Error SubscribeUpdates(IEventsObserver& observer, EventSubscriberConfig&& cfg);
	Error UnsubscribeUpdates(IEventsObserver& observer);

	int GetServerID() const noexcept { return sId_.load(std::memory_order_acquire); }

private:
	static constexpr auto kReplicationStatsTimeout = std::chrono::seconds(10);
	static constexpr uint32_t kMaxClusterProxyConnCount = 64;
	static constexpr uint32_t kMaxClusterProxyConnConcurrency = 1024;

	using LeaderRefT = const std::shared_ptr<client::Reindexer>&;
	using LocalItemSimpleActionFT = Error (ReindexerImpl::*)(std::string_view, Item&, const RdxContext&);
	using ProxiedItemSimpleActionFT = Error (client::Reindexer::*)(std::string_view, client::Item&) noexcept;
	using LocalItemQrActionFT = Error (ReindexerImpl::*)(std::string_view, Item&, LocalQueryResults&, const RdxContext&);
	using ProxiedItemQrActionFT = Error (client::Reindexer::*)(std::string_view, client::Item&, client::QueryResults&) noexcept;
	using LocalQueryActionFT = Error (ReindexerImpl::*)(const Query&, LocalQueryResults&, const RdxContext&);
	using ProxiedQueryActionFT = Error (client::Reindexer::*)(const Query&, client::QueryResults&) noexcept;

	class [[nodiscard]] ConnectionsMap {
	public:
		void SetParams(int clientThreads, int clientConns, int clientConnConcurrency);
		std::shared_ptr<client::Reindexer> Get(const DSN& dsn) RX_REQUIRES(!mtx_);
		void Shutdown();

	private:
		shared_timed_mutex mtx_;
		fast_hash_map<DSN, std::shared_ptr<client::Reindexer>> conns_;
		bool shutdown_ = false;
		uint32_t clientThreads_ = cluster::kDefaultClusterProxyConnThreads;
		uint32_t clientConns_ = cluster::kDefaultClusterProxyConnCount;
		uint32_t clientConnConcurrency_ = cluster::kDefaultClusterProxyCoroPerConn;
	};

	ReindexerImpl impl_;
	shared_timed_mutex mtx_;
	int32_t leaderId_ = -1;
	std::shared_ptr<client::Reindexer> leader_;
	ConnectionsMap clusterConns_;
	std::atomic<unsigned short> sId_;
	std::optional<int> replCfgHandlerID_;

	condition_variable processPingEvent_;
	mutex processPingEventMutex_;
	int lastPingLeaderId_ = -1;
	std::atomic_bool connected_ = false;

	int getServerIDRel() const noexcept { return sId_.load(std::memory_order_relaxed); }
	std::shared_ptr<client::Reindexer> getLeader(const cluster::RaftInfo& info) RX_REQUIRES(!mtx_);
	void resetLeader();
	template <typename R>
	ErrorCode getErrCode(const Error& err, R& r);
	template <typename R>
	void setErrorCode(R& r, Error&& err) {
		if constexpr (std::is_same<R, Transaction>::value) {
			r = Transaction(std::move(err));
		} else {
			r = std::move(err);
		}
	}

	template <auto ClientMethod, auto ImplMethod, typename... Args>
	Error shardingControlRequestAction(const RdxContext& ctx, Args&&... args) noexcept RX_REQUIRES(!mtx_);

	template <typename Fn, Fn fn, typename R, typename... Args>
	R localCall(const RdxContext& ctx, Args&... args);
	template <typename Fn, Fn fn, typename R, typename FnA, typename... Args>
	R proxyCall(const RdxContext& ctx, std::string_view nsName, const FnA& action, Args&... args) RX_REQUIRES(!mtx_);
	template <typename FnL, FnL fnl, typename... Args>
	Error baseFollowerAction(const RdxContext& ctx, LeaderRefT clientToLeader, Args&&... args);
	template <ProxiedItemSimpleActionFT fnl>
	Error itemFollowerAction(const RdxContext& ctx, LeaderRefT clientToLeader, std::string_view nsName, Item& item);
	template <ProxiedQueryActionFT fnl>
	Error resultFollowerAction(const RdxContext& ctx, LeaderRefT clientToLeader, const Query& query, LocalQueryResults& qr);
	template <ProxiedItemQrActionFT fnl>
	Error resultItemFollowerAction(const RdxContext& ctx, LeaderRefT clientToLeader, std::string_view nsName, Item& item,
								   LocalQueryResults& qr);
	void clientToCoreQueryResults(client::QueryResults&, LocalQueryResults&);
	bool shouldProxyQuery(const Query& q);

	ReindexerImpl::CallbackMap addCallbacks(ReindexerImpl::CallbackMap&& callbackMap) const;
};
}  // namespace reindexer
