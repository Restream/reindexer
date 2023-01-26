#pragma once

#include "clusterproxy.h"

namespace reindexer {

class ShardingProxy {
public:
	ShardingProxy(ReindexerConfig cfg) : impl_(std::move(cfg), activities_) {}
	Error Connect(const std::string &dsn, ConnectOpts opts);
	Error OpenNamespace(std::string_view nsName, const StorageOpts &opts, const NsReplicationOpts &replOpts, const RdxContext &ctx);
	Error AddNamespace(const NamespaceDef &nsDef, const NsReplicationOpts &replOpts, const RdxContext &ctx);
	Error CloseNamespace(std::string_view nsName, const RdxContext &ctx);
	Error DropNamespace(std::string_view nsName, const RdxContext &ctx);
	Error TruncateNamespace(std::string_view nsName, const RdxContext &ctx);
	Error RenameNamespace(std::string_view srcNsName, const std::string &dstNsName, const RdxContext &ctx);
	Error AddIndex(std::string_view nsName, const IndexDef &index, const RdxContext &ctx);
	Error UpdateIndex(std::string_view nsName, const IndexDef &index, const RdxContext &ctx);
	Error DropIndex(std::string_view nsName, const IndexDef &index, const RdxContext &ctx);
	Error SetSchema(std::string_view nsName, std::string_view schema, const RdxContext &ctx);
	Error GetSchema(std::string_view nsName, int format, std::string &schema, const RdxContext &ctx) {
		return impl_.GetSchema(nsName, format, schema, ctx);
	}
	Error EnumNamespaces(std::vector<NamespaceDef> &defs, EnumNamespacesOpts opts, const RdxContext &ctx) {
		return impl_.EnumNamespaces(defs, opts, ctx);
	}
	Error Insert(std::string_view nsName, Item &item, const RdxContext &ctx);
	Error Insert(std::string_view nsName, Item &item, QueryResults &result, const RdxContext &ctx);
	Error Update(std::string_view nsName, Item &item, const RdxContext &ctx);
	Error Update(std::string_view nsName, Item &item, QueryResults &result, const RdxContext &ctx);
	Error Update(const Query &query, QueryResults &result, const RdxContext &ctx);
	Error Upsert(std::string_view nsName, Item &item, const RdxContext &ctx);
	Error Upsert(std::string_view nsName, Item &item, QueryResults &result, const RdxContext &ctx);
	Error Delete(std::string_view nsName, Item &item, const RdxContext &ctx);
	Error Delete(std::string_view nsName, Item &item, QueryResults &result, const RdxContext &ctx);
	Error Delete(const Query &query, QueryResults &result, const RdxContext &ctx);
	Error Select(std::string_view sql, QueryResults &result, unsigned proxyFetchLimit, const RdxContext &ctx);
	Error Select(const Query &query, QueryResults &result, unsigned proxyFetchLimit, const RdxContext &ctx);
	Error Commit(std::string_view nsName, const RdxContext &ctx);
	Item NewItem(std::string_view nsName, const RdxContext &ctx) { return impl_.NewItem(nsName, ctx); }

	Transaction NewTransaction(std::string_view nsName, const RdxContext &ctx);
	Error CommitTransaction(Transaction &tr, QueryResults &result, const RdxContext &ctx);
	Error RollBackTransaction(Transaction &tr, const RdxContext &ctx) { return impl_.RollBackTransaction(tr, ctx); }

	Error GetMeta(std::string_view nsName, const std::string &key, std::string &data, const RdxContext &ctx) {
		return impl_.GetMeta(nsName, key, data, ctx);
	}
	Error GetMeta(std::string_view nsName, const std::string &key, std::vector<ShardedMeta> &data, const RdxContext &ctx);
	Error PutMeta(std::string_view nsName, const std::string &key, std::string_view data, const RdxContext &ctx);
	Error EnumMeta(std::string_view nsName, std::vector<std::string> &keys, const RdxContext &ctx);

	Error GetSqlSuggestions(std::string_view sqlQuery, int pos, std::vector<std::string> &suggestions, const RdxContext &ctx) {
		return impl_.GetSqlSuggestions(sqlQuery, pos, suggestions, ctx);
	}
	Error Status() { return impl_.Status(); }
	Error GetProtobufSchema(WrSerializer &ser, std::vector<std::string> &namespaces) { return impl_.GetProtobufSchema(ser, namespaces); }
	Error GetReplState(std::string_view nsName, ReplicationStateV2 &state, const RdxContext &ctx) {
		return impl_.GetReplState(nsName, state, ctx);
	}
	Error SetClusterizationStatus(std::string_view nsName, const ClusterizationStatus &status, const RdxContext &ctx) {
		return impl_.SetClusterizationStatus(nsName, status, ctx);
	}
	bool NeedTraceActivity() { return impl_.NeedTraceActivity(); }
	Error EnableStorage(const std::string &storagePath, bool skipPlaceholderCheck, const RdxContext &ctx) {
		return impl_.EnableStorage(storagePath, skipPlaceholderCheck, ctx);
	}
	Error InitSystemNamespaces() { return impl_.InitSystemNamespaces(); }
	Error GetSnapshot(std::string_view nsName, const SnapshotOpts &opts, Snapshot &snapshot, const RdxContext &ctx) {
		return impl_.GetSnapshot(nsName, opts, snapshot, ctx);
	}
	Error ApplySnapshotChunk(std::string_view nsName, const SnapshotChunk &ch, const RdxContext &ctx) {
		return impl_.ApplySnapshotChunk(nsName, ch, ctx);
	}
	Error CreateTemporaryNamespace(std::string_view baseName, std::string &resultName, const StorageOpts &opts, lsn_t nsVersion,
								   const RdxContext &ctx) {
		return impl_.CreateTemporaryNamespace(baseName, resultName, opts, nsVersion, ctx);
	}
	Error SetTagsMatcher(std::string_view nsName, TagsMatcher &&tm, const RdxContext &ctx) {
		return impl_.SetTagsMatcher(nsName, std::move(tm), ctx);
	}
	Error DumpIndex(std::ostream &os, std::string_view nsName, std::string_view index, const RdxContext &ctx) {
		return impl_.DumpIndex(os, nsName, index, ctx);
	}

	Error ClusterControlRequest(const ClusterControlRequestData &request) { return impl_.ClusterControlRequest(request); }
	Error SuggestLeader(const cluster::NodeData &suggestion, cluster::NodeData &response) {
		return impl_.SuggestLeader(suggestion, response);
	}
	Error LeadersPing(const cluster::NodeData &leader) { return impl_.LeadersPing(leader); }
	Error GetRaftInfo(cluster::RaftInfo &info, const RdxContext &ctx) { return impl_.GetRaftInfo(info, ctx); }

	void ShutdownCluster();

	template <typename QuerySerializer>
	RdxContext CreateRdxContext(const InternalRdxContext &baseCtx, QuerySerializer &&serialize) {
		using namespace std::string_view_literals;
		if (baseCtx.NeedTraceActivity()) {
			auto &ser = getActivitySerializer();
			ser.Reset();
			serialize(ser);
			return baseCtx.CreateRdxContext(ser.Slice(), activities_);
		}
		return baseCtx.CreateRdxContext(""sv, activities_);
	}

	template <typename QuerySerializer>
	RdxContext CreateRdxContext(const InternalRdxContext &baseCtx, QuerySerializer &&serialize, QueryResults &results) {
		using namespace std::string_view_literals;
		if (baseCtx.NeedTraceActivity()) {
			auto &ser = getActivitySerializer();
			ser.Reset();
			serialize(ser);
			return baseCtx.CreateRdxContext(ser.Slice(), activities_, results);
		}
		return baseCtx.CreateRdxContext(""sv, activities_, results);
	}

private:
	using ItemModifyFT = Error (client::Reindexer::*)(std::string_view, client::Item &);
	using ItemModifyQrFT = Error (client::Reindexer::*)(std::string_view, client::Item &, client::QueryResults &);

	static WrSerializer &getActivitySerializer() noexcept {
		thread_local static WrSerializer ser;
		return ser;
	}
	bool isSharderQuery(const Query &q) const;
	bool isWithSharding(const Query &q, const RdxContext &ctx) const {
		if (q.local_) {
			if (q.Type() != QuerySelect) {
				throw Error{errParams, "Only SELECT query could be LOCAL"};
			}
			return false;
		}
		if (q.count == 0 && q.calcTotal == ModeNoTotal && !q.joinQueries_.size() && !q.mergeQueries_.size()) {
			return false;  // Special case for tagsmatchers selects
		}
		if (q.IsWALQuery()) {
			return false;  // WAL queries are always local
		}
		if (!isWithSharding(ctx)) {
			return false;
		}
		return isSharderQuery(q);
	}
	bool isWithSharding(std::string_view nsName, const RdxContext &ctx) const noexcept {
		if (nsName.size() && (nsName[0] == '#' || nsName[0] == '@')) {
			return false;
		}
		if (!isShardingInitialized() || !isSharded(nsName)) {
			return false;
		}
		return isWithSharding(ctx);
	}
	bool isWithSharding(const RdxContext &ctx) const noexcept {
		if (int(ctx.ShardId()) == ShardingKeyType::ProxyOff) {
			return false;
		}
		return ctx.GetOriginLSN().isEmpty() && !ctx.HasEmmiterServer() && impl_.GetShardingConfig() && shardingRouter_ &&
			   int(ctx.ShardId()) == ShardingKeyType::NotSetShard;
	}
	bool isSharded(std::string_view nsName) const noexcept;

	void calculateNewLimitOfsset(size_t count, size_t totalCount, unsigned &limit, unsigned &offset);
	void addEmptyLocalQrWithShardID(const Query &query, QueryResults &result);
	reindexer::client::Item toClientItem(std::string_view ns, client::Reindexer *connection, reindexer::Item &item);
	template <typename ClientF, typename LocalF, typename T, typename Predicate, typename... Args>
	Error collectFromShardsByNs(const RdxContext &, const ClientF &, const LocalF &, std::vector<T> &result, const Predicate &,
								std::string_view nsName, Args &&...);
	template <typename Func, typename FLocal, typename... Args>
	Error delegateToShards(const RdxContext &, const Func &f, const FLocal &local, Args &&...args);
	template <typename Func, typename FLocal, typename... Args>
	Error delegateToShardsByNs(const RdxContext &, const Func &f, const FLocal &local, std::string_view nsName, Args &&...args);
	template <ItemModifyFT fn, typename LocalFT>
	Error modifyItemOnShard(const RdxContext &ctx, std::string_view nsName, Item &item, const LocalFT &localFn);
	template <ItemModifyQrFT fn, typename LocalFT>
	Error modifyItemOnShard(const RdxContext &ctx, std::string_view nsName, Item &item, QueryResults &result, const LocalFT &localFn);
	template <typename LocalFT>
	Error executeQueryOnShard(const Query &query, QueryResults &result, unsigned proxyFetchLimit, const RdxContext &, LocalFT &&) noexcept;
	template <typename CalucalteFT>
	Error executeQueryOnClient(client::Reindexer &connection, const Query &q, client::QueryResults &qrClient,
							   const CalucalteFT &limitOffsetCalc);
	bool isShardingInitialized() const noexcept { return shardingInitialized_.load(std::memory_order_acquire) && shardingRouter_; }

	ClusterProxy impl_;
	std::shared_ptr<sharding::LocatorService> shardingRouter_;
	std::atomic_bool shardingInitialized_ = {false};
	ActivityContainer activities_;
};

}  // namespace reindexer
