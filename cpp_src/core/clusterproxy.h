#pragma once

#include <condition_variable>
#include "client/itemimpl.h"
#include "client/reindexer.h"
#include "cluster/config.h"
#include "cluster/consts.h"
#include "core/reindexer_impl/reindexerimpl.h"
#include "tools/clusterproxyloghelper.h"

namespace reindexer {

#define CallProxyFunction(Fn) proxyCall<decltype(&ReindexerImpl::Fn), &ReindexerImpl::Fn, Error>

#define DefFunctor1(P1, F, Action)                                    \
	std::function<Error(const RdxContext &, LeaderRefT, P1)> action = \
		std::bind(&ClusterProxy::Action<decltype(&client::Reindexer::F), &client::Reindexer::F, P1>, this, _1, _2, _3)

#define DefFunctor2(P1, P2, F, Action)                                    \
	std::function<Error(const RdxContext &, LeaderRefT, P1, P2)> action = \
		std::bind(&ClusterProxy::Action<decltype(&client::Reindexer::F), &client::Reindexer::F, P1, P2>, this, _1, _2, _3, _4)

#define DefFunctor3(P1, P2, P3, F, Action)                                    \
	std::function<Error(const RdxContext &, LeaderRefT, P1, P2, P3)> action = \
		std::bind(&ClusterProxy::Action<decltype(&client::Reindexer::F), &client::Reindexer::F, P1, P2, P3>, this, _1, _2, _3, _4, _5)

class ClusterProxy {
public:
	ClusterProxy(ReindexerConfig cfg, ActivityContainer &activities, ReindexerImpl::CallbackMap &&proxyCallbacks);
	~ClusterProxy();
	Error Connect(const std::string &dsn, ConnectOpts opts = ConnectOpts());
	Error OpenNamespace(std::string_view nsName, const StorageOpts &opts, const NsReplicationOpts &replOpts, const RdxContext &ctx) {
		using namespace std::placeholders;
		DefFunctor3(std::string_view, const StorageOpts &, const NsReplicationOpts &, OpenNamespace, baseFollowerAction);
		clusterProxyLog(LogTrace, "[%d proxy] ClusterProxy::OpenNamespace", getServerIDRel());
		return CallProxyFunction(OpenNamespace)(ctx, nsName, action, nsName, opts, replOpts);
	}
	Error AddNamespace(const NamespaceDef &nsDef, const NsReplicationOpts &replOpts, const RdxContext &ctx) {
		using namespace std::placeholders;
		DefFunctor2(const NamespaceDef &, const NsReplicationOpts &, AddNamespace, baseFollowerAction);
		clusterProxyLog(LogTrace, "[%d proxy] ClusterProxy::AddNamespace", getServerIDRel());
		return CallProxyFunction(AddNamespace)(ctx, nsDef.name, action, nsDef, replOpts);
	}
	Error CloseNamespace(std::string_view nsName, const RdxContext &ctx) {
		using namespace std::placeholders;
		DefFunctor1(std::string_view, CloseNamespace, baseFollowerAction);
		clusterProxyLog(LogTrace, "[%d proxy] ClusterProxy::DropNamespace", getServerIDRel());
		return CallProxyFunction(CloseNamespace)(ctx, nsName, action, nsName);
	}
	Error DropNamespace(std::string_view nsName, const RdxContext &ctx) {
		using namespace std::placeholders;
		DefFunctor1(std::string_view, DropNamespace, baseFollowerAction);
		clusterProxyLog(LogTrace, "[%d proxy] ClusterProxy::DropNamespace", getServerIDRel());
		return CallProxyFunction(DropNamespace)(ctx, nsName, action, nsName);
	}
	Error TruncateNamespace(std::string_view nsName, const RdxContext &ctx) {
		using namespace std::placeholders;
		DefFunctor1(std::string_view, TruncateNamespace, baseFollowerAction);
		clusterProxyLog(LogTrace, "[%d proxy] ClusterProxy::TruncateNamespace", getServerIDRel());
		return CallProxyFunction(TruncateNamespace)(ctx, nsName, action, nsName);
	}
	Error RenameNamespace(std::string_view srcNsName, const std::string &dstNsName, const RdxContext &ctx) {
		using namespace std::placeholders;
		DefFunctor2(std::string_view, const std::string &, RenameNamespace, baseFollowerAction);
		clusterProxyLog(LogTrace, "[%d proxy] ClusterProxy::RenameNamespace", getServerIDRel());
		return CallProxyFunction(RenameNamespace)(ctx, std::string_view(), action, srcNsName, dstNsName);
	}
	Error AddIndex(std::string_view nsName, const IndexDef &index, const RdxContext &ctx) {
		using namespace std::placeholders;
		DefFunctor2(std::string_view, const IndexDef &, AddIndex, baseFollowerAction);
		clusterProxyLog(LogTrace, "[%d proxy] ClusterProxy::AddIndex", getServerIDRel());
		return CallProxyFunction(AddIndex)(ctx, nsName, action, nsName, index);
	}
	Error UpdateIndex(std::string_view nsName, const IndexDef &index, const RdxContext &ctx) {
		using namespace std::placeholders;
		DefFunctor2(std::string_view, const IndexDef &, UpdateIndex, baseFollowerAction);
		clusterProxyLog(LogTrace, "[%d proxy] ClusterProxy::UpdateIndex", getServerIDRel());
		return CallProxyFunction(UpdateIndex)(ctx, nsName, action, nsName, index);
	}
	Error DropIndex(std::string_view nsName, const IndexDef &index, const RdxContext &ctx) {
		using namespace std::placeholders;
		DefFunctor2(std::string_view, const IndexDef &, DropIndex, baseFollowerAction);
		clusterProxyLog(LogTrace, "[%d proxy] ClusterProxy::DropIndex", getServerIDRel());
		return CallProxyFunction(DropIndex)(ctx, nsName, action, nsName, index);
	}
	Error SetSchema(std::string_view nsName, std::string_view schema, const RdxContext &ctx) {
		using namespace std::placeholders;
		DefFunctor2(std::string_view, std::string_view, SetSchema, baseFollowerAction);
		clusterProxyLog(LogTrace, "[%d proxy] ClusterProxy::SetSchema", getServerIDRel());
		return CallProxyFunction(SetSchema)(ctx, nsName, action, nsName, schema);
	}
	Error GetSchema(std::string_view nsName, int format, std::string &schema, const RdxContext &ctx) {
		return impl_.GetSchema(nsName, format, schema, ctx);
	}
	Error EnumNamespaces(std::vector<NamespaceDef> &defs, EnumNamespacesOpts opts, const RdxContext &ctx) {
		return impl_.EnumNamespaces(defs, opts, ctx);
	}
	Error Insert(std::string_view nsName, Item &item, const RdxContext &ctx) {
		auto action = [this](const RdxContext &ctx, LeaderRefT clientToLeader, std::string_view nsName, Item &item) {
			return itemFollowerAction<&client::Reindexer::Insert>(ctx, clientToLeader, nsName, item);
		};
		return proxyCall<LocalItemSimpleActionFT, &ReindexerImpl::Insert, Error>(ctx, nsName, action, nsName, item);
	}
	Error Insert(std::string_view nsName, Item &item, LocalQueryResults &qr, const RdxContext &ctx) {
		auto action = [this](const RdxContext &ctx, LeaderRefT clientToLeader, std::string_view nsName, Item &item, LocalQueryResults &qr) {
			return resultItemFollowerAction<&client::Reindexer::Insert>(ctx, clientToLeader, nsName, item, qr);
		};
		return proxyCall<LocalItemQrActionFT, &ReindexerImpl::Insert, Error>(ctx, nsName, action, nsName, item, qr);
	}
	Error Update(std::string_view nsName, Item &item, const RdxContext &ctx) {
		auto action = [this](const RdxContext &ctx, LeaderRefT clientToLeader, std::string_view nsName, Item &item) {
			return itemFollowerAction<&client::Reindexer::Update>(ctx, clientToLeader, nsName, item);
		};
		return proxyCall<LocalItemSimpleActionFT, &ReindexerImpl::Update, Error>(ctx, nsName, action, nsName, item);
	}
	Error Update(std::string_view nsName, Item &item, LocalQueryResults &qr, const RdxContext &ctx) {
		auto action = [this](const RdxContext &ctx, LeaderRefT clientToLeader, std::string_view nsName, Item &item, LocalQueryResults &qr) {
			return resultItemFollowerAction<&client::Reindexer::Update>(ctx, clientToLeader, nsName, item, qr);
		};
		return proxyCall<LocalItemQrActionFT, &ReindexerImpl::Update, Error>(ctx, nsName, action, nsName, item, qr);
	}
	Error Update(const Query &q, LocalQueryResults &qr, const RdxContext &ctx) {
		auto action = [this](const RdxContext &ctx, LeaderRefT clientToLeader, const Query &q, LocalQueryResults &qr) {
			return resultFollowerAction<&client::Reindexer::Update>(ctx, clientToLeader, q, qr);
		};
		clusterProxyLog(LogTrace, "[%d proxy] ClusterProxy::Update query", getServerIDRel());
		return proxyCall<LocalQueryActionFT, &ReindexerImpl::Update, Error>(ctx, q.NsName(), action, q, qr);
	}
	Error Upsert(std::string_view nsName, Item &item, const RdxContext &ctx) {
		auto action = [this](const RdxContext &ctx, LeaderRefT clientToLeader, std::string_view nsName, Item &item) {
			return itemFollowerAction<&client::Reindexer::Upsert>(ctx, clientToLeader, nsName, item);
		};
		return proxyCall<LocalItemSimpleActionFT, &ReindexerImpl::Upsert, Error>(ctx, nsName, action, nsName, item);
	}
	Error Upsert(std::string_view nsName, Item &item, LocalQueryResults &qr, const RdxContext &ctx) {
		auto action = [this](const RdxContext &ctx, LeaderRefT clientToLeader, std::string_view nsName, Item &item, LocalQueryResults &qr) {
			return resultItemFollowerAction<&client::Reindexer::Upsert>(ctx, clientToLeader, nsName, item, qr);
		};
		return proxyCall<LocalItemQrActionFT, &ReindexerImpl::Upsert, Error>(ctx, nsName, action, nsName, item, qr);
	}
	Error Delete(std::string_view nsName, Item &item, const RdxContext &ctx) {
		auto action = [this](const RdxContext &ctx, LeaderRefT clientToLeader, std::string_view nsName, Item &item) {
			return itemFollowerAction<&client::Reindexer::Delete>(ctx, clientToLeader, nsName, item);
		};
		clusterProxyLog(LogTrace, "[%d proxy] ClusterProxy::Delete ITEM", getServerIDRel());
		return proxyCall<LocalItemSimpleActionFT, &ReindexerImpl::Delete, Error>(ctx, nsName, action, nsName, item);
	}
	Error Delete(std::string_view nsName, Item &item, LocalQueryResults &qr, const RdxContext &ctx) {
		auto action = [this](const RdxContext &ctx, LeaderRefT clientToLeader, std::string_view nsName, Item &item, LocalQueryResults &qr) {
			return resultItemFollowerAction<&client::Reindexer::Delete>(ctx, clientToLeader, nsName, item, qr);
		};
		return proxyCall<LocalItemQrActionFT, &ReindexerImpl::Delete, Error>(ctx, nsName, action, nsName, item, qr);
	}
	Error Delete(const Query &q, LocalQueryResults &qr, const RdxContext &ctx) {
		auto action = [this](const RdxContext &ctx, LeaderRefT clientToLeader, const Query &q, LocalQueryResults &qr) {
			return resultFollowerAction<&client::Reindexer::Delete>(ctx, clientToLeader, q, qr);
		};
		clusterProxyLog(LogTrace, "[%d proxy] ClusterProxy::Delete QUERY", getServerIDRel());
		return proxyCall<LocalQueryActionFT, &ReindexerImpl::Delete, Error>(ctx, q.NsName(), action, q, qr);
	}
	Error Select(const Query &q, LocalQueryResults &qr, const RdxContext &ctx) {
		using namespace std::placeholders;
		if (!shouldProxyQuery(q)) {
			clusterProxyLog(LogTrace, "[%d proxy] ClusterProxy::Select query local", getServerIDRel());
			return impl_.Select(q, qr, ctx);
		}
		const RdxDeadlineContext deadlineCtx(kReplicationStatsTimeout, ctx.GetCancelCtx());
		const RdxContext rdxDeadlineCtx = ctx.WithCancelCtx(deadlineCtx);

		auto action = [this](const RdxContext &ctx, LeaderRefT clientToLeader, const Query &q, LocalQueryResults &qr) {
			return resultFollowerAction<&client::Reindexer::Select>(ctx, clientToLeader, q, qr);
		};
		clusterProxyLog(LogTrace, "[%d proxy] ClusterProxy::Select query proxied", getServerIDRel());
		return proxyCall<LocalQueryActionFT, &ReindexerImpl::Select, Error>(rdxDeadlineCtx, q.NsName(), action, q, qr);
	}
	Error Commit(std::string_view nsName) { return impl_.Commit(nsName); }
	Item NewItem(std::string_view nsName, const RdxContext &ctx) { return impl_.NewItem(nsName, ctx); }

	Transaction NewTransaction(std::string_view nsName, const RdxContext &ctx) {
		using LocalFT = LocalTransaction (ReindexerImpl::*)(std::string_view, const RdxContext &);
		auto action = [this](const RdxContext &ctx, LeaderRefT clientToLeader, std::string_view nsName) {
			try {
				client::Reindexer l = clientToLeader->WithLSN(ctx.GetOriginLSN()).WithEmmiterServerId(GetServerID());
				return Transaction(impl_.NewTransaction(nsName, ctx), std::move(l));
			} catch (const Error &err) {
				return Transaction(err);
			}
		};
		clusterProxyLog(LogTrace, "[%d proxy] ClusterProxy::NewTransaction", getServerIDRel());
		return proxyCall<LocalFT, &ReindexerImpl::NewTransaction, Transaction>(ctx, nsName, action, nsName);
	}
	Error CommitTransaction(Transaction &tr, QueryResults &qr, bool txExpectsSharding, const RdxContext &ctx) {
		return tr.commit(GetServerID(), txExpectsSharding, impl_, qr, ctx);
	}
	Error RollBackTransaction(Transaction &tr, const RdxContext &ctx) { return tr.rollback(GetServerID(), ctx); }

	Error GetMeta(std::string_view nsName, const std::string &key, std::string &data, const RdxContext &ctx) {
		return impl_.GetMeta(nsName, key, data, ctx);
	}
	Error PutMeta(std::string_view nsName, const std::string &key, std::string_view data, const RdxContext &ctx) {
		using namespace std::placeholders;
		DefFunctor3(std::string_view, const std::string &, std::string_view, PutMeta, baseFollowerAction);
		clusterProxyLog(LogTrace, "[%d proxy] ClusterProxy::PutMeta", getServerIDRel());
		return CallProxyFunction(PutMeta)(ctx, nsName, action, nsName, key, data);
	}
	Error EnumMeta(std::string_view nsName, std::vector<std::string> &keys, const RdxContext &ctx) {
		return impl_.EnumMeta(nsName, keys, ctx);
	}

	Error GetSqlSuggestions(std::string_view sqlQuery, int pos, std::vector<std::string> &suggestions, const RdxContext &ctx) {
		return impl_.GetSqlSuggestions(sqlQuery, pos, suggestions, ctx);
	}
	Error Status() noexcept {
		if (connected_.load(std::memory_order_acquire)) {
			return {};
		}
		auto st = impl_.Status();
		if (st.ok()) {
			return Error(errNotValid, "Reindexer's cluster proxy layer was not initialized properly");
		}
		return st;
	}
	Error GetProtobufSchema(WrSerializer &ser, std::vector<std::string> &namespaces) { return impl_.GetProtobufSchema(ser, namespaces); }
	Error GetReplState(std::string_view nsName, ReplicationStateV2 &state, const RdxContext &ctx) {
		return impl_.GetReplState(nsName, state, ctx);
	}
	Error SetClusterizationStatus(std::string_view nsName, const ClusterizationStatus &status, const RdxContext &ctx) {
		return impl_.SetClusterizationStatus(nsName, status, ctx);
	}
	bool NeedTraceActivity() const noexcept { return impl_.NeedTraceActivity(); }
	Error InitSystemNamespaces() { return impl_.InitSystemNamespaces(); }
	Error ApplySnapshotChunk(std::string_view nsName, const SnapshotChunk &ch, const RdxContext &ctx) {
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
	Error GetRaftInfo(cluster::RaftInfo &info, const RdxContext &ctx) { return impl_.GetRaftInfo(true, info, ctx); }
	Error CreateTemporaryNamespace(std::string_view baseName, std::string &resultName, const StorageOpts &opts, lsn_t nsVersion,
								   const RdxContext &ctx) {
		return impl_.CreateTemporaryNamespace(baseName, resultName, opts, nsVersion, ctx);
	}
	Error GetSnapshot(std::string_view nsName, const SnapshotOpts &opts, Snapshot &snapshot, const RdxContext &ctx) {
		return impl_.GetSnapshot(nsName, opts, snapshot, ctx);
	}
	Error ClusterControlRequest(const ClusterControlRequestData &request) { return impl_.ClusterControlRequest(request); }
	Error SetTagsMatcher(std::string_view nsName, TagsMatcher &&tm, const RdxContext &ctx) {
		return impl_.SetTagsMatcher(nsName, std::move(tm), ctx);
	}
	Error DumpIndex(std::ostream &os, std::string_view nsName, std::string_view index, const RdxContext &ctx) {
		return impl_.DumpIndex(os, nsName, index, ctx);
	}
	void ShutdownCluster() {
		impl_.ShutdownCluster();
		clusterConns_.Shutdown();
		resetLeader();
	}

	intrusive_ptr<intrusive_atomic_rc_wrapper<const cluster::ShardingConfig>> GetShardingConfig() const noexcept {
		return impl_.shardingConfig_.Get();
	}
	Namespace::Ptr GetNamespacePtr(std::string_view nsName, const RdxContext &ctx) { return impl_.getNamespace(nsName, ctx); }
	Namespace::Ptr GetNamespacePtrNoThrow(std::string_view nsName, const RdxContext &ctx) { return impl_.getNamespaceNoThrow(nsName, ctx); }

	PayloadType GetPayloadType(std::string_view nsName) { return impl_.getPayloadType(nsName); }
	std::set<std::string> GetFTIndexes(std::string_view nsName) { return impl_.getFTIndexes(nsName); }

	[[nodiscard]] Error ResetShardingConfig(std::optional<cluster::ShardingConfig> config = std::nullopt) noexcept;
	void SaveNewShardingConfigFile(const cluster::ShardingConfig &config) const { impl_.saveNewShardingConfigFile(config); }

	[[nodiscard]] Error SaveShardingCfgCandidate(std::string_view config, int64_t sourceId, const RdxContext &ctx) noexcept;
	[[nodiscard]] Error ApplyShardingCfgCandidate(int64_t sourceId, const RdxContext &ctx) noexcept;
	[[nodiscard]] Error ResetOldShardingConfig(int64_t sourceId, const RdxContext &ctx) noexcept;
	[[nodiscard]] Error ResetShardingConfigCandidate(int64_t sourceId, const RdxContext &ctx) noexcept;
	[[nodiscard]] Error RollbackShardingConfigCandidate(int64_t sourceId, const RdxContext &ctx) noexcept;

	Error SubscribeUpdates(IUpdatesObserver *observer, const UpdatesFilters &filters, SubscriptionOpts opts) {
		return impl_.SubscribeUpdates(observer, filters, opts);
	}
	Error UnsubscribeUpdates(IUpdatesObserver *observer) { return impl_.UnsubscribeUpdates(observer); }
	// REINDEX_WITH_V3_FOLLOWERS
	int GetServerID() const noexcept { return sId_.load(std::memory_order_acquire); }

private:
	static constexpr auto kReplicationStatsTimeout = std::chrono::seconds(10);
	static constexpr uint32_t kMaxClusterProxyConnCount = 64;
	static constexpr uint32_t kMaxClusterProxyConnConcurrency = 1024;

	using LeaderRefT = const std::shared_ptr<client::Reindexer> &;
	using LocalItemSimpleActionFT = Error (ReindexerImpl::*)(std::string_view, Item &, const RdxContext &);
	using ProxiedItemSimpleActionFT = Error (client::Reindexer::*)(std::string_view, client::Item &);
	using LocalItemQrActionFT = Error (ReindexerImpl::*)(std::string_view, Item &, LocalQueryResults &, const RdxContext &);
	using ProxiedItemQrActionFT = Error (client::Reindexer::*)(std::string_view, client::Item &, client::QueryResults &);
	using LocalQueryActionFT = Error (ReindexerImpl::*)(const Query &, LocalQueryResults &, const RdxContext &);
	using ProxiedQueryActionFT = Error (client::Reindexer::*)(const Query &, client::QueryResults &);

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
		std::shared_ptr<client::Reindexer> Get(const std::string &dsn) {
			{
				shared_lock lck(mtx_);
				if (shutdown_) {
					throw Error(errTerminated, "Proxy is already shut down");
				}
				auto found = conns_.find(dsn);
				if (found != conns_.end()) {
					return found->second;
				}
			}

			client::ReindexerConfig cfg;
			cfg.AppName = "cluster_proxy";
			cfg.EnableCompression = true;
			cfg.RequestDedicatedThread = true;

			std::lock_guard lck(mtx_);
			cfg.SyncRxCoroCount = clientConnConcurrency_;
			if (shutdown_) {
				throw Error(errTerminated, "Proxy is already shut down");
			}
			auto found = conns_.find(dsn);
			if (found != conns_.end()) {
				return found->second;
			}
			auto res = std::make_shared<client::Reindexer>(cfg, clientConns_, clientThreads_);
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
		fast_hash_map<std::string, std::shared_ptr<client::Reindexer>> conns_;
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
	int configHandlerId_;

	std::condition_variable processPingEvent_;
	std::mutex processPingEventMutex_;
	int lastPingLeaderId_ = -1;
	std::atomic_bool connected_ = false;

	int getServerIDRel() const noexcept { return sId_.load(std::memory_order_relaxed); }
	std::shared_ptr<client::Reindexer> getLeader(const cluster::RaftInfo &info);
	void resetLeader();
	template <typename R>
	ErrorCode getErrCode(const Error &err, R &r);
	template <typename R>
	void setErrorCode(R &r, Error &&err) {
		if constexpr (std::is_same<R, Transaction>::value) {
			r = Transaction(std::move(err));
		} else {
			r = std::move(err);
		}
	}

	template <auto ClientMethod, auto ImplMethod, typename... Args>
	[[nodiscard]] Error shardingConfigCandidateAction(const RdxContext &ctx, Args &&...args) noexcept;

#if RX_ENABLE_CLUSTERPROXY_LOGS

	template <typename R, typename std::enable_if<std::is_same<R, Error>::value>::type * = nullptr>
	static void printErr(const R &r) {
		if (!r.ok()) {
			clusterProxyLog(LogTrace, "[cluster proxy] Err: %s", r.what());
		}
	}

	template <typename R, typename std::enable_if<std::is_same<R, Transaction>::value>::type * = nullptr>
	static void printErr(const R &r) {
		if (!r.Status().ok()) {
			clusterProxyLog(LogTrace, "[cluster proxy] Tx err: %s", r.Status().what());
		}
	}

#endif

	template <typename Fn, Fn fn, typename R, typename... Args>
	R localCall(const RdxContext &ctx, Args &&...args) {
		if constexpr (std::is_same_v<R, Transaction>) {
			return R((impl_.*fn)(std::forward<Args>(args)..., ctx));
		} else {
			return (impl_.*fn)(std::forward<Args>(args)..., ctx);
		}
	}
	template <typename Fn, Fn fn, typename R, typename FnA, typename... Args>
	R proxyCall(const RdxContext &ctx, std::string_view nsName, const FnA &action, Args &&...args) {
		R r;
		Error err;
		if (ctx.GetOriginLSN().isEmpty()) {
			ErrorCode errCode = errOK;
			bool allowCandidateRole = true;
			do {
				cluster::RaftInfo info;
				err = impl_.GetRaftInfo(allowCandidateRole, info, ctx);
				if (!err.ok()) {
					if (err.code() == errTimeout || err.code() == errCanceled) {
						err = Error(err.code(), "Unable to get cluster's leader: %s", err.what());
					}
					setErrorCode(r, std::move(err));
					return r;
				}
				if (info.role == cluster::RaftInfo::Role::None) {  // fast way for non-cluster node
					if (ctx.HasEmmiterServer()) {
						setErrorCode(r, Error(errLogic, "Request was proxied to non-cluster node"));
						return r;
					}
					r = localCall<Fn, fn, R, Args...>(ctx, std::forward<Args>(args)...);
					errCode = getErrCode(err, r);
					if (errCode == errWrongReplicationData && (!impl_.clusterConfig_ || !impl_.NamespaceIsInClusterConfig(nsName))) {
						break;
					}
					continue;
				}
				const bool nsInClusterConf = impl_.NamespaceIsInClusterConfig(nsName);
				if (!nsInClusterConf) {	 // ns is not in cluster
					clusterProxyLog(LogTrace, "[%d proxy] proxyCall ns not in cluster config (local)", getServerIDRel());
					const bool firstError = (errCode == errOK);
					r = localCall<Fn, fn, R, Args...>(ctx, std::forward<Args>(args)...);
					errCode = getErrCode(err, r);
					if (firstError) {
						continue;
					}
					break;
				}
				if (info.role == cluster::RaftInfo::Role::Leader) {
					resetLeader();
					clusterProxyLog(LogTrace, "[%d proxy] proxyCall RaftInfo::Role::Leader", getServerIDRel());
					r = localCall<Fn, fn, R, Args...>(ctx, std::forward<Args>(args)...);
#if RX_ENABLE_CLUSTERPROXY_LOGS
					printErr(r);
#endif
					// the only place, where errUpdateReplication may appear
				} else if (info.role == cluster::RaftInfo::Role::Follower) {
					if (ctx.HasEmmiterServer()) {
						setErrorCode(r, Error(errAlreadyProxied, "Request was proxied to follower node"));
						return r;
					}
					try {
						auto clientToLeader = getLeader(info);
						clusterProxyLog(LogTrace, "[%d proxy] proxyCall RaftInfo::Role::Follower", getServerIDRel());
						r = action(ctx, clientToLeader, std::forward<Args>(args)...);
					} catch (Error e) {
						setErrorCode(r, std::move(e));
					}
				} else if (info.role == cluster::RaftInfo::Role::Candidate) {
					allowCandidateRole = false;
					errCode = errWrongReplicationData;
					// Second attempt with awaiting of the role switch
					continue;
				}
				errCode = getErrCode(err, r);
			} while (errCode == errWrongReplicationData);
		} else {
			clusterProxyLog(LogTrace, "[%d proxy] proxyCall LSN not empty (local call)", getServerIDRel());
			r = localCall<Fn, fn, R, Args...>(ctx, std::forward<Args>(args)...);
			// errWrongReplicationData means, that leader of the current node doesn't match leader from LSN
			if (getErrCode(err, r) == errWrongReplicationData) {
				cluster::RaftInfo info;
				err = impl_.GetRaftInfo(false, info, ctx);
				if (!err.ok()) {
					if (err.code() == errTimeout || err.code() == errCanceled) {
						err = Error(err.code(), "Unable to get cluster's leader: %s", err.what());
					}
					setErrorCode(r, std::move(err));
					return r;
				}
				if (info.role != cluster::RaftInfo::Role::Follower) {
					return r;
				}
				std::unique_lock<std::mutex> lck(processPingEventMutex_);
				auto waitRes = processPingEvent_.wait_for(lck, cluster::kLeaderPingInterval * 10);	// Awaiting ping from current leader
				if (waitRes == std::cv_status::timeout || lastPingLeaderId_ != ctx.GetOriginLSN().Server()) {
					return r;
				}
				lck.unlock();
				return localCall<Fn, fn, R, Args...>(ctx, std::forward<Args>(args)...);
			}
		}
		return r;
	}
	template <typename FnL, FnL fnl, typename... Args>
	Error baseFollowerAction(const RdxContext &ctx, LeaderRefT clientToLeader, Args &&...args) {
		try {
			client::Reindexer l = clientToLeader->WithLSN(ctx.GetOriginLSN()).WithEmmiterServerId(sId_);
			const auto ward = ctx.BeforeClusterProxy();
			Error err = (l.*fnl)(std::forward<Args>(args)...);
			return err;
		} catch (const Error &err) {
			return err;
		}
	}
	template <ProxiedItemSimpleActionFT fnl>
	Error itemFollowerAction(const RdxContext &ctx, LeaderRefT clientToLeader, std::string_view nsName, Item &item) {
		try {
			Error err;

			client::Item clientItem = clientToLeader->NewItem(nsName);
			if (clientItem.Status().ok()) {
				auto jsonData = item.impl_->GetCJSON(true);
				err = clientItem.FromCJSON(jsonData);
				if (!err.ok()) {
					return err;
				}
				clientItem.SetPrecepts(item.impl_->GetPrecepts());
				client::Reindexer l = clientToLeader->WithLSN(ctx.GetOriginLSN()).WithEmmiterServerId(sId_);
				{
					const auto ward = ctx.BeforeClusterProxy();
					err = (l.*fnl)(nsName, clientItem);
				}
				if (!err.ok()) {
					return err;
				}
				*item.impl_ = ItemImpl(clientItem.impl_->Type(), clientItem.impl_->tagsMatcher());
				err = item.FromCJSON(clientItem.GetCJSON());
				item.setID(clientItem.GetID());
				item.setLSN(clientItem.GetLSN());
			} else {
				err = clientItem.Status();
			}
			return err;
		} catch (const Error &err) {
			return err;
		}
	}
	template <ProxiedQueryActionFT fnl>
	Error resultFollowerAction(const RdxContext &ctx, LeaderRefT clientToLeader, const Query &query, LocalQueryResults &qr) {
		try {
			Error err;
			client::Reindexer l = clientToLeader->WithLSN(ctx.GetOriginLSN()).WithEmmiterServerId(sId_);
			client::QueryResults clientResults;
			{
				const auto ward = ctx.BeforeClusterProxy();
				err = (l.*fnl)(query, clientResults);
			}
			if (!err.ok()) {
				return err;
			}
			if (!query.GetJoinQueries().empty() || !query.GetMergeQueries().empty() || !query.GetSubQueries().empty()) {
				return Error(errLogic, "Unable to proxy query with JOIN, MERGE or SUBQUERY");
			}
			clientToCoreQueryResults(clientResults, qr);
			return err;
		} catch (const Error &err) {
			return err;
		}
	}
	template <ProxiedItemQrActionFT fnl>
	Error resultItemFollowerAction(const RdxContext &ctx, LeaderRefT clientToLeader, std::string_view nsName, Item &item,
								   LocalQueryResults &qr) {
		try {
			Error err;
			client::Item clientItem = clientToLeader->NewItem(nsName);
			if (!clientItem.Status().ok()) {
				return clientItem.Status();
			}
			auto jsonData = item.impl_->GetCJSON(true);
			err = clientItem.FromCJSON(jsonData);
			if (!err.ok()) {
				return err;
			}
			clientItem.SetPrecepts(item.impl_->GetPrecepts());
			client::Reindexer l = clientToLeader->WithLSN(ctx.GetOriginLSN()).WithEmmiterServerId(sId_);
			client::QueryResults clientResults;
			{
				const auto ward = ctx.BeforeClusterProxy();
				err = (l.*fnl)(nsName, clientItem, clientResults);
			}
			if (!err.ok()) {
				return err;
			}
			item.setID(clientItem.GetID());
			item.setLSN(clientItem.GetLSN());
			clientToCoreQueryResults(clientResults, qr);
			return err;
		} catch (const Error &err) {
			return err;
		}
	}
	void clientToCoreQueryResults(client::QueryResults &, LocalQueryResults &);
	bool shouldProxyQuery(const Query &q);

	ReindexerImpl::CallbackMap addCallbacks(ReindexerImpl::CallbackMap &&callbackMap) const;
};
}  // namespace reindexer
