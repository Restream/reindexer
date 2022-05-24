#include "clusterproxy.h"
#include <shared_mutex>
#include "client/itemimpl.h"
#include "client/synccororeindexerimpl.h"
#include "client/transaction.h"
#include "cluster/clusterizator.h"
#include "cluster/sharding/sharding.h"
#include "core/defnsconfigs.h"
#include "core/queryresults/joinresults.h"
#include "core/reindexerimpl.h"
#include "core/type_consts.h"
#include "tools/logger.h"

using namespace std::placeholders;
using namespace std::string_view_literals;

namespace reindexer {

constexpr auto kReplicationStatsTimeout = std::chrono::seconds(10);

struct sinkArgs {
	template <typename... Args>
	sinkArgs(Args const &...) {}
};

template <typename... Args>
void clusterProxyLog(int level, const char *fmt, const Args &...args) {
#if RX_ENABLE_CLUSTERPROXY_LOGS
	auto str = fmt::sprintf(fmt, args...);
	logPrint(level, &str[0]);
#else
	sinkArgs{level, fmt, args...};	// -V607
#endif
}

reindexer::client::Item ClusterProxy::toClientItem(std::string_view ns, client::SyncCoroReindexer *connection, reindexer::Item &item) {
	assertrx(connection);
	Error err;
	reindexer::client::Item clientItem = connection->NewItem(ns);
	if (clientItem.Status().ok()) {
		err = clientItem.FromJSON(item.GetJSON());
		if (err.ok() && clientItem.Status().ok()) {
			clientItem.SetPrecepts(item.impl_->GetPrecepts());
			return clientItem;
		}
	}
	if (!err.ok()) throw err;
	throw clientItem.Status();
}

// This method may be used for cluster proxied qr only
// TODO: Remove it totally
void ClusterProxy::clientToCoreQueryResults(client::SyncCoroQueryResults &clientResults, LocalQueryResults &result) {
	assertrx(!clientResults.HaveJoined());
	assertrx(clientResults.GetMergedNSCount() <= 1);
	if (result.getMergedNSCount() == 0) {
		for (int i = 0; i < clientResults.GetMergedNSCount(); ++i) {
			result.addNSContext(clientResults.GetPayloadType(i), clientResults.GetTagsMatcher(i), FieldsSet(), nullptr);
		}
	}

	if (clientResults.GetExplainResults().size() > 0) {
		result.explainResults = clientResults.GetExplainResults();
	}
	if (clientResults.GetAggregationResults().size() > 0) {
		result.aggregationResults = clientResults.GetAggregationResults();
	}

	for (auto it = clientResults.begin(); it != clientResults.end(); ++it) {
		auto item = it.GetItem();
		if (!item.Status().ok()) {
			throw item.Status();
		}
		if (!item) {
			Item itemServer = impl_.NewItem(clientResults.GetNamespaces()[0]);
			result.AddItem(itemServer);
			continue;
		}

		ItemImpl itemimpl(result.getPayloadType(0), result.getTagsMatcher(0));
		Error err = itemimpl.FromJSON(item.GetJSON());
		if (!err.ok()) {
			throw err;
		}

		if (itemimpl.tagsMatcher().isUpdated()) {
			WrSerializer wrser;
			itemimpl.tagsMatcher().serialize(wrser);
			Serializer ser(wrser.Slice());
			result.getTagsMatcher(0).deserialize(ser, itemimpl.tagsMatcher().version(), itemimpl.tagsMatcher().stateToken());
		}
		itemimpl.Value().SetLSN(item.GetLSN());
		result.Add(ItemRef(it.itemParams_.id, itemimpl.Value(), it.itemParams_.proc, it.itemParams_.nsid, true));
		result.SaveRawData(std::move(itemimpl));
	}
	result.totalCount += clientResults.TotalCount();
}

template <typename FnL, FnL fnl, typename... Args>
Error ClusterProxy::baseFollowerAction(const InternalRdxContext &_ctx, std::shared_ptr<client::SyncCoroReindexer> clientToLeader,
									   Args &&...args) {
	try {
		client::SyncCoroReindexer l = clientToLeader->WithLSN(_ctx.LSN()).WithEmmiterServerId(sId_);
		Error err = (l.*fnl)(std::forward<Args>(args)...);
		return err;
	} catch (const Error &err) {
		return err;
	}
}

template <typename FnL, FnL fnl>
Error ClusterProxy::itemFollowerAction(const InternalRdxContext &ctx, std::shared_ptr<client::SyncCoroReindexer> clientToLeader,
									   std::string_view nsName, Item &item) {
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
			client::SyncCoroReindexer l = clientToLeader->WithLSN(ctx.LSN()).WithEmmiterServerId(sId_);
			err = (l.*fnl)(nsName, clientItem);
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

template <typename FnL, FnL fnl>
Error ClusterProxy::resultItemFollowerAction(const InternalRdxContext &ctx, std::shared_ptr<client::SyncCoroReindexer> clientToLeader,
											 std::string_view nsName, Item &item, LocalQueryResults &result) {
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
		client::SyncCoroReindexer l = clientToLeader->WithLSN(ctx.LSN()).WithEmmiterServerId(sId_);
		client::SyncCoroQueryResults clientResults;
		err = (l.*fnl)(nsName, clientItem, clientResults);
		if (!err.ok()) {
			return err;
		}
		item.setID(clientItem.GetID());
		item.setLSN(clientItem.GetLSN());
		clientToCoreQueryResults(clientResults, result);
		return err;
	} catch (const Error &err) {
		return err;
	}
}

template <typename FnL, FnL fnl>
Error ClusterProxy::resultFollowerAction(const InternalRdxContext &ctx, std::shared_ptr<client::SyncCoroReindexer> clientToLeader,
										 const Query &query, LocalQueryResults &result) {
	try {
		Error err;
		client::SyncCoroReindexer l = clientToLeader->WithLSN(ctx.LSN()).WithEmmiterServerId(sId_);
		client::SyncCoroQueryResults clientResults;
		err = (l.*fnl)(query, clientResults);
		if (!err.ok()) {
			return err;
		}
		assertrx(query.joinQueries_.empty());
		assertrx(query.mergeQueries_.empty());
		clientToCoreQueryResults(clientResults, result);
		return err;
	} catch (const Error &err) {
		return err;
	}
}

void ClusterProxy::ShutdownCluster() {
	impl_.ShutdownCluster();
	clusterConns_.Shutdown();
	resetLeader();
	if (shardingRouter_) {
		shardingRouter_->Shutdown();
	}
}

Transaction ClusterProxy::newTxFollowerAction(const InternalRdxContext &ctx, std::shared_ptr<client::SyncCoroReindexer> clientToLeader,
											  std::string_view nsName) {
	try {
		Error err;
		client::SyncCoroReindexer l = clientToLeader->WithLSN(ctx.LSN()).WithEmmiterServerId(sId_);
		Transaction tx = impl_.NewTransaction(nsName, ctx);
		if (isWithSharding(nsName, ctx)) {
			tx.SetLeader(std::move(l));
		} else {
			tx.SetClient(l.NewTransaction(nsName));
		}
		return tx;
	} catch (const Error &err) {
		return Transaction(err);
	}
}

Error ClusterProxy::transactionRollBackAction(const InternalRdxContext &ctx, std::shared_ptr<client::SyncCoroReindexer> clientToLeader,
											  Transaction &tx) {
	(void)clientToLeader;
	try {
		Error err;
		if (tx.clientTransaction_) {
			if (!tx.clientTransaction_->Status().ok()) return tx.clientTransaction_->Status();
			assertrx(tx.clientTransaction_->rx_);
			client::SyncCoroReindexerImpl *client = tx.clientTransaction_->rx_.get();
			err = client->RollBackTransaction(*tx.clientTransaction_.get(),
											  client::InternalRdxContext().WithLSN(ctx.LSN()).WithEmmiterServerId(sId_));
		} else {
			err = Error(errNotFound, "Proxied transaction doesn't exist");
		}
		return err;
	} catch (const Error &err) {
		return err;
	}
}

template <typename R, typename std::enable_if<std::is_same<R, Error>::value>::type * = nullptr>
static ErrorCode getErrCode(const Error &err, R &r) {
	if (!err.ok()) {
		r = err;
		return ErrorCode(err.code());
	}
	return ErrorCode(r.code());
}

template <typename R, typename std::enable_if<std::is_same<R, Transaction>::value>::type * = nullptr>
static ErrorCode getErrCode(const Error &err, R &r) {
	if (!err.ok()) {
		r = R(err);
		return ErrorCode(err.code());
	}
	return ErrorCode(r.Status().code());
}

template <typename R>
static void SetErrorCode(R &r, Error &&err) {
	if constexpr (std::is_same<R, Transaction>::value) {
		r = Transaction(std::move(err));
	} else {
		r = std::move(err);
	}
}

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

template <typename Fn, Fn fn, typename FnL, FnL fnl, typename R, typename FnA, typename... Args>
R ClusterProxy::proxyCall(const InternalRdxContext &_ctx, std::string_view nsName, FnA &action, Args &&...args) {
	R r;
	Error err;
	if (_ctx.LSN().isEmpty()) {
		ErrorCode errCode = errOK;
		do {
			cluster::RaftInfo info;
			err = impl_.GetRaftInfo(false, info, _ctx);
			if (!err.ok()) {
				if (err.code() == errTimeout || err.code() == errCanceled) {
					err = Error(err.code(), "Unable to get cluster's leader: %s", err.what());
				}
				SetErrorCode(r, std::move(err));
				return r;
			}
			if (info.role == cluster::RaftInfo::Role::None) {  // node is not in cluster
				if (_ctx.HasEmmiterID()) {
					SetErrorCode(r, Error(errLogic, "Request was proxied to non-cluster node"));
					return r;
				}
				r = (impl_.*fn)(std::forward<Args>(args)..., _ctx);
				errCode = getErrCode(err, r);
				if (errCode == errWrongReplicationData &&
					(!impl_.clusterConfig_ || !impl_.clusterizator_->NamespaceIsInClusterConfig(nsName))) {
					break;
				}
				continue;
			}
			const bool nsInClusterConf = impl_.clusterizator_->NamespaceIsInClusterConfig(nsName);
			if (!nsInClusterConf) {	 // ns is not in cluster
				clusterProxyLog(LogTrace, "[%d proxy]  proxyCall ns not in cluster config (local)", sId_.load(std::memory_order_relaxed));
				const bool firstError = (errCode == errOK);
				r = (impl_.*fn)(std::forward<Args>(args)..., _ctx);
				errCode = getErrCode(err, r);
				if (firstError) {
					continue;
				}
				break;
			}
			if (info.role == cluster::RaftInfo::Role::Leader) {
				resetLeader();
				clusterProxyLog(LogTrace, "[%d proxy] proxyCall RaftInfo::Role::Leader", sId_.load(std::memory_order_relaxed));
				r = (impl_.*fn)(std::forward<Args>(args)..., _ctx);
#if RX_ENABLE_CLUSTERPROXY_LOGS
				printErr(r);
#endif
				// the only place, where errUpdateReplication may appear
			} else if (info.role == cluster::RaftInfo::Role::Follower) {
				if (_ctx.HasEmmiterID()) {
					SetErrorCode(r, Error(errAlreadyProxied, "Request was proxied to follower node"));
					return r;
				}
				try {
					auto clientToLeader = getLeader(info);
					clusterProxyLog(LogTrace, "[%d proxy] proxyCall RaftInfo::Role::Follower", sId_.load(std::memory_order_relaxed));
					r = action(_ctx, clientToLeader, std::forward<Args>(args)...);
				} catch (Error e) {
					SetErrorCode(r, std::move(e));
				}
			}
			errCode = getErrCode(err, r);
		} while (errCode == errWrongReplicationData);
	} else {
		clusterProxyLog(LogTrace, "[%d proxy] proxyCall LSN not empty (local call)", sId_.load(std::memory_order_relaxed));
		r = (impl_.*fn)(std::forward<Args>(args)..., _ctx);
		// errWrongReplicationData means, that leader of the current node doesn't match leader from LSN
		if (getErrCode(err, r) == errWrongReplicationData) {
			cluster::RaftInfo info;
			err = impl_.GetRaftInfo(false, info, _ctx);
			if (!err.ok()) {
				if (err.code() == errTimeout || err.code() == errCanceled) {
					err = Error(err.code(), "Unable to get cluster's leader: %s", err.what());
				}
				SetErrorCode(r, std::move(err));
				return r;
			}
			if (info.role != cluster::RaftInfo::Role::Follower) {
				return r;
			}
			std::unique_lock<std::mutex> lck(processPingEventMutex_);
			auto waitRes = processPingEvent_.wait_for(lck, cluster::kLeaderPingInterval * 10);	// Awaiting ping from current leader
			if (waitRes == std::cv_status::timeout || lastPingLeaderId_ != _ctx.LSN().Server()) {
				return r;
			}
			lck.unlock();
			return (impl_.*fn)(std::forward<Args>(args)..., _ctx);
		}
	}
	return r;
}

std::shared_ptr<client::SyncCoroReindexer> ClusterProxy::getLeader(const cluster::RaftInfo &info) {
	{
		std::shared_lock lck(mtx_);
		if (info.leaderId == leaderId_) {
			return leader_;
		}
	}
	std::unique_lock lck(mtx_);
	if (info.leaderId == leaderId_) {
		return leader_;
	}
	leader_.reset();
	leaderId_ = -1;
	std::string leaderDsn;
	Error err = impl_.GetLeaderDsn(leaderDsn, sId_, info);
	if (!err.ok()) {
		throw err;
	}
	if (leaderDsn.empty()) {
		err = Error(errLogic, "Leader dsn is empty.");
		throw err;
	}
	leader_ = clusterConns_.Get(leaderDsn);
	leaderId_ = info.leaderId;

	return leader_;
}

void ClusterProxy::resetLeader() {
	std::unique_lock lck(mtx_);
	leader_.reset();
	leaderId_ = -1;
}

ClusterProxy::ClusterProxy(ReindexerConfig cfg) : impl_(std::move(cfg)), leaderId_(-1) {
	sId_ = impl_.configProvider_.GetReplicationConfig().serverID;
	configHandlerId_ = impl_.configProvider_.setHandler([this](ReplicationConfigData data) { sId_ = data.serverID; });
}
ClusterProxy::~ClusterProxy() { impl_.configProvider_.unsetHandler(configHandlerId_); }

Error ClusterProxy::Connect(const string &dsn, ConnectOpts opts) {
	Error err = impl_.Connect(dsn, opts);
	if (!err.ok()) {
		return err;
	}
	if (impl_.clusterConfig_) {
		clusterConns_.SetParams(impl_.clusterConfig_->proxyConnThreads, impl_.clusterConfig_->proxyConnCount,
								impl_.clusterConfig_->proxyConnConcurrency);
	}
	if (!shardingInitialized_ && impl_.shardingConfig_) {
		shardingRouter_ = std::make_shared<sharding::LocatorService>(impl_, *(impl_.shardingConfig_));
		err = shardingRouter_->Start();
		if (err.ok()) {
			shardingInitialized_ = true;
		}
	}
	return err;
}

#define CallProxyFunction(Fn)                                                                                                             \
	proxyCall<decltype(&ReindexerImpl::Fn), &ReindexerImpl::Fn, decltype(&client::SyncCoroReindexer::Fn), &client::SyncCoroReindexer::Fn, \
			  Error>

#define DefFunctor1(P1, F, Action)                                                                            \
	std::function<Error(const InternalRdxContext &, std::shared_ptr<client::SyncCoroReindexer>, P1)> action = \
		std::bind(&ClusterProxy::Action<decltype(&client::SyncCoroReindexer::F), &client::SyncCoroReindexer::F, P1>, this, _1, _2, _3);

#define DefFunctor2(P1, P2, F, Action)                                                                                       \
	std::function<Error(const InternalRdxContext &, std::shared_ptr<client::SyncCoroReindexer>, P1, P2)> action = std::bind( \
		&ClusterProxy::Action<decltype(&client::SyncCoroReindexer::F), &client::SyncCoroReindexer::F, P1, P2>, this, _1, _2, _3, _4);

template <typename Func, typename FLocal, typename... Args>
Error ClusterProxy::delegateToShards(Func f, const FLocal &local, Args &&...args) {
	Error status;
	try {
		auto connections = shardingRouter_->GetShardsConnections(status);
		if (!status.ok()) return status;
		for (auto &connection : *connections) {
			if (connection) {
				status = std::invoke(f, connection, std::forward<Args>(args)...);
				if (!status.ok()) return status;
			} else {
				status = local(std::forward<Args>(args)...);
			}
		}
	} catch (const Error &err) {
		return err;
	}

	return status;
}

template <typename Func, typename... Args>
Error ClusterProxy::delegateToShardsByNs(bool toAll, Func f, std::string_view nsName, [[maybe_unused]] Args &&...args) {
	Error status;
	try {
		auto connections = shardingRouter_->GetShardsConnections(nsName, -1, status);
		if (!status.ok()) return status;
		bool alreadyConnected = false;
		for (auto &connection : *connections) {
			if (connection.IsOnThisShard()) {
				if (!toAll) {
					return errAlreadyConnected;
				}
				alreadyConnected = true;
			} else {
				status = std::invoke(f, connection, nsName, std::forward<Args>(args)...);
			}
			if (toAll != status.ok()) return status;
		}
		if (alreadyConnected) status = errAlreadyConnected;
	} catch (const Error &err) {
		return err;
	}
	return status;
}

template <ClusterProxy::ItemModifyFun fn>
Error ClusterProxy::modifyItemOnShard(const InternalRdxContext &ctx, std::string_view nsName, Item &item,
									  const std::function<Error(std::string_view, Item &)> &localFn) {
	try {
		Error status;
		auto connection = shardingRouter_->GetShardConnectionWithId(nsName, item, status);
		if (!status.ok()) {
			return status;
		}
		if (connection) {
			client::Item clientItem = toClientItem(nsName, connection.get(), item);
			if (!clientItem.Status().ok()) return clientItem.Status();
			const auto timeout = ctx.GetTimeout();
			if (timeout.count() < 0) {
				return Error(errTimeout, "Item modify request timeout");
			}
			auto conn = connection->WithContext(ctx.DeadlineCtx()).WithTimeout(timeout);
			status = (conn.*fn)(nsName, clientItem);
			if (!status.ok()) {
				return status;
			}
			delete item.impl_;
			item.impl_ = new ItemImpl(clientItem.impl_->Type(), clientItem.impl_->tagsMatcher());
			Error err = item.impl_->FromJSON(clientItem.GetJSON());
			assertrx(err.ok());
			item.setID(clientItem.GetID());
			item.setShardID(connection.ShardId());
			item.setLSN(clientItem.GetLSN());
			return status;
		}
		status = localFn(nsName, item);
		if (!status.ok()) {
			return status;
		}
		item.setShardID(shardingRouter_->ActualShardId());
	} catch (const Error &err) {
		return err;
	}
	return Error();
}

template <typename Func, typename T, typename P>
Error ClusterProxy::collectFromShardsByNs(Func f, std::string_view nsName, std::vector<T> &result, P predicated) {
	Error status;
	try {
		auto connections = shardingRouter_->GetShardsConnections(nsName, -1, status);
		if (!status.ok()) return status;
		std::vector<T> resultPart;
		for (auto &connection : *connections) {
			resultPart.clear();
			if (connection.IsOnThisShard()) {
				continue;
			}
			status = std::invoke(f, connection, nsName, resultPart);
			if (!status.ok()) return status;
			result.reserve(result.size() + resultPart.size());
			for (auto &&p : resultPart) {
				if (predicated(p)) {
					result.emplace_back(std::move(p));
				}
			}
		}
	} catch (const Error &err) {
		return err;
	}
	return status;
}

template <ClusterProxy::ItemModifyFunQr fn>
Error ClusterProxy::modifyItemOnShard(const InternalRdxContext &ctx, std::string_view nsName, Item &item, QueryResults &result,
									  const std::function<Error(std::string_view, Item &, LocalQueryResults &)> &localFn) {
	Error status;
	try {
		auto connection = shardingRouter_->GetShardConnectionWithId(nsName, item, status);
		if (!status.ok()) {
			return status;
		}
		if (connection) {
			client::Item clientItem = toClientItem(nsName, connection.get(), item);
			client::SyncCoroQueryResults qrClient(result.Flags());
			if (!clientItem.Status().ok()) return clientItem.Status();

			const auto timeout = ctx.GetTimeout();
			if (timeout.count() < 0) {
				return Error(errTimeout, "Item modify request timeout");
			}
			auto conn = connection->WithContext(ctx.DeadlineCtx()).WithTimeout(timeout);

			status = (conn.*fn)(nsName, clientItem, qrClient);
			if (!status.ok()) {
				return status;
			}
			result.AddQr(std::move(qrClient), connection.ShardId());
			return status;
		}
		result.AddQr(LocalQueryResults(), shardingRouter_->ActualShardId());
		status = localFn(nsName, item, result.ToLocalQr(false));
		if (!status.ok()) {
			return status;
		}
	} catch (const Error &err) {
		return err;
	}
	return status;
}

Error ClusterProxy::executeQueryOnShard(
	const Query &query, QueryResults &result, const InternalRdxContext &ctx,
	const std::function<Error(const Query &, LocalQueryResults &, const InternalRdxContext &)> &localAction) noexcept {
	Error status;
	try {
		if (query.Type() == QueryTruncate) {
			status = shardingRouter_->AwaitShards(ctx);
			if (!status.ok()) return status;
			return delegateToShardsByNs(true, &client::SyncCoroReindexer::TruncateNamespace, query.Namespace());
		}

		auto connectionsPtr = shardingRouter_->GetShardsConnectionsWithId(query, status);
		if (!status.ok()) return status;

		assertrx(connectionsPtr);
		sharding::ConnectionsVector &connections = *connectionsPtr;

		if (query.Type() == QueryUpdate && connections.size() != 1) {
			return Error(errLogic, "Update request can be executed on one node only.");
		}

		if (query.Type() == QueryDelete && connections.size() != 1) {
			return Error(errLogic, "Delete request can be executed on one node only.");
		}

		const bool isDistributedQuery = connections.size() > 1;
		unsigned limit = query.count;
		unsigned offset = query.start;
		const Query *q = &query;
		std::unique_ptr<Query> distributedQuery;

		if (isDistributedQuery) {
			// Create pointer for distributed query to avoid copy for all of the rest cases
			distributedQuery = std::make_unique<Query>(query);
			q = distributedQuery.get();
			if (q->start != 0) {
				distributedQuery->ReqTotal();
			}
		}

		InternalRdxContext shardingCtx(ctx);
		shardingCtx.SetShardingParallelExecution(connections.size() > 1);

		size_t strictModeErrors = 0;
		for (size_t i = 0; i < connections.size(); ++i) {
			if (connections[i]) {
				const auto timeout = shardingCtx.GetTimeout();
				if (timeout.count() < 0) {
					return Error(errTimeout, "Sharded request timeout");
				}
				auto connection = connections[i]
									  ->WithShardingParallelExecution(connections.size() > 1)
									  .WithContext(shardingCtx.DeadlineCtx())
									  .WithTimeout(timeout);
				client::SyncCoroQueryResults qrClient(result.Flags());
				switch (q->Type()) {
					case QuerySelect: {
						if (distributedQuery) {
							distributedQuery->Limit(limit);
							distributedQuery->Offset(offset);
						}

						status = connection.Select(*q, qrClient);
						if (status.ok()) {
							if (limit != UINT_MAX) {
								if (qrClient.Count() >= limit) {
									limit = 0;
								} else {
									limit -= qrClient.Count();
								}
							}
							if (result.TotalCount() >= offset) {
								offset = 0;
							} else {
								offset -= result.TotalCount();
							}
						}
					} break;

					case QueryUpdate: {
						status = connection.Update(*q, qrClient);
						break;
					}
					case QueryDelete: {
						status = connection.Delete(*q, qrClient);
						break;
					}
					default:
						std::abort();
				}
				if (status.ok()) {
					result.AddQr(std::move(qrClient), connections[i].ShardId(), (i + 1) == connections.size());
				}
			} else {
				assertrx(i == 0);
				shardingCtx.WithShardId(shardingRouter_->ActualShardId(), connections.size() > 1);
				LocalQueryResults lqr;
				status = localAction(*q, lqr, shardingCtx);
				if (status.ok()) {
					result.AddQr(std::move(lqr), shardingRouter_->ActualShardId(), (i + 1) == connections.size());
					if (limit != UINT_MAX) {
						if (result.Count() >= limit) {
							limit = 0;
						} else {
							limit -= result.Count();
						}
					}
					if (result.TotalCount() >= offset) {
						offset = 0;
					} else {
						offset -= result.TotalCount();
					}
				}
			}
			if (!status.ok()) {
				if (status.code() != errStrictMode || ++strictModeErrors == connections.size()) {
					return status;
				}
			}
			if (q->calcTotal == ModeNoTotal && limit == 0 && (i + 1) != connections.size()) {
				result.RebuildMergedData();
				break;
			}
		}
	} catch (const Error &e) {
		return e;
	}
	return Error();
}

Error ClusterProxy::OpenNamespace(std::string_view nsName, const StorageOpts &opts, const NsReplicationOpts &replOpts,
								  const InternalRdxContext &ctx) {
	auto localOpen = [this, &ctx](std::string_view nsName, const StorageOpts &opts, const NsReplicationOpts &replOpts) {
		std::function<Error(const InternalRdxContext &, std::shared_ptr<client::SyncCoroReindexer>, std::string_view, const StorageOpts &,
							const NsReplicationOpts &)>
			action = std::bind(&ClusterProxy::baseFollowerAction<decltype(&client::SyncCoroReindexer::OpenNamespace),
																 &client::SyncCoroReindexer::OpenNamespace, std::string_view,
																 const StorageOpts &, const NsReplicationOpts &>,
							   this, _1, _2, _3, _4, _5);
		clusterProxyLog(LogTrace, "[%d proxy] ClusterProxy::OpenNamespace", sId_.load(std::memory_order_relaxed));
		return CallProxyFunction(OpenNamespace)(ctx, nsName, action, nsName, opts, replOpts);
	};

	if (isWithSharding(nsName, ctx)) {
		Error err = shardingRouter_->AwaitShards(ctx);
		if (!err.ok()) return err;

		return delegateToShards(&client::SyncCoroReindexer::OpenNamespace, localOpen, nsName, opts, replOpts);
	}
	return localOpen(nsName, opts, replOpts);
}

Error ClusterProxy::AddNamespace(const NamespaceDef &nsDef, const NsReplicationOpts &replOpts, const InternalRdxContext &ctx) {
	auto localAdd = [this, &ctx](const NamespaceDef &nsDef, const NsReplicationOpts &replOpts) -> Error {
		DefFunctor2(const NamespaceDef &, const NsReplicationOpts &, AddNamespace, baseFollowerAction);
		clusterProxyLog(LogTrace, "[%d proxy] ClusterProxy::AddNamespace", sId_.load(std::memory_order_relaxed));
		return CallProxyFunction(AddNamespace)(ctx, nsDef.name, action, nsDef, replOpts);
	};
	if (isWithSharding(nsDef.name, ctx)) {
		Error status = shardingRouter_->AwaitShards(ctx);
		if (!status.ok()) return status;

		auto connections = shardingRouter_->GetShardsConnections(status);
		if (!status.ok()) return status;
		for (auto &connection : *connections) {
			if (connection) {
				status = connection->AddNamespace(nsDef, replOpts);
			} else {
				status = localAdd(nsDef, replOpts);
			}
			if (!status.ok()) return status;
		}
		return status;
	}
	return localAdd(nsDef, replOpts);
}

Error ClusterProxy::CloseNamespace(std::string_view nsName, const InternalRdxContext &ctx) {
	if (isWithSharding(nsName, ctx)) {
		Error err = shardingRouter_->AwaitShards(ctx);
		if (!err.ok()) return err;

		err = delegateToShardsByNs(true, &client::SyncCoroReindexer::CloseNamespace, nsName);
		if (err.code() != errAlreadyConnected) return err;
	}
	DefFunctor1(std::string_view, CloseNamespace, baseFollowerAction);
	clusterProxyLog(LogTrace, "[%d proxy] ClusterProxy::CloseNamespace", sId_.load(std::memory_order_relaxed));
	return CallProxyFunction(CloseNamespace)(ctx, nsName, action, nsName);
}

Error ClusterProxy::DropNamespace(std::string_view nsName, const InternalRdxContext &ctx) {
	auto localDrop = [this, &ctx](std::string_view nsName) -> Error {
		DefFunctor1(std::string_view, DropNamespace, baseFollowerAction);
		clusterProxyLog(LogTrace, "[%d proxy] ClusterProxy::DropNamespace", sId_.load(std::memory_order_relaxed));
		return CallProxyFunction(DropNamespace)(ctx, nsName, action, nsName);
	};

	if (isWithSharding(nsName, ctx)) {
		Error err = shardingRouter_->AwaitShards(ctx);
		if (!err.ok()) return err;

		return delegateToShards(&client::SyncCoroReindexer::DropNamespace, localDrop, nsName);
	}
	return localDrop(nsName);
}

Error ClusterProxy::TruncateNamespace(std::string_view nsName, const InternalRdxContext &ctx) {
	auto localTruncate = [this, &ctx](std::string_view nsName) {
		DefFunctor1(std::string_view, TruncateNamespace, baseFollowerAction);
		clusterProxyLog(LogTrace, "[%d proxy] ClusterProxy::TruncateNamespace", sId_.load(std::memory_order_relaxed));
		return CallProxyFunction(TruncateNamespace)(ctx, nsName, action, nsName);
	};
	if (isWithSharding(nsName, ctx)) {
		Error err = shardingRouter_->AwaitShards(ctx);
		if (!err.ok()) return err;

		return delegateToShards(&client::SyncCoroReindexer::TruncateNamespace, localTruncate, nsName);
	}
	return localTruncate(nsName);
}

Error ClusterProxy::RenameNamespace(std::string_view srcNsName, const std::string &dstNsName, const InternalRdxContext &ctx) {
	auto localRename = [this, &ctx](std::string_view srcNsName, const std::string &dstNsName) {
		DefFunctor2(std::string_view, const std::string &, RenameNamespace, baseFollowerAction);
		clusterProxyLog(LogTrace, "[%d proxy] ClusterProxy::RenameNamespace", sId_.load(std::memory_order_relaxed));
		return CallProxyFunction(RenameNamespace)(ctx, std::string_view(), action, srcNsName, dstNsName);
	};
	if (isWithSharding(srcNsName, ctx)) {
		Error err = shardingRouter_->AwaitShards(ctx);
		if (!err.ok()) return err;

		return delegateToShards(&client::SyncCoroReindexer::RenameNamespace, localRename, srcNsName, dstNsName);
	}
	return localRename(srcNsName, dstNsName);
}

Error ClusterProxy::AddIndex(std::string_view nsName, const IndexDef &index, const InternalRdxContext &ctx) {
	auto localAddIndex = [this, &ctx](std::string_view nsName, const IndexDef &index) {
		DefFunctor2(std::string_view, const IndexDef &, AddIndex, baseFollowerAction);
		clusterProxyLog(LogTrace, "[%d proxy] ClusterProxy::AddIndex", sId_.load(std::memory_order_relaxed));
		return CallProxyFunction(AddIndex)(ctx, nsName, action, nsName, index);
	};

	if (isWithSharding(nsName, ctx)) {
		Error err = shardingRouter_->AwaitShards(ctx);
		if (!err.ok()) return err;

		return delegateToShards(&client::SyncCoroReindexer::AddIndex, localAddIndex, nsName, index);
	}
	return localAddIndex(nsName, index);
}

Error ClusterProxy::UpdateIndex(std::string_view nsName, const IndexDef &index, const InternalRdxContext &ctx) {
	auto localUpdateIndex = [this, &ctx](std::string_view nsName, const IndexDef &index) {
		DefFunctor2(std::string_view, const IndexDef &, UpdateIndex, baseFollowerAction);
		clusterProxyLog(LogTrace, "[%d proxy] ClusterProxy::UpdateIndex", sId_.load(std::memory_order_relaxed));
		return CallProxyFunction(UpdateIndex)(ctx, nsName, action, nsName, index);
	};

	if (isWithSharding(nsName, ctx)) {
		Error err = shardingRouter_->AwaitShards(ctx);
		if (!err.ok()) return err;

		return delegateToShards(&client::SyncCoroReindexer::UpdateIndex, localUpdateIndex, nsName, index);
	}
	return localUpdateIndex(nsName, index);
}

Error ClusterProxy::DropIndex(std::string_view nsName, const IndexDef &index, const InternalRdxContext &ctx) {
	auto localDropIndex = [this, &ctx](std::string_view nsName, const IndexDef &index) {
		DefFunctor2(std::string_view, const IndexDef &, DropIndex, baseFollowerAction);
		clusterProxyLog(LogTrace, "[%d proxy] ClusterProxy::DropIndex", sId_.load(std::memory_order_relaxed));
		return CallProxyFunction(DropIndex)(ctx, nsName, action, nsName, index);
	};
	if (isWithSharding(nsName, ctx)) {
		Error err = shardingRouter_->AwaitShards(ctx);
		if (!err.ok()) return err;

		return delegateToShards(&client::SyncCoroReindexer::DropIndex, localDropIndex, nsName, index);
	}
	return localDropIndex(nsName, index);
}

Error ClusterProxy::SetSchema(std::string_view nsName, std::string_view schema, const InternalRdxContext &ctx) {
	auto localSetSchema = [this, &ctx](std::string_view nsName, std::string_view schema) {
		DefFunctor2(std::string_view, std::string_view, SetSchema, baseFollowerAction);
		clusterProxyLog(LogTrace, "[%d proxy] ClusterProxy::SetSchema", sId_.load(std::memory_order_relaxed));
		return CallProxyFunction(SetSchema)(ctx, nsName, action, nsName, schema);
	};
	if (isWithSharding(nsName, ctx)) {
		Error err = shardingRouter_->AwaitShards(ctx);
		if (!err.ok()) return err;

		return delegateToShards(&client::SyncCoroReindexer::SetSchema, localSetSchema, nsName, schema);
	}
	return localSetSchema(nsName, schema);
}

Error ClusterProxy::GetSchema(std::string_view nsName, int format, std::string &schema, const InternalRdxContext &ctx) {
	return impl_.GetSchema(nsName, format, schema, ctx);
}

Error ClusterProxy::EnumNamespaces(vector<NamespaceDef> &defs, EnumNamespacesOpts opts, const InternalRdxContext &ctx) {
	return impl_.EnumNamespaces(defs, opts, ctx);
}

Error ClusterProxy::Insert(std::string_view nsName, Item &item, const InternalRdxContext &ctx) {
	auto insertFn = [this, ctx](std::string_view nsName, Item &item) -> Error {
		std::function<Error(const InternalRdxContext &, std::shared_ptr<client::SyncCoroReindexer>, std::string_view, Item &)> action =
			std::bind(&ClusterProxy::itemFollowerAction<Error (client::SyncCoroReindexer::*)(std::string_view, client::Item &),
														&client::SyncCoroReindexer::Insert>,
					  this, _1, _2, _3, _4);

		return proxyCall<Error (ReindexerImpl::*)(std::string_view, Item &, const InternalRdxContext &), &ReindexerImpl::Insert,
						 Error (client::SyncCoroReindexer::*)(std::string_view, client::Item &), &client::SyncCoroReindexer::Insert, Error>(
			ctx, nsName, action, nsName, item);
	};

	if (isWithSharding(nsName, ctx)) {
		return modifyItemOnShard<&client::SyncCoroReindexer::Insert>(ctx, nsName, item, insertFn);
	}
	return insertFn(nsName, item);
}

Error ClusterProxy::Insert(std::string_view nsName, Item &item, QueryResults &result, const InternalRdxContext &ctx) {
	auto insertFn = [this, ctx](std::string_view nsName, Item &item, LocalQueryResults &result) -> Error {
		std::function<Error(const InternalRdxContext &, std::shared_ptr<client::SyncCoroReindexer>, std::string_view, Item &,
							LocalQueryResults &)>
			action =
				std::bind(&ClusterProxy::resultItemFollowerAction<Error (client::SyncCoroReindexer::*)(std::string_view, client::Item &,
																									   client::SyncCoroQueryResults &),
																  &client::SyncCoroReindexer::Insert>,
						  this, _1, _2, _3, _4, _5);

		return proxyCall<Error (ReindexerImpl::*)(std::string_view, Item &, LocalQueryResults &, const InternalRdxContext &),
						 &ReindexerImpl::Insert,
						 Error (client::SyncCoroReindexer::*)(std::string_view, client::Item &, client::SyncCoroQueryResults &),
						 &client::SyncCoroReindexer::Insert, Error>(ctx, nsName, action, nsName, item, result);
	};

	result.SetQuery(nullptr);
	if (isWithSharding(nsName, ctx)) {
		return modifyItemOnShard<&client::SyncCoroReindexer::Insert>(ctx, nsName, item, result, insertFn);
	}

	try {
		return insertFn(nsName, item, result.ToLocalQr(true));
	} catch (Error &e) {
		return e;
	}
}

Error ClusterProxy::Update(std::string_view nsName, Item &item, const InternalRdxContext &ctx) {
	auto updateFn = [this, ctx](std::string_view nsName, Item &item) -> Error {
		std::function<Error(const InternalRdxContext &, std::shared_ptr<client::SyncCoroReindexer>, std::string_view, Item &)> action =
			std::bind(&ClusterProxy::itemFollowerAction<Error (client::SyncCoroReindexer::*)(std::string_view, client::Item &),
														&client::SyncCoroReindexer::Update>,
					  this, _1, _2, _3, _4);
		return proxyCall<Error (ReindexerImpl::*)(std::string_view, Item &, const InternalRdxContext &), &ReindexerImpl::Update,
						 Error (client::SyncCoroReindexer::*)(std::string_view, client::Item &), &client::SyncCoroReindexer::Update, Error>(
			ctx, nsName, action, nsName, item);
	};

	if (isWithSharding(nsName, ctx)) {
		return modifyItemOnShard<&client::SyncCoroReindexer::Update>(ctx, nsName, item, updateFn);
	}
	return updateFn(nsName, item);
}

Error ClusterProxy::Update(std::string_view nsName, Item &item, QueryResults &result, const InternalRdxContext &ctx) {
	auto updateFn = [this, ctx](std::string_view nsName, Item &item, LocalQueryResults &result) -> Error {
		std::function<Error(const InternalRdxContext &, std::shared_ptr<client::SyncCoroReindexer>, std::string_view, Item &,
							LocalQueryResults &)>
			action =
				std::bind(&ClusterProxy::resultItemFollowerAction<Error (client::SyncCoroReindexer::*)(std::string_view, client::Item &,
																									   client::SyncCoroQueryResults &),
																  &client::SyncCoroReindexer::Update>,
						  this, _1, _2, _3, _4, _5);

		return proxyCall<Error (ReindexerImpl::*)(std::string_view, Item &, LocalQueryResults &, const InternalRdxContext &),
						 &ReindexerImpl::Update,
						 Error (client::SyncCoroReindexer::*)(std::string_view, client::Item &, client::SyncCoroQueryResults &),
						 &client::SyncCoroReindexer::Update, Error>(ctx, nsName, action, nsName, item, result);
	};

	result.SetQuery(nullptr);
	if (isWithSharding(nsName, ctx)) {
		return modifyItemOnShard<&client::SyncCoroReindexer::Update>(ctx, nsName, item, result, updateFn);
	}

	try {
		return updateFn(nsName, item, result.ToLocalQr(true));
	} catch (Error &e) {
		return e;
	}
}

Error ClusterProxy::Update(const Query &query, QueryResults &result, const InternalRdxContext &ctx) {
	std::function<Error(const Query &, LocalQueryResults &, const InternalRdxContext &ctx)> updateFn =
		[this](const Query &q, LocalQueryResults &qr, const InternalRdxContext &ctx) -> Error {
		std::function<Error(const InternalRdxContext &, std::shared_ptr<client::SyncCoroReindexer>, const Query &, LocalQueryResults &)>
			action = std::bind(
				&ClusterProxy::resultFollowerAction<Error (client::SyncCoroReindexer::*)(const Query &, client::SyncCoroQueryResults &),
													&client::SyncCoroReindexer::Update>,
				this, _1, _2, _3, _4);
		clusterProxyLog(LogTrace, "[%d proxy] ClusterProxy::Update query", sId_);

		return proxyCall<Error (ReindexerImpl::*)(const Query &, LocalQueryResults &, const InternalRdxContext &), &ReindexerImpl::Update,
						 Error (client::SyncCoroReindexer::*)(const Query &, client::SyncCoroQueryResults &),
						 &client::SyncCoroReindexer::Update, Error>(ctx, q._namespace, action, q, qr);
	};

	result.SetQuery(&query);
	try {
		if (isWithSharding(query, ctx)) {
			return executeQueryOnShard(query, result, ctx, updateFn);
		}
		return updateFn(query, result.ToLocalQr(true), ctx);
	} catch (Error &e) {
		return e;
	}
}

Error ClusterProxy::Upsert(std::string_view nsName, Item &item, QueryResults &result, const InternalRdxContext &ctx) {
	auto upsertFn = [this, ctx](std::string_view nsName, Item &item, LocalQueryResults &result) -> Error {
		std::function<Error(const InternalRdxContext &, std::shared_ptr<client::SyncCoroReindexer>, std::string_view, Item &,
							LocalQueryResults &)>
			action =
				std::bind(&ClusterProxy::resultItemFollowerAction<Error (client::SyncCoroReindexer::*)(std::string_view, client::Item &,
																									   client::SyncCoroQueryResults &),
																  &client::SyncCoroReindexer::Upsert>,
						  this, _1, _2, _3, _4, _5);

		return proxyCall<Error (ReindexerImpl::*)(std::string_view, Item &, LocalQueryResults &, const InternalRdxContext &),
						 &ReindexerImpl::Upsert,
						 Error (client::SyncCoroReindexer::*)(std::string_view, client::Item &, client::SyncCoroQueryResults &),
						 &client::SyncCoroReindexer::Upsert, Error>(ctx, nsName, action, nsName, item, result);
	};

	result.SetQuery(nullptr);
	if (isWithSharding(nsName, ctx)) {
		return modifyItemOnShard<&client::SyncCoroReindexer::Upsert>(ctx, nsName, item, result, upsertFn);
	}

	try {
		return upsertFn(nsName, item, result.ToLocalQr(true));
	} catch (Error &e) {
		return e;
	}
}

Error ClusterProxy::Upsert(std::string_view nsName, Item &item, const InternalRdxContext &ctx) {
	auto upsertFn = [this, ctx](std::string_view nsName, Item &item) -> Error {
		std::function<Error(const InternalRdxContext &, std::shared_ptr<client::SyncCoroReindexer>, std::string_view, Item &)> action =
			std::bind(&ClusterProxy::itemFollowerAction<Error (client::SyncCoroReindexer::*)(std::string_view, client::Item &),
														&client::SyncCoroReindexer::Upsert>,
					  this, _1, _2, _3, _4);
		return proxyCall<Error (ReindexerImpl::*)(std::string_view, Item &, const InternalRdxContext &), &ReindexerImpl::Upsert,
						 Error (client::SyncCoroReindexer::*)(std::string_view, client::Item &), &client::SyncCoroReindexer::Upsert, Error>(
			ctx, nsName, action, nsName, item);
	};

	if (isWithSharding(nsName, ctx)) {
		return modifyItemOnShard<&client::SyncCoroReindexer::Upsert>(ctx, nsName, item, upsertFn);
	}
	return upsertFn(nsName, item);
}

Error ClusterProxy::Delete(std::string_view nsName, Item &item, const InternalRdxContext &ctx) {
	auto deleteFn = [this, ctx](std::string_view nsName, Item &item) -> Error {
		std::function<Error(const InternalRdxContext &, std::shared_ptr<client::SyncCoroReindexer>, std::string_view, Item &)> action =
			std::bind(&ClusterProxy::itemFollowerAction<Error (client::SyncCoroReindexer::*)(std::string_view, client::Item &),
														&client::SyncCoroReindexer::Delete>,
					  this, _1, _2, _3, _4);
		clusterProxyLog(LogTrace, "[%d proxy] ClusterProxy::Delete ITEM", sId_.load(std::memory_order_relaxed));
		return proxyCall<Error (ReindexerImpl::*)(std::string_view, Item &, const InternalRdxContext &), &ReindexerImpl::Delete,
						 Error (client::SyncCoroReindexer::*)(std::string_view, client::Item &), &client::SyncCoroReindexer::Delete, Error>(
			ctx, nsName, action, nsName, item);
	};

	if (isWithSharding(nsName, ctx)) {
		return modifyItemOnShard<&client::SyncCoroReindexer::Delete>(ctx, nsName, item, deleteFn);
	}
	return deleteFn(nsName, item);
}

Error ClusterProxy::Delete(std::string_view nsName, Item &item, QueryResults &result, const InternalRdxContext &ctx) {
	auto deleteFn = [this, ctx](std::string_view nsName, Item &item, LocalQueryResults &result) -> Error {
		std::function<Error(const InternalRdxContext &, std::shared_ptr<client::SyncCoroReindexer>, std::string_view, Item &,
							LocalQueryResults &)>
			action =
				std::bind(&ClusterProxy::resultItemFollowerAction<Error (client::SyncCoroReindexer::*)(std::string_view, client::Item &,
																									   client::SyncCoroQueryResults &),
																  &client::SyncCoroReindexer::Delete>,
						  this, _1, _2, _3, _4, _5);

		return proxyCall<Error (ReindexerImpl::*)(std::string_view, Item &, LocalQueryResults &, const InternalRdxContext &),
						 &ReindexerImpl::Delete,
						 Error (client::SyncCoroReindexer::*)(std::string_view, client::Item &, client::SyncCoroQueryResults &),
						 &client::SyncCoroReindexer::Delete, Error>(ctx, nsName, action, nsName, item, result);
	};

	result.SetQuery(nullptr);
	if (isWithSharding(nsName, ctx)) {
		return modifyItemOnShard<&client::SyncCoroReindexer::Delete>(ctx, nsName, item, result, deleteFn);
	}

	try {
		return deleteFn(nsName, item, result.ToLocalQr(true));
	} catch (Error &e) {
		return e;
	}
}

Error ClusterProxy::Delete(const Query &query, QueryResults &result, const InternalRdxContext &ctx) {
	std::function<Error(const Query &, LocalQueryResults &, const InternalRdxContext &ctx)> deleteFn =
		[this](const Query &q, LocalQueryResults &qr, const InternalRdxContext &ctx) -> Error {
		std::function<Error(const InternalRdxContext &, std::shared_ptr<client::SyncCoroReindexer>, const Query &, LocalQueryResults &)>
			action = std::bind(
				&ClusterProxy::resultFollowerAction<Error (client::SyncCoroReindexer::*)(const Query &, client::SyncCoroQueryResults &),
													&client::SyncCoroReindexer::Delete>,
				this, _1, _2, _3, _4);
		clusterProxyLog(LogTrace, "[%d proxy] ClusterProxy::Delete QUERY", sId_);
		return proxyCall<Error (ReindexerImpl::*)(const Query &, LocalQueryResults &, const InternalRdxContext &), &ReindexerImpl::Delete,
						 Error (client::SyncCoroReindexer::*)(const Query &, client::SyncCoroQueryResults &),
						 &client::SyncCoroReindexer::Delete, Error>(ctx, q._namespace, action, q, qr);
	};

	result.SetQuery(&query);
	try {
		if (isWithSharding(query, ctx)) {
			return executeQueryOnShard(query, result, ctx, deleteFn);
		}
		return deleteFn(query, result.ToLocalQr(true), ctx);
	} catch (Error &e) {
		return e;
	}
}

Error ClusterProxy::Select(std::string_view sql, QueryResults &result, const InternalRdxContext &ctx) {
	Query query;
	try {
		query.FromSQL(sql);
	} catch (const Error &err) {
		return err;
	}
	switch (query.type_) {
		case QuerySelect: {
			return Select(query, result, ctx);
		}
		case QueryDelete: {
			return Delete(query, result, ctx);
		}
		case QueryUpdate: {
			return Update(query, result, ctx);
		}
		case QueryTruncate: {
			return TruncateNamespace(query.Namespace(), ctx);
		}
		default:
			return Error(errLogic, "Incorrect sql type %d", query.type_);
	}
}

Error ClusterProxy::Select(const Query &query, QueryResults &result, const InternalRdxContext &ctx) {
	if (query.Type() != QuerySelect) {
		return Error(errLogic, "'Select' call request type is not equal to 'QuerySelect'.");
	}
	result.SetQuery(&query);
	try {
		if (isWithSharding(query, ctx)) {
			std::function<Error(const Query &, LocalQueryResults &, const InternalRdxContext &ctx)> selectFn =
				[this](const Query &q, LocalQueryResults &qr, const InternalRdxContext &ctx) -> Error { return impl_.Select(q, qr, ctx); };
			return executeQueryOnShard(query, result, ctx, selectFn);
		}
		if (!shouldProxyQuery(query)) {
			clusterProxyLog(LogTrace, "[%d proxy] ClusterProxy::Select query local", sId_.load(std::memory_order_relaxed));
			LocalQueryResults lqr;
			int actualShardId = ShardingKeyType::ProxyOff;
			if (shardingRouter_ &&
				shardingRouter_->IsSharded(query.Namespace())) {  // incorrect if query have join (true for 'select * from ns')
				actualShardId = shardingRouter_->ActualShardId();
				if (result.NeedOutputShardId()) {
					lqr.SetOutputShardId(actualShardId);
				}
			}
			result.AddQr(std::move(lqr), actualShardId);
			return impl_.Select(query, result.ToLocalQr(false), ctx);
		}
	} catch (const Error &ex) {
		return ex;
	}
	const InternalRdxContext deadlineCtx(ctx.HasDeadline() ? ctx : ctx.WithTimeout(kReplicationStatsTimeout));
	std::function<Error(const InternalRdxContext &, std::shared_ptr<client::SyncCoroReindexer>, const Query &, LocalQueryResults &)>
		action = std::bind(
			&ClusterProxy::resultFollowerAction<Error (client::SyncCoroReindexer::*)(const Query &, client::SyncCoroQueryResults &),
												&client::SyncCoroReindexer::Select>,
			this, _1, _2, _3, _4);
	clusterProxyLog(LogTrace, "[%d proxy] ClusterProxy::Select query proxied", sId_.load(std::memory_order_relaxed));
	try {
		return proxyCall<Error (ReindexerImpl::*)(const Query &, LocalQueryResults &, const InternalRdxContext &), &ReindexerImpl::Select,
						 Error (client::SyncCoroReindexer::*)(const Query &, client::SyncCoroQueryResults &),
						 &client::SyncCoroReindexer::Select, Error>(deadlineCtx, query._namespace, action, query, result.ToLocalQr(true));
	} catch (Error &e) {
		return e;
	}
}

Error ClusterProxy::Commit(std::string_view nsName, const InternalRdxContext &ctx) {
	if (isWithSharding(nsName, ctx)) {
		Error err = shardingRouter_->AwaitShards(ctx);
		if (!err.ok()) return err;

		err = delegateToShardsByNs(true, &client::SyncCoroReindexer::Commit, nsName);
		if (err.code() != errAlreadyConnected) return err;
	}
	return impl_.Commit(nsName);
}

Item ClusterProxy::NewItem(std::string_view nsName, const InternalRdxContext &ctx) { return impl_.NewItem(nsName, ctx); }

Transaction ClusterProxy::NewTransaction(std::string_view nsName, const InternalRdxContext &ctx) {
	const bool withSharding = isWithSharding(nsName, ctx);

	std::function<Transaction(const InternalRdxContext &, std::shared_ptr<client::SyncCoroReindexer>, std::string_view)> action =
		std::bind(&ClusterProxy::newTxFollowerAction, this, _1, _2, _3);
	clusterProxyLog(LogTrace, "[%d proxy] ClusterProxy::NewTransaction", sId_.load(std::memory_order_relaxed));
	Transaction tx = proxyCall<Transaction (ReindexerImpl::*)(std::string_view, const InternalRdxContext &), &ReindexerImpl::NewTransaction,
							   client::SyncCoroTransaction (client::SyncCoroReindexer::*)(std::string_view),
							   &client::SyncCoroReindexer::NewTransaction, Transaction>(ctx, nsName, action, nsName);

	if (withSharding) {
		tx.SetShardingRouter(shardingRouter_);
	}
	return tx;
}

Error ClusterProxy::CommitTransaction(Transaction &tr, QueryResults &result, const InternalRdxContext &ctx) {
	if (!tr.Status().ok()) {
		return Error(tr.Status().code(), "Unable to commit tx with error status: '%s'", tr.Status().what());
	}
	result.SetQuery(nullptr);
	if (isWithSharding(tr.GetNsName(), ctx)) {
		if (tr.shardId_ == ShardingKeyType::NotSetShard) {
			assertrx(false);
			return Error(errLogic, "Error commiting transaction with sharding: shard ID is not set");
		}
		if (!tr.IsProxied()) {
			clusterProxyLog(LogTrace, "[proxy] Local commit on shard  %d. SID: %d. Steps: %d", shardingRouter_->ActualShardId(),
							sId_.load(std::memory_order_relaxed), tr.GetSteps().size());
			try {
				result.AddQr(LocalQueryResults(), shardingRouter_->ActualShardId());
				return impl_.CommitTransaction(tr, result.ToLocalQr(false), ctx);
			} catch (Error &e) {
				return e;
			}
		}
	}

	if (tr.IsProxied()) {
		if (!tr.clientTransaction_->Status().ok()) return tr.clientTransaction_->Status();
		assertrx(tr.clientTransaction_->rx_);
		client::SyncCoroReindexerImpl *client = tr.clientTransaction_->rx_.get();
		client::InternalRdxContext c;
		if (!tr.IsProxiedWithShardingProxy()) {
			c = client::InternalRdxContext{}.WithLSN(ctx.LSN()).WithEmmiterServerId(sId_).WithShardingParallelExecution(
				ctx.IsShardingParallelExecution());
			clusterProxyLog(LogTrace, "[proxy] Proxying commit to leader. SID: %d", sId_.load(std::memory_order_relaxed));
		} else {
			c = client::InternalRdxContext{}.WithShardId(tr.shardId_, false);
			clusterProxyLog(LogTrace, "[proxy] Proxying commit to shard %d. SID: %d", tr.shardId_, sId_.load(std::memory_order_relaxed));
		}

		client::SyncCoroQueryResults clientResults;
		Error err = client->CommitTransaction(*tr.clientTransaction_.get(), clientResults, c);
		if (err.ok()) {
			try {
				result.AddQr(std::move(clientResults), tr.shardId_);
			} catch (Error &e) {
				return e;
			}
		}
		return err;
	}
	clusterProxyLog(LogTrace, "[proxy] Local commit (no sharding). SID: %d. Ctx shard id: %d", sId_.load(std::memory_order_relaxed),
					ctx.ShardId());
	try {
		result.AddQr(LocalQueryResults());
		return impl_.CommitTransaction(tr, result.ToLocalQr(false), ctx);
	} catch (Error &e) {
		return e;
	}
}

Error ClusterProxy::RollBackTransaction(Transaction &tr, const InternalRdxContext &ctx) {
	if (tr.clientTransaction_) {
		if (!tr.clientTransaction_->Status().ok()) return tr.clientTransaction_->Status();
		assertrx(tr.clientTransaction_->rx_);
		client::SyncCoroReindexerImpl *client = tr.clientTransaction_->rx_.get();
		client::SyncCoroQueryResults clientResults;
		return client->RollBackTransaction(*tr.clientTransaction_.get(),
										   client::InternalRdxContext().WithLSN(ctx.LSN()).WithEmmiterServerId(sId_));
	} else {
		return impl_.RollBackTransaction(tr, ctx);
	}
}

Error ClusterProxy::GetMeta(std::string_view nsName, const string &key, string &data, const InternalRdxContext &ctx) {
	if (isWithSharding(nsName, ctx)) {
		Error err = delegateToShardsByNs(false,
										 static_cast<Error (client::SyncCoroReindexer::*)(std::string_view, const string &, string &)>(
											 &client::SyncCoroReindexer::GetMeta),
										 nsName, key, data);
		if (err.code() != errAlreadyConnected) return err;
	}
	return impl_.GetMeta(nsName, key, data, ctx);
}

Error ClusterProxy::GetMeta(std::string_view nsName, const string &key, vector<ShardedMeta> &data, const InternalRdxContext &ctx) {
	if (isWithSharding(nsName, ctx)) {
		Error err = collectFromShardsByNs(
			[&key](sharding::ShardConnection &conn, std::string_view ns, vector<ShardedMeta> &d) { return conn->GetMeta(ns, key, d); },
			nsName, data);
		if (!err.ok()) return err;
		data.emplace_back();
		data.back().shardId = shardingRouter_->ActualShardId();
		return impl_.GetMeta(nsName, key, data.back().data, ctx);
	}

	data.emplace_back();
	data.back().shardId = ctx.ShardId();
	return impl_.GetMeta(nsName, key, data.back().data, ctx);
}

Error ClusterProxy::PutMeta(std::string_view nsName, const string &key, std::string_view data, const InternalRdxContext &ctx) {
	auto localPutMeta = [this, &ctx](std::string_view nsName, const string &key, std::string_view data) {
		std::function<Error(const InternalRdxContext &, std::shared_ptr<client::SyncCoroReindexer>, std::string_view, const string &,
							std::string_view)>
			action = std::bind(
				&ClusterProxy::baseFollowerAction<decltype(&client::SyncCoroReindexer::PutMeta), &client::SyncCoroReindexer::PutMeta,
												  std::string_view, const string &, std::string_view>,
				this, _1, _2, _3, _4, _5);
		clusterProxyLog(LogTrace, "[%d proxy] ClusterProxy::PutMeta", sId_.load(std::memory_order_relaxed));
		return CallProxyFunction(PutMeta)(ctx, nsName, action, nsName, key, data);
	};

	if (isWithSharding(nsName, ctx)) {
		Error err = delegateToShards(&client::SyncCoroReindexer::PutMeta, localPutMeta, nsName, key, data);
		if (err.code() != errAlreadyConnected) return err;
	}
	return localPutMeta(nsName, key, data);
}

Error ClusterProxy::EnumMeta(std::string_view nsName, vector<string> &keys, const InternalRdxContext &ctx) {
	if (isWithSharding(nsName, ctx)) {
		fast_hash_set<std::string> allKeys;
		Error err = collectFromShardsByNs(
			[](sharding::ShardConnection &conn, std::string_view ns, vector<string> &d) { return conn->EnumMeta(ns, d); }, nsName, keys,
			[&allKeys](const std::string &key) { return allKeys.emplace(key).second; });
		std::vector<std::string> part;
		err = impl_.EnumMeta(nsName, part, ctx);
		if (!err.ok()) return err;
		keys.reserve(keys.size() + part.size());
		for (auto &&p : part) {
			if (allKeys.find(p) == allKeys.end()) {
				keys.emplace_back(std::move(p));
			}
		}
		return Error();
	}
	return impl_.EnumMeta(nsName, keys, ctx);
}

bool ClusterProxy::isWithSharding(const Query &q, const InternalRdxContext &ctx) const {
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
	if (shardingRouter_->IsSharded(q.Namespace())) {
		return true;
	}
	for (const auto &jq : q.joinQueries_) {
		if (shardingRouter_->IsSharded(jq.Namespace())) {
			return true;
		}
	}
	for (const auto &mq : q.mergeQueries_) {
		if (shardingRouter_->IsSharded(mq.Namespace())) {
			return true;
		}
	}
	return false;
}

bool ClusterProxy::isWithSharding(std::string_view nsName, const InternalRdxContext &ctx) const noexcept {
	if (nsName.size() && (nsName[0] == '#' || nsName[0] == '@')) {
		return false;
	}
	if (!shardingRouter_ || !shardingRouter_->IsSharded(nsName)) {
		return false;
	}
	return isWithSharding(ctx);
}

bool ClusterProxy::isWithSharding(const InternalRdxContext &ctx) const noexcept {
	if (int(ctx.ShardId()) == ShardingKeyType::ProxyOff) {
		return false;
	}
	const bool ret = ctx.LSN().isEmpty() && !ctx.HasEmmiterID() && impl_.shardingConfig_ && shardingRouter_ &&
					 int(ctx.ShardId()) == ShardingKeyType::NotSetShard;
	return ret;
}

bool ClusterProxy::shouldProxyQuery(const Query &q) {
	assertrx(q.Type() == QuerySelect);
	if (kReplicationStatsNamespace != q._namespace) {
		return false;
	}
	if (q.HasLimit()) {
		return false;
	}
	if (q.joinQueries_.size() || q.mergeQueries_.size()) {
		throw Error(errParams, "Joins and merges are not allowed for #replicationstats queries");
	}
	bool hasTypeCond = false;
	bool isAsyncReplQuery = false;
	bool isClusterReplQuery = false;
	constexpr auto kConditionError =
		"Query to #replicationstats has to contain one of the following conditions: type=async or type=cluster"sv;
	for (auto it = q.entries.cbegin(), end = q.entries.cend(); it != end; ++it) {
		if (it->HoldsOrReferTo<QueryEntry>() && it->Value<QueryEntry>().index == "type"sv) {
			auto nextIt = it;
			++nextIt;
			auto &entry = it->Value<QueryEntry>();
			if (hasTypeCond || entry.condition != CondEq || entry.values.size() != 1 || entry.values[0].Type() != KeyValueString ||
				it->operation != OpAnd || (nextIt != end && nextIt->operation == OpOr)) {
				throw Error(errParams, kConditionError);
			}
			auto str = entry.values[0].As<std::string>();
			if (str == cluster::kAsyncReplStatsType) {
				isAsyncReplQuery = true;
			} else if (str == cluster::kClusterReplStatsType) {
				isClusterReplQuery = true;
			} else {
				throw Error(errParams, kConditionError);
			}
			hasTypeCond = true;
			if (isClusterReplQuery && isAsyncReplQuery) {
				throw Error(errParams, kConditionError);
			}
		}
	}
	if (!isClusterReplQuery && !isAsyncReplQuery) {
		throw Error(errParams, kConditionError);
	}
	return isClusterReplQuery;
}

}  // namespace reindexer
