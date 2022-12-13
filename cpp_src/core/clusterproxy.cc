#include "clusterproxy.h"
#include <shared_mutex>
#include "client/itemimpl.h"
#include "client/reindexerimpl.h"
#include "cluster/clusterizator.h"
#include "cluster/sharding/sharding.h"
#include "core/defnsconfigs.h"
#include "core/queryresults/joinresults.h"
#include "core/reindexerimpl.h"
#include "core/type_consts.h"
#include "parallelexecutor.h"
#include "tools/clusterproxyloghelper.h"
#include "tools/logger.h"

using namespace std::placeholders;
using namespace std::string_view_literals;

namespace reindexer {

constexpr auto kReplicationStatsTimeout = std::chrono::seconds(10);

reindexer::client::Item ClusterProxy::toClientItem(std::string_view ns, client::Reindexer *connection, reindexer::Item &item) {
	assertrx(connection);
	Error err;
	reindexer::client::Item clientItem = connection->NewItem(ns);
	if (clientItem.Status().ok()) {
		err = clientItem.FromJSON(item.GetJSON());
		if (err.ok() && clientItem.Status().ok()) {
			clientItem.SetPrecepts(item.impl_->GetPrecepts());
			if (item.impl_->tagsMatcher().isUpdated()) {
				// Add new names missing in JSON from tm
				clientItem.impl_->addTagNamesFrom(item.impl_->tagsMatcher());
			}
			return clientItem;
		}
	}
	if (!err.ok()) throw err;
	throw clientItem.Status();
}

// This method may be used for cluster proxied qr only
// TODO: Remove it totally
void ClusterProxy::clientToCoreQueryResults(client::QueryResults &clientResults, LocalQueryResults &result) {
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
			Item itemServer = impl_.NewItem(clientResults.GetNamespaces()[0], RdxContext());
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
Error ClusterProxy::baseFollowerAction(const RdxContext &ctx, const std::shared_ptr<client::Reindexer> &clientToLeader, Args &&...args) {
	try {
		client::Reindexer l = clientToLeader->WithLSN(ctx.GetOriginLSN()).WithEmmiterServerId(sId_);
		const auto ward = ctx.BeforeClusterProxy();
		Error err = (l.*fnl)(std::forward<Args>(args)...);
		return err;
	} catch (const Error &err) {
		return err;
	}
}

template <typename FnL, FnL fnl>
Error ClusterProxy::itemFollowerAction(const RdxContext &ctx, const std::shared_ptr<client::Reindexer> &clientToLeader,
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

template <typename FnL, FnL fnl>
Error ClusterProxy::resultItemFollowerAction(const RdxContext &ctx, const std::shared_ptr<client::Reindexer> &clientToLeader,
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
		clientToCoreQueryResults(clientResults, result);
		return err;
	} catch (const Error &err) {
		return err;
	}
}

template <typename FnL, FnL fnl>
Error ClusterProxy::resultFollowerAction(const RdxContext &ctx, const std::shared_ptr<client::Reindexer> &clientToLeader,
										 const Query &query, LocalQueryResults &result) {
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

Transaction ClusterProxy::newTxFollowerAction(bool isWithSharding, const RdxContext &ctx,
											  const std::shared_ptr<client::Reindexer> &clientToLeader, std::string_view nsName) {
	try {
		client::Reindexer l = clientToLeader->WithLSN(ctx.GetOriginLSN()).WithEmmiterServerId(sId_);
		return isWithSharding ? Transaction(impl_.NewTransaction(nsName, ctx), std::move(l))
							  : Transaction(impl_.NewTransaction(nsName, ctx), l.NewTransaction(nsName));
	} catch (const Error &err) {
		return Transaction(err);
	}
}

template <>
ErrorCode ClusterProxy::getErrCode<Error>(const Error &err, Error &r) {
	if (!err.ok()) {
		r = err;
		return ErrorCode(err.code());
	}
	return ErrorCode(r.code());
}

template <>
ErrorCode ClusterProxy::getErrCode<Transaction>(const Error &err, Transaction &r) {
	if (!err.ok()) {
		r = Transaction(err);
		return ErrorCode(err.code());
	}
	return ErrorCode(r.Status().code());
}

template <typename R>
void ClusterProxy::setErrorCode(R &r, Error &&err) {
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

template <typename Fn, Fn fn, typename R, typename... Args>
R ClusterProxy::localCall(const RdxContext &ctx, Args &&...args) {
	if constexpr (std::is_same_v<R, Transaction>) {
		return R((impl_.*fn)(std::forward<Args>(args)..., ctx));
	} else {
		return (impl_.*fn)(std::forward<Args>(args)..., ctx);
	}
}

template <typename Fn, Fn fn, typename FnL, FnL fnl, typename R, typename FnA, typename... Args>
R ClusterProxy::proxyCall(const RdxContext &_ctx, std::string_view nsName, FnA &action, Args &&...args) {
	R r;
	Error err;
	if (_ctx.GetOriginLSN().isEmpty()) {
		ErrorCode errCode = errOK;
		bool allowCandidateRole = true;
		do {
			cluster::RaftInfo info;
			err = impl_.GetRaftInfo(allowCandidateRole, info, _ctx);
			if (!err.ok()) {
				if (err.code() == errTimeout || err.code() == errCanceled) {
					err = Error(err.code(), "Unable to get cluster's leader: %s", err.what());
				}
				setErrorCode(r, std::move(err));
				return r;
			}
			if (info.role == cluster::RaftInfo::Role::None) {  // fast way for non-cluster node
				if (_ctx.HasEmmiterServer()) {
					setErrorCode(r, Error(errLogic, "Request was proxied to non-cluster node"));
					return r;
				}
				r = localCall<Fn, fn, R, Args...>(_ctx, std::forward<Args>(args)...);
				errCode = getErrCode(err, r);
				if (errCode == errWrongReplicationData &&
					(!impl_.clusterConfig_ || !impl_.clusterizator_->NamespaceIsInClusterConfig(nsName))) {
					break;
				}
				continue;
			}
			const bool nsInClusterConf = impl_.clusterizator_->NamespaceIsInClusterConfig(nsName);
			if (!nsInClusterConf) {	 // ns is not in cluster
				clusterProxyLog(LogTrace, "[%d proxy] proxyCall ns not in cluster config (local)", sId_.load(std::memory_order_relaxed));
				const bool firstError = (errCode == errOK);
				r = localCall<Fn, fn, R, Args...>(_ctx, std::forward<Args>(args)...);
				errCode = getErrCode(err, r);
				if (firstError) {
					continue;
				}
				break;
			}
			if (info.role == cluster::RaftInfo::Role::Leader) {
				resetLeader();
				clusterProxyLog(LogTrace, "[%d proxy] proxyCall RaftInfo::Role::Leader", sId_.load(std::memory_order_relaxed));
				r = localCall<Fn, fn, R, Args...>(_ctx, std::forward<Args>(args)...);
#if RX_ENABLE_CLUSTERPROXY_LOGS
				printErr(r);
#endif
				// the only place, where errUpdateReplication may appear
			} else if (info.role == cluster::RaftInfo::Role::Follower) {
				if (_ctx.HasEmmiterServer()) {
					setErrorCode(r, Error(errAlreadyProxied, "Request was proxied to follower node"));
					return r;
				}
				try {
					auto clientToLeader = getLeader(info);
					clusterProxyLog(LogTrace, "[%d proxy] proxyCall RaftInfo::Role::Follower", sId_.load(std::memory_order_relaxed));
					r = action(_ctx, clientToLeader, std::forward<Args>(args)...);
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
		clusterProxyLog(LogTrace, "[%d proxy] proxyCall LSN not empty (local call)", sId_.load(std::memory_order_relaxed));
		r = localCall<Fn, fn, R, Args...>(_ctx, std::forward<Args>(args)...);
		// errWrongReplicationData means, that leader of the current node doesn't match leader from LSN
		if (getErrCode(err, r) == errWrongReplicationData) {
			cluster::RaftInfo info;
			err = impl_.GetRaftInfo(false, info, _ctx);
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
			if (waitRes == std::cv_status::timeout || lastPingLeaderId_ != _ctx.GetOriginLSN().Server()) {
				return r;
			}
			lck.unlock();
			return localCall<Fn, fn, R, Args...>(_ctx, std::forward<Args>(args)...);
		}
	}
	return r;
}

std::shared_ptr<client::Reindexer> ClusterProxy::getLeader(const cluster::RaftInfo &info) {
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

Error ClusterProxy::Connect(const std::string &dsn, ConnectOpts opts) {
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

#define CallProxyFunction(Fn) \
	proxyCall<decltype(&ReindexerImpl::Fn), &ReindexerImpl::Fn, decltype(&client::Reindexer::Fn), &client::Reindexer::Fn, Error>

#define DefFunctor1(P1, F, Action)                                                            \
	std::function<Error(const RdxContext &, std::shared_ptr<client::Reindexer>, P1)> action = \
		std::bind(&ClusterProxy::Action<decltype(&client::Reindexer::F), &client::Reindexer::F, P1>, this, _1, _2, _3);

#define DefFunctor2(P1, P2, F, Action)                                                            \
	std::function<Error(const RdxContext &, std::shared_ptr<client::Reindexer>, P1, P2)> action = \
		std::bind(&ClusterProxy::Action<decltype(&client::Reindexer::F), &client::Reindexer::F, P1, P2>, this, _1, _2, _3, _4);

template <typename Func, typename FLocal, typename... Args>
Error ClusterProxy::delegateToShards(const RdxContext &rdxCtx, Func f, const FLocal &&local, [[maybe_unused]] Args &&...args) {
	Error status;
	try {
		auto connections = shardingRouter_->GetShardsConnections(status);
		if (!status.ok()) return status;

		ParallelExecutor execNodes(shardingRouter_->ActualShardId());
		return execNodes.Exec(rdxCtx, connections, std::move(f), std::move(local), std::forward<Args>(args)...);
	} catch (const Error &err) {
		return err;
	}

	return status;
}

template <typename Func, typename FLocal, typename... Args>
Error ClusterProxy::delegateToShardsByNs(const RdxContext &rdxCtx, Func f, const FLocal &&local, std::string_view nsName,
										 [[maybe_unused]] Args &&...args) {
	Error status;
	try {
		auto connections = shardingRouter_->GetShardsConnections(nsName, -1, status);
		if (!status.ok()) return status;

		ParallelExecutor execNodes(shardingRouter_->ActualShardId());
		return execNodes.Exec(rdxCtx, connections, std::move(f), std::move(local), nsName, std::forward<Args>(args)...);
	} catch (const Error &err) {
		return err;
	}
	return status;
}

template <ClusterProxy::ItemModifyFun fn>
Error ClusterProxy::modifyItemOnShard(const InternalRdxContext &ctx, const RdxContext &rdxCtx, std::string_view nsName, Item &item,
									  const std::function<Error(std::string_view, Item &)> &&localFn) {
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

			const auto ward = rdxCtx.BeforeShardingProxy();
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

template <typename Func, typename FLocal, typename T, typename P, typename... Args>
Error ClusterProxy::collectFromShardsByNs(const RdxContext &rdxCtx, Func f, const FLocal &&local, std::vector<T> &result, P predicate,
										  std::string_view nsName, [[maybe_unused]] Args &&...args) {
	Error status;
	try {
		auto connections = shardingRouter_->GetShardsConnections(nsName, -1, status);
		if (!status.ok()) return status;

		ParallelExecutor execNodes(shardingRouter_->ActualShardId());
		return execNodes.ExecCollect(rdxCtx, connections, std::move(f), std::move(local), result, std::move(predicate), nsName,
									 std::forward<Args>(args)...);
	} catch (const Error &err) {
		return err;
	}
}

template <ClusterProxy::ItemModifyFunQr fn>
Error ClusterProxy::modifyItemOnShard(const InternalRdxContext &ctx, const RdxContext &rdxCtx, std::string_view nsName, Item &item,
									  QueryResults &result,
									  const std::function<Error(std::string_view, Item &, LocalQueryResults &)> &&localFn) {
	Error status;
	try {
		auto connection = shardingRouter_->GetShardConnectionWithId(nsName, item, status);
		if (!status.ok()) {
			return status;
		}
		if (connection) {
			client::Item clientItem = toClientItem(nsName, connection.get(), item);
			client::QueryResults qrClient(result.Flags(), 0, false);
			if (!clientItem.Status().ok()) return clientItem.Status();

			const auto timeout = ctx.GetTimeout();
			if (timeout.count() < 0) {
				return Error(errTimeout, "Item modify request timeout");
			}
			auto conn = connection->WithContext(ctx.DeadlineCtx()).WithTimeout(timeout);

			const auto ward = rdxCtx.BeforeShardingProxy();
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

Error ClusterProxy::executeQueryOnClient(client::Reindexer &connection, const Query &q, client::QueryResults &qrClient,
										 const std::function<void(size_t count, size_t totalCount)> &limitOffsetCalc) {
	Error status;
	switch (q.Type()) {
		case QuerySelect: {
			status = connection.Select(q, qrClient);
			if (q.sortingEntries_.empty()) {
				limitOffsetCalc(qrClient.Count(), qrClient.TotalCount());
			}
			break;
		}
		case QueryUpdate: {
			status = connection.Update(q, qrClient);
			break;
		}
		case QueryDelete: {
			status = connection.Delete(q, qrClient);
			break;
		}
		default:
			std::abort();
	}
	return status;
}

void ClusterProxy::calculateNewLimitOfsset(size_t count, size_t totalCount, unsigned &limit, unsigned &offset) {
	if (limit != UINT_MAX) {
		if (count >= limit) {
			limit = 0;
		} else {
			limit -= count;
		}
	}
	if (totalCount >= offset) {
		offset = 0;
	} else {
		offset -= totalCount;
	}
}

void ClusterProxy::addEmptyLocalQrWithShardID(const Query &query, QueryResults &result) {
	LocalQueryResults lqr;
	int actualShardId = ShardingKeyType::NotSharded;
	if (shardingRouter_ && isSharderQuery(query) && !query.local_) {
		actualShardId = shardingRouter_->ActualShardId();
	}
	result.AddQr(std::move(lqr), actualShardId);
}

Error ClusterProxy::executeQueryOnShard(
	const Query &query, QueryResults &result, unsigned proxyFetchLimit, const InternalRdxContext &ctx, const RdxContext &rdxCtx,
	std::function<Error(const Query &, LocalQueryResults &, const RdxContext &)> &&localAction) noexcept {
	Error status;
	try {
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

		InternalRdxContext shardingCtx(ctx);

		if (!isDistributedQuery) {
			assert(connections.size() == 1);

			if (connections[0]) {
				shardingCtx.SetShardingParallelExecution(false);
				const auto timeout = shardingCtx.GetTimeout();
				if (timeout.count() < 0) {
					return Error(errTimeout, "Sharded request timeout");
				}

				auto connection =
					connections[0]->WithShardingParallelExecution(false).WithContext(shardingCtx.DeadlineCtx()).WithTimeout(timeout);
				client::QueryResults qrClient(result.Flags(), proxyFetchLimit, true);
				status = executeQueryOnClient(connection, query, qrClient, [](size_t, size_t) {});
				if (status.ok()) {
					result.AddQr(std::move(qrClient), connections[0].ShardId(), true);
				}

			} else {
				shardingCtx.WithShardId(shardingRouter_->ActualShardId(), false);
				LocalQueryResults lqr;

				status = localAction(query, lqr, rdxCtx);
				if (status.ok()) {
					result.AddQr(std::move(lqr), shardingRouter_->ActualShardId(), true);
				}
			}
			return status;
		} else if (query.count == UINT_MAX && query.start == 0 && query.sortingEntries_.empty()) {
			ParallelExecutor exec(shardingRouter_->ActualShardId());
			return exec.ExecSelect(query, result, connections, shardingCtx, rdxCtx, std::move(localAction));
		} else {
			unsigned limit = query.count;
			unsigned offset = query.start;

			Query distributedQuery(query);
			if (!distributedQuery.sortingEntries_.empty()) {
				const auto ns = impl_.getNamespace(distributedQuery.Namespace(), rdxCtx)->getMainNs();
				result.SetOrdering(distributedQuery, *ns, rdxCtx);
			}
			if (distributedQuery.count != UINT_MAX && !distributedQuery.sortingEntries_.empty()) {
				distributedQuery.Limit(distributedQuery.start + distributedQuery.count);
			}
			if (distributedQuery.start != 0) {
				if (distributedQuery.sortingEntries_.empty()) {
					distributedQuery.ReqTotal();
				} else {
					distributedQuery.Offset(0);
				}
			}

			shardingCtx.SetShardingParallelExecution(true);

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
					client::QueryResults qrClient(result.Flags(), proxyFetchLimit, false);

					if (distributedQuery.sortingEntries_.empty()) {
						distributedQuery.Limit(limit);
						distributedQuery.Offset(offset);
					}
					status = executeQueryOnClient(connection, distributedQuery, qrClient,
												  [&limit, &offset, this](size_t count, size_t totalCount) {
													  calculateNewLimitOfsset(count, totalCount, limit, offset);
												  });
					if (status.ok()) {
						result.AddQr(std::move(qrClient), connections[i].ShardId(), (i + 1) == connections.size());
					}
				} else {
					assertrx(i == 0);
					shardingCtx.WithShardId(shardingRouter_->ActualShardId(), connections.size() > 1);
					LocalQueryResults lqr;
					status = localAction(distributedQuery, lqr, rdxCtx);
					if (status.ok()) {
						if (distributedQuery.sortingEntries_.empty()) {
							calculateNewLimitOfsset(lqr.Count(), lqr.TotalCount(), limit, offset);
						}
						result.AddQr(std::move(lqr), shardingRouter_->ActualShardId(), (i + 1) == connections.size());
					}
				}
				if (!status.ok()) {
					if (status.code() != errStrictMode || ++strictModeErrors == connections.size()) {
						return status;
					}
				}
				if (distributedQuery.calcTotal == ModeNoTotal && limit == 0 && (i + 1) != connections.size()) {
					result.RebuildMergedData();
					break;
				}
			}
		}
	} catch (const Error &e) {
		return e;
	}
	return Error{};
}

static void printPkValue(const Item::FieldRef &f, WrSerializer &ser) {
	ser << f.Name() << " = ";
	f.operator Variant().Dump(ser);
}

static WrSerializer &printPkFields(const Item &item, WrSerializer &ser) {
	size_t jsonPathIdx = 0;
	const FieldsSet fields = item.PkFields();
	for (auto it = fields.begin(); it != fields.end(); ++it) {
		if (it != fields.begin()) ser << " AND ";
		int field = *it;
		if (field == IndexValueType::SetByJsonPath) {
			assert(jsonPathIdx < fields.getTagsPathsLength());
			printPkValue(item[fields.getJsonPath(jsonPathIdx++)], ser);
		} else {
			printPkValue(item[field], ser);
		}
	}
	return ser;
}

Error ClusterProxy::OpenNamespace(std::string_view nsName, const StorageOpts &opts, const NsReplicationOpts &replOpts,
								  const InternalRdxContext &ctx) {
	WrSerializer ser;
	const auto rdxCtx =
		ctx.CreateRdxContext(ctx.NeedTraceActivity() ? (ser << "OPEN NAMESPACE " << nsName).Slice() : ""sv, impl_.activities_);

	auto localOpen = [this, &rdxCtx](std::string_view nsName, const StorageOpts &opts, const NsReplicationOpts &replOpts) {
		std::function<Error(const RdxContext &, std::shared_ptr<client::Reindexer>, std::string_view, const StorageOpts &,
							const NsReplicationOpts &)>
			action =
				std::bind(&ClusterProxy::baseFollowerAction<decltype(&client::Reindexer::OpenNamespace), &client::Reindexer::OpenNamespace,
															std::string_view, const StorageOpts &, const NsReplicationOpts &>,
						  this, _1, _2, _3, _4, _5);
		clusterProxyLog(LogTrace, "[%d proxy] ClusterProxy::OpenNamespace", sId_.load(std::memory_order_relaxed));

		return CallProxyFunction(OpenNamespace)(rdxCtx, nsName, action, nsName, opts, replOpts);
	};

	if (isWithSharding(nsName, ctx)) {
		Error err = shardingRouter_->AwaitShards(ctx);
		if (!err.ok()) return err;

		return delegateToShards(rdxCtx, &client::Reindexer::OpenNamespace, std::move(localOpen), nsName, opts, replOpts);
	}
	return localOpen(nsName, opts, replOpts);
}

Error ClusterProxy::AddNamespace(const NamespaceDef &nsDef, const NsReplicationOpts &replOpts, const InternalRdxContext &ctx) {
	WrSerializer ser;
	const auto rdxCtx =
		ctx.CreateRdxContext(ctx.NeedTraceActivity() ? (ser << "CREATE NAMESPACE " << nsDef.name).Slice() : ""sv, impl_.activities_);

	auto localAdd = [this, &rdxCtx](const NamespaceDef &nsDef, const NsReplicationOpts &replOpts) -> Error {
		DefFunctor2(const NamespaceDef &, const NsReplicationOpts &, AddNamespace, baseFollowerAction);
		clusterProxyLog(LogTrace, "[%d proxy] ClusterProxy::AddNamespace", sId_.load(std::memory_order_relaxed));
		return CallProxyFunction(AddNamespace)(rdxCtx, nsDef.name, action, nsDef, replOpts);
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
	WrSerializer ser;
	const auto rdxCtx =
		ctx.CreateRdxContext(ctx.NeedTraceActivity() ? (ser << "CLOSE NAMESPACE " << nsName).Slice() : ""sv, impl_.activities_);

	auto localClose = [this, &rdxCtx](std::string_view nsName) -> Error {
		DefFunctor1(std::string_view, CloseNamespace, baseFollowerAction);
		clusterProxyLog(LogTrace, "[%d proxy] ClusterProxy::DropNamespace", sId_.load(std::memory_order_relaxed));
		return CallProxyFunction(CloseNamespace)(rdxCtx, nsName, action, nsName);
	};

	if (isWithSharding(nsName, ctx)) {
		Error err = shardingRouter_->AwaitShards(ctx);
		if (!err.ok()) return err;
		return delegateToShardsByNs(rdxCtx, &client::Reindexer::CloseNamespace, std::move(localClose), nsName);
	}
	return localClose(nsName);
}

Error ClusterProxy::DropNamespace(std::string_view nsName, const InternalRdxContext &ctx) {
	WrSerializer ser;
	const auto rdxCtx =
		ctx.CreateRdxContext(ctx.NeedTraceActivity() ? (ser << "DROP NAMESPACE " << nsName).Slice() : ""sv, impl_.activities_);

	auto localDrop = [this, &rdxCtx](std::string_view nsName) -> Error {
		DefFunctor1(std::string_view, DropNamespace, baseFollowerAction);
		clusterProxyLog(LogTrace, "[%d proxy] ClusterProxy::DropNamespace", sId_.load(std::memory_order_relaxed));
		return CallProxyFunction(DropNamespace)(rdxCtx, nsName, action, nsName);
	};

	if (isWithSharding(nsName, ctx)) {
		Error err = shardingRouter_->AwaitShards(ctx);
		if (!err.ok()) return err;

		return delegateToShards(rdxCtx, &client::Reindexer::DropNamespace, std::move(localDrop), nsName);
	}
	return localDrop(nsName);
}

Error ClusterProxy::TruncateNamespace(std::string_view nsName, const InternalRdxContext &ctx) {
	WrSerializer ser;
	const auto rdxCtx = ctx.CreateRdxContext(ctx.NeedTraceActivity() ? (ser << "TRUNCATE " << nsName).Slice() : ""sv, impl_.activities_);

	auto localTruncate = [this, &rdxCtx](std::string_view nsName) {
		DefFunctor1(std::string_view, TruncateNamespace, baseFollowerAction);
		clusterProxyLog(LogTrace, "[%d proxy] ClusterProxy::TruncateNamespace", sId_.load(std::memory_order_relaxed));
		return CallProxyFunction(TruncateNamespace)(rdxCtx, nsName, action, nsName);
	};
	if (isWithSharding(nsName, ctx)) {
		Error err = shardingRouter_->AwaitShards(ctx);
		if (!err.ok()) return err;

		return delegateToShards(rdxCtx, &client::Reindexer::TruncateNamespace, std::move(localTruncate), nsName);
	}
	return localTruncate(nsName);
}

Error ClusterProxy::RenameNamespace(std::string_view srcNsName, const std::string &dstNsName, const InternalRdxContext &ctx) {
	WrSerializer ser;
	const auto rdxCtx = ctx.CreateRdxContext(
		ctx.NeedTraceActivity() ? (ser << "RENAME " << srcNsName << " to " << dstNsName).Slice() : ""sv, impl_.activities_);

	auto localRename = [this, &rdxCtx](std::string_view srcNsName, const std::string &dstNsName) {
		DefFunctor2(std::string_view, const std::string &, RenameNamespace, baseFollowerAction);
		clusterProxyLog(LogTrace, "[%d proxy] ClusterProxy::RenameNamespace", sId_.load(std::memory_order_relaxed));
		return CallProxyFunction(RenameNamespace)(rdxCtx, std::string_view(), action, srcNsName, dstNsName);
	};
	if (isWithSharding(srcNsName, ctx)) {
		Error err = shardingRouter_->AwaitShards(ctx);
		if (!err.ok()) return err;

		return delegateToShards(rdxCtx, &client::Reindexer::RenameNamespace, std::move(localRename), srcNsName, dstNsName);
	}
	return localRename(srcNsName, dstNsName);
}

Error ClusterProxy::AddIndex(std::string_view nsName, const IndexDef &index, const InternalRdxContext &ctx) {
	const auto makeCtxStr = [nsName, &index](WrSerializer &ser) -> WrSerializer & {
		return ser << "CREATE INDEX " << index.name_ << " ON " << nsName;
	};
	WrSerializer ser;
	const auto rdxCtx = ctx.CreateRdxContext(ctx.NeedTraceActivity() ? makeCtxStr(ser).Slice() : ""sv, impl_.activities_);

	auto localAddIndex = [this, &rdxCtx](std::string_view nsName, const IndexDef &index) {
		DefFunctor2(std::string_view, const IndexDef &, AddIndex, baseFollowerAction);
		clusterProxyLog(LogTrace, "[%d proxy] ClusterProxy::AddIndex", sId_.load(std::memory_order_relaxed));
		return CallProxyFunction(AddIndex)(rdxCtx, nsName, action, nsName, index);
	};

	if (isWithSharding(nsName, ctx)) {
		Error err = shardingRouter_->AwaitShards(ctx);
		if (!err.ok()) return err;

		return delegateToShards(rdxCtx, &client::Reindexer::AddIndex, std::move(localAddIndex), nsName, index);
	}
	return localAddIndex(nsName, index);
}

Error ClusterProxy::UpdateIndex(std::string_view nsName, const IndexDef &index, const InternalRdxContext &ctx) {
	WrSerializer ser;
	const auto rdxCtx = ctx.CreateRdxContext(
		ctx.NeedTraceActivity() ? (ser << "UPDATE INDEX " << index.name_ << " ON " << nsName).Slice() : ""sv, impl_.activities_);

	auto localUpdateIndex = [this, &rdxCtx](std::string_view nsName, const IndexDef &index) {
		DefFunctor2(std::string_view, const IndexDef &, UpdateIndex, baseFollowerAction);
		clusterProxyLog(LogTrace, "[%d proxy] ClusterProxy::UpdateIndex", sId_.load(std::memory_order_relaxed));
		return CallProxyFunction(UpdateIndex)(rdxCtx, nsName, action, nsName, index);
	};

	if (isWithSharding(nsName, ctx)) {
		Error err = shardingRouter_->AwaitShards(ctx);
		if (!err.ok()) return err;

		return delegateToShards(rdxCtx, &client::Reindexer::UpdateIndex, std::move(localUpdateIndex), nsName, index);
	}
	return localUpdateIndex(nsName, index);
}

Error ClusterProxy::DropIndex(std::string_view nsName, const IndexDef &index, const InternalRdxContext &ctx) {
	WrSerializer ser;
	const auto rdxCtx = ctx.CreateRdxContext(
		ctx.NeedTraceActivity() ? (ser << "DROP INDEX " << index.name_ << " ON " << nsName).Slice() : ""sv, impl_.activities_);

	auto localDropIndex = [this, &rdxCtx](std::string_view nsName, const IndexDef &index) {
		DefFunctor2(std::string_view, const IndexDef &, DropIndex, baseFollowerAction);
		clusterProxyLog(LogTrace, "[%d proxy] ClusterProxy::DropIndex", sId_.load(std::memory_order_relaxed));
		return CallProxyFunction(DropIndex)(rdxCtx, nsName, action, nsName, index);
	};
	if (isWithSharding(nsName, ctx)) {
		Error err = shardingRouter_->AwaitShards(ctx);
		if (!err.ok()) return err;

		return delegateToShards(rdxCtx, &client::Reindexer::DropIndex, std::move(localDropIndex), nsName, index);
	}
	return localDropIndex(nsName, index);
}

Error ClusterProxy::SetSchema(std::string_view nsName, std::string_view schema, const InternalRdxContext &ctx) {
	WrSerializer ser;
	const auto rdxCtx =
		ctx.CreateRdxContext(ctx.NeedTraceActivity() ? (ser << "SET SCHEMA ON " << nsName).Slice() : ""sv, impl_.activities_);

	auto localSetSchema = [this, &rdxCtx](std::string_view nsName, std::string_view schema) {
		DefFunctor2(std::string_view, std::string_view, SetSchema, baseFollowerAction);
		clusterProxyLog(LogTrace, "[%d proxy] ClusterProxy::SetSchema", sId_.load(std::memory_order_relaxed));
		return CallProxyFunction(SetSchema)(rdxCtx, nsName, action, nsName, schema);
	};
	if (isWithSharding(nsName, ctx)) {
		Error err = shardingRouter_->AwaitShards(ctx);
		if (!err.ok()) return err;

		return delegateToShards(rdxCtx, &client::Reindexer::SetSchema, std::move(localSetSchema), nsName, schema);
	}
	return localSetSchema(nsName, schema);
}

Error ClusterProxy::GetSchema(std::string_view nsName, int format, std::string &schema, const InternalRdxContext &ctx) {
	WrSerializer ser;
	const auto rdxCtx =
		ctx.CreateRdxContext(ctx.NeedTraceActivity() ? (ser << "GET SCHEMA ON " << nsName).Slice() : ""sv, impl_.activities_);
	return impl_.GetSchema(nsName, format, schema, rdxCtx);
}

Error ClusterProxy::EnumNamespaces(std::vector<NamespaceDef> &defs, EnumNamespacesOpts opts, const InternalRdxContext &ctx) {
	const auto rdxCtx = ctx.CreateRdxContext("SELECT NAMESPACES", impl_.activities_);
	return impl_.EnumNamespaces(defs, opts, rdxCtx);
}

Error ClusterProxy::Insert(std::string_view nsName, Item &item, const InternalRdxContext &ctx) {
	WrSerializer ser;
	const auto rdxCtx = ctx.CreateRdxContext(ctx.NeedTraceActivity() ? (ser << "INSERT INTO " << nsName).Slice() : ""sv, impl_.activities_);

	auto insertFn = [this, &rdxCtx](std::string_view nsName, Item &item) -> Error {
		std::function<Error(const RdxContext &, std::shared_ptr<client::Reindexer>, std::string_view, Item &)> action = std::bind(
			&ClusterProxy::itemFollowerAction<Error (client::Reindexer::*)(std::string_view, client::Item &), &client::Reindexer::Insert>,
			this, _1, _2, _3, _4);

		return proxyCall<Error (ReindexerImpl::*)(std::string_view, Item &, const RdxContext &), &ReindexerImpl::Insert,
						 Error (client::Reindexer::*)(std::string_view, client::Item &), &client::Reindexer::Insert, Error>(
			rdxCtx, nsName, action, nsName, item);
	};

	if (isWithSharding(nsName, ctx)) {
		return modifyItemOnShard<&client::Reindexer::Insert>(ctx, rdxCtx, nsName, item, std::move(insertFn));
	}
	return insertFn(nsName, item);
}

Error ClusterProxy::Insert(std::string_view nsName, Item &item, QueryResults &result, const InternalRdxContext &ctx) {
	WrSerializer ser;
	const auto rdxCtx = ctx.CreateRdxContext(ctx.NeedTraceActivity() ? (ser << "INSERT INTO " << nsName).Slice() : ""sv, impl_.activities_);

	auto insertFn = [this, &rdxCtx](std::string_view nsName, Item &item, LocalQueryResults &result) -> Error {
		std::function<Error(const RdxContext &, std::shared_ptr<client::Reindexer>, std::string_view, Item &, LocalQueryResults &)> action =
			std::bind(
				&ClusterProxy::resultItemFollowerAction<
					Error (client::Reindexer::*)(std::string_view, client::Item &, client::QueryResults &), &client::Reindexer::Insert>,
				this, _1, _2, _3, _4, _5);

		return proxyCall<Error (ReindexerImpl::*)(std::string_view, Item &, LocalQueryResults &, const RdxContext &),
						 &ReindexerImpl::Insert, Error (client::Reindexer::*)(std::string_view, client::Item &, client::QueryResults &),
						 &client::Reindexer::Insert, Error>(rdxCtx, nsName, action, nsName, item, result);
	};

	result.SetQuery(nullptr);
	if (isWithSharding(nsName, ctx)) {
		return modifyItemOnShard<&client::Reindexer::Insert>(ctx, rdxCtx, nsName, item, result, std::move(insertFn));
	}

	try {
		return insertFn(nsName, item, result.ToLocalQr(true));
	} catch (Error &e) {
		return e;
	}
}

Error ClusterProxy::Update(std::string_view nsName, Item &item, const InternalRdxContext &ctx) {
	WrSerializer ser;
	ser << "UPDATE " << nsName << " WHERE ";
	printPkFields(item, ser);

	const auto rdxCtx = ctx.CreateRdxContext(ctx.NeedTraceActivity() ? ser.Slice() : ""sv, impl_.activities_);

	auto updateFn = [this, &rdxCtx](std::string_view nsName, Item &item) -> Error {
		std::function<Error(const RdxContext &, std::shared_ptr<client::Reindexer>, std::string_view, Item &)> action = std::bind(
			&ClusterProxy::itemFollowerAction<Error (client::Reindexer::*)(std::string_view, client::Item &), &client::Reindexer::Update>,
			this, _1, _2, _3, _4);
		return proxyCall<Error (ReindexerImpl::*)(std::string_view, Item &, const RdxContext &), &ReindexerImpl::Update,
						 Error (client::Reindexer::*)(std::string_view, client::Item &), &client::Reindexer::Update, Error>(
			rdxCtx, nsName, action, nsName, item);
	};

	if (isWithSharding(nsName, ctx)) {
		return modifyItemOnShard<&client::Reindexer::Update>(ctx, rdxCtx, nsName, item, std::move(updateFn));
	}
	return updateFn(nsName, item);
}

Error ClusterProxy::Update(std::string_view nsName, Item &item, QueryResults &result, const InternalRdxContext &ctx) {
	WrSerializer ser;
	ser << "UPDATE " << nsName << " WHERE ";
	printPkFields(item, ser);

	const auto rdxCtx = ctx.CreateRdxContext(ctx.NeedTraceActivity() ? ser.Slice() : ""sv, impl_.activities_);

	auto updateFn = [this, &rdxCtx](std::string_view nsName, Item &item, LocalQueryResults &result) -> Error {
		std::function<Error(const RdxContext &, std::shared_ptr<client::Reindexer>, std::string_view, Item &, LocalQueryResults &)> action =
			std::bind(
				&ClusterProxy::resultItemFollowerAction<
					Error (client::Reindexer::*)(std::string_view, client::Item &, client::QueryResults &), &client::Reindexer::Update>,
				this, _1, _2, _3, _4, _5);

		return proxyCall<Error (ReindexerImpl::*)(std::string_view, Item &, LocalQueryResults &, const RdxContext &),
						 &ReindexerImpl::Update, Error (client::Reindexer::*)(std::string_view, client::Item &, client::QueryResults &),
						 &client::Reindexer::Update, Error>(rdxCtx, nsName, action, nsName, item, result);
	};

	result.SetQuery(nullptr);
	if (isWithSharding(nsName, ctx)) {
		return modifyItemOnShard<&client::Reindexer::Update>(ctx, rdxCtx, nsName, item, result, std::move(updateFn));
	}

	try {
		return updateFn(nsName, item, result.ToLocalQr(true));
	} catch (Error &e) {
		return e;
	}
}

Error ClusterProxy::Update(const Query &query, QueryResults &result, const InternalRdxContext &ctx) {
	WrSerializer ser;

	const auto rdxCtx = ctx.CreateRdxContext(ctx.NeedTraceActivity() ? query.GetSQL(ser).Slice() : ""sv, impl_.activities_, result);

	std::function<Error(const Query &, LocalQueryResults &, const RdxContext &ctx)> updateFn = [this](const Query &q, LocalQueryResults &qr,
																									  const RdxContext &rdxCtx) -> Error {
		std::function<Error(const RdxContext &, std::shared_ptr<client::Reindexer>, const Query &, LocalQueryResults &)> action =
			std::bind(&ClusterProxy::resultFollowerAction<Error (client::Reindexer::*)(const Query &, client::QueryResults &),
														  &client::Reindexer::Update>,
					  this, _1, _2, _3, _4);
		clusterProxyLog(LogTrace, "[%d proxy] ClusterProxy::Update query", sId_);

		return proxyCall<Error (ReindexerImpl::*)(const Query &, LocalQueryResults &, const RdxContext &), &ReindexerImpl::Update,
						 Error (client::Reindexer::*)(const Query &, client::QueryResults &), &client::Reindexer::Update, Error>(
			rdxCtx, q._namespace, action, q, qr);
	};

	result.SetQuery(&query);
	try {
		if (isWithSharding(query, ctx)) {
			return executeQueryOnShard(query, result, 0, ctx, rdxCtx, std::move(updateFn));
		}
		addEmptyLocalQrWithShardID(query, result);
		return updateFn(query, result.ToLocalQr(false), rdxCtx);
	} catch (Error &e) {
		return e;
	}
}

Error ClusterProxy::Upsert(std::string_view nsName, Item &item, QueryResults &result, const InternalRdxContext &ctx) {
	WrSerializer ser;
	ser << "UPSERT INTO " << nsName << " WHERE ";
	printPkFields(item, ser);
	const auto rdxCtx = ctx.CreateRdxContext(ctx.NeedTraceActivity() ? ser.Slice() : ""sv, impl_.activities_);

	auto upsertFn = [this, &rdxCtx](std::string_view nsName, Item &item, LocalQueryResults &result) -> Error {
		std::function<Error(const RdxContext &, std::shared_ptr<client::Reindexer>, std::string_view, Item &, LocalQueryResults &)> action =
			std::bind(
				&ClusterProxy::resultItemFollowerAction<
					Error (client::Reindexer::*)(std::string_view, client::Item &, client::QueryResults &), &client::Reindexer::Upsert>,
				this, _1, _2, _3, _4, _5);

		return proxyCall<Error (ReindexerImpl::*)(std::string_view, Item &, LocalQueryResults &, const RdxContext &),
						 &ReindexerImpl::Upsert, Error (client::Reindexer::*)(std::string_view, client::Item &, client::QueryResults &),
						 &client::Reindexer::Upsert, Error>(rdxCtx, nsName, action, nsName, item, result);
	};

	result.SetQuery(nullptr);
	if (isWithSharding(nsName, ctx)) {
		return modifyItemOnShard<&client::Reindexer::Upsert>(ctx, rdxCtx, nsName, item, result, std::move(upsertFn));
	}

	try {
		return upsertFn(nsName, item, result.ToLocalQr(true));
	} catch (Error &e) {
		return e;
	}
}

Error ClusterProxy::Upsert(std::string_view nsName, Item &item, const InternalRdxContext &ctx) {
	WrSerializer ser;
	ser << "UPSERT INTO " << nsName << " WHERE ";
	printPkFields(item, ser);
	const auto rdxCtx = ctx.CreateRdxContext(ctx.NeedTraceActivity() ? ser.Slice() : ""sv, impl_.activities_);

	auto upsertFn = [this, &rdxCtx](std::string_view nsName, Item &item) -> Error {
		std::function<Error(const RdxContext &, std::shared_ptr<client::Reindexer>, std::string_view, Item &)> action = std::bind(
			&ClusterProxy::itemFollowerAction<Error (client::Reindexer::*)(std::string_view, client::Item &), &client::Reindexer::Upsert>,
			this, _1, _2, _3, _4);
		return proxyCall<Error (ReindexerImpl::*)(std::string_view, Item &, const RdxContext &), &ReindexerImpl::Upsert,
						 Error (client::Reindexer::*)(std::string_view, client::Item &), &client::Reindexer::Upsert, Error>(
			rdxCtx, nsName, action, nsName, item);
	};

	if (isWithSharding(nsName, ctx)) {
		return modifyItemOnShard<&client::Reindexer::Upsert>(ctx, rdxCtx, nsName, item, std::move(upsertFn));
	}
	return upsertFn(nsName, item);
}

Error ClusterProxy::Delete(std::string_view nsName, Item &item, const InternalRdxContext &ctx) {
	WrSerializer ser;
	ser << "DELETE FROM " << nsName << " WHERE ";
	printPkFields(item, ser);

	const auto rdxCtx = ctx.CreateRdxContext(ctx.NeedTraceActivity() ? ser.Slice() : ""sv, impl_.activities_);

	auto deleteFn = [this, &rdxCtx](std::string_view nsName, Item &item) -> Error {
		std::function<Error(const RdxContext &, std::shared_ptr<client::Reindexer>, std::string_view, Item &)> action = std::bind(
			&ClusterProxy::itemFollowerAction<Error (client::Reindexer::*)(std::string_view, client::Item &), &client::Reindexer::Delete>,
			this, _1, _2, _3, _4);
		clusterProxyLog(LogTrace, "[%d proxy] ClusterProxy::Delete ITEM", sId_.load(std::memory_order_relaxed));
		return proxyCall<Error (ReindexerImpl::*)(std::string_view, Item &, const RdxContext &), &ReindexerImpl::Delete,
						 Error (client::Reindexer::*)(std::string_view, client::Item &), &client::Reindexer::Delete, Error>(
			rdxCtx, nsName, action, nsName, item);
	};

	if (isWithSharding(nsName, ctx)) {
		return modifyItemOnShard<&client::Reindexer::Delete>(ctx, rdxCtx, nsName, item, std::move(deleteFn));
	}
	return deleteFn(nsName, item);
}

Error ClusterProxy::Delete(std::string_view nsName, Item &item, QueryResults &result, const InternalRdxContext &ctx) {
	WrSerializer ser;
	ser << "DELETE FROM " << nsName << " WHERE ";
	printPkFields(item, ser);

	const auto rdxCtx = ctx.CreateRdxContext(ctx.NeedTraceActivity() ? ser.Slice() : ""sv, impl_.activities_);

	auto deleteFn = [this, &rdxCtx](std::string_view nsName, Item &item, LocalQueryResults &result) -> Error {
		std::function<Error(const RdxContext &, std::shared_ptr<client::Reindexer>, std::string_view, Item &, LocalQueryResults &)> action =
			std::bind(
				&ClusterProxy::resultItemFollowerAction<
					Error (client::Reindexer::*)(std::string_view, client::Item &, client::QueryResults &), &client::Reindexer::Delete>,
				this, _1, _2, _3, _4, _5);

		return proxyCall<Error (ReindexerImpl::*)(std::string_view, Item &, LocalQueryResults &, const RdxContext &),
						 &ReindexerImpl::Delete, Error (client::Reindexer::*)(std::string_view, client::Item &, client::QueryResults &),
						 &client::Reindexer::Delete, Error>(rdxCtx, nsName, action, nsName, item, result);
	};

	result.SetQuery(nullptr);
	if (isWithSharding(nsName, ctx)) {
		return modifyItemOnShard<&client::Reindexer::Delete>(ctx, rdxCtx, nsName, item, result, std::move(deleteFn));
	}

	try {
		return deleteFn(nsName, item, result.ToLocalQr(true));
	} catch (Error &e) {
		return e;
	}
}

Error ClusterProxy::Delete(const Query &query, QueryResults &result, const InternalRdxContext &ctx) {
	WrSerializer ser;
	const auto rdxCtx = ctx.CreateRdxContext(ctx.NeedTraceActivity() ? (query.GetSQL(ser)).Slice() : ""sv, impl_.activities_);

	std::function<Error(const Query &, LocalQueryResults &, const RdxContext &ctx)> deleteFn = [this](const Query &q, LocalQueryResults &qr,
																									  const RdxContext &rdxCtx) -> Error {
		std::function<Error(const RdxContext &, std::shared_ptr<client::Reindexer>, const Query &, LocalQueryResults &)> action =
			std::bind(&ClusterProxy::resultFollowerAction<Error (client::Reindexer::*)(const Query &, client::QueryResults &),
														  &client::Reindexer::Delete>,
					  this, _1, _2, _3, _4);
		clusterProxyLog(LogTrace, "[%d proxy] ClusterProxy::Delete QUERY", sId_);
		return proxyCall<Error (ReindexerImpl::*)(const Query &, LocalQueryResults &, const RdxContext &), &ReindexerImpl::Delete,
						 Error (client::Reindexer::*)(const Query &, client::QueryResults &), &client::Reindexer::Delete, Error>(
			rdxCtx, q._namespace, action, q, qr);
	};

	result.SetQuery(&query);
	try {
		if (isWithSharding(query, ctx)) {
			return executeQueryOnShard(query, result, 0, ctx, rdxCtx, std::move(deleteFn));
		}
		addEmptyLocalQrWithShardID(query, result);
		return deleteFn(query, result.ToLocalQr(false), rdxCtx);
	} catch (Error &e) {
		return e;
	}
}

Error ClusterProxy::Select(std::string_view sql, QueryResults &result, unsigned proxyFetchLimit, const InternalRdxContext &ctx) {
	Query query;
	try {
		query.FromSQL(sql);
	} catch (const Error &err) {
		return err;
	}
	switch (query.type_) {
		case QuerySelect: {
			return Select(query, result, proxyFetchLimit, ctx);
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

Error ClusterProxy::Select(const Query &query, QueryResults &result, unsigned proxyFetchLimit, const InternalRdxContext &ctx) {
	if (query.Type() != QuerySelect) {
		return Error(errLogic, "'Select' call request type is not equal to 'QuerySelect'.");
	}

	WrSerializer ser;
	if (ctx.NeedTraceActivity()) query.GetSQL(ser, false);

	result.SetQuery(&query);
	try {
		if (isWithSharding(query, ctx)) {
			const auto rdxCtx = ctx.CreateRdxContext(ctx.NeedTraceActivity() ? ser.Slice() : "", impl_.activities_, result);
			std::function<Error(const Query &, LocalQueryResults &, const RdxContext &rdxCtx)> selectFn =
				[this](const Query &q, LocalQueryResults &qr, const RdxContext &rdxCtx) -> Error { return impl_.Select(q, qr, rdxCtx); };
			return executeQueryOnShard(query, result, proxyFetchLimit, ctx, rdxCtx, std::move(selectFn));
		}
		if (!shouldProxyQuery(query)) {
			const auto rdxCtx = ctx.CreateRdxContext(ctx.NeedTraceActivity() ? ser.Slice() : "", impl_.activities_, result);
			clusterProxyLog(LogTrace, "[%d proxy] ClusterProxy::Select query local", sId_.load(std::memory_order_relaxed));
			addEmptyLocalQrWithShardID(query, result);
			return impl_.Select(query, result.ToLocalQr(false), rdxCtx);
		}
	} catch (const Error &ex) {
		return ex;
	}
	const InternalRdxContext deadlineCtx(ctx.HasDeadline() ? ctx : ctx.WithTimeout(kReplicationStatsTimeout));
	const auto rdxCtxDeadLine = deadlineCtx.CreateRdxContext(deadlineCtx.NeedTraceActivity() ? ser.Slice() : "", impl_.activities_, result);

	std::function<Error(const RdxContext &, std::shared_ptr<client::Reindexer>, const Query &, LocalQueryResults &)> action =
		std::bind(&ClusterProxy::resultFollowerAction<Error (client::Reindexer::*)(const Query &, client::QueryResults &),
													  &client::Reindexer::Select>,
				  this, _1, _2, _3, _4);
	clusterProxyLog(LogTrace, "[%d proxy] ClusterProxy::Select query proxied", sId_.load(std::memory_order_relaxed));
	try {
		return proxyCall<Error (ReindexerImpl::*)(const Query &, LocalQueryResults &, const RdxContext &), &ReindexerImpl::Select,
						 Error (client::Reindexer::*)(const Query &, client::QueryResults &), &client::Reindexer::Select, Error>(
			rdxCtxDeadLine, query._namespace, action, query, result.ToLocalQr(true));
	} catch (Error &e) {
		return e;
	}
}

Error ClusterProxy::Commit(std::string_view nsName, const InternalRdxContext &ctx) {
	auto localCommit = [this](std::string_view nsName) -> Error { return impl_.Commit(nsName); };
	if (isWithSharding(nsName, ctx)) {
		Error err = shardingRouter_->AwaitShards(ctx);
		if (!err.ok()) return err;
		WrSerializer ser;
		const auto rdxCtx = ctx.CreateRdxContext(ctx.NeedTraceActivity() ? (ser << "COMMIT " << nsName).Slice() : ""sv, impl_.activities_);
		return delegateToShardsByNs(rdxCtx, &client::Reindexer::Commit, std::move(localCommit), nsName);
	}
	return localCommit(nsName);
}

Item ClusterProxy::NewItem(std::string_view nsName, const InternalRdxContext &ctx) {
	WrSerializer ser;
	const auto rdxCtx =
		ctx.CreateRdxContext(ctx.NeedTraceActivity() ? (ser << "CREATE ITEM FOR " << nsName).Slice() : ""sv, impl_.activities_);

	return impl_.NewItem(nsName, rdxCtx);
}

Transaction ClusterProxy::NewTransaction(std::string_view nsName, const InternalRdxContext &ctx) {
	const RdxContext rdxCtx = ctx.CreateRdxContext("START TRANSACTION"sv, impl_.activities_);

	const bool withSharding = isWithSharding(nsName, ctx);

	std::function<Transaction(const RdxContext &, std::shared_ptr<client::Reindexer>, std::string_view)> action =
		std::bind(&ClusterProxy::newTxFollowerAction, this, withSharding, _1, _2, _3);

	clusterProxyLog(LogTrace, "[%d proxy] ClusterProxy::NewTransaction", sId_.load(std::memory_order_relaxed));

	if (withSharding) {
		return Transaction(
			proxyCall<LocalTransaction (ReindexerImpl::*)(std::string_view, const RdxContext &), &ReindexerImpl::NewTransaction,
					  client::Transaction (client::Reindexer::*)(std::string_view), &client::Reindexer::NewTransaction, Transaction>(
				rdxCtx, nsName, action, nsName),
			shardingRouter_);
	}
	return proxyCall<LocalTransaction (ReindexerImpl::*)(std::string_view, const RdxContext &), &ReindexerImpl::NewTransaction,
					 client::Transaction (client::Reindexer::*)(std::string_view), &client::Reindexer::NewTransaction, Transaction>(
		rdxCtx, nsName, action, nsName);
}

Error ClusterProxy::CommitTransaction(Transaction &tr, QueryResults &result, const InternalRdxContext &ctx) {
	if (!tr.Status().ok()) {
		return Error(tr.Status().code(), "Unable to commit tx with error status: '%s'", tr.Status().what());
	}

	WrSerializer ser;
	const RdxContext rdxCtx = ctx.CreateRdxContext(
		ctx.NeedTraceActivity() ? (ser << "COMMIT TRANSACTION "sv << tr.GetNsName()).Slice() : ""sv, impl_.activities_);

	result.SetQuery(nullptr);
	const bool withSharding = isWithSharding(tr.GetNsName(), ctx);
	return tr.commit(sId_.load(), withSharding, impl_, result, rdxCtx);
}

Error ClusterProxy::RollBackTransaction(Transaction &tr, const InternalRdxContext &ctx) {
	WrSerializer ser;
	const RdxContext rdxCtx = ctx.CreateRdxContext(
		ctx.NeedTraceActivity() ? (ser << "ROLLBACK TRANSACTION "sv << tr.GetNsName()).Slice() : ""sv, impl_.activities_);
	return tr.rollback(sId_, rdxCtx);
}

Error ClusterProxy::GetMeta(std::string_view nsName, const std::string &key, std::string &data, const InternalRdxContext &ctx) {
	WrSerializer ser;
	const RdxContext rdxCtx = ctx.CreateRdxContext(
		ctx.NeedTraceActivity() ? (ser << "SELECT META FROM " << nsName << " WHERE KEY = '" << key << '\'').Slice() : ""sv,
		impl_.activities_);

	return impl_.GetMeta(nsName, key, data, rdxCtx);
}

Error ClusterProxy::GetMeta(std::string_view nsName, const std::string &key, std::vector<ShardedMeta> &data,
							const InternalRdxContext &ctx) {
	WrSerializer ser;
	const RdxContext rdxCtx = ctx.CreateRdxContext(
		ctx.NeedTraceActivity() ? (ser << "SELECT SHARDED_META FROM " << nsName << " WHERE KEY = '" << key << '\'').Slice() : ""sv,
		impl_.activities_);

	auto localGetMeta = [this, &rdxCtx](std::string_view nsName, const std::string &key, std::vector<ShardedMeta> &data, int shardId) {
		data.emplace_back();
		data.back().shardId = shardId;
		return impl_.GetMeta(nsName, key, data.back().data, rdxCtx);
	};

	if (isWithSharding(nsName, ctx)) {
		auto predicate = [](const ShardedMeta &) noexcept { return true; };
		Error err = collectFromShardsByNs(
			rdxCtx,
			static_cast<Error (client::Reindexer::*)(std::string_view, const std::string &, std::vector<ShardedMeta> &)>(
				&client::Reindexer::GetMeta),
			std::move(localGetMeta), data, predicate, nsName, key);
		return err;
	}

	return localGetMeta(nsName, key, data, ctx.ShardId());
}

Error ClusterProxy::PutMeta(std::string_view nsName, const std::string &key, std::string_view data, const InternalRdxContext &ctx) {
	WrSerializer ser;
	const RdxContext rdxCtx = ctx.CreateRdxContext(
		ctx.NeedTraceActivity() ? (ser << "UPDATE " << nsName << " SET META = '" << data << "' WHERE KEY = '" << key << '\'').Slice()
								: ""sv,
		impl_.activities_);

	auto localPutMeta = [this, &rdxCtx](std::string_view nsName, const std::string &key, std::string_view data) {
		std::function<Error(const RdxContext &, std::shared_ptr<client::Reindexer>, std::string_view, const std::string &,
							std::string_view)>
			action = std::bind(&ClusterProxy::baseFollowerAction<decltype(&client::Reindexer::PutMeta), &client::Reindexer::PutMeta,
																 std::string_view, const std::string &, std::string_view>,
							   this, _1, _2, _3, _4, _5);
		clusterProxyLog(LogTrace, "[%d proxy] ClusterProxy::PutMeta", sId_.load(std::memory_order_relaxed));
		return CallProxyFunction(PutMeta)(rdxCtx, nsName, action, nsName, key, data);
	};

	if (isWithSharding(nsName, ctx)) {
		return delegateToShards(rdxCtx, &client::Reindexer::PutMeta, std::move(localPutMeta), nsName, key, data);
	}
	return localPutMeta(nsName, key, data);
}

Error ClusterProxy::EnumMeta(std::string_view nsName, std::vector<std::string> &keys, const InternalRdxContext &ctx) {
	WrSerializer ser;
	const RdxContext rdxCtx =
		ctx.CreateRdxContext(ctx.NeedTraceActivity() ? (ser << "SELECT META FROM " << nsName).Slice() : ""sv, impl_.activities_);

	auto localEnumMeta = [this, &rdxCtx](std::string_view nsName, std::vector<std::string> &keys, [[maybe_unused]] int shardId) {
		return impl_.EnumMeta(nsName, keys, rdxCtx);
	};

	if (isWithSharding(nsName, ctx)) {
		fast_hash_set<std::string> allKeys;
		Error err = collectFromShardsByNs(
			rdxCtx, &client::Reindexer::EnumMeta, std::move(localEnumMeta), keys,
			[&allKeys](const std::string &key) { return allKeys.emplace(key).second; }, nsName);

		return err;
	}
	return localEnumMeta(nsName, keys, 0 /*not used*/);
}

Error ClusterProxy::EnableStorage(const std::string &storagePath, bool skipPlaceholderCheck, const InternalRdxContext &ctx) {
	WrSerializer ser;
	const auto rdxCtx =
		ctx.CreateRdxContext(ctx.NeedTraceActivity() ? (ser << "ENABLE STORAGE " << storagePath).Slice() : ""sv, impl_.activities_);

	return impl_.EnableStorage(storagePath, skipPlaceholderCheck, rdxCtx);
}

Error ClusterProxy::GetSnapshot(std::string_view nsName, const SnapshotOpts &opts, Snapshot &snapshot, const InternalRdxContext &ctx) {
	WrSerializer ser;
	const auto rdxCtx =
		ctx.CreateRdxContext(ctx.NeedTraceActivity() ? (ser << "APPLY SNAPSHOT RECORD ON "sv << nsName).Slice() : ""sv, impl_.activities_);

	return impl_.GetSnapshot(nsName, opts, snapshot, rdxCtx);
}

Error ClusterProxy::ApplySnapshotChunk(std::string_view nsName, const SnapshotChunk &ch, const InternalRdxContext &ctx) {
	WrSerializer ser;
	const auto rdxCtx =
		ctx.CreateRdxContext(ctx.NeedTraceActivity() ? (ser << "APPLY SNAPSHOT RECORD ON "sv << nsName).Slice() : ""sv, impl_.activities_);

	return impl_.ApplySnapshotChunk(nsName, ch, rdxCtx);
}

Error ClusterProxy::GetRaftInfo(cluster::RaftInfo &info, const InternalRdxContext &ctx) {
	const RdxContext rdxCtx = ctx.CreateRdxContext(""sv, impl_.activities_);
	return impl_.GetRaftInfo(true, info, rdxCtx);
}

Error ClusterProxy::CreateTemporaryNamespace(std::string_view baseName, std::string &resultName, const StorageOpts &opts, lsn_t nsVersion,
											 const InternalRdxContext &ctx) {
	WrSerializer ser;
	resultName = impl_.generateTemporaryNamespaceName(baseName);
	const auto rdxCtx =
		ctx.CreateRdxContext(ctx.NeedTraceActivity() ? (ser << "CREATE NAMESPACE " << resultName).Slice() : ""sv, impl_.activities_);
	return impl_.CreateTemporaryNamespace(baseName, resultName, opts, nsVersion, rdxCtx);
}

bool ClusterProxy::isSharderQuery(const Query &q) const {
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
	return isSharderQuery(q);
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
			if (hasTypeCond || entry.condition != CondEq || entry.values.size() != 1 ||
				!entry.values[0].Type().Is<KeyValueType::String>() || it->operation != OpAnd ||
				(nextIt != end && nextIt->operation == OpOr)) {
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
