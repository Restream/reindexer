#include "clusterproxy.h"
#include <shared_mutex>
#include "client/itemimpl.h"
#include "client/synccororeindexerimpl.h"
#include "client/transaction.h"
#include "cluster/clusterizator.h"
#include "core/defnsconfigs.h"
#include "core/queryresults/joinresults.h"
#include "core/reindexerimpl.h"
#include "core/type_consts.h"
#include "tools/logger.h"

#ifdef WITH_SHARDING
#include "cluster/sharding/sharding.h"
#endif	// WITH_SHARDING

using namespace std::placeholders;
using namespace std::string_view_literals;

namespace reindexer {

constexpr auto kReplicationStatsTimeout = std::chrono::seconds(10);

reindexer::client::Item ClusterProxy::toClientItem(std::string_view ns, client::SyncCoroReindexer *connection, reindexer::Item &item) {
	assert(connection);
	Error err;
	reindexer::client::Item clientItem = connection->NewItem(ns);
	if (clientItem.Status().ok()) {
		err = clientItem.FromCJSON(item.GetCJSON(true));
		if (err.ok() && clientItem.Status().ok()) {
			clientItem.SetPrecepts(item.impl_->GetPrecepts());
			return clientItem;
		}
	}
	if (!err.ok()) throw err;
	throw clientItem.Status();
}

void ClusterProxy::copyJoinedData(const Query &query, const client::SyncCoroQueryResults::Iterator &it,
								  const client::SyncCoroQueryResults &clientResults, QueryResults &result) {
	const auto &joinedData = it.GetJoined();
	if (joinedData.size() > 0) {
		if (it.itemParams_.nsid >= (int)result.joined_.size()) {
			result.joined_.resize(it.itemParams_.nsid + 1);
		}
		joins::NamespaceResults &nsJoinRes = result.joined_[it.itemParams_.nsid];
		nsJoinRes.SetJoinedSelectorsCount(query.joinQueries_.size());
		int contextIndex = 0;
		for (int ns = 0; ns < it.itemParams_.nsid; ++ns) {
			if (ns == 0) {
				contextIndex += query.joinQueries_.size();
			} else {
				size_t mergeQueryIndex = ns - 1;
				assert(mergeQueryIndex < query.mergeQueries_.size());
				contextIndex += query.mergeQueries_[mergeQueryIndex].joinQueries_.size();
			}
		}
		for (size_t joinedField = 0; joinedField < joinedData.size(); ++joinedField, ++contextIndex) {
			QueryResults qrJoined;
			const auto &joinedItems = joinedData[joinedField];
			for (const auto &itemData : joinedItems) {
				ItemImpl itemimpl = ItemImpl(clientResults.GetPayloadType(contextIndex), clientResults.GetTagsMatcher(contextIndex));
				Error err = itemimpl.FromCJSON(itemData.data);
				if (!err.ok()) throw err;

				qrJoined.Add(ItemRef(itemData.id, itemimpl.Value(), itemData.proc, itemData.nsid, true));
				result.SaveRawData(std::move(itemimpl));
			}
			nsJoinRes.Insert(it.itemParams_.id, joinedField, std::move(qrJoined));
		}
	}
}

void ClusterProxy::clientToCoreQueryResults(client::SyncCoroQueryResults &clientResults, QueryResults &result, bool createTm,
											const std::unordered_set<int32_t> &stateTokenList) {
	clientToCoreQueryResultsInternal(clientResults, result, createTm, stateTokenList);
}
void ClusterProxy::clientToCoreQueryResults(const Query &query, client::SyncCoroQueryResults &clientResults, QueryResults &results,
											bool createTm, const std::unordered_set<int32_t> &stateTokenList) {
	clientToCoreQueryResultsInternal(
		clientResults, results, createTm, stateTokenList,
		[this, &query](const client::SyncCoroQueryResults::Iterator &it, const client::SyncCoroQueryResults &clientResults,
					   QueryResults &results) { copyJoinedData(query, it, clientResults, results); });
}

void ClusterProxy::clientToCoreQueryResultsInternal(client::SyncCoroQueryResults &clientResults, QueryResults &result, bool createTm,
													const std::unordered_set<int32_t> &stateTokenList,
													CopyJoinResultsType copyJoinResults) {
	auto checkInList = [&stateTokenList](int32_t stateToken) -> bool { return stateTokenList.find(stateToken) != stateTokenList.end(); };

	if (result.getMergedNSCount() == 0) {
		for (int i = 0; i < clientResults.GetMergedNSCount(); ++i) {
			result.addNSContext(clientResults.GetPayloadType(i), clientResults.GetTagsMatcher(i), FieldsSet(), nullptr);
			createTm = false;
		}
	}
	if (!createTm && stateTokenList.size() > 1 && checkInList(result.getTagsMatcher(0).stateToken())) {
		createTm = true;
	}

	if (createTm) {
		assert(result.getMergedNSCount() == 1);
		WrSerializer wrser;
		result.getTagsMatcher(0).serialize(wrser);
		Serializer ser(wrser.Slice());
		TagsMatcher tm;
		while (checkInList(tm.stateToken())) {
			tm = TagsMatcher{};
		}
		tm.deserialize(ser);
		result.getTagsMatcher(0) = tm;
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

		result.Add(ItemRef(it.itemParams_.id, itemimpl.Value(), it.itemParams_.proc, it.itemParams_.nsid, true));
		result.SaveRawData(std::move(itemimpl));
		if (copyJoinResults) {
			copyJoinResults(it, clientResults, result);
		}
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
			err = item.FromCJSON(clientItem.GetCJSON());
			item.setID(clientItem.GetID());
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
											 std::string_view nsName, Item &item, QueryResults &result) {
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
		if (clientItem.GetID() != -1) {
			item.setID(clientItem.GetID());
		}
		clientToCoreQueryResults(clientResults, result, false, std::unordered_set<int32_t>());
		return err;
	} catch (const Error &err) {
		return err;
	}
}

template <typename FnL, FnL fnl>
Error ClusterProxy::resultFollowerAction(const InternalRdxContext &ctx, std::shared_ptr<client::SyncCoroReindexer> clientToLeader,
										 const Query &query, QueryResults &result) {
	try {
		Error err;
		client::SyncCoroReindexer l = clientToLeader->WithLSN(ctx.LSN()).WithEmmiterServerId(sId_);
		client::SyncCoroQueryResults clientResults;
		err = (l.*fnl)(query, clientResults);
		if (!err.ok()) {
			return err;
		}

		clientToCoreQueryResults(query, clientResults, result, false, std::unordered_set<int32_t>());
		return err;
	} catch (const Error &err) {
		return err;
	}
}

void ClusterProxy::ShutdownCluster() {
	impl_.ShutdownCluster();
	clusterConns_.Shutdown();
	resetLeader();
#ifdef WITH_SHARDING
	if (shardingRouter_) {
		shardingRouter_->Shutdown();
	}
#endif	// WITH_SHARDING
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
			assert(tx.clientTransaction_->rx_);
			client::SyncCoroReindexerImpl *client = tx.clientTransaction_->rx_.get();
			client::InternalRdxContext clientCtx =
				client::InternalRdxContext{}.WithLSN(ctx.LSN()).WithEmmiterServerId(sId_).WithShardingParallelExecution(
					ctx.IsShardingParallelExecution());
			err = client->RollBackTransaction(*tx.clientTransaction_.get(), clientCtx);
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
				logPrintf(LogTrace, "[%d proxy]  proxyCall ns not in cluster config (local)", sId_);
				r = (impl_.*fn)(std::forward<Args>(args)..., _ctx);
				errCode = getErrCode(err, r);
				continue;
			}
			if (info.role == cluster::RaftInfo::Role::Leader) {
				resetLeader();
				logPrintf(LogTrace, "[%d proxy] proxyCall RaftInfo::Role::Leader", sId_);
				r = (impl_.*fn)(std::forward<Args>(args)..., _ctx);
				// the only place, where errUpdateReplication may appear
			} else if (info.role == cluster::RaftInfo::Role::Follower) {
				if (_ctx.HasEmmiterID()) {
					SetErrorCode(r, Error(errAlreadyProxied, "Request was proxied to follower node"));
					return r;
				}
				try {
					auto clientToLeader = getLeader(info);
					logPrintf(LogTrace, "[%d proxy] proxyCall RaftInfo::Role::Follower", sId_);
					r = action(_ctx, clientToLeader, std::forward<Args>(args)...);
				} catch (Error e) {
					SetErrorCode(r, std::move(e));
				}
			}
			errCode = getErrCode(err, r);
		} while (errCode == errWrongReplicationData);
	} else {
		logPrintf(LogTrace, "[%d proxy] proxyCall LSN not empty (local call)", sId_);
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
#ifdef WITH_SHARDING
	if (!shardingInitialized_ && impl_.shardingConfig_) {
		shardingRouter_ = std::make_shared<sharding::LocatorService>(impl_, *(impl_.shardingConfig_));
		err = shardingRouter_->Start();
		if (err.ok()) {
			shardingInitialized_ = true;
		}
	}
#endif	// WITH_SHARDING
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

template <typename Func, typename... Args>
Error ClusterProxy::delegateToShards(Func f, [[maybe_unused]] Args &&...args) {
#ifdef WITH_SHARDING
	Error status;
	auto connections = shardingRouter_->GetShardsConnections(status);
	if (!status.ok()) return status;
	bool alreadyConnected = false;
	for (auto &connection : connections) {
		if (connection) {
			status = std::invoke(f, connection, std::forward<Args>(args)...);
		} else {
			alreadyConnected = true;
		}
		if (!status.ok()) return status;
	}
	if (alreadyConnected) status = errAlreadyConnected;
	return status;
#else	// WITH_SHARDING
	(void)f;
	return Error(errForbidden, "Sharding is disabled");
#endif	// WITH_SHARDING
}

template <typename Func, typename... Args>
Error ClusterProxy::delegateToShardsByNs(bool toAll, Func f, std::string_view nsName, [[maybe_unused]] Args &&...args) {
#ifdef WITH_SHARDING
	Error status;
	auto connections = shardingRouter_->GetShardsConnections(nsName, status);
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
	return status;
#else	// WITH_SHARDING
	(void)toAll;
	(void)f;
	(void)nsName;
	return Error(errForbidden, "Sharding is disabled");
#endif	// WITH_SHARDING
}

Error ClusterProxy::modifyItemOnShard(std::string_view nsName, Item &item, ItemModifyMode mode) {
#ifdef WITH_SHARDING
	Error status;
	auto connection = shardingRouter_->GetShardConnection(nsName, item, status);
	if (!status.ok()) {
		return status;
	}
	if (connection) {
		client::Item clientItem = toClientItem(nsName, connection.get(), item);
		if (!clientItem.Status().ok()) return clientItem.Status();
		switch (mode) {
			case ModeInsert:
				return connection->Insert(nsName, clientItem);
			case ModeUpsert: {
				return connection->Upsert(nsName, clientItem);
			}
			case ModeUpdate:
				return connection->Update(nsName, clientItem);
			case ModeDelete:
				return connection->Delete(nsName, clientItem);
		}
	}
	return errAlreadyConnected;
#else	// WITH_SHARDING
	(void)mode;
	(void)item;
	(void)nsName;
	return Error(errForbidden, "Sharding is disabled");
#endif	// WITH_SHARDING
}

Error ClusterProxy::executeQueryOnShard([[maybe_unused]] const Query &query, [[maybe_unused]] QueryResults &result,
										[[maybe_unused]] InternalRdxContext &ctx,
										[[maybe_unused]] const std::function<Error(const Query &, QueryResults &)> &localAction) noexcept {
#ifdef WITH_SHARDING
	Error status;
	try {
		if (query.Type() == QueryTruncate) {
			return delegateToShardsByNs(true, &client::SyncCoroReindexer::TruncateNamespace, query.Namespace());
		}
		auto connections = shardingRouter_->GetShardsConnections(query, status);
		if (!status.ok()) return status;

		unsigned limit = query.count;
		unsigned offset = query.start;
		Query q = query;

		if (q.start != 0) {
			q.ReqTotal();
		}

		ctx.SetShardingParallelExecution(connections.size() > 1);
		bool createTm = false;
		if (connections.size() > 1) {
			createTm = true;
		}
		std::unordered_set<int32_t> stateTokenList;
		for (size_t i = 0; i < connections.size(); ++i) {
			if (connections[i]) {
				auto connection = connections[i]->WithShardingParallelExecution(connections.size() > 1);
				client::SyncCoroQueryResults qr;
				switch (q.Type()) {
					case QuerySelect: {
						q.Limit(limit);
						q.Offset(offset);
						status = connection.Select(q, qr);
						if (qr.Count() >= limit) {
							limit = 0;
						} else {
							limit -= qr.Count();
						}
						if (result.TotalCount() >= offset) {
							offset = 0;
						} else {
							offset -= result.TotalCount();
						}
						break;
					}
					case QueryUpdate: {
						status = connection.Update(q, qr);
						break;
					}
					case QueryDelete: {
						status = connection.Delete(q, qr);
						break;
					}
					default:
						std::abort();
				}
				if (status.ok()) {
					stateTokenList.insert(qr.GetTagsMatcher(0).stateToken());
					clientToCoreQueryResults(query, qr, result, createTm, stateTokenList);
					createTm = false;
				}
			} else {
				assert(i == 0);
				status = localAction(q, result);
				if (result.Count() >= limit) {
					limit = 0;
				} else {
					limit -= result.Count();
				}
				if (result.TotalCount() >= offset) {
					offset = 0;
				} else {
					offset -= result.TotalCount();
				}
				if (!status.ok()) return status;
				stateTokenList.insert(result.getTagsMatcher(0).stateToken());
			}
			if (!status.ok()) return status;
			if (query.calcTotal != ModeAccurateTotal && limit == 0) {
				break;
			}
		}
	} catch (const Error &e) {
		status = e;
	}
	return status;
#else	// WITH_SHARDING
	return Error(errForbidden, "Sharding is disabled");
#endif	// WITH_SHARDING
}

Error ClusterProxy::OpenNamespace(std::string_view nsName, const StorageOpts &opts, const NsReplicationOpts &replOpts,
								  const InternalRdxContext &ctx) {
	if (isWithSharding(nsName, ctx)) {
		Error err = delegateToShardsByNs(true, &client::SyncCoroReindexer::OpenNamespace, nsName, opts, replOpts);
		if (err.code() != errAlreadyConnected) return err;
	}
	std::function<Error(const InternalRdxContext &, std::shared_ptr<client::SyncCoroReindexer>, std::string_view, const StorageOpts &,
						const NsReplicationOpts &)>
		action = std::bind(&ClusterProxy::baseFollowerAction<decltype(&client::SyncCoroReindexer::OpenNamespace),
															 &client::SyncCoroReindexer::OpenNamespace, std::string_view,
															 const StorageOpts &, const NsReplicationOpts &>,
						   this, _1, _2, _3, _4, _5);
	logPrintf(LogTrace, "[%d proxy] ClusterProxy::OpenNamespace", sId_);
	return CallProxyFunction(OpenNamespace)(ctx, nsName, action, nsName, opts, replOpts);
}

Error ClusterProxy::AddNamespace(const NamespaceDef &nsDef, const NsReplicationOpts &replOpts, const InternalRdxContext &ctx) {
#ifdef WITH_SHARDING
	if (isWithSharding(nsDef.name, ctx)) {
		Error status;
		auto connections = shardingRouter_->GetShardsConnections(status);
		if (!status.ok()) return status;
		bool alreadyConnected = false;
		for (auto &connection : connections) {
			if (connection) {
				status = connection->AddNamespace(nsDef, replOpts);
			} else {
				alreadyConnected = true;
			}
			if (!status.ok()) return status;
		}
		if (!alreadyConnected) return status;
	}
#endif	// WITH_SHARDING
	DefFunctor2(const NamespaceDef &, const NsReplicationOpts &, AddNamespace, baseFollowerAction);
	logPrintf(LogTrace, "[%d proxy] ClusterProxy::AddNamespace", sId_);
	return CallProxyFunction(AddNamespace)(ctx, nsDef.name, action, nsDef, replOpts);
}

Error ClusterProxy::CloseNamespace(std::string_view nsName, const InternalRdxContext &ctx) {
	if (isWithSharding(nsName, ctx)) {
		Error err = delegateToShardsByNs(true, &client::SyncCoroReindexer::CloseNamespace, nsName);
		if (err.code() != errAlreadyConnected) return err;
	}
	DefFunctor1(std::string_view, CloseNamespace, baseFollowerAction);
	logPrintf(LogTrace, "[%d proxy] ClusterProxy::CloseNamespace", sId_);
	return CallProxyFunction(CloseNamespace)(ctx, nsName, action, nsName);
}

Error ClusterProxy::DropNamespace(std::string_view nsName, const InternalRdxContext &ctx) {
	if (isWithSharding(nsName, ctx)) {
		Error err = delegateToShards(&client::SyncCoroReindexer::DropNamespace, nsName);
		if (err.code() != errAlreadyConnected) return err;
	}
	DefFunctor1(std::string_view, DropNamespace, baseFollowerAction);
	logPrintf(LogTrace, "[%d proxy] ClusterProxy::DropNamespace", sId_);
	return CallProxyFunction(DropNamespace)(ctx, nsName, action, nsName);
}

Error ClusterProxy::TruncateNamespace(std::string_view nsName, const InternalRdxContext &ctx) {
	if (isWithSharding(nsName, ctx)) {
		Error err = delegateToShards(&client::SyncCoroReindexer::TruncateNamespace, nsName);
		if (err.code() != errAlreadyConnected) return err;
	}
	DefFunctor1(std::string_view, TruncateNamespace, baseFollowerAction);
	logPrintf(LogTrace, "[%d proxy] ClusterProxy::TruncateNamespace", sId_);
	return CallProxyFunction(TruncateNamespace)(ctx, nsName, action, nsName);
}

Error ClusterProxy::RenameNamespace(std::string_view srcNsName, const std::string &dstNsName, const InternalRdxContext &ctx) {
	if (isWithSharding(srcNsName, ctx)) {
		Error err = delegateToShards(&client::SyncCoroReindexer::RenameNamespace, srcNsName, dstNsName);
		if (err.code() != errAlreadyConnected) return err;
	}
	DefFunctor2(std::string_view, const std::string &, RenameNamespace, baseFollowerAction);
	logPrintf(LogTrace, "[%d proxy] ClusterProxy::RenameNamespace", sId_);
	return CallProxyFunction(RenameNamespace)(ctx, std::string_view(), action, srcNsName, dstNsName);
}

Error ClusterProxy::AddIndex(std::string_view nsName, const IndexDef &index, const InternalRdxContext &ctx) {
	if (isWithSharding(nsName, ctx)) {
		Error err = delegateToShards(&client::SyncCoroReindexer::AddIndex, nsName, index);
		if (err.code() != errAlreadyConnected) return err;
	}
	DefFunctor2(std::string_view, const IndexDef &, AddIndex, baseFollowerAction);
	logPrintf(LogTrace, "[%d proxy] ClusterProxy::AddIndex", sId_);
	return CallProxyFunction(AddIndex)(ctx, nsName, action, nsName, index);
}

Error ClusterProxy::UpdateIndex(std::string_view nsName, const IndexDef &index, const InternalRdxContext &ctx) {
	if (isWithSharding(nsName, ctx)) {
		Error err = delegateToShards(&client::SyncCoroReindexer::UpdateIndex, nsName, index);
		if (err.code() != errAlreadyConnected) return err;
	}
	DefFunctor2(std::string_view, const IndexDef &, UpdateIndex, baseFollowerAction);
	logPrintf(LogTrace, "[%d proxy] ClusterProxy::UpdateIndex", sId_);
	return CallProxyFunction(UpdateIndex)(ctx, nsName, action, nsName, index);
}

Error ClusterProxy::DropIndex(std::string_view nsName, const IndexDef &index, const InternalRdxContext &ctx) {
	if (isWithSharding(nsName, ctx)) {
		Error err = delegateToShards(&client::SyncCoroReindexer::DropIndex, nsName, index);
		if (err.code() != errAlreadyConnected) return err;
	}
	DefFunctor2(std::string_view, const IndexDef &, DropIndex, baseFollowerAction);
	logPrintf(LogTrace, "[%d proxy] ClusterProxy::DropIndex", sId_);
	return CallProxyFunction(DropIndex)(ctx, nsName, action, nsName, index);
}

Error ClusterProxy::SetSchema(std::string_view nsName, std::string_view schema, const InternalRdxContext &ctx) {
	if (isWithSharding(nsName, ctx)) {
		Error err = delegateToShards(&client::SyncCoroReindexer::SetSchema, nsName, schema);
		if (err.code() != errAlreadyConnected) return err;
	}
	DefFunctor2(std::string_view, std::string_view, SetSchema, baseFollowerAction);
	logPrintf(LogTrace, "[%d proxy] ClusterProxy::SetSchema", sId_);
	return CallProxyFunction(SetSchema)(ctx, nsName, action, nsName, schema);
}

Error ClusterProxy::GetSchema(std::string_view nsName, int format, std::string &schema, const InternalRdxContext &ctx) {
	if (isWithSharding(nsName, ctx)) {
		Error err = delegateToShardsByNs(false, &client::SyncCoroReindexer::GetSchema, nsName, format, schema);
		if (err.code() != errAlreadyConnected) return err;
	}
	return impl_.GetSchema(nsName, format, schema, ctx);
}

Error ClusterProxy::EnumNamespaces(vector<NamespaceDef> &defs, EnumNamespacesOpts opts, const InternalRdxContext &ctx) {
#ifdef WITH_SHARDING
	if (isWithSharding(ctx)) {
		Error status;
		if (auto connection = shardingRouter_->GetProxyShardConnection(false, status)) {
			if (!status.ok()) return status;
			return connection->EnumNamespaces(defs, opts);
		}
	}
#endif	// WITH_SHARDING
	return impl_.EnumNamespaces(defs, opts, ctx);
}

Error ClusterProxy::Insert(std::string_view nsName, Item &item, const InternalRdxContext &ctx) {
	if (isWithSharding(nsName, ctx)) {
		Error err = modifyItemOnShard(nsName, item, ModeInsert);
		if (err.code() != errAlreadyConnected) return err;
	}
	std::function<Error(const InternalRdxContext &, std::shared_ptr<client::SyncCoroReindexer>, std::string_view, Item &)> action =
		std::bind(&ClusterProxy::itemFollowerAction<Error (client::SyncCoroReindexer::*)(std::string_view, client::Item &),
													&client::SyncCoroReindexer::Insert>,
				  this, _1, _2, _3, _4);

	return proxyCall<Error (ReindexerImpl::*)(std::string_view, Item &, const InternalRdxContext &), &ReindexerImpl::Insert,
					 Error (client::SyncCoroReindexer::*)(std::string_view, client::Item &), &client::SyncCoroReindexer::Insert, Error>(
		ctx, nsName, action, nsName, item);
}

Error ClusterProxy::Insert(std::string_view nsName, Item &item, QueryResults &result, const InternalRdxContext &ctx) {
	if (isWithSharding(nsName, ctx)) {
		Error err = modifyItemOnShard(nsName, item, ModeInsert);
		if (err.code() != errAlreadyConnected) return err;
	}
	std::function<Error(const InternalRdxContext &, std::shared_ptr<client::SyncCoroReindexer>, std::string_view, Item &, QueryResults &)>
		action = std::bind(&ClusterProxy::resultItemFollowerAction<Error (client::SyncCoroReindexer::*)(std::string_view, client::Item &,
																										client::SyncCoroQueryResults &),
																   &client::SyncCoroReindexer::Insert>,
						   this, _1, _2, _3, _4, _5);
	return proxyCall<Error (ReindexerImpl::*)(std::string_view, Item &, QueryResults &, const InternalRdxContext &), &ReindexerImpl::Insert,
					 Error (client::SyncCoroReindexer::*)(std::string_view, client::Item &, client::SyncCoroQueryResults &),
					 &client::SyncCoroReindexer::Insert, Error>(ctx, nsName, action, nsName, item, result);
}

Error ClusterProxy::Update(std::string_view nsName, Item &item, const InternalRdxContext &ctx) {
	if (isWithSharding(nsName, ctx)) {
		Error err = modifyItemOnShard(nsName, item, ModeUpdate);
		if (err.code() != errAlreadyConnected) return err;
	}
	std::function<Error(const InternalRdxContext &, std::shared_ptr<client::SyncCoroReindexer>, std::string_view, Item &)> action =
		std::bind(&ClusterProxy::itemFollowerAction<Error (client::SyncCoroReindexer::*)(std::string_view, client::Item &),
													&client::SyncCoroReindexer::Update>,
				  this, _1, _2, _3, _4);
	return proxyCall<Error (ReindexerImpl::*)(std::string_view, Item &, const InternalRdxContext &), &ReindexerImpl::Update,
					 Error (client::SyncCoroReindexer::*)(std::string_view, client::Item &), &client::SyncCoroReindexer::Update, Error>(
		ctx, nsName, action, nsName, item);
}

Error ClusterProxy::Update(std::string_view nsName, Item &item, QueryResults &result, const InternalRdxContext &ctx) {
	if (isWithSharding(nsName, ctx)) {
		Error err = modifyItemOnShard(nsName, item, ModeUpdate);
		if (err.code() != errAlreadyConnected) return err;
	}
	std::function<Error(const InternalRdxContext &, std::shared_ptr<client::SyncCoroReindexer>, std::string_view, Item &, QueryResults &)>
		action = std::bind(&ClusterProxy::resultItemFollowerAction<Error (client::SyncCoroReindexer::*)(std::string_view, client::Item &,
																										client::SyncCoroQueryResults &),
																   &client::SyncCoroReindexer::Update>,
						   this, _1, _2, _3, _4, _5);
	return proxyCall<Error (ReindexerImpl::*)(std::string_view, Item &, QueryResults &, const InternalRdxContext &), &ReindexerImpl::Update,
					 Error (client::SyncCoroReindexer::*)(std::string_view, client::Item &, client::SyncCoroQueryResults &),
					 &client::SyncCoroReindexer::Update, Error>(ctx, nsName, action, nsName, item, result);
}

Error ClusterProxy::Update(const Query &query, QueryResults &result, const InternalRdxContext &ctx) {
	InternalRdxContext localCtx{ctx};
	std::function<Error(const Query &, QueryResults &)> updateFn = [this, &localCtx](const Query &q, QueryResults &qr) -> Error {
		std::function<Error(const InternalRdxContext &, std::shared_ptr<client::SyncCoroReindexer>, const Query &, QueryResults &)> action =
			std::bind(
				&ClusterProxy::resultFollowerAction<Error (client::SyncCoroReindexer::*)(const Query &, client::SyncCoroQueryResults &),
													&client::SyncCoroReindexer::Update>,
				this, _1, _2, _3, _4);
		logPrintf(LogTrace, "[%d proxy] ClusterProxy::Update query", sId_);
		return proxyCall<Error (ReindexerImpl::*)(const Query &, QueryResults &, const InternalRdxContext &), &ReindexerImpl::Update,
						 Error (client::SyncCoroReindexer::*)(const Query &, client::SyncCoroQueryResults &),
						 &client::SyncCoroReindexer::Update, Error>(localCtx, q._namespace, action, q, qr);
	};
	if (isWithSharding(query.Namespace(), ctx)) {
		return executeQueryOnShard(query, result, localCtx, updateFn);
	}
	return updateFn(query, result);
}

Error ClusterProxy::Upsert(std::string_view nsName, Item &item, QueryResults &result, const InternalRdxContext &ctx) {
	if (isWithSharding(nsName, ctx)) {
		Error err = modifyItemOnShard(nsName, item, ModeUpsert);
		if (err.code() != errAlreadyConnected) return err;
	}
	std::function<Error(const InternalRdxContext &, std::shared_ptr<client::SyncCoroReindexer>, std::string_view, Item &, QueryResults &)>
		action = std::bind(&ClusterProxy::resultItemFollowerAction<Error (client::SyncCoroReindexer::*)(std::string_view, client::Item &,
																										client::SyncCoroQueryResults &),
																   &client::SyncCoroReindexer::Upsert>,
						   this, _1, _2, _3, _4, _5);

	return proxyCall<Error (ReindexerImpl::*)(std::string_view, Item &, QueryResults &, const InternalRdxContext &), &ReindexerImpl::Upsert,
					 Error (client::SyncCoroReindexer::*)(std::string_view, client::Item &, client::SyncCoroQueryResults &),
					 &client::SyncCoroReindexer::Upsert, Error>(ctx, nsName, action, nsName, item, result);
}

Error ClusterProxy::Upsert(std::string_view nsName, Item &item, const InternalRdxContext &ctx) {
	if (isWithSharding(nsName, ctx)) {
		Error err = modifyItemOnShard(nsName, item, ModeUpsert);
		if (err.code() != errAlreadyConnected) return err;
	}
	std::function<Error(const InternalRdxContext &, std::shared_ptr<client::SyncCoroReindexer>, std::string_view, Item &)> action =
		std::bind(&ClusterProxy::itemFollowerAction<Error (client::SyncCoroReindexer::*)(std::string_view, client::Item &),
													&client::SyncCoroReindexer::Upsert>,
				  this, _1, _2, _3, _4);
	return proxyCall<Error (ReindexerImpl::*)(std::string_view, Item &, const InternalRdxContext &), &ReindexerImpl::Upsert,
					 Error (client::SyncCoroReindexer::*)(std::string_view, client::Item &), &client::SyncCoroReindexer::Upsert, Error>(
		ctx, nsName, action, nsName, item);
}

Error ClusterProxy::Delete(std::string_view nsName, Item &item, const InternalRdxContext &ctx) {
	if (isWithSharding(nsName, ctx)) {
		Error err = modifyItemOnShard(nsName, item, ModeDelete);
		if (err.code() != errAlreadyConnected) return err;
	}
	std::function<Error(const InternalRdxContext &, std::shared_ptr<client::SyncCoroReindexer>, std::string_view, Item &)> action =
		std::bind(&ClusterProxy::itemFollowerAction<Error (client::SyncCoroReindexer::*)(std::string_view, client::Item &),
													&client::SyncCoroReindexer::Delete>,
				  this, _1, _2, _3, _4);
	logPrintf(LogTrace, "[%d proxy] ClusterProxy::Delete ITEM", sId_);
	return proxyCall<Error (ReindexerImpl::*)(std::string_view, Item &, const InternalRdxContext &), &ReindexerImpl::Delete,
					 Error (client::SyncCoroReindexer::*)(std::string_view, client::Item &), &client::SyncCoroReindexer::Delete, Error>(
		ctx, nsName, action, nsName, item);
}

Error ClusterProxy::Delete(std::string_view nsName, Item &item, QueryResults &result, const InternalRdxContext &ctx) {
	if (isWithSharding(nsName, ctx)) {
		Error err = modifyItemOnShard(nsName, item, ModeDelete);
		if (err.code() != errAlreadyConnected) return err;
	}
	std::function<Error(const InternalRdxContext &, std::shared_ptr<client::SyncCoroReindexer>, std::string_view, Item &, QueryResults &)>
		action = std::bind(&ClusterProxy::resultItemFollowerAction<Error (client::SyncCoroReindexer::*)(std::string_view, client::Item &,
																										client::SyncCoroQueryResults &),
																   &client::SyncCoroReindexer::Delete>,
						   this, _1, _2, _3, _4, _5);

	return proxyCall<Error (ReindexerImpl::*)(std::string_view, Item &, QueryResults &, const InternalRdxContext &), &ReindexerImpl::Delete,
					 Error (client::SyncCoroReindexer::*)(std::string_view, client::Item &, client::SyncCoroQueryResults &),
					 &client::SyncCoroReindexer::Delete, Error>(ctx, nsName, action, nsName, item, result);
}

Error ClusterProxy::Delete(const Query &query, QueryResults &result, const InternalRdxContext &ctx) {
	InternalRdxContext localCtx{ctx};
	std::function<Error(const Query &, QueryResults &)> deleteFn = [this, &localCtx](const Query &q, QueryResults &qr) -> Error {
		std::function<Error(const InternalRdxContext &, std::shared_ptr<client::SyncCoroReindexer>, const Query &, QueryResults &)> action =
			std::bind(
				&ClusterProxy::resultFollowerAction<Error (client::SyncCoroReindexer::*)(const Query &, client::SyncCoroQueryResults &),
													&client::SyncCoroReindexer::Delete>,
				this, _1, _2, _3, _4);
		logPrintf(LogTrace, "[%d proxy] ClusterProxy::Delete QUERY", sId_);
		return proxyCall<Error (ReindexerImpl::*)(const Query &, QueryResults &, const InternalRdxContext &), &ReindexerImpl::Delete,
						 Error (client::SyncCoroReindexer::*)(const Query &, client::SyncCoroQueryResults &),
						 &client::SyncCoroReindexer::Delete, Error>(localCtx, q._namespace, action, q, qr);
	};
	if (isWithSharding(query.Namespace(), ctx)) {
		return executeQueryOnShard(query, result, localCtx, deleteFn);
	}
	return deleteFn(query, result);
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
	InternalRdxContext localCtx{ctx};
	if (isWithSharding(query.Namespace(), ctx)) {
		std::function<Error(const Query &, QueryResults &)> selectFn = [this, &ctx](const Query &q, QueryResults &qr) -> Error {
			return impl_.Select(q, qr, ctx);
		};
		return executeQueryOnShard(query, result, localCtx, selectFn);
	}
	try {
		if (!shouldProxyQuery(query)) {
			logPrintf(LogTrace, "[%d proxy] ClusterProxy::Select query local", sId_);
			return impl_.Select(query, result, localCtx);
		}
	} catch (const Error &ex) {
		return ex;
	}
	InternalRdxContext deadlineCtx(localCtx.HasDeadline() ? localCtx : localCtx.WithTimeout(kReplicationStatsTimeout));
	std::function<Error(const InternalRdxContext &, std::shared_ptr<client::SyncCoroReindexer>, const Query &, QueryResults &)> action =
		std::bind(&ClusterProxy::resultFollowerAction<Error (client::SyncCoroReindexer::*)(const Query &, client::SyncCoroQueryResults &),
													  &client::SyncCoroReindexer::Select>,
				  this, _1, _2, _3, _4);
	logPrintf(LogTrace, "[%d proxy] ClusterProxy::Select query proxied", sId_);
	return proxyCall<Error (ReindexerImpl::*)(const Query &, QueryResults &, const InternalRdxContext &), &ReindexerImpl::Select,
					 Error (client::SyncCoroReindexer::*)(const Query &, client::SyncCoroQueryResults &),
					 &client::SyncCoroReindexer::Select, Error>(deadlineCtx, query._namespace, action, query, result);
}

Error ClusterProxy::Commit(std::string_view nsName, const InternalRdxContext &ctx) {
	if (isWithSharding(nsName, ctx)) {
		Error err = delegateToShardsByNs(true, &client::SyncCoroReindexer::Commit, nsName);
		if (err.code() != errAlreadyConnected) return err;
	}
	return impl_.Commit(nsName);
}

Item ClusterProxy::NewItem(std::string_view nsName, const InternalRdxContext &ctx) { return impl_.NewItem(nsName, ctx); }

Transaction ClusterProxy::NewTransaction(std::string_view nsName, const InternalRdxContext &ctx) {
	const bool withSharding = isWithSharding(nsName, ctx);

	std::function<Transaction(const InternalRdxContext &, std::shared_ptr<client::SyncCoroReindexer>, std::string_view)> action =
		std::bind(&ClusterProxy::newTxFollowerAction, this, _1, _2, _3);
	logPrintf(LogTrace, "[%d proxy] ClusterProxy::NewTransaction", sId_);
	Transaction tx = proxyCall<Transaction (ReindexerImpl::*)(std::string_view, const InternalRdxContext &), &ReindexerImpl::NewTransaction,
							   client::SyncCoroTransaction (client::SyncCoroReindexer::*)(std::string_view),
							   &client::SyncCoroReindexer::NewTransaction, Transaction>(ctx, nsName, action, nsName);
#ifdef WITH_SHARDING
	if (withSharding) {
		tx.SetShardingRouter(shardingRouter_);
	}
#else	// WITH_SHARDING
	(void)withSharding;
#endif	// WITH_SHARDING
	return tx;
}

Error ClusterProxy::CommitTransaction(Transaction &tr, QueryResults &result, const InternalRdxContext &ctx) {
#ifdef WITH_SHARDING
	if (isWithSharding(tr.GetNsName(), ctx)) {
		if (tr.shardId_ == IndexValueType::NotSet) {
			return Error(errLogic, "Error commiting transaction with sharding: shard ID is not set");
		}
		if (tr.clientTransaction_) {
			Error status;
			auto connection = shardingRouter_->GetShardConnection(tr.shardId_, true, status, tr.GetNsName());
			if (!status.ok()) return status;
			if (connection) {
				client::SyncCoroQueryResults clientResults;
				status = connection->CommitTransaction(*tr.clientTransaction_, clientResults);
				if (!status.ok()) {
					return status;
				}
				clientToCoreQueryResults(clientResults, result, false, std::unordered_set<int32_t>());
				return status;
			}
		}
	}
#endif	// WITH_SHARDING
	if (tr.clientTransaction_) {
		if (!tr.clientTransaction_->Status().ok()) return tr.clientTransaction_->Status();
		assert(tr.clientTransaction_->rx_);
		client::SyncCoroReindexerImpl *client = tr.clientTransaction_->rx_.get();
		client::InternalRdxContext c =
			client::InternalRdxContext{}.WithLSN(ctx.LSN()).WithEmmiterServerId(sId_).WithShardingParallelExecution(
				ctx.IsShardingParallelExecution());
		client::SyncCoroQueryResults clientResults;
		Error err = client->CommitTransaction(*tr.clientTransaction_.get(), clientResults, c);
		if (err.ok()) {
			clientToCoreQueryResults(clientResults, result, false, std::unordered_set<int32_t>());
		}
		return err;

	} else {
		return impl_.CommitTransaction(tr, result, ctx);
	}
}

Error ClusterProxy::RollBackTransaction(Transaction &tr, const InternalRdxContext &ctx) {
#ifdef WITH_SHARDING
	if (isWithSharding(tr.GetNsName(), ctx)) {
		if (tr.shardId_ == IndexValueType::NotSet) {
			return Error(errLogic, "Error rolling back transaction with sharding: shard ID is not set");
		}
		Error status;
		auto connection = shardingRouter_->GetShardConnection(tr.shardId_, false, status, tr.GetNsName());
		if (!status.ok()) return status;
		if (connection) {
			return connection->RollBackTransaction(*tr.clientTransaction_);
		}
	}
#endif	// WITH_SHARDING
	std::function<Error(const InternalRdxContext &, std::shared_ptr<client::SyncCoroReindexer>, Transaction &)> action =
		std::bind(&ClusterProxy::transactionRollBackAction, this, _1, _2, _3);
	logPrintf(LogTrace, "[%d proxy] ClusterProxy::RollBackTransaction", sId_);
	return proxyCall<Error (ReindexerImpl::*)(Transaction &, const InternalRdxContext &), &ReindexerImpl::RollBackTransaction,
					 Error (client::SyncCoroReindexerImpl::*)(client::SyncCoroTransaction &, const client::InternalRdxContext &),
					 &client::SyncCoroReindexerImpl::RollBackTransaction, Error>(ctx, tr.GetNsName(), action, tr);
}

Error ClusterProxy::GetMeta(std::string_view nsName, const string &key, string &data, const InternalRdxContext &ctx) {
	if (isWithSharding(nsName, ctx)) {
		Error err = delegateToShardsByNs(false, &client::SyncCoroReindexer::GetMeta, nsName, key, data);
		if (err.code() != errAlreadyConnected) return err;
	}
	return impl_.GetMeta(nsName, key, data, ctx);
}

Error ClusterProxy::PutMeta(std::string_view nsName, const string &key, std::string_view data, const InternalRdxContext &ctx) {
	if (isWithSharding(nsName, ctx)) {
		Error err = delegateToShards(&client::SyncCoroReindexer::PutMeta, nsName, key, data);
		if (err.code() != errAlreadyConnected) return err;
	}
	std::function<Error(const InternalRdxContext &, std::shared_ptr<client::SyncCoroReindexer>, std::string_view, const string &,
						std::string_view)>
		action =
			std::bind(&ClusterProxy::baseFollowerAction<decltype(&client::SyncCoroReindexer::PutMeta), &client::SyncCoroReindexer::PutMeta,
														std::string_view, const string &, std::string_view>,
					  this, _1, _2, _3, _4, _5);
	logPrintf(LogTrace, "[%d proxy] ClusterProxy::PutMeta", sId_);
	return CallProxyFunction(PutMeta)(ctx, nsName, action, nsName, key, data);
}

Error ClusterProxy::EnumMeta(std::string_view nsName, vector<string> &keys, const InternalRdxContext &ctx) {
	if (isWithSharding(nsName, ctx)) {
		Error err = delegateToShardsByNs(false, &client::SyncCoroReindexer::EnumMeta, nsName, keys);
		if (err.code() != errAlreadyConnected) return err;
	}
	return impl_.EnumMeta(nsName, keys, ctx);
}

bool ClusterProxy::isWithSharding(std::string_view nsName, const InternalRdxContext &ctx) const noexcept {
	if (nsName.size() && (nsName[0] == '#' || nsName[0] == '@')) {
		return false;
	}
	return isWithSharding(ctx);
}

bool ClusterProxy::isWithSharding(const InternalRdxContext &ctx) const noexcept {
#ifdef WITH_SHARDING
	if (int(ctx.ShardId()) == ShardingKeyType::ShardingProxyOff) {
		return false;
	}
	const bool ret = ctx.LSN().isEmpty() && !ctx.HasEmmiterID() && impl_.shardingConfig_ && shardingRouter_ &&
					 int(ctx.ShardId()) == IndexValueType::NotSet;
	return ret;
#else	// WITH_SHARDING
	(void)ctx;
	return false;
#endif	// WITH_SHARDING
}

bool ClusterProxy::shouldProxyQuery(const Query &q) {
	assert(q.Type() == QuerySelect);
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
