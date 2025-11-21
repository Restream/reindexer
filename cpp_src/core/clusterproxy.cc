#include "clusterproxy.h"
#include "client/itemimplbase.h"
#include "cluster/clustercontrolrequest.h"
#include "cluster/consts.h"
#include "cluster/sharding/shardingcontrolrequest.h"
#include "core/system_ns_names.h"
#include "estl/shared_mutex.h"
#include "namespacedef.h"
#include "tools/catch_and_return.h"
#include "tools/clusterproxyloghelper.h"

namespace reindexer {

#if RX_ENABLE_CLUSTERPROXY_LOGS
template <typename R, typename std::enable_if<std::is_same<R, Error>::value>::type* = nullptr>
static void printErr(const R& r) {
	if (!r.ok()) {
		clusterProxyLog(LogTrace, "[cluster proxy] Err: {}", r.what());
	}
}

template <typename R, typename std::enable_if<std::is_same<R, Transaction>::value>::type* = nullptr>
static void printErr(const R& r) {
	if (!r.Status().ok()) {
		clusterProxyLog(LogTrace, "[cluster proxy] Tx err: {}", r.Status().what());
	}
}
#endif

#define CallProxyFunction(Fn) proxyCall<decltype(&ReindexerImpl::Fn), &ReindexerImpl::Fn, Error>

#define DefFunctor1(P1, F, Action)                                   \
	std::function<Error(const RdxContext&, LeaderRefT, P1)> action = \
		std::bind(&ClusterProxy::Action<decltype(&client::Reindexer::F), &client::Reindexer::F, P1>, this, _1, _2, _3)

#define DefFunctor2(P1, P2, F, Action)                                   \
	std::function<Error(const RdxContext&, LeaderRefT, P1, P2)> action = \
		std::bind(&ClusterProxy::Action<decltype(&client::Reindexer::F), &client::Reindexer::F, P1, P2>, this, _1, _2, _3, _4)

#define DefFunctor3(P1, P2, P3, F, Action)                                   \
	std::function<Error(const RdxContext&, LeaderRefT, P1, P2, P3)> action = \
		std::bind(&ClusterProxy::Action<decltype(&client::Reindexer::F), &client::Reindexer::F, P1, P2, P3>, this, _1, _2, _3, _4, _5)

using namespace std::string_view_literals;

// This method is for simple modify-requests, proxied by cluster (and for #replicationstats request)
// This QR's can contain only items and aggregations from single namespace
void ClusterProxy::clientToCoreQueryResults(client::QueryResults& clientResults, LocalQueryResults& result) {
	if (clientResults.HaveJoined()) {
		throw Error(errLogic, "Queries with non-empty joined data are not supported by Cluster Proxy");
	}
	if (result.getMergedNSCount() != 0 || result.totalCount != 0) {
		throw Error(errLogic, "Target query results are not empty. Query results merging is not supported by Cluster Proxy");
	}

	const auto itemsCnt = clientResults.Count();
	for (int nsid = 0, nss = clientResults.GetMergedNSCount(); nsid < nss; ++nsid) {
		auto& incTagsVec = clientResults.GetIncarnationTags();
		lsn_t incTag;
		if (!incTagsVec.empty()) {
			if (incTagsVec.size() != 1) [[unlikely]] {
				throw Error(errLogic, "Unexpected incarnation tags count {} for single-node query", incTagsVec.size());
			}
			if (incTagsVec[0].tags.size() > unsigned(nsid)) [[likely]] {
				incTag = incTagsVec[0].tags[nsid];
			} else if (itemsCnt) {
				throw Error(errLogic, "Missing incarnation tag for ns id {} in non-empty result", nsid);
			}
		}
		result.addNSContext(clientResults.GetPayloadType(nsid), clientResults.GetTagsMatcher(nsid), FieldsFilter(), nullptr,
							std::move(incTag));
	}
	result.explainResults = clientResults.GetExplainResults();
	result.aggregationResults = clientResults.GetAggregationResults();
	result.totalCount = clientResults.TotalCount();
	if (!itemsCnt) {
		return;
	}

	const auto& pt = result.getPayloadType(0);
	const auto& tm = result.getTagsMatcher(0);
	const auto cNamespaces = clientResults.GetNamespaces();
	WrSerializer wser;
	for (auto& it : clientResults) {
		if (it.GetNSID() != 0) [[unlikely]] {
			throw Error(errLogic,
						"Unexpected items from non-main namespace ({}). Cluster proxy does not support proxying for joined/merged items",
						cNamespaces.at(it.GetNSID()));
		}
		wser.Reset();
		auto err = it.GetCJSON(wser, false);
		if (!err.ok()) {
			throw err;
		}

		ItemImpl itemimpl(pt, tm);
		itemimpl.FromCJSON(wser.Slice());

		const auto& itemParams = it.GetItemParams();
		itemimpl.Value().SetLSN(itemParams.lsn);
		result.AddItemRef(itemParams.rank, itemParams.id, itemimpl.Value(), itemParams.nsid, true);
		result.SaveRawData(std::move(itemimpl));
	}
}

template <>
ErrorCode ClusterProxy::getErrCode<Error>(const Error& err, Error& r) {
	if (!err.ok()) {
		r = err;
		return ErrorCode(err.code());
	}
	return ErrorCode(r.code());
}

template <>
ErrorCode ClusterProxy::getErrCode<Transaction>(const Error& err, Transaction& r) {
	if (!err.ok()) {
		r = Transaction(err);
		return ErrorCode(err.code());
	}
	return ErrorCode(r.Status().code());
}

std::shared_ptr<client::Reindexer> ClusterProxy::getLeader(const cluster::RaftInfo& info) {
	{
		shared_lock lck(mtx_);
		if (info.leaderId == leaderId_) {
			return leader_;
		}
	}
	lock_guard lck(mtx_);
	if (info.leaderId == leaderId_) {
		return leader_;
	}
	leader_.reset();
	leaderId_ = -1;
	DSN leaderDsn;
	impl_.getLeaderDsn(leaderDsn, GetServerID(), info);
	if (!leaderDsn.Parser().isValid()) {
		throw Error(errLogic, "Leader dsn is not valid. {}", leaderDsn);
	}
	leader_ = clusterConns_.Get(leaderDsn);
	leaderId_ = info.leaderId;

	return leader_;
}

void ClusterProxy::resetLeader() {
	lock_guard lck(mtx_);
	leader_.reset();
	leaderId_ = -1;
}

ClusterProxy::ClusterProxy(ReindexerConfig cfg, ActivityContainer& activities, ReindexerImpl::CallbackMap&& proxyCallbacks)
	: impl_(std::move(cfg), activities, addCallbacks(std::move(proxyCallbacks))), leaderId_(-1) {
	sId_.store(impl_.configProvider_.GetReplicationConfig().serverID, std::memory_order_release);
	replCfgHandlerID_ = impl_.configProvider_.setHandler(
		[this](const ReplicationConfigData& data) { sId_.store(data.serverID, std::memory_order_release); });
}
ClusterProxy::~ClusterProxy() {
	if (replCfgHandlerID_.has_value()) {
		impl_.configProvider_.unsetHandler(*replCfgHandlerID_);
	}
}

ReindexerImpl::CallbackMap ClusterProxy::addCallbacks(ReindexerImpl::CallbackMap&& callbackMap) const {
	// TODO: add callbacks for actions of ClusterProxy level
	return std::move(callbackMap);
}

Error ClusterProxy::Connect(const std::string& dsn, ConnectOpts opts) {
	Error err = impl_.Connect(dsn, opts);
	if (!err.ok()) {
		return err;
	}
	if (!connected_.load(std::memory_order_relaxed)) {
		if (impl_.clusterConfig_) {
			clusterConns_.SetParams(impl_.clusterConfig_->proxyConnThreads, impl_.clusterConfig_->proxyConnCount,
									impl_.clusterConfig_->proxyConnConcurrency);
		}
		connected_.store(err.ok(), std::memory_order_release);
	}
	return err;
}

Error ClusterProxy::OpenNamespace(std::string_view nsName, const StorageOpts& opts, const NsReplicationOpts& replOpts,
								  const RdxContext& ctx) {
	using namespace std::placeholders;
	DefFunctor3(std::string_view, const StorageOpts&, const NsReplicationOpts&, OpenNamespace, baseFollowerAction);
	clusterProxyLog(LogTrace, "[{} proxy] ClusterProxy::OpenNamespace", getServerIDRel());
	return CallProxyFunction(OpenNamespace)(ctx, nsName, action, nsName, opts, replOpts);
}

Error ClusterProxy::AddNamespace(const NamespaceDef& nsDef, const NsReplicationOpts& replOpts, const RdxContext& ctx) {
	using namespace std::placeholders;
	DefFunctor2(const NamespaceDef&, const NsReplicationOpts&, AddNamespace, baseFollowerAction);
	clusterProxyLog(LogTrace, "[{} proxy] ClusterProxy::AddNamespace", getServerIDRel());
	return CallProxyFunction(AddNamespace)(ctx, nsDef.name, action, nsDef, replOpts);
}

Error ClusterProxy::CloseNamespace(std::string_view nsName, const RdxContext& ctx) {
	using namespace std::placeholders;
	DefFunctor1(std::string_view, CloseNamespace, baseFollowerAction);
	clusterProxyLog(LogTrace, "[{} proxy] ClusterProxy::DropNamespace", getServerIDRel());
	return CallProxyFunction(CloseNamespace)(ctx, nsName, action, nsName);
}

Error ClusterProxy::DropNamespace(std::string_view nsName, const RdxContext& ctx) {
	using namespace std::placeholders;
	DefFunctor1(std::string_view, DropNamespace, baseFollowerAction);
	clusterProxyLog(LogTrace, "[{} proxy] ClusterProxy::DropNamespace", getServerIDRel());
	return CallProxyFunction(DropNamespace)(ctx, nsName, action, nsName);
}

Error ClusterProxy::TruncateNamespace(std::string_view nsName, const RdxContext& ctx) {
	using namespace std::placeholders;
	DefFunctor1(std::string_view, TruncateNamespace, baseFollowerAction);
	clusterProxyLog(LogTrace, "[{} proxy] ClusterProxy::TruncateNamespace", getServerIDRel());
	return CallProxyFunction(TruncateNamespace)(ctx, nsName, action, nsName);
}

Error ClusterProxy::RenameNamespace(std::string_view srcNsName, const std::string& dstNsName, const RdxContext& ctx) {
	using namespace std::placeholders;
	DefFunctor2(std::string_view, const std::string&, RenameNamespace, baseFollowerAction);
	clusterProxyLog(LogTrace, "[{} proxy] ClusterProxy::RenameNamespace", getServerIDRel());
	return CallProxyFunction(RenameNamespace)(ctx, std::string_view(), action, srcNsName, dstNsName);
}

Error ClusterProxy::AddIndex(std::string_view nsName, const IndexDef& index, const RdxContext& ctx) {
	using namespace std::placeholders;
	DefFunctor2(std::string_view, const IndexDef&, AddIndex, baseFollowerAction);
	clusterProxyLog(LogTrace, "[{} proxy] ClusterProxy::AddIndex", getServerIDRel());
	return CallProxyFunction(AddIndex)(ctx, nsName, action, nsName, index);
}

Error ClusterProxy::UpdateIndex(std::string_view nsName, const IndexDef& index, const RdxContext& ctx) {
	using namespace std::placeholders;
	DefFunctor2(std::string_view, const IndexDef&, UpdateIndex, baseFollowerAction);
	clusterProxyLog(LogTrace, "[{} proxy] ClusterProxy::UpdateIndex", getServerIDRel());
	return CallProxyFunction(UpdateIndex)(ctx, nsName, action, nsName, index);
}

Error ClusterProxy::DropIndex(std::string_view nsName, const IndexDef& index, const RdxContext& ctx) {
	using namespace std::placeholders;
	DefFunctor2(std::string_view, const IndexDef&, DropIndex, baseFollowerAction);
	clusterProxyLog(LogTrace, "[{} proxy] ClusterProxy::DropIndex", getServerIDRel());
	return CallProxyFunction(DropIndex)(ctx, nsName, action, nsName, index);
}

Error ClusterProxy::SetSchema(std::string_view nsName, std::string_view schema, const RdxContext& ctx) {
	using namespace std::placeholders;
	DefFunctor2(std::string_view, std::string_view, SetSchema, baseFollowerAction);
	clusterProxyLog(LogTrace, "[{} proxy] ClusterProxy::SetSchema", getServerIDRel());
	return CallProxyFunction(SetSchema)(ctx, nsName, action, nsName, schema);
}

Error ClusterProxy::GetSchema(std::string_view nsName, int format, std::string& schema, const RdxContext& ctx) {
	return impl_.GetSchema(nsName, format, schema, ctx);
}

Error ClusterProxy::EnumNamespaces(std::vector<NamespaceDef>& defs, EnumNamespacesOpts opts, const RdxContext& ctx) {
	return impl_.EnumNamespaces(defs, opts, ctx);
}

Error ClusterProxy::Insert(std::string_view nsName, Item& item, const RdxContext& ctx) {
	auto action = [this](const RdxContext& ctx, LeaderRefT clientToLeader, std::string_view nsName, Item& item) {
		return itemFollowerAction<&client::Reindexer::Insert>(ctx, clientToLeader, nsName, item);
	};
	return proxyCall<LocalItemSimpleActionFT, &ReindexerImpl::Insert, Error>(ctx, nsName, action, nsName, item);
}

Error ClusterProxy::Insert(std::string_view nsName, Item& item, LocalQueryResults& qr, const RdxContext& ctx) {
	auto action = [this](const RdxContext& ctx, LeaderRefT clientToLeader, std::string_view nsName, Item& item, LocalQueryResults& qr) {
		return resultItemFollowerAction<&client::Reindexer::Insert>(ctx, clientToLeader, nsName, item, qr);
	};
	return proxyCall<LocalItemQrActionFT, &ReindexerImpl::Insert, Error>(ctx, nsName, action, nsName, item, qr);
}

Error ClusterProxy::Update(std::string_view nsName, Item& item, const RdxContext& ctx) {
	auto action = [this](const RdxContext& ctx, LeaderRefT clientToLeader, std::string_view nsName, Item& item) {
		return itemFollowerAction<&client::Reindexer::Update>(ctx, clientToLeader, nsName, item);
	};
	return proxyCall<LocalItemSimpleActionFT, &ReindexerImpl::Update, Error>(ctx, nsName, action, nsName, item);
}

Error ClusterProxy::Update(std::string_view nsName, Item& item, LocalQueryResults& qr, const RdxContext& ctx) {
	auto action = [this](const RdxContext& ctx, LeaderRefT clientToLeader, std::string_view nsName, Item& item, LocalQueryResults& qr) {
		return resultItemFollowerAction<&client::Reindexer::Update>(ctx, clientToLeader, nsName, item, qr);
	};
	return proxyCall<LocalItemQrActionFT, &ReindexerImpl::Update, Error>(ctx, nsName, action, nsName, item, qr);
}

Error ClusterProxy::Update(const Query& q, LocalQueryResults& qr, const RdxContext& ctx) {
	auto action = [this](const RdxContext& ctx, LeaderRefT clientToLeader, const Query& q, LocalQueryResults& qr) {
		return resultFollowerAction<&client::Reindexer::Update>(ctx, clientToLeader, q, qr);
	};
	clusterProxyLog(LogTrace, "[{} proxy] ClusterProxy::Update query", getServerIDRel());
	return proxyCall<LocalQueryActionFT, &ReindexerImpl::Update, Error>(ctx, q.NsName(), action, q, qr);
}

Error ClusterProxy::Upsert(std::string_view nsName, Item& item, const RdxContext& ctx) {
	auto action = [this](const RdxContext& ctx, LeaderRefT clientToLeader, std::string_view nsName, Item& item) {
		return itemFollowerAction<&client::Reindexer::Upsert>(ctx, clientToLeader, nsName, item);
	};
	return proxyCall<LocalItemSimpleActionFT, &ReindexerImpl::Upsert, Error>(ctx, nsName, action, nsName, item);
}

Error ClusterProxy::Upsert(std::string_view nsName, Item& item, LocalQueryResults& qr, const RdxContext& ctx) {
	auto action = [this](const RdxContext& ctx, LeaderRefT clientToLeader, std::string_view nsName, Item& item, LocalQueryResults& qr) {
		return resultItemFollowerAction<&client::Reindexer::Upsert>(ctx, clientToLeader, nsName, item, qr);
	};
	return proxyCall<LocalItemQrActionFT, &ReindexerImpl::Upsert, Error>(ctx, nsName, action, nsName, item, qr);
}

Error ClusterProxy::Delete(std::string_view nsName, Item& item, const RdxContext& ctx) {
	auto action = [this](const RdxContext& ctx, LeaderRefT clientToLeader, std::string_view nsName, Item& item) {
		return itemFollowerAction<&client::Reindexer::Delete>(ctx, clientToLeader, nsName, item);
	};
	clusterProxyLog(LogTrace, "[{} proxy] ClusterProxy::Delete ITEM", getServerIDRel());
	return proxyCall<LocalItemSimpleActionFT, &ReindexerImpl::Delete, Error>(ctx, nsName, action, nsName, item);
}

Error ClusterProxy::Delete(std::string_view nsName, Item& item, LocalQueryResults& qr, const RdxContext& ctx) {
	auto action = [this](const RdxContext& ctx, LeaderRefT clientToLeader, std::string_view nsName, Item& item, LocalQueryResults& qr) {
		return resultItemFollowerAction<&client::Reindexer::Delete>(ctx, clientToLeader, nsName, item, qr);
	};
	return proxyCall<LocalItemQrActionFT, &ReindexerImpl::Delete, Error>(ctx, nsName, action, nsName, item, qr);
}

Error ClusterProxy::Delete(const Query& q, LocalQueryResults& qr, const RdxContext& ctx) {
	auto action = [this](const RdxContext& ctx, LeaderRefT clientToLeader, const Query& q, LocalQueryResults& qr) {
		return resultFollowerAction<&client::Reindexer::Delete>(ctx, clientToLeader, q, qr);
	};
	clusterProxyLog(LogTrace, "[{} proxy] ClusterProxy::Delete QUERY", getServerIDRel());
	return proxyCall<LocalQueryActionFT, &ReindexerImpl::Delete, Error>(ctx, q.NsName(), action, q, qr);
}

Error ClusterProxy::Select(const Query& q, LocalQueryResults& qr, const RdxContext& ctx) {
	using namespace std::placeholders;
	if (!shouldProxyQuery(q)) {
		clusterProxyLog(LogTrace, "[{} proxy] ClusterProxy::Select query local", getServerIDRel());
		return impl_.Select(q, qr, ctx);
	}
	const RdxDeadlineContext deadlineCtx(kReplicationStatsTimeout, ctx.GetCancelCtx());
	const RdxContext rdxDeadlineCtx = ctx.WithCancelCtx(deadlineCtx);

	auto action = [this](const RdxContext& ctx, LeaderRefT clientToLeader, const Query& q, LocalQueryResults& qr) {
		return resultFollowerAction<&client::Reindexer::Select>(ctx, clientToLeader, q, qr);
	};
	clusterProxyLog(LogTrace, "[{} proxy] ClusterProxy::Select query proxied", getServerIDRel());
	auto err = proxyCall<LocalQueryActionFT, &ReindexerImpl::Select, Error>(rdxDeadlineCtx, q.NsName(), action, q, qr);
	if (err.code() == errNetwork) {
		// Force leader's check, if proxy returned errNetwork. New error does not matter
		std::ignore = impl_.ClusterControlRequest(ClusterControlRequestData(ForceElectionsCommand{}));
	}
	return err;
}

Transaction ClusterProxy::NewTransaction(std::string_view nsName, const RdxContext& ctx) {
	using LocalFT = LocalTransaction (ReindexerImpl::*)(std::string_view, const RdxContext&);
	auto action = [this](const RdxContext& ctx, LeaderRefT clientToLeader, std::string_view nsName) {
		try {
			client::Reindexer l = clientToLeader->WithEmitterServerId(GetServerID());
			return Transaction(impl_.NewTransaction(nsName, ctx), std::move(l));
		} catch (const Error& err) {
			return Transaction(err);
		}
	};
	clusterProxyLog(LogTrace, "[{} proxy] ClusterProxy::NewTransaction", getServerIDRel());
	return proxyCall<LocalFT, &ReindexerImpl::NewTransaction, Transaction>(ctx, nsName, action, nsName);
}

Error ClusterProxy::CommitTransaction(Transaction& tr, QueryResults& qr, bool txExpectsSharding, const RdxContext& ctx) {
	return tr.commit(GetServerID(), txExpectsSharding, impl_, qr, ctx);
}

Error ClusterProxy::RollBackTransaction(Transaction& tr, const RdxContext& ctx) {
	//
	return tr.rollback(GetServerID(), ctx);
}

Error ClusterProxy::GetMeta(std::string_view nsName, const std::string& key, std::string& data, const RdxContext& ctx) {
	return impl_.GetMeta(nsName, key, data, ctx);
}

Error ClusterProxy::PutMeta(std::string_view nsName, const std::string& key, std::string_view data, const RdxContext& ctx) {
	using namespace std::placeholders;
	DefFunctor3(std::string_view, const std::string&, std::string_view, PutMeta, baseFollowerAction);
	clusterProxyLog(LogTrace, "[{} proxy] ClusterProxy::PutMeta", getServerIDRel());
	return CallProxyFunction(PutMeta)(ctx, nsName, action, nsName, key, data);
}

Error ClusterProxy::EnumMeta(std::string_view nsName, std::vector<std::string>& keys, const RdxContext& ctx) {
	return impl_.EnumMeta(nsName, keys, ctx);
}

Error ClusterProxy::DeleteMeta(std::string_view nsName, const std::string& key, const RdxContext& ctx) {
	using namespace std::placeholders;
	DefFunctor2(std::string_view, const std::string&, DeleteMeta, baseFollowerAction);
	clusterProxyLog(LogTrace, "[{} proxy] ClusterProxy::DeleteMeta", getServerIDRel());
	return CallProxyFunction(DeleteMeta)(ctx, nsName, action, nsName, key);
}

Error ClusterProxy::GetSqlSuggestions(std::string_view sqlQuery, int pos, std::vector<std::string>& suggestions, const RdxContext& ctx) {
	return impl_.GetSqlSuggestions(sqlQuery, pos, suggestions, ctx);
}

Error ClusterProxy::Status() noexcept {
	if (connected_.load(std::memory_order_acquire)) {
		return {};
	}
	auto st = impl_.Status();
	if (st.ok()) {
		return Error(errNotValid, "Reindexer's cluster proxy layer was not initialized properly");
	}
	return st;
}

Error ClusterProxy::GetProtobufSchema(WrSerializer& ser, std::vector<std::string>& namespaces) {
	//
	return impl_.GetProtobufSchema(ser, namespaces);
}

Error ClusterProxy::GetReplState(std::string_view nsName, ReplicationStateV2& state, const RdxContext& ctx) {
	return impl_.GetReplState(nsName, state, ctx);
}

Error ClusterProxy::SetClusterOperationStatus(std::string_view nsName, const ClusterOperationStatus& status, const RdxContext& ctx) {
	return impl_.SetClusterOperationStatus(nsName, status, ctx);
}

Error ClusterProxy::ApplySnapshotChunk(std::string_view nsName, const SnapshotChunk& ch, const RdxContext& ctx) {
	return impl_.ApplySnapshotChunk(nsName, ch, ctx);
}

Error ClusterProxy::SuggestLeader(const cluster::NodeData& suggestion, cluster::NodeData& response) {
	return impl_.SuggestLeader(suggestion, response);
}

Error ClusterProxy::LeadersPing(const cluster::NodeData& leader) {
	Error err = impl_.LeadersPing(leader);
	if (err.ok()) {
		unique_lock lck(processPingEventMutex_);
		lastPingLeaderId_ = leader.serverId;
		lck.unlock();
		processPingEvent_.notify_all();
	}
	return err;
}

Error ClusterProxy::GetRaftInfo(cluster::RaftInfo& info, const RdxContext& ctx) {
	//
	return impl_.GetRaftInfo(true, info, ctx);
}

Error ClusterProxy::CreateTemporaryNamespace(std::string_view baseName, std::string& resultName, const StorageOpts& opts, lsn_t nsVersion,
											 const RdxContext& ctx) {
	return impl_.CreateTemporaryNamespace(baseName, resultName, opts, nsVersion, ctx);
}

Error ClusterProxy::GetSnapshot(std::string_view nsName, const SnapshotOpts& opts, Snapshot& snapshot, const RdxContext& ctx) {
	return impl_.GetSnapshot(nsName, opts, snapshot, ctx);
}

Error ClusterProxy::SetTagsMatcher(std::string_view nsName, TagsMatcher&& tm, const RdxContext& ctx) {
	return impl_.SetTagsMatcher(nsName, std::move(tm), ctx);
}

Error ClusterProxy::DumpIndex(std::ostream& os, std::string_view nsName, std::string_view index, const RdxContext& ctx) {
	return impl_.DumpIndex(os, nsName, index, ctx);
}

void ClusterProxy::ShutdownCluster() {
	impl_.ShutdownCluster();
	clusterConns_.Shutdown();
	resetLeader();
}

Namespace::Ptr ClusterProxy::GetNamespacePtr(std::string_view nsName, const RdxContext& ctx) { return impl_.getNamespace(nsName, ctx); }

Namespace::Ptr ClusterProxy::GetNamespacePtrNoThrow(std::string_view nsName, const RdxContext& ctx) {
	return impl_.getNamespaceNoThrow(nsName, ctx);
}

PayloadType ClusterProxy::GetPayloadType(std::string_view nsName) { return impl_.getPayloadType(nsName); }

bool ClusterProxy::IsFulltextOrVector(std::string_view nsName, std::string_view indexName) const {
	return impl_.isFulltextOrVector(nsName, indexName);
}

bool ClusterProxy::shouldProxyQuery(const Query& q) {
	assertrx_throw(q.Type() == QuerySelect);
	if (kReplicationStatsNamespace != q.NsName()) {
		return false;
	}
	if (q.GetJoinQueries().size() || q.GetMergeQueries().size() || q.GetSubQueries().size()) {
		throw Error(errParams, "Joins, merges and subqueries are not allowed for #replicationstats queries");
	}
	bool hasTypeCond = false;
	bool isAsyncReplQuery = false;
	bool isClusterReplQuery = false;
	constexpr auto kConditionError =
		"Query to #replicationstats has to contain one of the following conditions: type='async' or type='cluster'"sv;
	for (auto it = q.Entries().cbegin(), end = q.Entries().cend(); it != end; ++it) {
		if (it->Is<QueryEntry>() && it->Value<QueryEntry>().FieldName() == "type"sv) {
			auto nextIt = it;
			++nextIt;
			auto& entry = it->Value<QueryEntry>();
			if (hasTypeCond || entry.Condition() != CondEq || entry.Values().size() != 1 ||
				!entry.Values()[0].Type().Is<KeyValueType::String>() || it->operation != OpAnd ||
				(nextIt != end && nextIt->operation == OpOr)) {
				throw Error(errParams, kConditionError);
			}
			auto str = entry.Values()[0].As<std::string>();
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

Error ClusterProxy::ResetShardingConfig(std::optional<cluster::ShardingConfig> config) noexcept {
	try {
		impl_.shardingConfig_.Set(std::move(config));
		return impl_.tryLoadShardingConf();
	}
	CATCH_AND_RETURN
}

void ClusterProxy::SaveNewShardingConfigFile(const cluster::ShardingConfig& config) const {
	//
	impl_.saveNewShardingConfigFile(config);
}

#ifdef _MSC_VER
#define REINDEXER_FUNC_NAME __FUNCSIG__
#else
#define REINDEXER_FUNC_NAME __PRETTY_FUNCTION__
#endif

template <auto ClientMethod, auto ImplMethod, typename... Args>
Error ClusterProxy::shardingControlRequestAction(const RdxContext& ctx, Args&&... args) noexcept {
	try {
		const auto action = [this](const RdxContext& c, LeaderRefT l, Args&&... aa) {
			return baseFollowerAction<decltype(ClientMethod), ClientMethod>(c, l, std::forward<Args>(aa)...);
		};

		clusterProxyLog(LogTrace, "[{} proxy] {}", getServerIDRel(), REINDEXER_FUNC_NAME);
		// kReplicationStatsNamespace required for impl_.NamespaceIsInClusterConfig(nsName) in proxyCall was true always
		return proxyCall<decltype(ImplMethod), ImplMethod, Error>(ctx, kReplicationStatsNamespace, action, std::forward<Args>(args)...);
	}
	CATCH_AND_RETURN
}

Error ClusterProxy::ShardingControlRequest(const sharding::ShardingControlRequestData& request, sharding::ShardingControlResponseData&,
										   const RdxContext& ctx) {
	using Type = sharding::ControlCmdType;
	switch (request.type) {
		case Type::SaveCandidate: {
			const auto& data = std::get<sharding::SaveConfigCommand>(request.data);
			return shardingControlRequestAction<&client::Reindexer::SaveNewShardingConfig, &ReindexerImpl::saveShardingCfgCandidate>(
				ctx, data.config, data.sourceId);
		}
		case Type::ResetOldSharding: {
			const auto& data = std::get<sharding::ResetConfigCommand>(request.data);
			return shardingControlRequestAction<&client::Reindexer::ResetOldShardingConfig, &ReindexerImpl::resetOldShardingConfig>(
				ctx, data.sourceId);
		}
		case Type::ResetCandidate: {
			const auto& data = std::get<sharding::ResetConfigCommand>(request.data);
			return shardingControlRequestAction<&client::Reindexer::ResetShardingConfigCandidate,
												&ReindexerImpl::resetShardingConfigCandidate>(ctx, data.sourceId);
		}
		case Type::RollbackCandidate: {
			const auto& data = std::get<sharding::ResetConfigCommand>(request.data);
			return shardingControlRequestAction<&client::Reindexer::RollbackShardingConfigCandidate,
												&ReindexerImpl::rollbackShardingConfigCandidate>(ctx, data.sourceId);
		}
		case Type::ApplyNew: {
			const auto& data = std::get<sharding::ApplyConfigCommand>(request.data);
			return shardingControlRequestAction<&client::Reindexer::ApplyNewShardingConfig, &ReindexerImpl::applyShardingCfgCandidate>(
				ctx, data.sourceId);
		}
		case Type::GetNodeConfig:
		case Type::ApplyLeaderConfig:
		default:
			break;
	}
	return {};
}

Error ClusterProxy::SubscribeUpdates(IEventsObserver& observer, EventSubscriberConfig&& cfg) {
	return impl_.SubscribeUpdates(observer, std::move(cfg));
}

Error ClusterProxy::UnsubscribeUpdates(IEventsObserver& observer) {
	//
	return impl_.UnsubscribeUpdates(observer);
}

void ClusterProxy::ConnectionsMap::SetParams(int clientThreads, int clientConns, int clientConnConcurrency) {
	lock_guard lck(mtx_);
	clientThreads_ = clientThreads > 0 ? clientThreads : cluster::kDefaultClusterProxyConnThreads;
	clientConns_ = clientConns > 0 ? (std::min(uint32_t(clientConns), kMaxClusterProxyConnCount)) : cluster::kDefaultClusterProxyConnCount;
	clientConnConcurrency_ = clientConnConcurrency > 0 ? (std::min(uint32_t(clientConnConcurrency), kMaxClusterProxyConnConcurrency))
													   : cluster::kDefaultClusterProxyCoroPerConn;
}

std::shared_ptr<client::Reindexer> ClusterProxy::ConnectionsMap::Get(const DSN& dsn) {
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

	lock_guard lck(mtx_);
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

void ClusterProxy::ConnectionsMap::Shutdown() {
	lock_guard lck(mtx_);
	shutdown_ = true;
	for (auto& conn : conns_) {
		conn.second->Stop();
	}
}

// No forwarding in proxy calls - the same args may be used multiple times in a row
template <typename Fn, Fn fn, typename R, typename... Args>
R ClusterProxy::localCall(const RdxContext& ctx, Args&... args) {
	if constexpr (std::is_same_v<R, Transaction>) {
		return R((impl_.*fn)(args..., ctx));
	} else {
		return (impl_.*fn)(args..., ctx);
	}
}

template <typename Fn, Fn fn, typename R, typename FnA, typename... Args>
R ClusterProxy::proxyCall(const RdxContext& ctx, std::string_view nsName, const FnA& action, Args&... args) {
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
					err = Error(err.code(), "Unable to get cluster's leader: {}", err.what());
				}
				setErrorCode(r, std::move(err));
				return r;
			}
			if (info.role == cluster::RaftInfo::Role::None) {  // fast way for non-cluster node
				if (ctx.HasEmitterServer()) {
					setErrorCode(r, Error(errLogic, "Request was proxied to non-cluster node"));
					return r;
				}
				r = localCall<Fn, fn, R, Args...>(ctx, args...);
				errCode = getErrCode(err, r);
				if (errCode == errWrongReplicationData && (!impl_.clusterConfig_ || !impl_.NamespaceIsInClusterConfig(nsName))) {
					break;
				}
				continue;
			}
			const bool nsInClusterConf = impl_.NamespaceIsInClusterConfig(nsName);
			if (!nsInClusterConf) {	 // ns is not in cluster
				clusterProxyLog(LogTrace, "[{} proxy] proxyCall ns not in cluster config (local)", getServerIDRel());
				const bool firstError = (errCode == errOK);
				r = localCall<Fn, fn, R, Args...>(ctx, args...);
				errCode = getErrCode(err, r);
				if (firstError) {
					continue;
				}
				break;
			}
			if (info.role == cluster::RaftInfo::Role::Leader) {
				resetLeader();
				clusterProxyLog(LogTrace, "[{} proxy] proxyCall RaftInfo::Role::Leader", getServerIDRel());
				r = localCall<Fn, fn, R, Args...>(ctx, args...);
#if RX_ENABLE_CLUSTERPROXY_LOGS
				printErr(r);
#endif
				// the only place, where errUpdateReplication may appear
			} else if (info.role == cluster::RaftInfo::Role::Follower) {
				if (ctx.HasEmitterServer()) {
					setErrorCode(r, Error(errAlreadyProxied, "Request was proxied to follower node"));
					return r;
				}
				try {
					auto clientToLeader = getLeader(info);
					clusterProxyLog(LogTrace, "[{} proxy] proxyCall RaftInfo::Role::Follower", getServerIDRel());
					r = action(ctx, clientToLeader, args...);
				} catch (Error& e) {
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
		clusterProxyLog(LogTrace, "[{} proxy] proxyCall LSN not empty: {} (local call)", getServerIDRel(), ctx.GetOriginLSN());
		r = localCall<Fn, fn, R, Args...>(ctx, args...);
		// errWrongReplicationData means, that leader of the current node doesn't match leader from LSN
		if (getErrCode(err, r) == errWrongReplicationData) {
			cluster::RaftInfo info;
			err = impl_.GetRaftInfo(false, info, ctx);
			if (!err.ok()) {
				if (err.code() == errTimeout || err.code() == errCanceled) {
					err = Error(err.code(), "Unable to get cluster's leader: {}", err.what());
				}
				setErrorCode(r, std::move(err));
				return r;
			}
			if (info.role != cluster::RaftInfo::Role::Follower) {
				return r;
			}
			unique_lock lck(processPingEventMutex_);
			auto waitRes = processPingEvent_.wait_for(lck, cluster::kLeaderPingInterval * 10);	// Awaiting ping from current leader
			if (waitRes == std::cv_status::timeout || lastPingLeaderId_ != ctx.GetOriginLSN().Server()) {
				return r;
			}
			lck.unlock();
			return localCall<Fn, fn, R, Args...>(ctx, args...);
		}
	}
	return r;
}

template <typename FnL, FnL fnl, typename... Args>
Error ClusterProxy::baseFollowerAction(const RdxContext& ctx, LeaderRefT clientToLeader, Args&&... args) {
	try {
		client::Reindexer l = clientToLeader->WithEmitterServerId(sId_);
		const auto ward = ctx.BeforeClusterProxy();
		Error err = (l.*fnl)(std::forward<Args>(args)...);
		return err;
	} catch (const Error& err) {
		return err;
	}
}

template <ClusterProxy::ProxiedItemSimpleActionFT fnl>
Error ClusterProxy::itemFollowerAction(const RdxContext& ctx, LeaderRefT clientToLeader, std::string_view nsName, Item& item) {
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
			client::Reindexer l = clientToLeader->WithEmitterServerId(sId_);
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
	} catch (const Error& err) {
		return err;
	}
}

template <ClusterProxy::ProxiedQueryActionFT fnl>
Error ClusterProxy::resultFollowerAction(const RdxContext& ctx, LeaderRefT clientToLeader, const Query& query, LocalQueryResults& qr) {
	try {
		Error err;
		client::Reindexer l = clientToLeader->WithEmitterServerId(sId_);
		client::QueryResults clientResults;
		{
			const auto ward = ctx.BeforeClusterProxy();
			err = (l.*fnl)(query, clientResults);
		}
		if (!err.ok()) {
			return err;
		}
		if (!query.GetMergeQueries().empty()) {
			return Error(errLogic, "Unable to proxy query with MERGE");
		}
		if (!query.GetJoinQueries().empty() && query.Type() == QuerySelect) {
			return Error(errLogic, "Unable to proxy SELECT query with JOIN");
		}
		clientToCoreQueryResults(clientResults, qr);
		return err;
	} catch (const Error& err) {
		return err;
	}
}

template <ClusterProxy::ProxiedItemQrActionFT fnl>
Error ClusterProxy::resultItemFollowerAction(const RdxContext& ctx, LeaderRefT clientToLeader, std::string_view nsName, Item& item,
											 LocalQueryResults& qr) {
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
		client::Reindexer l = clientToLeader->WithEmitterServerId(sId_);
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
	} catch (const Error& err) {
		return err;
	}
}

}  // namespace reindexer
