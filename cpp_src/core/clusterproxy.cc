#include "clusterproxy.h"
#include "cluster/sharding/shardingcontrolrequest.h"
#include "core/cjson/jsonbuilder.h"
#include "core/defnsconfigs.h"
#include "estl/shared_mutex.h"

#include "namespacedef.h"
#include "tools/catch_and_return.h"

namespace reindexer {

using namespace std::string_view_literals;

// This method is for simple modify-requests, proxied by cluster (and for #replicationstats request)
// This QR's can contain only items and aggregations from single namespace
void ClusterProxy::clientToCoreQueryResults(client::QueryResults& clientResults, LocalQueryResults& result) {
	QueryResults qr;
	if (clientResults.HaveJoined()) {
		throw Error(errLogic, "JOIN queries are not supported by Cluster Proxy");
	}
	if (clientResults.GetMergedNSCount() > 1) {
		throw Error(errLogic, "MERGE queries are not supported by Cluster Proxy");
	}
	if (result.getMergedNSCount() != 0 || result.totalCount != 0) {
		throw Error(errLogic, "Query results merging is not supported by Cluster Proxy");
	}

	const auto itemsCnt = clientResults.Count();
	if (clientResults.GetMergedNSCount() > 0) {
		auto& incTags = clientResults.GetIncarnationTags()[0].tags;
		if (itemsCnt || incTags.size()) {
			result.addNSContext(clientResults.GetPayloadType(0), clientResults.GetTagsMatcher(0), FieldsSet(), nullptr,
								incTags.size() ? incTags[0] : lsn_t());
		}
	}
	result.explainResults = clientResults.GetExplainResults();
	result.aggregationResults = clientResults.GetAggregationResults();
	result.totalCount = clientResults.TotalCount();
	if (!itemsCnt) {
		return;
	}

	RdxContext dummyCtx;
	auto& localPt = result.getPayloadType(0);
	auto& localTm = result.getTagsMatcher(0);
	auto cNamespaces = clientResults.GetNamespaces();
	for (auto it = clientResults.begin(), itEnd = clientResults.end(); it != itEnd; ++it) {
		auto item = it.GetItem();
		if (!item.Status().ok()) {
			throw item.Status();
		}
		if (!item) {
			Item itemServer = impl_.NewItem(cNamespaces[0], dummyCtx);
			result.AddItem(itemServer);
			continue;
		}

		ItemImpl itemimpl(localPt, localTm);
		Error err = itemimpl.FromJSON(item.GetJSON());
		if (!err.ok()) {
			throw err;
		}

		if (itemimpl.tagsMatcher().isUpdated()) {
			WrSerializer wrser;
			itemimpl.tagsMatcher().serialize(wrser);
			Serializer ser(wrser.Slice());
			localTm.deserialize(ser, itemimpl.tagsMatcher().version(), itemimpl.tagsMatcher().stateToken());
		}
		itemimpl.Value().SetLSN(item.GetLSN());
		result.Add(ItemRef(it.itemParams_.id, itemimpl.Value(), it.itemParams_.proc, it.itemParams_.nsid, true));
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
	std::lock_guard lck(mtx_);
	if (info.leaderId == leaderId_) {
		return leader_;
	}
	leader_.reset();
	leaderId_ = -1;
	DSN leaderDsn;
	Error err = impl_.getLeaderDsn(leaderDsn, GetServerID(), info);
	if (!err.ok()) {
		throw err;
	}
	if (!leaderDsn.Parser().isValid()) {
		throw Error(errLogic, "Leader dsn is not valid. %s", leaderDsn);
	}
	leader_ = clusterConns_.Get(leaderDsn);
	leaderId_ = info.leaderId;

	return leader_;
}

void ClusterProxy::resetLeader() {
	std::lock_guard lck(mtx_);
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

bool ClusterProxy::shouldProxyQuery(const Query& q) {
	assertrx(q.Type() == QuerySelect);
	if (kReplicationStatsNamespace != q.NsName()) {
		return false;
	}
	if (q.HasLimit()) {
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

		clusterProxyLog(LogTrace, "[%d proxy] %s", getServerIDRel(), REINDEXER_FUNC_NAME);
		// kReplicationStatsNamespace required for impl_.NamespaceIsInClusterConfig(nsName) in proxyCall was true always
		return proxyCall<decltype(ImplMethod), ImplMethod, Error>(ctx, kReplicationStatsNamespace, action, std::forward<Args>(args)...);
	}
	CATCH_AND_RETURN
}

Error ClusterProxy::ShardingControlRequest(const sharding::ShardingControlRequestData& request, sharding::ShardingControlResponseData&,
										   const RdxContext& ctx) noexcept {
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

}  // namespace reindexer
