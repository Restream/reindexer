#include "client/cororpcclient.h"
#include <stdio.h>
#include <functional>
#include "client/itemimpl.h"
#include "client/snapshot.h"
#include "cluster/clustercontrolrequest.h"
#include "core/namespace/namespacestat.h"
#include "core/namespace/snapshot/snapshot.h"
#include "core/namespacedef.h"
#include "gason/gason.h"
#include "tools/errors.h"
#include "vendor/gason/gason.h"

using std::string;
using std::vector;

namespace reindexer {
namespace client {

using reindexer::net::cproto::CoroRPCAnswer;

CoroRPCClient::CoroRPCClient(const CoroReindexerConfig& config, Namespaces::PtrT sharedNamespaces)
	: namespaces_(sharedNamespaces ? std::move(sharedNamespaces) : make_intrusive<Namespaces::IntrusiveT>()), config_(config) {
	conn_.SetConnectionStateHandler([this](Error err) { onConnectionState(std::move(err)); });
}

CoroRPCClient::~CoroRPCClient() { Stop(); }

Error CoroRPCClient::Connect(const string& dsn, ev::dynamic_loop& loop, const client::ConnectOpts& opts) {
	std::lock_guard lck(mtx_);
	if (conn_.IsRunning()) {
		return Error(errLogic, "Client is already started");
	}

	cproto::CoroClientConnection::ConnectData connectData;
	if (!connectData.uri.parse(dsn)) {
		return Error(errParams, "%s is not valid uri", dsn);
	}
	if (connectData.uri.scheme() != "cproto") {
		return Error(errParams, "Scheme must be cproto");
	}
	connectData.opts = cproto::CoroClientConnection::Options(config_.NetTimeout, config_.NetTimeout, opts.IsCreateDBIfMissing(),
															 opts.HasExpectedClusterID(), opts.ExpectedClusterID(),
															 config_.ReconnectAttempts, config_.EnableCompression, config_.AppName);
	conn_.Start(loop, std::move(connectData));
	loop_ = &loop;
	return errOK;
}

Error CoroRPCClient::Stop() {
	if (conn_.IsRunning()) {
		std::lock_guard lck(mtx_);
		terminate_ = true;
		conn_.Stop();
		resubWg_.wait();
		loop_ = nullptr;
		terminate_ = false;
	}
	return errOK;
}

Error CoroRPCClient::AddNamespace(const NamespaceDef& nsDef, const InternalRdxContext& ctx, const NsReplicationOpts& replOpts) {
	WrSerializer ser, serReplOpts;
	nsDef.GetJSON(ser);
	replOpts.GetJSON(serReplOpts);
	auto status = conn_.Call(mkCommand(cproto::kCmdOpenNamespace, &ctx), ser.Slice(), serReplOpts.Slice()).Status();

	if (!status.ok()) return status;

	namespaces_->Add(nsDef.name);
	return errOK;
}

Error CoroRPCClient::OpenNamespace(std::string_view nsName, const InternalRdxContext& ctx, const StorageOpts& sopts,
								   const NsReplicationOpts& replOpts) {
	NamespaceDef nsDef(string(nsName), sopts);
	return AddNamespace(nsDef, ctx, replOpts);
}

Error CoroRPCClient::CloseNamespace(std::string_view nsName, const InternalRdxContext& ctx) {
	return conn_.Call(mkCommand(cproto::kCmdCloseNamespace, &ctx), nsName).Status();
}

Error CoroRPCClient::DropNamespace(std::string_view nsName, const InternalRdxContext& ctx) {
	return conn_.Call(mkCommand(cproto::kCmdDropNamespace, &ctx), nsName).Status();
}

Error CoroRPCClient::CreateTemporaryNamespace(std::string_view baseName, std::string& resultName, const InternalRdxContext& ctx,
											  const StorageOpts& opts, lsn_t version) {
	try {
		NamespaceDef nsDef(std::string(baseName), opts);
		WrSerializer ser;
		nsDef.GetJSON(ser);
		auto ans = conn_.Call(mkCommand(cproto::kCmdCreateTmpNamespace, &ctx), ser.Slice(), int64_t(version));

		if (ans.Status().ok()) {
			resultName = ans.GetArgs(1)[0].As<std::string>();
			namespaces_->Add(resultName);
		}
		return ans.Status();
	} catch (const Error& err) {
		return err;
	}
}

Error CoroRPCClient::TruncateNamespace(std::string_view nsName, const InternalRdxContext& ctx) {
	return conn_.Call(mkCommand(cproto::kCmdTruncateNamespace, &ctx), nsName).Status();
}

Error CoroRPCClient::RenameNamespace(std::string_view srcNsName, const std::string& dstNsName, const InternalRdxContext& ctx) {
	auto status = conn_.Call(mkCommand(cproto::kCmdRenameNamespace, &ctx), srcNsName, dstNsName).Status();
	if (!status.ok() && status.code() != errTimeout) return status;
	if (srcNsName != dstNsName) {
		namespaces_->Erase(srcNsName);
		namespaces_->Erase(dstNsName);
	}
	return status;
}

Error CoroRPCClient::Insert(std::string_view nsName, Item& item, const InternalRdxContext& ctx) {
	return modifyItem(nsName, item, nullptr, ModeInsert, config_.NetTimeout, ctx);
}

Error CoroRPCClient::Insert(std::string_view nsName, client::Item& item, CoroQueryResults& result, const InternalRdxContext& ctx) {
	return modifyItem(nsName, item, &result, ModeInsert, config_.NetTimeout, ctx);
}

Error CoroRPCClient::Update(std::string_view nsName, Item& item, const InternalRdxContext& ctx) {
	return modifyItem(nsName, item, nullptr, ModeUpdate, config_.NetTimeout, ctx);
}
Error CoroRPCClient::Update(std::string_view nsName, client::Item& item, CoroQueryResults& result, const InternalRdxContext& ctx) {
	return modifyItem(nsName, item, &result, ModeUpdate, config_.NetTimeout, ctx);
}

Error CoroRPCClient::Upsert(std::string_view nsName, Item& item, const InternalRdxContext& ctx) {
	return modifyItem(nsName, item, nullptr, ModeUpsert, config_.NetTimeout, ctx);
}
Error CoroRPCClient::Upsert(std::string_view nsName, client::Item& item, CoroQueryResults& result, const InternalRdxContext& ctx) {
	return modifyItem(nsName, item, &result, ModeUpsert, config_.NetTimeout, ctx);
}

Error CoroRPCClient::Delete(std::string_view nsName, Item& item, const InternalRdxContext& ctx) {
	return modifyItem(nsName, item, nullptr, ModeDelete, config_.NetTimeout, ctx);
}
Error CoroRPCClient::Delete(std::string_view nsName, client::Item& item, CoroQueryResults& result, const InternalRdxContext& ctx) {
	return modifyItem(nsName, item, &result, ModeDelete, config_.NetTimeout, ctx);
}

Error CoroRPCClient::modifyItem(std::string_view nsName, Item& item, CoroQueryResults* results, int mode, milliseconds netTimeout,
								const InternalRdxContext& ctx) {
	WrSerializer ser;
	item.impl_->GetPrecepts(ser);

	bool withNetTimeout = (netTimeout.count() > 0);
	for (int tryCount = 0;; tryCount++) {
		auto netDeadline = conn_.Now() + netTimeout;
		auto ret = conn_.Call(mkCommand(cproto::kCmdModifyItem, netTimeout, &ctx), nsName, int(FormatCJson), item.GetCJSON(), mode,
							  ser.Slice(), item.GetStateToken(), 0);
		if (ret.Status().ok()) {
			try {
				auto args = ret.GetArgs(2);
				CoroQueryResults qr(&conn_, {getNamespace(nsName)}, p_string(args[0]), int(args[1]), 0, config_.FetchAmount,
									config_.NetTimeout);
				if (qr.Status().ok()) {
					for (std::string_view ns : qr.GetNamespaces()) {
						getNamespace(ns)->UpdateTagsMatcher(qr.GetTagsMatcher(ns));
					}
				}
				if (results) {
					*results = std::move(qr);
					return results->Status();
				}
				if (qr.Count() == 1) {
					if ((qr.queryParams_.flags & kResultsFormatMask) != kResultsPure) {
						item = qr.begin().GetItem();
					} else {
						item.setID(qr.begin().GetItem().GetID());
					}
				}
				return qr.Status();
			} catch (const Error& err) {
				return err;
			}
		} else {
			if (ret.Status().code() != errStateInvalidated || tryCount > 2) {
				return ret.Status();
			}
			if (withNetTimeout) {
				netTimeout = std::chrono::duration_cast<std::chrono::milliseconds>(netDeadline - conn_.Now());
			}
			CoroQueryResults qr;
			InternalRdxContext ctxCompl = ctx.WithCompletion(nullptr).WithShardId(ShardingKeyType::ShardingProxyOff, false);
			auto err = selectImpl(Query(string(nsName)).Limit(0), qr, netTimeout, ctxCompl);
			if (err.code() == errTimeout) {
				return Error(errTimeout, "Request timeout");
			}
			if (withNetTimeout) {
				netTimeout = std::chrono::duration_cast<std::chrono::milliseconds>(netDeadline - conn_.Now());
			}
			auto newItem = NewItem(nsName);
			if (!newItem.Status().ok()) return newItem.Status();
			err = newItem.FromJSON(item.impl_->GetJSON());
			if (!err.ok()) return err;

			item = std::move(newItem);
		}
	}
}

Item CoroRPCClient::NewItem(std::string_view nsName) {
	try {
		return getNamespace(nsName)->NewItem();
	} catch (const Error& err) {
		return Item(err);
	}
}

Error CoroRPCClient::GetMeta(std::string_view nsName, const string& key, string& data, const InternalRdxContext& ctx) {
	try {
		auto ret = conn_.Call(mkCommand(cproto::kCmdGetMeta, &ctx), nsName, key);
		if (ret.Status().ok()) {
			data = ret.GetArgs(1)[0].As<string>();
		}
		return ret.Status();
	} catch (const Error& err) {
		return err;
	}
}

Error CoroRPCClient::PutMeta(std::string_view nsName, const string& key, std::string_view data, const InternalRdxContext& ctx) {
	return conn_.Call(mkCommand(cproto::kCmdPutMeta, &ctx), nsName, key, data).Status();
}

Error CoroRPCClient::EnumMeta(std::string_view nsName, vector<string>& keys, const InternalRdxContext& ctx) {
	try {
		auto ret = conn_.Call(mkCommand(cproto::kCmdEnumMeta, &ctx), nsName);
		if (ret.Status().ok()) {
			auto args = ret.GetArgs();
			keys.clear();
			keys.reserve(args.size());
			for (auto& k : args) {
				keys.push_back(k.As<string>());
			}
		}
		return ret.Status();
	} catch (const Error& err) {
		return err;
	}
}

Error CoroRPCClient::Delete(const Query& query, CoroQueryResults& result, const InternalRdxContext& ctx) {
	WrSerializer ser;
	query.Serialize(ser);

	CoroQueryResults::NsArray nsArray;
	query.WalkNested(true, true, [this, &nsArray](const Query& q) { nsArray.push_back(getNamespace(q._namespace)); });

	result = CoroQueryResults(&conn_, std::move(nsArray), 0, config_.FetchAmount, config_.NetTimeout);

	auto ret =
		conn_.Call(mkCommand(cproto::kCmdDeleteQuery, &ctx), ser.Slice(), kResultsWithItemID | kResultsWithPayloadTypes | kResultsCJson);
	try {
		if (ret.Status().ok()) {
			auto args = ret.GetArgs(2);
			result.Bind(p_string(args[0]), int(args[1]));
		}
	} catch (const Error& err) {
		return err;
	}
	return ret.Status();
}

Error CoroRPCClient::Update(const Query& query, CoroQueryResults& result, const InternalRdxContext& ctx) {
	WrSerializer ser;
	query.Serialize(ser);

	CoroQueryResults::NsArray nsArray;
	query.WalkNested(true, true, [this, &nsArray](const Query& q) { nsArray.push_back(getNamespace(q._namespace)); });

	result = CoroQueryResults(&conn_, std::move(nsArray), 0, config_.FetchAmount, config_.NetTimeout);
	auto ret =
		conn_.Call(mkCommand(cproto::kCmdUpdateQuery, &ctx), ser.Slice(), kResultsWithItemID | kResultsWithPayloadTypes | kResultsCJson);
	try {
		if (ret.Status().ok()) {
			auto args = ret.GetArgs(2);
			result.Bind(p_string(args[0]), int(args[1]));
			for (size_t i = 0; i < nsArray.size(); ++i) {
				getNamespace(nsArray[i]->name)->UpdateTagsMatcher(result.GetTagsMatcher(i));
			}
		}
	} catch (const Error& err) {
		return err;
	}
	return ret.Status();
}

void vec2pack(const h_vector<int32_t, 4>& vec, WrSerializer& ser) {
	// Get array of payload Type Versions
	ser.PutVarUint(vec.size());
	for (auto v : vec) ser.PutVarUint(v);
	return;
}

Error CoroRPCClient::Select(std::string_view querySQL, CoroQueryResults& result, const InternalRdxContext& ctx) {
	try {
		Query query;
		query.FromSQL(querySQL);
		switch (query.type_) {
			case QuerySelect:
				return Select(query, result, ctx);
			case QueryDelete:
				return Delete(query, result, ctx);

			case QueryUpdate:
				return Update(query, result, ctx);
			case QueryTruncate:
				return TruncateNamespace(query._namespace, ctx);
			default:
				return Error(errParams, "Incorrect qyery type");
		}
	} catch (const Error& err) {
		return err;
	}
}

Error CoroRPCClient::selectImpl(const Query& query, CoroQueryResults& result, milliseconds netTimeout, const InternalRdxContext& ctx) {
	bool hasJoins = !query.joinQueries_.empty();
	if (!hasJoins) {
		for (auto& mq : query.mergeQueries_) {
			if (!mq.joinQueries_.empty()) {
				hasJoins = true;
				break;
			}
		}
	}
	int flags = result.fetchFlags_ ? result.fetchFlags_ : (kResultsWithPayloadTypes | kResultsCJson);
	if (hasJoins) {
		flags |= kResultsWithItemID;
	}
	CoroQueryResults::NsArray nsArray;
	WrSerializer qser;
	query.Serialize(qser);
	query.WalkNested(true, true, [this, &nsArray](const Query& q) { nsArray.push_back(getNamespace(q._namespace)); });
	h_vector<int32_t, 4> vers;
	for (auto& ns : nsArray) {
		auto tm = ns->GetTagsMatcher();
		vers.push_back(tm.version() ^ tm.stateToken());
	}
	WrSerializer pser;
	vec2pack(vers, pser);
	result = CoroQueryResults(&conn_, std::move(nsArray), result.fetchFlags_, config_.FetchAmount, config_.NetTimeout);

	auto ret = conn_.Call(mkCommand(cproto::kCmdSelect, netTimeout, &ctx), qser.Slice(), flags, config_.FetchAmount, pser.Slice());
	try {
		if (ret.Status().ok()) {
			auto args = ret.GetArgs(2);
			result.Bind(p_string(args[0]), int(args[1]));
		}
	} catch (const Error& err) {
		return err;
	}
	return ret.Status();
}

Error CoroRPCClient::Commit(std::string_view nsName, const InternalRdxContext& ctx) {
	return conn_.Call(mkCommand(cproto::kCmdCommit, &ctx), nsName).Status();
}

Error CoroRPCClient::AddIndex(std::string_view nsName, const IndexDef& iDef, const InternalRdxContext& ctx) {
	WrSerializer ser;
	iDef.GetJSON(ser);
	return conn_.Call(mkCommand(cproto::kCmdAddIndex, &ctx), nsName, ser.Slice()).Status();
}

Error CoroRPCClient::UpdateIndex(std::string_view nsName, const IndexDef& iDef, const InternalRdxContext& ctx) {
	WrSerializer ser;
	iDef.GetJSON(ser);
	return conn_.Call(mkCommand(cproto::kCmdUpdateIndex, &ctx), nsName, ser.Slice()).Status();
}

Error CoroRPCClient::DropIndex(std::string_view nsName, const IndexDef& idx, const InternalRdxContext& ctx) {
	return conn_.Call(mkCommand(cproto::kCmdDropIndex, &ctx), nsName, idx.name_).Status();
}

Error CoroRPCClient::SetSchema(std::string_view nsName, std::string_view schema, const InternalRdxContext& ctx) {
	return conn_.Call(mkCommand(cproto::kCmdSetSchema, &ctx), nsName, schema).Status();
}

Error CoroRPCClient::GetSchema(std::string_view nsName, int format, std::string& schema, const InternalRdxContext& ctx) {
	try {
		auto ret = conn_.Call(mkCommand(cproto::kCmdGetSchema, &ctx), nsName, format);
		if (ret.Status().ok()) {
			schema = ret.GetArgs(1)[0].As<string>();
		}
		return ret.Status();
	} catch (const Error& err) {
		return err;
	}
}

Error CoroRPCClient::EnumNamespaces(vector<NamespaceDef>& defs, EnumNamespacesOpts opts, const InternalRdxContext& ctx) {
	try {
		auto ret = conn_.Call(mkCommand(cproto::kCmdEnumNamespaces, &ctx), int(opts.options_), p_string(&opts.filter_));
		if (ret.Status().ok()) {
			gason::JsonParser parser;
			auto json = ret.GetArgs(1)[0].As<string>();
			auto root = parser.Parse(giftStr(json));

			for (auto& nselem : root["items"]) {
				NamespaceDef def;
				def.FromJSON(nselem);
				defs.emplace_back(std::move(def));
			}
		}
		return ret.Status();
	} catch (const Error& err) {
		return err;
	} catch (const gason::Exception& err) {
		return Error(errParseJson, "EnumNamespaces: %s", err.what());
	}
}

Error CoroRPCClient::EnumDatabases(vector<string>& dbList, const InternalRdxContext& ctx) {
	try {
		auto ret = conn_.Call(mkCommand(cproto::kCmdEnumDatabases, &ctx), 0);
		if (ret.Status().ok()) {
			gason::JsonParser parser;
			auto json = ret.GetArgs(1)[0].As<string>();
			auto root = parser.Parse(giftStr(json));
			for (auto& elem : root["databases"]) {
				dbList.emplace_back(elem.As<string>());
			}
		}
		return ret.Status();
	} catch (const Error& err) {
		return err;
	} catch (const gason::Exception& err) {
		return Error(errParseJson, "EnumDatabases: %s", err.what());
	}
}

Error CoroRPCClient::GetSqlSuggestions(std::string_view query, int pos, std::vector<std::string>& suggests) {
	try {
		auto ret = conn_.Call(mkCommand(cproto::kCmdGetSQLSuggestions), query, pos);
		if (ret.Status().ok()) {
			auto rargs = ret.GetArgs();
			suggests.clear();
			suggests.reserve(rargs.size());

			for (auto& rarg : rargs) suggests.push_back(rarg.As<string>());
		}
		return ret.Status();
	} catch (const Error& err) {
		return err;
	}
}

Error CoroRPCClient::Status(bool forceCheck, const InternalRdxContext& ctx) {
	if (!conn_.IsRunning()) {
		return Error(errParams, "Client is not running");
	}
	return conn_.Status(forceCheck, config_.NetTimeout, ctx.execTimeout(), ctx.getCancelCtx());
}

Namespace* CoroRPCClient::getNamespace(std::string_view nsName) { return namespaces_->Get(nsName); }

cproto::CommandParams CoroRPCClient::mkCommand(cproto::CmdCode cmd, const InternalRdxContext* ctx) const noexcept {
	return mkCommand(cmd, config_.NetTimeout, ctx);
}

cproto::CommandParams CoroRPCClient::mkCommand(cproto::CmdCode cmd, milliseconds netTimeout, const InternalRdxContext* ctx) noexcept {
	if (ctx) {
		return {cmd,
				std::max(netTimeout, ctx->execTimeout()),
				ctx->execTimeout(),
				ctx->lsn(),
				ctx->emmiterServerId(),
				ctx->shardId(),
				ctx->getCancelCtx(),
				ctx->IsShardingParallelExecution()};
	}
	return {cmd, netTimeout, std::chrono::milliseconds(0), lsn_t(), -1, IndexValueType::NotSet, nullptr, false};
}

CoroTransaction CoroRPCClient::NewTransaction(std::string_view nsName, const InternalRdxContext& ctx) {
	auto ret = conn_.Call(mkCommand(cproto::kCmdStartTransaction, &ctx), nsName);
	auto err = ret.Status();
	if (err.ok()) {
		try {
			auto args = ret.GetArgs(1);
			return CoroTransaction(this, int64_t(args[0]), config_.NetTimeout, ctx.execTimeout(), getNamespace(nsName));
		} catch (Error& e) {
			err = std::move(e);
		}
	}
	return CoroTransaction(std::move(err));
}

Error CoroRPCClient::CommitTransaction(CoroTransaction& tr, CoroQueryResults& result, const InternalRdxContext& ctx) {
	Error returnErr;
	if (tr.rpcClient_) {
		result = CoroQueryResults(&tr.rpcClient_->conn_, {tr.ns_}, 0, config_.FetchAmount, config_.NetTimeout);
		auto ret = tr.rpcClient_->conn_.Call(mkCommand(cproto::kCmdCommitTx, &ctx), tr.txId_);
		returnErr = ret.Status();
		try {
			if (ret.Status().ok()) {
				auto args = ret.GetArgs(2);
				result.Bind(p_string(args[0]), int(args[1]));
				tr.ns_->UpdateTagsMatcher(result.GetTagsMatcher(0));
			}
		} catch (const Error& err) {
			returnErr = err;
		}
	} else {
		returnErr = Error(errLogic, "connection is nullptr");
	}
	tr.clear();
	return returnErr;
}

Error CoroRPCClient::RollBackTransaction(CoroTransaction& tr, const InternalRdxContext& ctx) {
	Error ret;
	if (tr.rpcClient_) {
		ret = tr.rpcClient_->conn_.Call(mkCommand(cproto::kCmdRollbackTx, &ctx), tr.txId_).Status();
	} else {
		ret = Error(errLogic, "connection is nullptr");
	}
	tr.clear();
	return ret;
}

Error CoroRPCClient::GetReplState(std::string_view nsName, ReplicationStateV2& state, const InternalRdxContext& ctx) {
	WrSerializer ser;
	auto ret = conn_.Call(mkCommand(cproto::kCmdGetReplState, &ctx), nsName);
	if (ret.Status().ok()) {
		try {
			auto json = ret.GetArgs(1)[0].As<string>();
			state.FromJSON(giftStr(json));
		} catch (Error& err) {
			return err;
		}
	}
	return ret.Status();
}

Error CoroRPCClient::SetClusterizationStatus(std::string_view nsName, const ClusterizationStatus& status, const InternalRdxContext& ctx) {
	WrSerializer ser;
	status.GetJSON(ser);
	return conn_.Call(mkCommand(cproto::kCmdSetClusterizationStatus, &ctx), nsName, ser.Slice()).Status();
}

Error CoroRPCClient::GetSnapshot(std::string_view nsName, const SnapshotOpts& opts, Snapshot& snapshot, const InternalRdxContext& ctx) {
	WrSerializer ser;
	opts.GetJSON(ser);
	auto ret = conn_.Call(mkCommand(cproto::kCmdGetSnapshot, &ctx), nsName, ser.Slice());
	try {
		if (ret.Status().ok()) {
			auto args = ret.GetArgs(3);
			int64_t count = int64_t(args[1]);
			snapshot = Snapshot(&conn_, int(args[0]), count, int64_t(args[2]), lsn_t(int64_t(args[3])),
								count > 0 ? p_string(args[4]) : p_string(), config_.NetTimeout);
		}
	} catch (const Error& err) {
		return err;
	}
	return ret.Status();
}

Error CoroRPCClient::ApplySnapshotChunk(std::string_view nsName, const SnapshotChunk& ch, const InternalRdxContext& ctx) {
	WrSerializer ser;
	ch.Serilize(ser);
	return conn_.Call(mkCommand(cproto::kCmdApplySnapshotCh, &ctx), nsName, ser.Slice()).Status();
}

Error CoroRPCClient::SetTagsMatcher(std::string_view nsName, TagsMatcher&& tm, const InternalRdxContext& ctx) {
	WrSerializer ser;
	tm.serialize(ser);
	auto err = conn_.Call(mkCommand(cproto::kCmdSetTagsMatcher, &ctx), nsName, int64_t(tm.stateToken()), int64_t(tm.version()), ser.Slice())
				   .Status();
	if (err.ok()) {
		getNamespace(nsName)->TryReplaceTagsMatcher(std::move(tm), false);
	}
	return err;
}

Error CoroRPCClient::SuggestLeader(const NodeData& suggestion, NodeData& response, const InternalRdxContext& ctx) {
	WrSerializer ser;
	suggestion.GetJSON(ser);
	auto ret = conn_.Call(mkCommand(cproto::kCmdSuggestLeader, &ctx), ser.Slice());
	if (ret.Status().ok()) {
		try {
			gason::JsonParser parser;
			auto json = ret.GetArgs(1)[0].As<string>();
			auto root = parser.Parse(giftStr(json));
			response.FromJSON(root);
		} catch (Error& err) {
			return err;
		}
	}
	return ret.Status();
}

Error CoroRPCClient::ClusterControlRequest(const ClusterControlRequestData& request, const InternalRdxContext& ctx) {
	WrSerializer ser;
	request.GetJSON(ser);
	return conn_.Call(mkCommand(cproto::kCmdClusterControlRequest, &ctx), ser.Slice()).Status();
}

int64_t CoroRPCClient::AddConnectionStateObserver(ConnectionStateHandlerT callback) {
	do {
		const auto tm = std::chrono::steady_clock::now().time_since_epoch().count();
		if (observers_.find(tm) == observers_.end()) {
			observers_.emplace(std::make_pair(tm, std::move(callback)));
			return tm;
		}
	} while (true);
}

Error CoroRPCClient::RemoveConnectionStateObserver(int64_t id) {
	return observers_.erase(id) ? Error() : Error(errNotFound, "Callback with id %d does not exist", id);
}

Error CoroRPCClient::LeadersPing(const CoroRPCClient::NodeData& leader, const InternalRdxContext& ctx) {
	WrSerializer ser;
	leader.GetJSON(ser);
	return conn_.Call(mkCommand(cproto::kCmdLeadersPing, &ctx), ser.Slice()).Status();
}

Error CoroRPCClient::GetRaftInfo(RaftInfo& info, const InternalRdxContext& ctx) {
	auto ret = conn_.Call(mkCommand(cproto::kCmdGetRaftInfo, &ctx));
	if (ret.Status().ok()) {
		try {
			gason::JsonParser parser;
			auto json = ret.GetArgs(1)[0].As<string>();
			auto root = parser.Parse(giftStr(json));
			info.FromJSON(root);
		} catch (Error& err) {
			return err;
		}
	}
	return ret.Status();
}

}  // namespace client
}  // namespace reindexer
