#include "client/rpcclient.h"
#include "client/connectopts.h"
#include "client/itemimplbase.h"
#include "client/snapshot.h"
#include "cluster/clustercontrolrequest.h"
#include "cluster/sharding/shardingcontrolrequest.h"
#include "core/namespace/namespacestat.h"
#include "core/namespacedef.h"
#include "core/schema.h"
#include "estl/dummy_mutex.h"
#include "estl/gift_str.h"
#include "gason/gason.h"
#include "tools/catch_and_return.h"
#include "tools/cpucheck.h"
#include "tools/errors.h"

namespace reindexer {
namespace client {

RPCClient::RPCClient(const ReindexerConfig& config, INamespaces::PtrT sharedNamespaces)
	: namespaces_(sharedNamespaces ? std::move(sharedNamespaces) : INamespaces::PtrT(new NamespacesImpl<DummyMutex>())), config_(config) {
	reindexer::CheckRequiredSSESupport();

	conn_.SetConnectionStateHandler([this](Error err) { onConnectionState(std::move(err)); });
}

RPCClient::~RPCClient() { Stop(); }

Error RPCClient::Connect(const DSN& dsn, ev::dynamic_loop& loop, const client::ConnectOpts& opts) {
	using namespace std::string_view_literals;

	if (coroutine::current() == 0) {
		return Error(errLogic, "Coroutine client's Connect can't be called from main routine (coroutine ID is 0). DSN: {}", dsn);
	}

	lock_guard lck(mtx_);
	if (conn_.IsRunning()) {
		return Error(errLogic, "Client is already started. DSN: {}", dsn);
	}

	cproto::CoroClientConnection::ConnectData connectData{.uri = dsn.Parser(), .opts = {}};
	if (!connectData.uri.isValid()) {
		return Error(errParams, "{} is not valid uri", dsn);
	}
#ifdef _WIN32
	if (connectData.uri.scheme() != "cproto"sv && connectData.uri.scheme() != "cprotos"sv) {
		return Error(errParams, "Scheme must be cproto or cprotos");
	}
#else
	if (connectData.uri.scheme() != "cproto"sv && connectData.uri.scheme() != "ucproto"sv && connectData.uri.scheme() != "cprotos"sv) {
		return Error(errParams, "Scheme must be either cproto, cprotos or ucproto");
	}
#endif

	connectData.opts = cproto::CoroClientConnection::Options(
		config_.NetTimeout, config_.NetTimeout, opts.IsCreateDBIfMissing(), opts.HasExpectedClusterID(), opts.ExpectedClusterID(),
		config_.ReconnectAttempts, config_.EnableCompression, config_.RequestDedicatedThread, config_.AppName, config_.ReplToken);
	conn_.Start(loop, std::move(connectData));
	loop_ = &loop;
	return errOK;
}

void RPCClient::Stop() {
	if (conn_.IsRunning()) {
		lock_guard lck(mtx_);
		terminate_ = true;
		conn_.Stop();
		loop_ = nullptr;
		terminate_ = false;
	}
}

Error RPCClient::AddNamespace(const NamespaceDef& nsDef, const InternalRdxContext& ctx, const NsReplicationOpts& replOpts) {
	WrSerializer ser, serReplOpts;
	nsDef.GetJSON(ser);
	replOpts.GetJSON(serReplOpts);
	auto status = conn_.Call(mkCommand(cproto::kCmdOpenNamespace, &ctx), ser.Slice(), serReplOpts.Slice()).Status();

	if (!status.ok()) {
		return status;
	}

	namespaces_->Add(nsDef.name);
	return errOK;
}

Error RPCClient::OpenNamespace(std::string_view nsName, const InternalRdxContext& ctx, const StorageOpts& sopts,
							   const NsReplicationOpts& replOpts) {
	NamespaceDef nsDef(std::string(nsName), sopts);
	return AddNamespace(nsDef, ctx, replOpts);
}

Error RPCClient::CloseNamespace(std::string_view nsName, const InternalRdxContext& ctx) {
	return conn_.Call(mkCommand(cproto::kCmdCloseNamespace, &ctx), nsName).Status();
}

Error RPCClient::DropNamespace(std::string_view nsName, const InternalRdxContext& ctx) {
	auto status = conn_.Call(mkCommand(cproto::kCmdDropNamespace, &ctx), nsName).Status();
	if (status.ok()) {
		namespaces_->Erase(nsName);
	}
	return status;
}

Error RPCClient::CreateTemporaryNamespace(std::string_view baseName, std::string& resultName, const InternalRdxContext& ctx,
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

Error RPCClient::TruncateNamespace(std::string_view nsName, const InternalRdxContext& ctx) {
	return conn_.Call(mkCommand(cproto::kCmdTruncateNamespace, &ctx), nsName).Status();
}

Error RPCClient::RenameNamespace(std::string_view srcNsName, std::string_view dstNsName, const InternalRdxContext& ctx) {
	auto status = conn_.Call(mkCommand(cproto::kCmdRenameNamespace, &ctx), srcNsName, dstNsName).Status();
	if (!status.ok() && status.code() != errTimeout) {
		return status;
	}
	if (srcNsName != dstNsName) {
		namespaces_->Erase(srcNsName);
		namespaces_->Erase(dstNsName);
	}
	return status;
}

Error RPCClient::Insert(std::string_view nsName, Item& item, RPCDataFormat format, const InternalRdxContext& ctx) {
	return (format == RPCDataFormat::CJSON) ? modifyItemCJSON(nsName, item, nullptr, ModeInsert, config_.NetTimeout, ctx)
											: modifyItemFormat(nsName, item, format, ModeInsert, config_.NetTimeout, ctx);
}

Error RPCClient::Insert(std::string_view nsName, client::Item& item, CoroQueryResults& result, const InternalRdxContext& ctx) {
	return modifyItemCJSON(nsName, item, &result, ModeInsert, config_.NetTimeout, ctx);
}

Error RPCClient::Update(std::string_view nsName, Item& item, RPCDataFormat format, const InternalRdxContext& ctx) {
	return (format == RPCDataFormat::CJSON) ? modifyItemCJSON(nsName, item, nullptr, ModeUpdate, config_.NetTimeout, ctx)
											: modifyItemFormat(nsName, item, format, ModeUpdate, config_.NetTimeout, ctx);
}
Error RPCClient::Update(std::string_view nsName, client::Item& item, CoroQueryResults& result, const InternalRdxContext& ctx) {
	return modifyItemCJSON(nsName, item, &result, ModeUpdate, config_.NetTimeout, ctx);
}

Error RPCClient::Upsert(std::string_view nsName, Item& item, RPCDataFormat format, const InternalRdxContext& ctx) {
	return (format == RPCDataFormat::CJSON) ? modifyItemCJSON(nsName, item, nullptr, ModeUpsert, config_.NetTimeout, ctx)
											: modifyItemFormat(nsName, item, format, ModeUpsert, config_.NetTimeout, ctx);
}
Error RPCClient::Upsert(std::string_view nsName, client::Item& item, CoroQueryResults& result, const InternalRdxContext& ctx) {
	return modifyItemCJSON(nsName, item, &result, ModeUpsert, config_.NetTimeout, ctx);
}

Error RPCClient::Delete(std::string_view nsName, Item& item, RPCDataFormat format, const InternalRdxContext& ctx) {
	return (format == RPCDataFormat::CJSON) ? modifyItemCJSON(nsName, item, nullptr, ModeDelete, config_.NetTimeout, ctx)
											: modifyItemFormat(nsName, item, format, ModeDelete, config_.NetTimeout, ctx);
}
Error RPCClient::Delete(std::string_view nsName, client::Item& item, CoroQueryResults& result, const InternalRdxContext& ctx) {
	return modifyItemCJSON(nsName, item, &result, ModeDelete, config_.NetTimeout, ctx);
}

Error RPCClient::modifyItemCJSON(std::string_view nsName, Item& item, CoroQueryResults* results, int mode, milliseconds netTimeout,
								 const InternalRdxContext& ctx) {
	WrSerializer ser;
	item.impl_->GetPrecepts(ser);

	bool withNetTimeout = (netTimeout.count() > 0);
	for (int tryCount = 0;; tryCount++) {
		auto netDeadline = conn_.Now() + netTimeout;
		auto ret = conn_.Call(mkCommand(cproto::kCmdModifyItem, netTimeout, &ctx), nsName, int(DataFormat::FormatCJson), item.GetCJSON(),
							  mode, ser.Slice(), item.GetStateToken(), 0);
		if (ret.Status().ok()) {
			try {
				const auto args = ret.GetArgs(2);
				CoroQueryResults qr(nullptr, &conn_, {getNamespace(nsName)}, p_string(args[0]),
									RPCQrId{int(args[1]), args.size() > 2 ? int64_t(args[2]) : -1}, 0, config_.FetchAmount,
									config_.NetTimeout, false);
				assertrx(qr.IsBound());	 // Bind must always happens inside QR's constructor. Just extra check
				if (results) {
					*results = std::move(qr);
					return results->Status();
				}
				if (qr.Count() == 1) {
					auto it = qr.begin();
					if ((qr.i_.queryParams_.flags & kResultsFormatMask) != kResultsPure) {
						item = it.GetItem();
					} else {
						item.setID(it.GetID());
						item.setShardID(it.GetShardID());
						item.setLSN(it.GetLSN());
						item.impl_->setTagsMatcher(qr.GetTagsMatcher(0));
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
			InternalRdxContext ctxCompl = ctx.WithCompletion(nullptr).WithShardId(ShardingKeyType::ProxyOff, false);
			auto err = selectImpl(Query(std::string(nsName)).Limit(0), qr, netTimeout, ctxCompl);
			if (err.code() == errTimeout) {
				return Error(errTimeout, "Request timeout");
			}
			if (withNetTimeout) {
				netTimeout = std::chrono::duration_cast<std::chrono::milliseconds>(netDeadline - conn_.Now());
			}
			auto newItem = NewItem(nsName);
			if (!newItem.Status().ok()) {
				return newItem.Status();
			}
			err = newItem.FromJSON(item.impl_->GetJSON());
			if (!err.ok()) {
				return err;
			}
			if (item.impl_->tagsMatcher().isUpdated()) {
				// Add new names missing in JSON from tm
				newItem.impl_->addTagNamesFrom(item.impl_->tagsMatcher());
			}
			item = std::move(newItem);
		}
	}
}

Error RPCClient::modifyItemFormat(std::string_view nsName, Item& item, RPCDataFormat format, int mode, milliseconds netTimeout,
								  const InternalRdxContext& ctx) {
	WrSerializer ser;
	item.impl_->GetPrecepts(ser);

	std::string_view data;
	switch (format) {
		case RPCDataFormat::MsgPack:
			data = item.GetMsgPack();
			break;
		case RPCDataFormat::CJSON:
			return Error(errParams, "Unsupported format: {}", int(format));
	}
	auto ret = conn_.Call(mkCommand(cproto::kCmdModifyItem, netTimeout, &ctx), nsName, int(format), data, mode, ser.Slice(),
						  item.GetStateToken(), 0);
	if (ret.Status().ok()) {
		try {
			const auto args = ret.GetArgs(2);
			CoroQueryResults qr(nullptr, &conn_, {getNamespace(nsName)}, p_string(args[0]),
								RPCQrId{int(args[1]), args.size() > 2 ? int64_t(args[2]) : -1}, 0, config_.FetchAmount, config_.NetTimeout,
								false);
			if (qr.Count() == 1) {
				auto it = qr.begin();
				if ((qr.i_.queryParams_.flags & kResultsFormatMask) != kResultsPure) {
					item = it.GetItem();
				} else {
					item.setID(it.GetID());
					item.setShardID(it.GetShardID());
					item.setLSN(it.GetLSN());
					item.impl_->setTagsMatcher(qr.GetTagsMatcher(0));
				}
			}
			return qr.Status();
		} catch (const Error& err) {
			return err;
		}
	}
	return ret.Status();
}

Error RPCClient::modifyItemRaw(std::string_view nsName, std::string_view cjson, int mode, std::chrono::milliseconds netTimeout,
							   const InternalRdxContext& ctx) {
	if (ItemImplBase::HasBundledTm(cjson)) {
		return Error(errParams, "Raw CJSON interface does not support CJSON with bundled tags matcher");
	}

	const auto stateToken = getNamespace(nsName)->GetStateToken();
	const auto ret = conn_.Call(mkCommand(cproto::kCmdModifyItem, netTimeout, &ctx), nsName, int(DataFormat::FormatCJson), cjson, mode,
								std::string_view(), stateToken, 0);

	if (!ret.Status().ok()) {
		return ret.Status();
	}

	try {
		const auto args = ret.GetArgs(2);
		const auto rawResult = std::string_view(args[0]);
		ResultSerializer ser(rawResult);
		if (ser.ContainsPayloads()) {
			ResultSerializer::QueryParams qdata;
			ResultSerializer::ParsingData pdata;
			auto nsPtr = getNamespace(nsName);
			ser.GetRawQueryParams(
				qdata,
				[&ser, nsPtr = std::move(nsPtr)](int nsIdx) {
					const uint32_t stateToken = ser.GetVarUInt();
					const int version = ser.GetVarUInt();
					TagsMatcher newTm;
					newTm.deserialize(ser, version, stateToken);
					if (nsIdx != 0) {
						throw Error(errLogic, "Unexpected namespace index in item modification response: {}", nsIdx);
					}
					nsPtr->TryReplaceTagsMatcher(std::move(newTm));
					PayloadType("tmp").clone()->deserialize(ser);
				},
				ResultSerializer::Options{ResultSerializer::LazyMode | ResultSerializer::ClearAggregations}, pdata);
		}
	} catch (const Error& err) {
		return err;
	}
	return Error();
}

Error RPCClient::GetMeta(std::string_view nsName, const std::string& key, std::string& data, const InternalRdxContext& ctx) {
	try {
		auto ret = conn_.Call(mkCommand(cproto::kCmdGetMeta, &ctx), nsName, key);
		if (ret.Status().ok()) {
			data = ret.GetArgs(1)[0].As<std::string>();
		}
		return ret.Status();
	} catch (const Error& err) {
		return err;
	}
}

Error RPCClient::GetMeta(std::string_view nsName, const std::string& key, std::vector<ShardedMeta>& data, const InternalRdxContext& ctx) {
	try {
		const int flags = 1;
		auto ret = conn_.Call(mkCommand(cproto::kCmdGetMeta, &ctx), nsName, key, flags);
		if (ret.Status().ok()) {
			auto args = ret.GetArgs();
			data.reserve(args.size());
			for (auto& k : args) {
				auto json = k.As<std::string>();
				data.emplace_back();
				auto err = data.back().FromJSON(giftStr(json));
				if (!err.ok()) {
					return err;
				}
			}
		}
		return ret.Status();
	} catch (const Error& err) {
		return err;
	}
}

Error RPCClient::PutMeta(std::string_view nsName, const std::string& key, std::string_view data, const InternalRdxContext& ctx) {
	return conn_.Call(mkCommand(cproto::kCmdPutMeta, &ctx), nsName, key, data).Status();
}

Error RPCClient::EnumMeta(std::string_view nsName, std::vector<std::string>& keys, const InternalRdxContext& ctx) {
	try {
		auto ret = conn_.Call(mkCommand(cproto::kCmdEnumMeta, &ctx), nsName);
		if (ret.Status().ok()) {
			auto args = ret.GetArgs();
			keys.clear();
			keys.reserve(args.size());
			for (auto& k : args) {
				keys.emplace_back(k.As<std::string>());
			}
		}
		return ret.Status();
	} catch (const Error& err) {
		return err;
	}
}

Error RPCClient::DeleteMeta(std::string_view nsName, const std::string& key, const InternalRdxContext& ctx) {
	return conn_.Call(mkCommand(cproto::kCmdDeleteMeta, &ctx), nsName, key).Status();
}

Error RPCClient::Delete(const Query& query, CoroQueryResults& result, const InternalRdxContext& ctx) {
	WrSerializer ser;
	query.Serialize(ser);

	CoroQueryResults::NsArray nsArray;
	query.WalkNested(true, true, false, [this, &nsArray](const Query& q) { nsArray.emplace_back(getNamespace(q.NsName())); });

	const int flags = result.i_.fetchFlags_ ? result.i_.fetchFlags_ : (kResultsWithItemID | kResultsWithPayloadTypes | kResultsCJson);
	result = CoroQueryResults(&conn_, std::move(nsArray), flags, config_.FetchAmount, config_.NetTimeout, result.i_.lazyMode_);
	auto ret = conn_.Call(mkCommand(cproto::kCmdDeleteQuery, &ctx), ser.Slice(), flags);
	try {
		if (ret.Status().ok()) {
			const auto args = ret.GetArgs(2);
			result.Bind(p_string(args[0]), RPCQrId{int(args[1]), -1}, &query);
		}
	} catch (const Error& err) {
		return err;
	}
	return ret.Status();
}

Error RPCClient::Update(const Query& query, CoroQueryResults& result, const InternalRdxContext& ctx) {
	WrSerializer ser;
	query.Serialize(ser);

	CoroQueryResults::NsArray nsArray;
	query.WalkNested(true, true, false, [this, &nsArray](const Query& q) { nsArray.push_back(getNamespace(q.NsName())); });

	const int flags = result.i_.fetchFlags_ ? result.i_.fetchFlags_ : (kResultsWithItemID | kResultsWithPayloadTypes | kResultsCJson);
	result = CoroQueryResults(&conn_, std::move(nsArray), flags, config_.FetchAmount, config_.NetTimeout, result.i_.lazyMode_);
	auto ret = conn_.Call(mkCommand(cproto::kCmdUpdateQuery, &ctx), ser.Slice(), flags);
	try {
		if (ret.Status().ok()) {
			const auto args = ret.GetArgs(2);
			result.Bind(p_string(args[0]), RPCQrId{int(args[1]), -1}, &query);
		}
	} catch (const Error& err) {
		return err;
	}
	return ret.Status();
}

void vec2pack(const h_vector<int32_t, 4>& vec, WrSerializer& ser) {
	// Get array of payload Type Versions
	ser.PutVarUint(vec.size());
	for (auto v : vec) {
		ser.PutVarUint(v);
	}
}

Error RPCClient::ExecSQL(std::string_view querySQL, CoroQueryResults& result, const InternalRdxContext& ctx) {
	try {
		auto query = Query::FromSQL(querySQL);
		switch (query.type_) {
			case QuerySelect:
				return Select(query, result, ctx);
			case QueryDelete:
				return Delete(query, result, ctx);
			case QueryUpdate:
				return Update(query, result, ctx);
			case QueryTruncate:
				return TruncateNamespace(query.NsName(), ctx);
			default:
				return Error(errParams, "Incorrect qyery type");
		}
	} catch (const Error& err) {
		return err;
	}
}

Error RPCClient::selectImpl(const Query& query, CoroQueryResults& result, milliseconds netTimeout, const InternalRdxContext& ctx) {
	const int flags = result.i_.fetchFlags_ ? (result.i_.fetchFlags_) : (kResultsWithPayloadTypes | kResultsCJson);
	CoroQueryResults::NsArray nsArray;
	WrSerializer qser;
	query.Serialize(qser);
	query.WalkNested(true, true, false, [this, &nsArray](const Query& q) { nsArray.push_back(getNamespace(q.NsName())); });
	h_vector<int32_t, 4> vers;
	for (auto& ns : nsArray) {
		auto tm = ns->GetTagsMatcher();
		vers.push_back(tm.version() ^ tm.stateToken());
	}
	WrSerializer pser;
	vec2pack(vers, pser);
	const int kInitialFetchAmount = result.FetchAmount() > 0 ? result.FetchAmount() : config_.FetchAmount;
	result =
		CoroQueryResults(&conn_, std::move(nsArray), result.i_.fetchFlags_, kInitialFetchAmount, config_.NetTimeout, result.i_.lazyMode_);

	auto ret = conn_.Call(mkCommand(cproto::kCmdSelect, netTimeout, &ctx), qser.Slice(), flags, kInitialFetchAmount, pser.Slice());
	try {
		if (ret.Status().ok()) {
			const auto args = ret.GetArgs(2);
			if (result.i_.sessionTs_.time_since_epoch().count() == 0) {
				auto ts = conn_.LoginTs();
				if (ts.has_value()) {
					result.i_.sessionTs_ = ts.value();
				} else {
					return Error(errLogic, "LoginTs must contain value.");
				}
			}
			result.Bind(p_string(args[0]), RPCQrId{int(args[1]), args.size() > 2 ? int64_t(args[2]) : -1}, &query);
		}
	} catch (const Error& err) {
		return err;
	}
	return ret.Status();
}

Error RPCClient::AddIndex(std::string_view nsName, const IndexDef& iDef, const InternalRdxContext& ctx) {
	WrSerializer ser;
	iDef.GetJSON(ser);
	return conn_.Call(mkCommand(cproto::kCmdAddIndex, &ctx), nsName, ser.Slice()).Status();
}

Error RPCClient::UpdateIndex(std::string_view nsName, const IndexDef& iDef, const InternalRdxContext& ctx) {
	WrSerializer ser;
	iDef.GetJSON(ser);
	return conn_.Call(mkCommand(cproto::kCmdUpdateIndex, &ctx), nsName, ser.Slice()).Status();
}

Error RPCClient::DropIndex(std::string_view nsName, const IndexDef& idx, const InternalRdxContext& ctx) {
	return conn_.Call(mkCommand(cproto::kCmdDropIndex, &ctx), nsName, idx.Name()).Status();
}

Error RPCClient::SetSchema(std::string_view nsName, std::string_view schema, const InternalRdxContext& ctx) {
	return conn_.Call(mkCommand(cproto::kCmdSetSchema, &ctx), nsName, schema).Status();
}

Error RPCClient::GetSchema(std::string_view nsName, int format, std::string& schema, const InternalRdxContext& ctx) {
	try {
		auto ret = conn_.Call(mkCommand(cproto::kCmdGetSchema, &ctx), nsName, format);
		if (ret.Status().ok()) {
			schema = ret.GetArgs(1)[0].As<std::string>();
		}
		return ret.Status();
	} catch (const Error& err) {
		return err;
	}
}

Error RPCClient::EnumNamespaces(std::vector<NamespaceDef>& defs, EnumNamespacesOpts opts, const InternalRdxContext& ctx) {
	try {
		auto ret = conn_.Call(mkCommand(cproto::kCmdEnumNamespaces, &ctx), int(opts.options_), p_string(&opts.filter_));
		if (ret.Status().ok()) {
			gason::JsonParser parser;
			auto json = ret.GetArgs(1)[0].As<std::string>();
			auto root = parser.Parse(giftStr(json));

			defs.resize(0);
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
		return Error(errParseJson, "EnumNamespaces: {}", err.what());
	}
}

Error RPCClient::EnumDatabases(std::vector<std::string>& dbList, const InternalRdxContext& ctx) {
	try {
		auto ret = conn_.Call(mkCommand(cproto::kCmdEnumDatabases, &ctx), 0);
		if (ret.Status().ok()) {
			gason::JsonParser parser;
			auto json = ret.GetArgs(1)[0].As<std::string>();
			auto root = parser.Parse(giftStr(json));
			dbList.resize(0);
			for (auto& elem : root["databases"]) {
				dbList.emplace_back(elem.As<std::string>());
			}
		}
		return ret.Status();
	} catch (const Error& err) {
		return err;
	} catch (const gason::Exception& err) {
		return Error(errParseJson, "EnumDatabases: {}", err.what());
	}
}

Error RPCClient::GetSqlSuggestions(std::string_view query, int pos, std::vector<std::string>& suggests) {
	try {
		auto ret = conn_.Call(mkCommand(cproto::kCmdGetSQLSuggestions), query, pos);
		if (ret.Status().ok()) {
			auto rargs = ret.GetArgs();
			suggests.clear();
			suggests.reserve(rargs.size());

			for (auto& rarg : rargs) {
				suggests.push_back(rarg.As<std::string>());
			}
		}
		return ret.Status();
	} catch (const Error& err) {
		return err;
	}
}

Error RPCClient::Status(bool forceCheck, const InternalRdxContext& ctx) {
	if (!conn_.IsRunning()) {
		return Error(errParams, "Client is not running");
	}
	return conn_.Status(forceCheck, std::max(config_.NetTimeout, ctx.execTimeout()), ctx.execTimeout(), ctx.getCancelCtx());
}

Error RPCClient::Version(std::string& version, const InternalRdxContext& ctx) {
	if (!conn_.IsRunning()) {
		return Error(errParams, "Client is not running");
	}

	if (auto err = Status(true, ctx); !err.ok()) {
		return err;
	}

	auto versionOpt = conn_.RxServerVersion();
	if (!versionOpt) {
		return Error(errLogic, "Unable to detect the version of the connection server");
	}
	version = std::move(*versionOpt);

	return {};
}

std::shared_ptr<Namespace> RPCClient::getNamespace(std::string_view nsName) { return namespaces_->Get(nsName); }

cproto::CommandParams RPCClient::mkCommand(cproto::CmdCode cmd, const InternalRdxContext* ctx) const noexcept {
	return mkCommand(cmd, config_.NetTimeout, ctx);
}

cproto::CommandParams RPCClient::mkCommand(cproto::CmdCode cmd, cproto::CoroClientConnection::TimePointT requiredTs,
										   const InternalRdxContext* ctx) const noexcept {
	auto params = mkCommand(cmd, config_.NetTimeout, ctx);
	params.requiredLoginTs.emplace(requiredTs);
	return params;
}

cproto::CommandParams RPCClient::mkCommand(cproto::CmdCode cmd, milliseconds netTimeout, const InternalRdxContext* ctx) noexcept {
	if (ctx) {
		return {cmd,
				std::max(netTimeout, ctx->execTimeout()),
				ctx->execTimeout().count() ? ctx->execTimeout() : netTimeout,
				ctx->lsn(),
				ctx->emitterServerId(),
				ctx->shardId(),
				ctx->getCancelCtx(),
				ctx->IsShardingParallelExecution()};
	}
	return {cmd, netTimeout, netTimeout, lsn_t(), -1, ShardingKeyType::NotSetShard, nullptr, false};
}

CoroTransaction RPCClient::NewTransaction(std::string_view nsName, const InternalRdxContext& ctx) noexcept {
	Error err;
	try {
		auto ret = conn_.Call(mkCommand(cproto::kCmdStartTransaction, &ctx), nsName);
		err = ret.Status();
		if (err.ok()) {
			try {
				auto args = ret.GetArgs(1);
				return CoroTransaction(this, int64_t(args[0]), config_.NetTimeout, ctx.execTimeout(), getNamespace(nsName),
									   ctx.emitterServerId());
			} catch (Error& e) {
				err = std::move(e);
			}
		}
	} catch (std::exception& e) {
		err = std::move(e);
	} catch (...) {
		err = Error(errSystem, "Unknow exception in Reindexer client");
	}
	return CoroTransaction(std::move(err));
}

Error RPCClient::CommitTransaction(CoroTransaction& tr, CoroQueryResults& result, const InternalRdxContext& ctx) {
	Error returnErr;
	auto conn = tr.getConn();
	if (conn) {
		const int flags = result.i_.fetchFlags_ ? result.i_.fetchFlags_ : (kResultsWithItemID | kResultsWithPayloadTypes);
		result = CoroQueryResults(conn, {tr.i_.ns_}, flags, config_.FetchAmount, config_.NetTimeout, false);
		auto ret = conn->Call(mkCommand(cproto::kCmdCommitTx, tr.i_.sessionTs_, &ctx), tr.i_.txId_, flags);
		returnErr = ret.Status();
		try {
			if (ret.Status().ok()) {
				auto args = ret.GetArgs(2);
				result.Bind(p_string(args[0]), RPCQrId{int(args[1]), args.size() > 2 ? int64_t(args[2]) : -1}, nullptr);
				tr.i_.ns_->TryReplaceTagsMatcher(result.GetTagsMatcher(0));
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

Error RPCClient::RollBackTransaction(CoroTransaction& tr, const InternalRdxContext& ctx) {
	Error ret;
	auto conn = tr.getConn();
	if (conn) {
		ret = conn->Call(mkCommand(cproto::kCmdRollbackTx, tr.i_.sessionTs_, &ctx), tr.i_.txId_).Status();
	} else {
		ret = Error(errLogic, "connection is nullptr");
	}
	tr.clear();
	return ret;
}

Error RPCClient::GetReplState(std::string_view nsName, ReplicationStateV2& state, const InternalRdxContext& ctx) {
	WrSerializer ser;
	auto ret = conn_.Call(mkCommand(cproto::kCmdGetReplState, &ctx), nsName);
	if (ret.Status().ok()) {
		try {
			auto json = ret.GetArgs(1)[0].As<std::string>();
			state.FromJSON(giftStr(json));
		} catch (Error& err) {
			return err;
		}
	}
	return ret.Status();
}

Error RPCClient::SetClusterOperationStatus(std::string_view nsName, const ClusterOperationStatus& status, const InternalRdxContext& ctx) {
	WrSerializer ser;
	status.GetJSON(ser);
	return conn_.Call(mkCommand(cproto::kCmdSetClusterOperationStatus, &ctx), nsName, ser.Slice()).Status();
}

Error RPCClient::GetSnapshot(std::string_view nsName, const SnapshotOpts& opts, Snapshot& snapshot, const InternalRdxContext& ctx) {
	WrSerializer ser;
	opts.GetJSON(ser);
	auto ret = conn_.Call(mkCommand(cproto::kCmdGetSnapshot, &ctx), nsName, ser.Slice());
	try {
		if (ret.Status().ok()) {
			auto args = ret.GetArgs(3);
			int64_t count = int64_t(args[1]);
			snapshot = Snapshot(&conn_, int(args[0]), count, int64_t(args[2]), lsn_t(int64_t(args[3])),
								count > 0 ? p_string(args[4]) : p_string(), config_.NetTimeout);
			const unsigned nextArgNum = count > 0 ? 5 : 4;
			if (args.size() >= nextArgNum + 1) {
				snapshot.ClusterOperationStat(ClusterOperationStatus{
					.leaderId = int(args[nextArgNum]), .role = reindexer::ClusterOperationStatus::Role(int(args[nextArgNum + 1]))});
			}
		}
	} catch (const Error& err) {
		return err;
	}
	return ret.Status();
}

Error RPCClient::ApplySnapshotChunk(std::string_view nsName, const SnapshotChunk& ch, const InternalRdxContext& ctx) {
	WrSerializer ser;
	ch.Serilize(ser);
	return conn_.Call(mkCommand(cproto::kCmdApplySnapshotCh, &ctx), nsName, ser.Slice()).Status();
}

Error RPCClient::SetTagsMatcher(std::string_view nsName, TagsMatcher&& tm, const InternalRdxContext& ctx) {
	WrSerializer ser;
	tm.serialize(ser);
	auto err = conn_.Call(mkCommand(cproto::kCmdSetTagsMatcher, &ctx), nsName, int64_t(tm.stateToken()), int64_t(tm.version()), ser.Slice())
				   .Status();
	if (err.ok()) {
		getNamespace(nsName)->TryReplaceTagsMatcher(std::move(tm), false);
	}
	return err;
}

Error RPCClient::SuggestLeader(const NodeData& suggestion, NodeData& response, const InternalRdxContext& ctx) {
	WrSerializer ser;
	suggestion.GetJSON(ser);
	auto ret = conn_.Call(mkCommand(cproto::kCmdSuggestLeader, &ctx), ser.Slice());
	if (ret.Status().ok()) {
		try {
			gason::JsonParser parser;
			auto json = ret.GetArgs(1)[0].As<std::string>();
			auto root = parser.Parse(giftStr(json));
			return response.FromJSON(root);
		} catch (Error& err) {
			return err;
		}
	}
	return ret.Status();
}

Error RPCClient::ClusterControlRequest(const ClusterControlRequestData& request, const InternalRdxContext& ctx) {
	WrSerializer ser;
	request.GetJSON(ser);
	return conn_.Call(mkCommand(cproto::kCmdClusterControlRequest, &ctx), ser.Slice()).Status();
}

int64_t RPCClient::AddConnectionStateObserver(ConnectionStateHandlerT callback) {
	do {
		const auto tm = steady_clock_w::now().time_since_epoch().count();
		if (observers_.find(tm) == observers_.end()) {
			observers_.emplace(std::make_pair(tm, std::move(callback)));
			return tm;
		}
	} while (true);
}

Error RPCClient::RemoveConnectionStateObserver(int64_t id) {
	return observers_.erase(id) ? Error() : Error(errNotFound, "Callback with id {} does not exist", id);
}

Error RPCClient::LeadersPing(const RPCClient::NodeData& leader, const InternalRdxContext& ctx) {
	WrSerializer ser;
	leader.GetJSON(ser);
	return conn_.Call(mkCommand(cproto::kCmdLeadersPing, &ctx), ser.Slice()).Status();
}

Error RPCClient::GetRaftInfo(RaftInfo& info, const InternalRdxContext& ctx) {
	auto ret = conn_.Call(mkCommand(cproto::kCmdGetRaftInfo, &ctx));
	if (ret.Status().ok()) {
		try {
			gason::JsonParser parser;
			auto json = ret.GetArgs(1)[0].As<std::string>();
			auto root = parser.Parse(giftStr(json));
			return info.FromJSON(root);
		} catch (Error& err) {
			return err;
		}
	}
	return ret.Status();
}

Error RPCClient::ShardingControlRequest(const sharding::ShardingControlRequestData& request,
										sharding::ShardingControlResponseData& response, const InternalRdxContext& ctx) noexcept {
	try {
		WrSerializer ser;
		request.GetJSON(ser);
		auto ret = conn_.Call(mkCommand(cproto::kShardingControlRequest, &ctx), ser.Slice());
		if (ret.Status().ok()) {
			try {
				if (auto args = ret.GetArgs(); args.size() == 1) {
					auto json = args[0].As<std::string>();
					auto err = response.FromJSON(giftStr(json));
					if (!err.ok()) {
						return err;
					}
				}
			} catch (Error& err) {
				return err;
			}
		}
		return ret.Status();
	}
	CATCH_AND_RETURN
}

}  // namespace client
}  // namespace reindexer
