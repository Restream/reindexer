#include "rpcserver.h"
#include <sys/stat.h>
#include <sstream>
#include "cluster/clustercontrolrequest.h"
#include "cluster/raftmanager.h"
#include "core/cjson/jsonbuilder.h"
#include "core/iclientsstats.h"
#include "core/namespace/namespacestat.h"
#include "core/namespace/snapshot/snapshot.h"
#include "core/transactionimpl.h"
#include "net/cproto/cproto.h"
#include "net/cproto/serverconnection.h"
#include "net/listener.h"
#include "reindexer_version.h"
#include "vendor/msgpack/msgpack.h"

namespace reindexer_server {
using namespace std::string_view_literals;

const reindexer::SemVersion kMinUnknownReplSupportRxVersion("2.6.0");
const size_t kMaxTxCount = 1024;

RPCServer::RPCServer(DBManager &dbMgr, LoggerWrapper &logger, IClientsStats *clientsStats, const ServerConfig &scfg,
					 IStatsWatcher *statsCollector)
	: dbMgr_(dbMgr),
	  serverConfig_(scfg),
	  logger_(logger),
	  statsWatcher_(statsCollector),
	  clientsStats_(clientsStats),
	  startTs_(std::chrono::system_clock::now()) {}

RPCServer::~RPCServer() {}

Error RPCServer::Ping(cproto::Context &) {
	//
	return 0;
}

static std::atomic<int> connCounter = {0};

Error RPCServer::Login(cproto::Context &ctx, p_string login, p_string password, p_string db, cproto::optional<bool> createDBIfMissing,
					   cproto::optional<bool> checkClusterID, cproto::optional<int> expectedClusterID,
					   cproto::optional<p_string> clientRxVersion, cproto::optional<p_string> appName) {
	if (ctx.GetClientData()) {
		return Error(errParams, "Already logged in");
	}

	std::unique_ptr<RPCClientData> clientData(new RPCClientData);

	clientData->connID = connCounter.fetch_add(1, std::memory_order_relaxed);
	clientData->auth = AuthContext(login.toString(), password.toString());
	clientData->txStats = std::make_shared<reindexer::TxStats>();

	auto dbName = db.toString();
	if (checkClusterID.hasValue() && checkClusterID.value()) {
		assert(expectedClusterID.hasValue());
		clientData->auth.SetExpectedClusterID(expectedClusterID.value());
	}
	auto status = dbMgr_.Login(dbName, clientData->auth);
	if (!status.ok()) {
		return status;
	}

	if (clientRxVersion.hasValue()) {
		clientData->rxVersion = SemVersion(std::string_view(clientRxVersion.value()));
	} else {
		clientData->rxVersion = SemVersion();
	}

	if (clientsStats_) {
		reindexer::ClientConnectionStat conn;
		conn.connectionStat = ctx.writer->GetConnectionStat();
		conn.ip = std::string(ctx.clientAddr);
		reindexer::deepCopy(conn.userName, clientData->auth.Login());
		reindexer::deepCopy(conn.dbName, clientData->auth.DBName());
		conn.userRights = std::string(UserRoleName(clientData->auth.UserRights()));
		reindexer::deepCopy(conn.clientVersion, clientData->rxVersion.StrippedString());
		conn.appName = appName.hasValue() ? appName.value().toString() : std::string();
		conn.txStats = clientData->txStats;
		clientsStats_->AddConnection(clientData->connID, std::move(conn));
	}

	ctx.SetClientData(std::move(clientData));
	if (statsWatcher_) {
		statsWatcher_->OnClientConnected(dbName, statsSourceName());
	}
	int64_t startTs = std::chrono::duration_cast<std::chrono::seconds>(startTs_.time_since_epoch()).count();
	static std::string_view version = REINDEX_VERSION;

	status = db.length() ? OpenDatabase(ctx, db, createDBIfMissing) : errOK;
	if (status.ok()) {
		ctx.Return({cproto::Arg(p_string(&version)), cproto::Arg(startTs)}, status);
	} else {
		std::cerr << status.what() << std::endl;
	}

	return status;
}

static RPCClientData *getClientDataUnsafe(cproto::Context &ctx) { return dynamic_cast<RPCClientData *>(ctx.GetClientData()); }

static RPCClientData *getClientDataSafe(cproto::Context &ctx) {
	auto ret = dynamic_cast<RPCClientData *>(ctx.GetClientData());
	if (!ret) std::abort();	 // It should be set by middleware
	return ret;
}

Error RPCServer::OpenDatabase(cproto::Context &ctx, p_string db, cproto::optional<bool> createDBIfMissing) {
	auto *clientData = getClientDataSafe(ctx);
	if (clientData->auth.HaveDB()) {
		return Error(errParams, "Database already opened");
	}
	auto status = dbMgr_.OpenDatabase(db.toString(), clientData->auth, createDBIfMissing.hasValue() && createDBIfMissing.value());
	if (!status.ok()) {
		clientData->auth.ResetDB();
	}
	return status;
}

Error RPCServer::CloseDatabase(cproto::Context &ctx) {
	auto clientData = getClientDataSafe(ctx);
	cleanupTmpNamespaces(*clientData, ctx.clientAddr);
	clientData->auth.ResetDB();
	return errOK;
}
Error RPCServer::DropDatabase(cproto::Context &ctx) {
	auto clientData = getClientDataSafe(ctx);
	return dbMgr_.DropDatabase(clientData->auth);
}

Error RPCServer::CheckAuth(cproto::Context &ctx) {
	cproto::ClientData *ptr = ctx.GetClientData();
	auto clientData = dynamic_cast<RPCClientData *>(ptr);

	if (ctx.call->cmd == cproto::kCmdLogin || ctx.call->cmd == cproto::kCmdPing) {
		return errOK;
	}

	if (!clientData) {
		return Error(errForbidden, "You should login");
	}

	return errOK;
}

void RPCServer::OnClose(cproto::Context &ctx, const Error &err) {
	(void)err;

	auto clientData = getClientDataUnsafe(ctx);
	if (clientData) {
		cleanupTmpNamespaces(*clientData);
	}
	if (clientData && statsWatcher_) {
		statsWatcher_->OnClientDisconnected(clientData->auth.DBName(), statsSourceName());
	}
	if (clientData && clientsStats_) {
		clientsStats_->DeleteConnection(clientData->connID);
	}
	logger_.info("RPC: Client disconnected");
}

void RPCServer::OnResponse(cproto::Context &ctx) {
	if (statsWatcher_) {
		auto clientData = getClientDataUnsafe(ctx);
		auto dbName = (clientData != nullptr) ? clientData->auth.DBName() : "<unknown>";
		statsWatcher_->OnOutputTraffic(dbName, statsSourceName(), ctx.stat.sizeStat.respSizeBytes);
		if (ctx.stat.sizeStat.respSizeBytes) {
			// Don't update stats on responses like "updates push"
			statsWatcher_->OnInputTraffic(dbName, statsSourceName(), ctx.stat.sizeStat.reqSizeBytes);
		}
	}
}

void RPCServer::Logger(cproto::Context &ctx, const Error &err, const cproto::Args &ret) {
	auto clientData = getClientDataUnsafe(ctx);
	WrSerializer ser;

	if (clientData) {
		ser << "c='"sv << clientData->connID << "' db='"sv << clientData->auth.Login() << "@"sv << clientData->auth.DBName() << "' "sv;
	} else {
		ser << "- - "sv;
	}

	if (ctx.call) {
		ser << cproto::CmdName(ctx.call->cmd) << " "sv;
		ctx.call->args.Dump(ser);
	} else {
		ser << '-';
	}

	ser << " -> "sv << (err.ok() ? "OK"sv : err.what());
	if (ret.size()) {
		ser << ' ';
		ret.Dump(ser);
	}

	HandlerStat statDiff = HandlerStat() - ctx.stat.allocStat;
	ser << ' ' << statDiff.GetTimeElapsed() << "us"sv;

	if (serverConfig_.DebugAllocs) {
		ser << " |  allocs: "sv << statDiff.GetAllocsCnt() << ", allocated: " << statDiff.GetAllocsBytes() << " byte(s)";
	}

	logger_.info("{}", ser.Slice());
}

Error RPCServer::OpenNamespace(cproto::Context &ctx, p_string nsDefJson, cproto::optional<p_string> v) {
	NamespaceDef nsDef;
	NsReplicationOpts replOpts;
	Error err;
	if (v.hasValue()) {
		err = replOpts.FromJSON(giftStr(v.value()));
		if (!err.ok()) return err;
	}

	err = nsDef.FromJSON(giftStr(nsDefJson));
	if (!err.ok()) return err;
	if (!nsDef.indexes.empty()) {
		return getDB(ctx, kRoleDataRead).AddNamespace(nsDef, replOpts);
	}
	return getDB(ctx, kRoleDataRead).OpenNamespace(nsDef.name, nsDef.storage, replOpts);
}

Error RPCServer::DropNamespace(cproto::Context &ctx, p_string ns) {
	auto err = getDB(ctx, kRoleDBAdmin).DropNamespace(ns);
	if (err.ok()) {
		auto clientData = getClientDataSafe(ctx);
		clientData->tmpNss.erase(std::string_view(ns));
	}
	return err;
}

Error RPCServer::TruncateNamespace(cproto::Context &ctx, p_string ns) { return getDB(ctx, kRoleDBAdmin).TruncateNamespace(ns); }

Error RPCServer::RenameNamespace(cproto::Context &ctx, p_string srcNsName, p_string dstNsName) {
	auto err = getDB(ctx, kRoleDBAdmin).RenameNamespace(srcNsName, dstNsName.toString());
	if (err.ok()) {
		auto clientData = getClientDataSafe(ctx);
		clientData->tmpNss.erase(std::string_view(srcNsName));
	}
	return err;
}

Error RPCServer::CreateTemporaryNamespace(cproto::Context &ctx, p_string nsDefJson, int64_t v) {
	NamespaceDef nsDef;
	nsDef.FromJSON(giftStr(nsDefJson));
	std::string resultName;
	auto err = getDB(ctx, kRoleDataRead).CreateTemporaryNamespace(nsDef.name, resultName, nsDef.storage, lsn_t(v));
	if (err.ok()) {
		auto clientData = getClientDataSafe(ctx);
		clientData->tmpNss.emplace(resultName);
		ctx.Return({cproto::Arg(p_string(&resultName))});
	}
	return err;
}

Error RPCServer::CloseNamespace(cproto::Context &ctx, p_string ns) {
	// Do not close.
	// TODO: add reference counters
	// return getDB(ctx, kRoleDataRead)->CloseNamespace(ns);
	return getDB(ctx, kRoleDataRead).Commit(ns);
}

Error RPCServer::EnumNamespaces(cproto::Context &ctx, cproto::optional<int> opts, cproto::optional<p_string> filter) {
	vector<NamespaceDef> nsDefs;
	EnumNamespacesOpts eopts;
	if (opts.hasValue()) eopts.options_ = opts.value();
	if (filter.hasValue()) eopts.filter_ = filter.value();

	auto err = getDB(ctx, kRoleDataRead).EnumNamespaces(nsDefs, eopts);
	if (!err.ok()) {
		return err;
	}
	WrSerializer ser;
	ser << "{\"items\":[";
	for (unsigned i = 0; i < nsDefs.size(); i++) {
		if (i != 0) ser << ',';
		nsDefs[i].GetJSON(ser);
	}
	ser << "]}";
	auto resSlice = ser.Slice();

	ctx.Return({cproto::Arg(p_string(&resSlice))});
	return errOK;
}

Error RPCServer::EnumDatabases(cproto::Context &ctx) {
	auto dbList = dbMgr_.EnumDatabases();

	WrSerializer ser;
	JsonBuilder jb(ser);
	span<string> array(&dbList[0], dbList.size());
	jb.Array("databases"sv, array);
	jb.End();

	auto resSlice = ser.Slice();
	ctx.Return({cproto::Arg(p_string(&resSlice))});
	return errOK;
}

Error RPCServer::AddIndex(cproto::Context &ctx, p_string ns, p_string indexDef) {
	IndexDef iDef;
	auto err = iDef.FromJSON(giftStr(indexDef));
	if (!err.ok()) {
		return err;
	}
	return getDB(ctx, kRoleDBAdmin).AddIndex(ns, iDef);
}

Error RPCServer::UpdateIndex(cproto::Context &ctx, p_string ns, p_string indexDef) {
	IndexDef iDef;
	auto err = iDef.FromJSON(giftStr(indexDef));
	if (!err.ok()) {
		return err;
	}
	return getDB(ctx, kRoleDBAdmin).UpdateIndex(ns, iDef);
}

Error RPCServer::DropIndex(cproto::Context &ctx, p_string ns, p_string index) {
	IndexDef idef(index.toString());
	return getDB(ctx, kRoleDBAdmin).DropIndex(ns, idef);
}

Error RPCServer::SetSchema(cproto::Context &ctx, p_string ns, p_string schema) {
	return getDB(ctx, kRoleDBAdmin).SetSchema(ns, std::string_view(schema));
}

Error RPCServer::GetSchema(cproto::Context &ctx, p_string ns, int format) {
	string schema;
	auto err = getDB(ctx, kRoleDataRead).GetSchema(ns, format, schema);
	if (!err.ok()) {
		return err;
	}
	ctx.Return({cproto::Arg(schema)});
	return errOK;
}

Error RPCServer::StartTransaction(cproto::Context &ctx, p_string nsName) {
	int64_t id = -1;
	try {
		id = addTx(ctx, nsName);
	} catch (reindexer::Error &e) {
		return e;
	}
	ctx.Return({cproto::Arg(id)});
	return Error();
}

Error RPCServer::AddTxItem(cproto::Context &ctx, int format, p_string itemData, int mode, p_string perceptsPack, int stateToken,
						   int64_t txID) {
	Transaction &tr = getTx(ctx, txID);

	auto item = tr.NewItem();
	Error err;
	if (!item.Status().ok()) {
		return item.Status();
	}

	err = processTxItem(static_cast<DataFormat>(format), itemData, item, static_cast<ItemModifyMode>(mode), stateToken);
	if (err.code() == errTagsMissmatch) {
		item = getDB(ctx, kRoleDataWrite).NewItem(tr.GetName());
		if (item.Status().ok()) {
			err = processTxItem(static_cast<DataFormat>(format), itemData, item, static_cast<ItemModifyMode>(mode), stateToken);
		} else {
			return item.Status();
		}
	}
	if (!err.ok()) {
		return err;
	}
	if (perceptsPack.length()) {
		Serializer ser(perceptsPack);
		const uint64_t preceptsCount = ser.GetVarUint();
		vector<string> precepts;
		precepts.reserve(preceptsCount);
		for (unsigned prIndex = 0; prIndex < preceptsCount; ++prIndex) {
			precepts.emplace_back(ser.GetVString());
		}
		item.SetPrecepts(std::move(precepts));
	}
	return tr.Modify(std::move(item), ItemModifyMode(mode), ctx.call->lsn);
}

Error RPCServer::DeleteQueryTx(cproto::Context &ctx, p_string queryBin, int64_t txID) {
	auto db = getDB(ctx, kRoleDataWrite);

	Transaction &tr = getTx(ctx, txID);
	Query query;
	Serializer ser(queryBin.data(), queryBin.size());
	query.Deserialize(ser);
	query.type_ = QueryDelete;
	tr.Modify(std::move(query), ctx.call->lsn);
	return Error();
}

Error RPCServer::UpdateQueryTx(cproto::Context &ctx, p_string queryBin, int64_t txID) {
	auto db = getDB(ctx, kRoleDataWrite);

	Transaction &tr = getTx(ctx, txID);
	Query query;
	Serializer ser(queryBin.data(), queryBin.size());
	query.Deserialize(ser);
	query.type_ = QueryUpdate;
	return tr.Modify(std::move(query), ctx.call->lsn);
}

Error RPCServer::PutMetaTx(cproto::Context &ctx, p_string key, p_string data, int64_t txID) {
	auto db = getDB(ctx, kRoleDataWrite);

	Transaction &tr = getTx(ctx, txID);
	return tr.PutMeta(key, data, ctx.call->lsn);
}

Error RPCServer::CommitTx(cproto::Context &ctx, int64_t txId, cproto::optional<int> flagsOpts) {
	auto db = getDB(ctx, kRoleDataWrite);

	Transaction &tr = getTx(ctx, txId);
	QueryResults qres;
	auto err = db.CommitTransaction(tr, qres);
	if (err.ok()) {
		auto tmp = qres.GetNamespaces();
		int32_t ptVers = -1;
		ResultFetchOpts opts;
		int flags;
		if (flagsOpts.hasValue()) {
			flags = flagsOpts.value();
		} else {
			flags = kResultsWithItemID;
			if (tr.IsTagsUpdated()) flags |= kResultsWithPayloadTypes;
		}
		if (tr.IsTagsUpdated()) {
			opts = ResultFetchOpts{flags, span<int32_t>(&ptVers, 1), 0, INT_MAX};
		} else {
			opts = ResultFetchOpts{flags, {}, 0, INT_MAX};
		}
		err = sendResults(ctx, qres, -1, opts);
	}
	clearTx(ctx, txId);
	return err;
}

Error RPCServer::RollbackTx(cproto::Context &ctx, int64_t txId) {
	auto db = getDB(ctx, kRoleDataWrite);

	Error err;
	try {
		Transaction &tr = getTx(ctx, txId);
		err = db.RollBackTransaction(tr);
	} catch (Error &e) {
		return e;
	}

	clearTx(ctx, txId);
	return err;
}

Error RPCServer::ModifyItem(cproto::Context &ctx, p_string ns, int format, p_string itemData, int mode, p_string perceptsPack,
							int stateToken, int /*txID*/) {
	using std::chrono::steady_clock;
	using std::chrono::milliseconds;
	using std::chrono::duration_cast;

	auto db = getDB(ctx, kRoleDataWrite);
	auto execTimeout = ctx.call->execTimeout;
	auto beginT = steady_clock::now();
	auto item = Item(db.NewItem(ns));
	if (execTimeout.count() > 0) {
		execTimeout -= duration_cast<milliseconds>(beginT - steady_clock::now());
		if (execTimeout.count() <= 0) {
			return errCanceled;
		}
	}
	bool tmUpdated = false, sendItemBack = false;
	Error err;
	if (!item.Status().ok()) {
		return item.Status();
	}

	switch (format) {
		case FormatJson:
			err = item.Unsafe().FromJSON(itemData, nullptr, mode == ModeDelete);
			break;
		case FormatCJson:
			if (item.GetStateToken() != stateToken) {
				err = Error(errStateInvalidated, "stateToken mismatch:  %08X, need %08X. Can't process item", stateToken,
							item.GetStateToken());
			} else {
				// TODO: To delete an item with sharding you need
				// not only PK fields, but all the sharding keys
				err = item.Unsafe().FromCJSON(itemData, mode == ModeDelete);
			}
			break;
		case FormatMsgPack: {
			size_t offset = 0;
			err = item.FromMsgPack(itemData, offset);
			break;
		}
		default:
			err = Error(-1, "Invalid source item format %d", format);
	}
	if (!err.ok()) {
		return err;
	}
	tmUpdated = item.IsTagsUpdated();

	if (perceptsPack.length()) {
		Serializer ser(perceptsPack);
		const uint64_t preceptsCount = ser.GetVarUint();
		vector<string> precepts;
		for (unsigned prIndex = 0; prIndex < preceptsCount; ++prIndex) {
			precepts.emplace_back(ser.GetVString());
		}
		item.SetPrecepts(std::move(precepts));
		sendItemBack = preceptsCount;
	}

	QueryResults qres;
	if (sendItemBack) {
		switch (mode) {
			case ModeUpsert:
				err = db.WithTimeout(execTimeout).Upsert(ns, item, qres);
				break;
			case ModeInsert:
				err = db.WithTimeout(execTimeout).Insert(ns, item, qres);
				break;
			case ModeUpdate:
				err = db.WithTimeout(execTimeout).Update(ns, item, qres);
				break;
			case ModeDelete:
				err = db.WithTimeout(execTimeout).Delete(ns, item, qres);
				break;
		}
		if (!err.ok()) {
			return err;
		}
	} else {
		switch (mode) {
			case ModeUpsert:
				err = db.WithTimeout(execTimeout).Upsert(ns, item);
				break;
			case ModeInsert:
				err = db.WithTimeout(execTimeout).Insert(ns, item);
				break;
			case ModeUpdate:
				err = db.WithTimeout(execTimeout).Update(ns, item);
				break;
			case ModeDelete:
				err = db.WithTimeout(execTimeout).Delete(ns, item);
				break;
		}
		if (err.ok()) {
			if (item.GetID() != -1) {
				qres.AddItem(item);
			}
		} else {
			return err;
		}
	}
	int32_t ptVers = -1;
	ResultFetchOpts opts;
	if (tmUpdated) {
		opts = ResultFetchOpts{kResultsWithItemID | kResultsWithPayloadTypes, span<int32_t>(&ptVers, 1), 0, INT_MAX};
	} else {
		opts = ResultFetchOpts{kResultsWithItemID, {}, 0, INT_MAX};
	}
	if (sendItemBack) {
		if (format == FormatMsgPack) {
			opts.flags |= kResultsMsgPack;
		} else {
			opts.flags |= kResultsCJson;
		}
	}

	return sendResults(ctx, qres, -1, opts);
}

Error RPCServer::DeleteQuery(cproto::Context &ctx, p_string queryBin, cproto::optional<int> flagsOpts) {
	Query query;
	Serializer ser(queryBin.data(), queryBin.size());
	query.Deserialize(ser);
	query.type_ = QueryDelete;

	QueryResults qres;
	auto err = getDB(ctx, kRoleDataWrite).Delete(query, qres);
	if (!err.ok()) {
		return err;
	}

	int32_t ptVersion = -1;
	int flags = kResultsWithItemID;
	if (flagsOpts.hasValue()) flags = flagsOpts.value();
	ResultFetchOpts opts{flags, {&ptVersion, 1}, 0, INT_MAX};
	return sendResults(ctx, qres, -1, opts);
}

Error RPCServer::UpdateQuery(cproto::Context &ctx, p_string queryBin, cproto::optional<int> flagsOpts) {
	Query query;
	Serializer ser(queryBin.data(), queryBin.size());
	query.Deserialize(ser);
	query.type_ = QueryUpdate;

	QueryResults qres;
	auto err = getDB(ctx, kRoleDataWrite).Update(query, qres);
	if (!err.ok()) {
		return err;
	}

	int32_t ptVersion = -1;
	int flags = kResultsWithItemID | kResultsWithPayloadTypes | kResultsCJson;
	if (flagsOpts.hasValue()) flags = flagsOpts.value();
	ResultFetchOpts opts{flags, {&ptVersion, 1}, 0, INT_MAX};
	return sendResults(ctx, qres, -1, opts);
}

Reindexer RPCServer::getDB(cproto::Context &ctx, UserRole role) {
	auto clientData = getClientDataUnsafe(ctx);
	if (clientData) {
		Reindexer *db = nullptr;
		auto status = clientData->auth.GetDB(role, &db);
		if (!status.ok()) {
			throw status;
		}
		if (db != nullptr) {
			auto rx = db->WithTimeout(ctx.call->execTimeout)
						  .WithLSN(ctx.call->lsn)
						  .WithEmmiterServerId(ctx.call->emmiterServerId)
						  .WithShardId(ctx.call->shardId);
			if (db->NeedTraceActivity()) {
				return rx.WithActivityTracer(ctx.clientAddr, clientData->auth.Login(), clientData->connID);
			}
			return rx;
		}
	}
	throw Error(errParams, "Database is not opened, you should open it first");
}

void RPCServer::cleanupTmpNamespaces(RPCClientData &clientData, std::string_view activity) {
	Reindexer *dbPtr = nullptr;
	auto status = clientData.auth.GetDB(kRoleDBAdmin, &dbPtr);
	if (!status.ok() || dbPtr == nullptr) {
		logger_.info("RPC: Unable to cleanup tmp namespaces:{}", status.what());
		return;
	}
	auto db = dbPtr->NeedTraceActivity() ? dbPtr->WithActivityTracer(activity, clientData.auth.Login(), clientData.connID) : *dbPtr;

	for (auto &ns : clientData.tmpNss) {
		auto e = db.DropNamespace(ns);
		logger_.info("RPC: Dropping tmp ns {}:{}", ns, e.ok() ? "OK"sv : e.what());
	}
}

Error RPCServer::sendResults(cproto::Context &ctx, QueryResults &qres, int reqId, const ResultFetchOpts &opts) {
	WrResultSerializer rser(opts);
	bool doClose = rser.PutResults(&qres);
	if (doClose && reqId >= 0) {
		freeQueryResults(ctx, reqId);
		reqId = -1;
	}
	std::string_view resSlice = rser.Slice();
	ctx.Return({cproto::Arg(p_string(&resSlice)), cproto::Arg(int(reqId))});
	return Error();
}

Error RPCServer::processTxItem(DataFormat format, std::string_view itemData, Item &item, ItemModifyMode mode,
							   int stateToken) const noexcept {
	switch (format) {
		case FormatJson:
			// TODO: To delete an item with sharding you need
			// not only PK fields, but all the sharding keys
			return item.FromJSON(itemData, nullptr, mode == ModeDelete);
		case FormatCJson:
			if (item.GetStateToken() != stateToken) {
				return Error(errStateInvalidated, "stateToken mismatch:  %08X, need %08X. Can't process tx item", stateToken,
							 item.GetStateToken());
			} else {
				// TODO: To delete an item with sharding you need
				// not only PK fields, but all the sharding keys
				return item.FromCJSON(itemData, mode == ModeDelete);
			}
		case FormatMsgPack: {
			size_t offset = 0;
			return item.FromMsgPack(itemData, offset);
		}
		default:
			return Error(-1, "Invalid source item format %d", format);
	}
}

QueryResults &RPCServer::getQueryResults(cproto::Context &ctx, int &id) {
	auto data = getClientDataSafe(ctx);

	if (id < 0) {
		for (id = 0; id < int(data->results.size()); ++id) {
			if (!data->results[id].second) {
				data->results[id] = {QueryResults(), true};
				return data->results[id].first;
			}
		}

		if (data->results.size() >= cproto::kMaxConcurentQueries) throw Error(errLogic, "Too many parallel queries");
		id = data->results.size();
		data->results.push_back({QueryResults(), true});
	}

	if (id >= int(data->results.size())) {
		throw Error(errLogic, "Invalid query id");
	}
	return data->results[id].first;
}

Transaction &RPCServer::getTx(cproto::Context &ctx, int64_t id) {
	auto data = getClientDataSafe(ctx);

	if (size_t(id) >= data->txs.size() || data->txs[id].IsFree()) {
		throw Error(errLogic, "Invalid tx id %d", id);
	}
	return data->txs[id];
}

int64_t RPCServer::addTx(cproto::Context &ctx, std::string_view nsName) {
	auto db = getDB(ctx, kRoleDataWrite);
	int64_t id = -1;
	auto data = getClientDataSafe(ctx);
	for (size_t i = 0; i < data->txs.size(); ++i) {
		if (data->txs[i].IsFree()) {
			id = i;
			break;
		}
	}
	if (data->txs.size() >= kMaxTxCount && id < 0) {
		throw Error(errForbidden, "Too many active transactions");
	}

	auto tr = db.NewTransaction(nsName);
	if (!tr.Status().ok()) {
		throw tr.Status();
	}
	assert(data->txStats);
	data->txStats->txCount += 1;
	if (id >= 0) {
		data->txs[id] = std::move(tr);
		return id;
	}
	data->txs.emplace_back(std::move(tr));
	return int64_t(data->txs.size() - 1);
}

void RPCServer::clearTx(cproto::Context &ctx, uint64_t txId) {
	auto data = getClientDataSafe(ctx);
	if (txId >= data->txs.size()) {
		throw Error(errLogic, "Invalid tx id %d", txId);
	}
	assert(data->txStats);
	data->txStats->txCount -= 1;
	data->txs[txId] = Transaction();
}

Snapshot &RPCServer::getSnapshot(cproto::Context &ctx, int &id) {
	auto data = getClientDataSafe(ctx);

	if (id < 0) {
		for (id = 0; id < int(data->snapshots.size()); ++id) {
			if (!data->snapshots[id].second) {
				data->snapshots[id] = {Snapshot(), true};
				return data->snapshots[id].first;
			}
		}

		if (data->snapshots.size() >= cproto::kMaxConcurentSnapshots) throw Error(errLogic, "Too many paralell snapshots");
		id = data->snapshots.size();
		data->snapshots.emplace_back(std::make_pair(Snapshot(), true));
	}

	if (id >= int(data->snapshots.size())) {
		throw Error(errLogic, "Invalid snapshot id");
	}
	return data->snapshots[id].first;
}

Error RPCServer::fetchSnapshotRecords(cproto::Context &ctx, int id, int64_t offset, bool putHeader) {
	cproto::Args args;
	WrSerializer ser;
	std::string_view resSlice;
	Snapshot *snapshot = nullptr;
	if (offset < 0) {
		freeSnapshot(ctx, id);
		return Error();
	}
	try {
		snapshot = &getSnapshot(ctx, id);
	} catch (Error &e) {
		return e;
	}

	if (putHeader) {
		args.emplace_back(int(id));
		args.emplace_back(int64_t(snapshot->Size()));
		args.emplace_back(int64_t(snapshot->RawDataSize()));
		args.emplace_back(int64_t(snapshot->NsVersion()));
	}
	auto it = snapshot->begin() + offset;
	if (it != snapshot->end()) {
		auto ch = it.Chunk();
		ch.Serilize(ser);
		resSlice = ser.Slice();
		args.emplace_back(p_string(&resSlice));
	}
	if (++it == snapshot->end()) {
		freeSnapshot(ctx, id);
	}
	ctx.Return(std::move(args));

	return Error();
}

void RPCServer::freeSnapshot(cproto::Context &ctx, int id) {
	auto data = getClientDataSafe(ctx);
	if (id >= int(data->snapshots.size()) || id < 0) {
		throw Error(errLogic, "Invalid snapshot id");
	}
	data->snapshots[id] = {Snapshot(), false};
}

void RPCServer::freeQueryResults(cproto::Context &ctx, int id) {
	auto data = getClientDataSafe(ctx);
	if (id >= int(data->results.size()) || id < 0) {
		throw Error(errLogic, "Invalid query id");
	}
	data->results[id] = {QueryResults(), false};
}

static h_vector<int32_t, 4> pack2vec(p_string pack) {
	// Get array of payload Type Versions
	Serializer ser(pack.data(), pack.size());
	h_vector<int32_t, 4> vec;
	int cnt = ser.GetVarUint();
	for (int i = 0; i < cnt; i++) vec.push_back(ser.GetVarUint());
	return vec;
}

Error RPCServer::Select(cproto::Context &ctx, p_string queryBin, int flags, int limit, p_string ptVersionsPck) {
	Query query;
	Serializer ser(queryBin);
	query.Deserialize(ser);

	if (query.IsWALQuery()) {
		auto data = getClientDataSafe(ctx);
		query.Where(string("#slave_version"sv), CondEq, data->rxVersion.StrippedString());
	}

	int id = -1;
	QueryResults &qres = getQueryResults(ctx, id);
	auto ret = getDB(ctx, kRoleDataRead).Select(query, qres);
	if (!ret.ok()) {
		freeQueryResults(ctx, id);
		return ret;
	}
	auto ptVersions = pack2vec(ptVersionsPck);
	ResultFetchOpts opts{flags, ptVersions, 0, unsigned(limit)};

	return fetchResults(ctx, id, opts);
}

Error RPCServer::SelectSQL(cproto::Context &ctx, p_string querySql, int flags, int limit, p_string ptVersionsPck) {
	int id = -1;
	QueryResults &qres = getQueryResults(ctx, id);
	auto ret = getDB(ctx, kRoleDataRead).Select(querySql, qres);
	if (!ret.ok()) {
		freeQueryResults(ctx, id);
		return ret;
	}
	auto ptVersions = pack2vec(ptVersionsPck);
	ResultFetchOpts opts{flags, ptVersions, 0, unsigned(limit)};

	return fetchResults(ctx, id, opts);
}

Error RPCServer::FetchResults(cproto::Context &ctx, int reqId, int flags, int offset, int limit) {
	flags &= ~kResultsWithPayloadTypes;

	ResultFetchOpts opts = {flags, {}, unsigned(offset), unsigned(limit)};
	return fetchResults(ctx, reqId, opts);
}

Error RPCServer::CloseResults(cproto::Context &ctx, int reqId) {
	freeQueryResults(ctx, reqId);
	return Error();
}

Error RPCServer::fetchResults(cproto::Context &ctx, int reqId, const ResultFetchOpts &opts) {
	QueryResults &qres = getQueryResults(ctx, reqId);
	return sendResults(ctx, qres, reqId, opts);
}

Error RPCServer::GetSQLSuggestions(cproto::Context &ctx, p_string query, int pos) {
	vector<string> suggests;
	Error err = getDB(ctx, kRoleDataRead).GetSqlSuggestions(query, pos, suggests);

	if (err.ok()) {
		cproto::Args ret;
		ret.reserve(suggests.size());
		for (auto &suggest : suggests) ret.push_back(cproto::Arg(suggest));
		ctx.Return(ret);
	}
	return err;
}

Error RPCServer::GetReplState(cproto::Context &ctx, p_string ns) {
	ReplicationStateV2 state;
	auto err = getDB(ctx, kRoleDataRead).GetReplState(ns, state);
	if (err.ok()) {
		WrSerializer ser;
		JsonBuilder json(ser);
		state.GetJSON(json);
		json.End();
		auto slice = ser.Slice();
		ctx.Return({cproto::Arg(p_string(&slice))});
	}
	return err;
}

Error RPCServer::SetClusterizationStatus(cproto::Context &ctx, p_string ns, p_string serStatus) {
	ClusterizationStatus status;
	auto err = status.FromJSON(giftStr(serStatus));
	if (!err.ok()) {
		return err;
	}
	return getDB(ctx, kRoleDBAdmin).SetClusterizationStatus(ns, status);
}

Error RPCServer::GetSnapshot(cproto::Context &ctx, p_string ns, p_string optsJson) {
	int id = -1;
	Snapshot *snapshot = nullptr;
	try {
		snapshot = &getSnapshot(ctx, id);
	} catch (Error &e) {
		return e;
	}
	SnapshotOpts opts;
	auto ret = opts.FromJSON(giftStr(optsJson));
	if (!ret.ok()) {
		freeSnapshot(ctx, id);
		return ret;
	}
	ret = getDB(ctx, kRoleDataRead).GetSnapshot(ns, opts, *snapshot);
	if (!ret.ok()) {
		freeSnapshot(ctx, id);
		return ret;
	}

	return fetchSnapshotRecords(ctx, id, 0, true);
}

Error RPCServer::FetchSnapshot(cproto::Context &ctx, int id, int64_t offset) { return fetchSnapshotRecords(ctx, id, offset, false); }

Error RPCServer::ApplySnapshotChunk(cproto::Context &ctx, p_string ns, p_string rec) {
	SnapshotChunk ch;
	Serializer ser(rec);
	ch.Deserialize(ser);
	return getDB(ctx, kRoleDataWrite).ApplySnapshotChunk(ns, ch);
}

Error RPCServer::Commit(cproto::Context &ctx, p_string ns) { return getDB(ctx, kRoleDataWrite).Commit(ns); }

Error RPCServer::GetMeta(cproto::Context &ctx, p_string ns, p_string key) {
	string data;
	auto err = getDB(ctx, kRoleDataRead).GetMeta(ns, key.toString(), data);
	if (!err.ok()) {
		return err;
	}

	ctx.Return({cproto::Arg(data)});
	return Error();
}

Error RPCServer::PutMeta(cproto::Context &ctx, p_string ns, p_string key, p_string data) {
	return getDB(ctx, kRoleDataWrite).PutMeta(ns, key.toString(), data);
}

Error RPCServer::EnumMeta(cproto::Context &ctx, p_string ns) {
	vector<string> keys;
	auto err = getDB(ctx, kRoleDataWrite).EnumMeta(ns, keys);
	if (!err.ok()) {
		return err;
	}
	cproto::Args ret;
	for (auto &key : keys) {
		ret.push_back(cproto::Arg(key));
	}
	ctx.Return(ret);
	return Error();
}

Error RPCServer::SubscribeUpdates(cproto::Context & /*ctx*/, int /*flag*/, cproto::optional<p_string> /*filterJson*/,
								  cproto::optional<int> /*options*/) {
	return Error(errForbidden, "Updates subscription is no longer supported");
}

Error RPCServer::SuggestLeader(cproto::Context &ctx, p_string suggestion) {
	if (!serverConfig_.EnableCluster) {
		return Error(errForbidden, "Cluster is not enabled for this node");
	}
	cluster::NodeData sug, res;
	auto err = sug.FromJSON(giftStr(suggestion));
	if (err.ok()) {
		err = getDB(ctx, kRoleDataWrite).SuggestLeader(sug, res);
	}
	if (err.ok()) {
		WrSerializer ser;
		res.GetJSON(ser);
		auto serSlice = ser.Slice();
		ctx.Return({cproto::Arg(p_string(&serSlice))});
	}
	return err;
}

Error RPCServer::LeadersPing(cproto::Context &ctx, p_string leader) {
	if (!serverConfig_.EnableCluster) {
		return Error(errForbidden, "Cluster is not enabled for this node");
	}
	cluster::NodeData l;
	auto err = l.FromJSON(giftStr(leader));
	if (err.ok()) {
		err = getDB(ctx, kRoleDataWrite).LeadersPing(l);
	}
	return err;
}

Error RPCServer::GetRaftInfo(cproto::Context &ctx) {
	if (!serverConfig_.EnableCluster) {
		return Error(errForbidden, "Cluster is not enabled for this node");
	}
	cluster::RaftInfo info;
	auto err = getDB(ctx, kRoleDataWrite).GetRaftInfo(info);
	if (err.ok()) {
		WrSerializer ser;
		info.GetJSON(ser);
		auto serSlice = ser.Slice();
		ctx.Return({cproto::Arg(p_string(&serSlice))});
	}
	return err;
}

Error RPCServer::ClusterControlRequest(cproto::Context &ctx, p_string data) {
	ClusterControlRequestData req;
	Error err = req.FromJSON(giftStr(data));
	if (!err.ok()) {
		return err;
	}
	return getDB(ctx, kRoleDBAdmin).ClusterControlRequest(req);
}

bool RPCServer::Start(const string &addr, ev::dynamic_loop &loop) {
	dispatcher_.Register(cproto::kCmdPing, this, &RPCServer::Ping);
	dispatcher_.Register(cproto::kCmdLogin, this, &RPCServer::Login, true);
	dispatcher_.Register(cproto::kCmdOpenDatabase, this, &RPCServer::OpenDatabase, true);
	dispatcher_.Register(cproto::kCmdCloseDatabase, this, &RPCServer::CloseDatabase);
	dispatcher_.Register(cproto::kCmdDropDatabase, this, &RPCServer::DropDatabase);
	dispatcher_.Register(cproto::kCmdOpenNamespace, this, &RPCServer::OpenNamespace, true);
	dispatcher_.Register(cproto::kCmdDropNamespace, this, &RPCServer::DropNamespace);
	dispatcher_.Register(cproto::kCmdTruncateNamespace, this, &RPCServer::TruncateNamespace);
	dispatcher_.Register(cproto::kCmdRenameNamespace, this, &RPCServer::RenameNamespace);
	dispatcher_.Register(cproto::kCmdCloseNamespace, this, &RPCServer::CloseNamespace);
	dispatcher_.Register(cproto::kCmdEnumNamespaces, this, &RPCServer::EnumNamespaces, true);
	dispatcher_.Register(cproto::kCmdEnumDatabases, this, &RPCServer::EnumDatabases);
	dispatcher_.Register(cproto::kCmdAddIndex, this, &RPCServer::AddIndex);
	dispatcher_.Register(cproto::kCmdUpdateIndex, this, &RPCServer::UpdateIndex);
	dispatcher_.Register(cproto::kCmdDropIndex, this, &RPCServer::DropIndex);
	dispatcher_.Register(cproto::kCmdSetSchema, this, &RPCServer::SetSchema);
	dispatcher_.Register(cproto::kCmdGetSchema, this, &RPCServer::GetSchema);
	dispatcher_.Register(cproto::kCmdCommit, this, &RPCServer::Commit);
	dispatcher_.Register(cproto::kCmdStartTransaction, this, &RPCServer::StartTransaction);
	dispatcher_.Register(cproto::kCmdAddTxItem, this, &RPCServer::AddTxItem);
	dispatcher_.Register(cproto::kCmdDeleteQueryTx, this, &RPCServer::DeleteQueryTx);
	dispatcher_.Register(cproto::kCmdUpdateQueryTx, this, &RPCServer::UpdateQueryTx);
	dispatcher_.Register(cproto::kCmdCommitTx, this, &RPCServer::CommitTx, true);
	dispatcher_.Register(cproto::kCmdRollbackTx, this, &RPCServer::RollbackTx);
	dispatcher_.Register(cproto::kCmdModifyItem, this, &RPCServer::ModifyItem);
	dispatcher_.Register(cproto::kCmdDeleteQuery, this, &RPCServer::DeleteQuery, true);
	dispatcher_.Register(cproto::kCmdUpdateQuery, this, &RPCServer::UpdateQuery, true);
	dispatcher_.Register(cproto::kCmdSelect, this, &RPCServer::Select);
	dispatcher_.Register(cproto::kCmdSelectSQL, this, &RPCServer::SelectSQL);
	dispatcher_.Register(cproto::kCmdFetchResults, this, &RPCServer::FetchResults);
	dispatcher_.Register(cproto::kCmdCloseResults, this, &RPCServer::CloseResults);
	dispatcher_.Register(cproto::kCmdGetSQLSuggestions, this, &RPCServer::GetSQLSuggestions);
	dispatcher_.Register(cproto::kCmdGetMeta, this, &RPCServer::GetMeta);
	dispatcher_.Register(cproto::kCmdPutMeta, this, &RPCServer::PutMeta);
	dispatcher_.Register(cproto::kCmdEnumMeta, this, &RPCServer::EnumMeta);
	dispatcher_.Register(cproto::kCmdSubscribeUpdates, this, &RPCServer::SubscribeUpdates, true);
	dispatcher_.Register(cproto::kCmdGetReplState, this, &RPCServer::GetReplState);
	dispatcher_.Register(cproto::kCmdCreateTmpNamespace, this, &RPCServer::CreateTemporaryNamespace);
	dispatcher_.Register(cproto::kCmdSetClusterizationStatus, this, &RPCServer::SetClusterizationStatus);
	dispatcher_.Register(cproto::kCmdGetSnapshot, this, &RPCServer::GetSnapshot);
	dispatcher_.Register(cproto::kCmdFetchSnapshot, this, &RPCServer::FetchSnapshot);
	dispatcher_.Register(cproto::kCmdApplySnapshotCh, this, &RPCServer::ApplySnapshotChunk);
	dispatcher_.Register(cproto::kCmdPutTxMeta, this, &RPCServer::PutMetaTx);
	dispatcher_.Register(cproto::kCmdSuggestLeader, this, &RPCServer::SuggestLeader);
	dispatcher_.Register(cproto::kCmdLeadersPing, this, &RPCServer::LeadersPing);
	dispatcher_.Register(cproto::kCmdGetRaftInfo, this, &RPCServer::GetRaftInfo);
	dispatcher_.Register(cproto::kCmdClusterControlRequest, this, &RPCServer::ClusterControlRequest);
	dispatcher_.Middleware(this, &RPCServer::CheckAuth);
	dispatcher_.OnClose(this, &RPCServer::OnClose);
	dispatcher_.OnResponse(this, &RPCServer::OnResponse);

	if (logger_) {
		dispatcher_.Logger(this, &RPCServer::Logger);
	}

	auto factory = cproto::ServerConnection::NewFactory(dispatcher_, serverConfig_.EnableConnectionsStats);
	if (serverConfig_.RPCThreadingMode == ServerConfig::kDedicatedThreading || serverConfig_.EnableCluster) {
		listener_.reset(new ForkedListener(loop, factory));
	} else {
		listener_.reset(new Listener(loop, factory));
	}

	return listener_->Bind(addr);
}

}  // namespace reindexer_server
