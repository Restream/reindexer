#include "rpcserver.h"
#include <sys/stat.h>
#include <sstream>
#include "core/cjson/jsonbuilder.h"
#include "core/iclientsstats.h"
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
	  startTs_(std::chrono::system_clock::now()),
	  qrWatcher_(serverConfig_.RPCQrIdleTimeout) {}

RPCServer::~RPCServer() {
	if (qrWatcherThread_.joinable()) {
		Stop();
	}
	listener_.reset();
}

Error RPCServer::Ping(cproto::Context &) {
	//
	return {};
}

static std::atomic<int> connCounter;

Error RPCServer::Login(cproto::Context &ctx, p_string login, p_string password, p_string db, std::optional<bool> createDBIfMissing,
					   std::optional<bool> checkClusterID, std::optional<int> expectedClusterID, std::optional<p_string> clientRxVersion,
					   std::optional<p_string> appName) {
	if (ctx.GetClientData()) {
		return Error(errParams, "Already logged in");
	}

	std::unique_ptr<RPCClientData> clientData(new RPCClientData);

	clientData->connID = connCounter.fetch_add(1, std::memory_order_relaxed);
	clientData->pusher.SetWriter(ctx.writer);
	clientData->subscribed = false;
	clientData->auth = AuthContext(login.toString(), password.toString());
	clientData->txStats = std::make_shared<reindexer::TxStats>();

	auto dbName = db.toString();
	if (checkClusterID && *checkClusterID) {
		assertrx(expectedClusterID);
		clientData->auth.SetExpectedClusterID(*expectedClusterID);
	}
	auto status = dbMgr_.Login(dbName, clientData->auth);
	if (!status.ok()) {
		return status;
	}

	if (clientRxVersion) {
		clientData->rxVersion = SemVersion(std::string_view(*clientRxVersion));
	} else {
		clientData->rxVersion = SemVersion();
	}
	if (clientData->rxVersion < kMinUnknownReplSupportRxVersion) {
		clientData->pusher.SetFilter([](WALRecord &rec) {
			if (rec.type == WalCommitTransaction || rec.type == WalInitTransaction || rec.type == WalSetSchema) {
				return true;
			}
			rec.inTransaction = false;
			return false;
		});
	}

	if (clientsStats_) {
		reindexer::ClientConnectionStat conn;
		conn.connectionStat = ctx.writer->GetConnectionStat();
		conn.ip = std::string(ctx.clientAddr);
		conn.userName = clientData->auth.Login();
		conn.dbName = clientData->auth.DBName();
		conn.userRights = std::string(UserRoleName(clientData->auth.UserRights()));
		conn.clientVersion = clientData->rxVersion.StrippedString();
		conn.appName = appName ? appName->toString() : std::string();
		conn.txStats = clientData->txStats;
		conn.updatesPusher = &clientData->pusher;
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
	}

	return status;
}

static RPCClientData *getClientDataUnsafe(cproto::Context &ctx) { return dynamic_cast<RPCClientData *>(ctx.GetClientData()); }

static RPCClientData *getClientDataSafe(cproto::Context &ctx) {
	auto ret = dynamic_cast<RPCClientData *>(ctx.GetClientData());
	if rx_unlikely (!ret) std::abort();	 // It has to be set by the middleware
	return ret;
}

Error RPCServer::OpenDatabase(cproto::Context &ctx, p_string db, std::optional<bool> createDBIfMissing) {
	auto *clientData = getClientDataSafe(ctx);
	if (clientData->auth.HaveDB()) {
		return Error(errParams, "Database already opened");
	}
	auto status = dbMgr_.OpenDatabase(db.toString(), clientData->auth, createDBIfMissing && *createDBIfMissing);
	if (!status.ok()) {
		clientData->auth.ResetDB();
	}
	return status;
}

Error RPCServer::CloseDatabase(cproto::Context &ctx) {
	auto clientData = getClientDataSafe(ctx);
	clientData->auth.ResetDB();
	return errOK;
}
Error RPCServer::DropDatabase(cproto::Context &ctx) {
	auto clientData = getClientDataSafe(ctx);
	if (statsWatcher_) {
		// Avoid database access from the stats collecting thread during database drop
		auto statsSuspend = statsWatcher_->SuspendStatsThread();
		return dbMgr_.DropDatabase(clientData->auth);
	}
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
	(void)ctx;
	(void)err;

	auto clientData = getClientDataUnsafe(ctx);
	if (clientData) {
		if (statsWatcher_) {
			statsWatcher_->OnClientDisconnected(clientData->auth.DBName(), statsSourceName());
		}
		if (clientsStats_) {
			clientsStats_->DeleteConnection(clientData->connID);
		}
		for (auto &qrId : clientData->results) {
			if (qrId.main >= 0) {
				try {
					qrWatcher_.FreeQueryResults(qrId, false);
				} catch (...) {
				}
			}
		}
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

Error RPCServer::execSqlQueryByType(std::string_view sqlQuery, QueryResults &res, cproto::Context &ctx) {
	try {
		reindexer::Query q;
		q.FromSQL(sqlQuery);
		switch (q.Type()) {
			case QuerySelect:
				return getDB(ctx, kRoleDataRead).Select(q, res);
			case QueryDelete:
				return getDB(ctx, kRoleDataWrite).Delete(q, res);
			case QueryUpdate:
				return getDB(ctx, kRoleDataWrite).Update(q, res);
			case QueryTruncate:
				return getDB(ctx, kRoleDBAdmin).TruncateNamespace(q._namespace);
			default:
				return Error(errParams, "unknown query type %d", q.Type());
		}
	} catch (Error &e) {
		return e;
	}
}

void RPCServer::Logger(cproto::Context &ctx, const Error &err, const cproto::Args &ret) {
	const auto clientData = getClientDataUnsafe(ctx);
	WrSerializer ser;
	if (clientData) {
		ser << "c='"sv << clientData->connID << "' db='"sv << clientData->auth.Login() << '@' << clientData->auth.DBName() << "' "sv;
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

Error RPCServer::OpenNamespace(cproto::Context &ctx, p_string nsDefJson) {
	NamespaceDef nsDef;

	nsDef.FromJSON(giftStr(nsDefJson));
	if (!nsDef.indexes.empty()) {
		return getDB(ctx, kRoleDataRead).AddNamespace(nsDef);
	}
	return getDB(ctx, kRoleDataRead).OpenNamespace(nsDef.name, nsDef.storage);
}

Error RPCServer::DropNamespace(cproto::Context &ctx, p_string ns) {
	//
	return getDB(ctx, kRoleDBAdmin).DropNamespace(ns);
}

Error RPCServer::TruncateNamespace(cproto::Context &ctx, p_string ns) { return getDB(ctx, kRoleDBAdmin).TruncateNamespace(ns); }

Error RPCServer::RenameNamespace(cproto::Context &ctx, p_string srcNsName, p_string dstNsName) {
	return getDB(ctx, kRoleDBAdmin).RenameNamespace(srcNsName, dstNsName.toString());
}

Error RPCServer::CloseNamespace(cproto::Context &ctx, p_string ns) {
	// Do not close.
	// TODO: add reference counters
	// return getDB(ctx, kRoleDataRead)->CloseNamespace(ns);
	return getDB(ctx, kRoleDataRead).Commit(ns);
}

Error RPCServer::EnumNamespaces(cproto::Context &ctx, std::optional<int> opts, std::optional<p_string> filter) {
	std::vector<NamespaceDef> nsDefs;
	EnumNamespacesOpts eopts;
	if (opts) eopts.options_ = *opts;
	if (filter) eopts.filter_ = *filter;

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
	span<std::string> array(&dbList[0], dbList.size());
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

Error RPCServer::StartTransaction(cproto::Context &ctx, p_string nsName) {
	int64_t id = -1;
	try {
		id = addTx(ctx, nsName);
	} catch (reindexer::Error &e) {
		return e;
	}
	ctx.Return({cproto::Arg(id)});
	return errOK;
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
		uint64_t preceptsCount = ser.GetVarUint();
		std::vector<std::string> precepts;
		precepts.reserve(preceptsCount);
		for (unsigned prIndex = 0; prIndex < preceptsCount; prIndex++) {
			precepts.emplace_back(ser.GetVString());
		}
		item.SetPrecepts(precepts);
	}
	tr.Modify(std::move(item), ItemModifyMode(mode));

	return err;
}

Error RPCServer::DeleteQueryTx(cproto::Context &ctx, p_string queryBin, int64_t txID) {
	auto db = getDB(ctx, kRoleDataWrite);

	Transaction &tr = getTx(ctx, txID);
	Query query;
	Serializer ser(queryBin.data(), queryBin.size());
	try {
		query.Deserialize(ser);
	} catch (Error &err) {
		return err;
	}
	query.type_ = QueryDelete;
	tr.Modify(std::move(query));
	return errOK;
}

Error RPCServer::UpdateQueryTx(cproto::Context &ctx, p_string queryBin, int64_t txID) {
	auto db = getDB(ctx, kRoleDataWrite);

	Transaction &tr = getTx(ctx, txID);
	Query query;
	Serializer ser(queryBin.data(), queryBin.size());
	try {
		query.Deserialize(ser);
	} catch (Error &err) {
		return err;
	}
	query.type_ = QueryUpdate;
	tr.Modify(std::move(query));
	return errOK;
}

Error RPCServer::CommitTx(cproto::Context &ctx, int64_t txId, std::optional<int> flagsOpts) {
	auto db = getDB(ctx, kRoleDataWrite);

	Transaction &tr = getTx(ctx, txId);
	QueryResults qres;
	auto err = db.CommitTransaction(tr, qres);
	if (err.ok()) {
		int32_t ptVers = -1;
		ResultFetchOpts opts;
		int flags;
		if (flagsOpts) {
			flags = *flagsOpts;
		} else {
			flags = kResultsWithItemID;
			if (tr.IsTagsUpdated()) flags |= kResultsWithPayloadTypes;
		}
		if (tr.IsTagsUpdated()) {
			opts = ResultFetchOpts{
				.flags = flags, .ptVersions = {&ptVers, 1}, .fetchOffset = 0, .fetchLimit = INT_MAX, .withAggregations = true};
		} else {
			opts = ResultFetchOpts{.flags = flags, .ptVersions = {}, .fetchOffset = 0, .fetchLimit = INT_MAX, .withAggregations = true};
		}
		clearTx(ctx, txId);
		return sendResults(ctx, qres, RPCQrId(), opts);
	}
	clearTx(ctx, txId);
	return err;
}

Error RPCServer::RollbackTx(cproto::Context &ctx, int64_t txId) {
	auto db = getDB(ctx, kRoleDataWrite);

	Transaction &tr = getTx(ctx, txId);
	auto err = db.RollBackTransaction(tr);
	clearTx(ctx, txId);
	return err;
}

Error RPCServer::ModifyItem(cproto::Context &ctx, p_string ns, int format, p_string itemData, int mode, p_string perceptsPack,
							int stateToken, int /*txID*/) {
	using std::chrono::steady_clock;
	using std::chrono::milliseconds;
	using std::chrono::duration_cast;

	auto db = getDB(ctx, kRoleDataWrite);
	auto execTimeout = ctx.call->execTimeout_;
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
				err = item.Unsafe().FromCJSON(itemData, mode == ModeDelete);
			}
			break;
		case FormatMsgPack: {
			size_t offset = 0;
			err = item.FromMsgPack(itemData, offset);
			break;
		}
		default:
			err = Error(errNotValid, "Invalid source item format %d", format);
	}
	if (!err.ok()) {
		return err;
	}
	tmUpdated = item.IsTagsUpdated();

	if (perceptsPack.length()) {
		Serializer ser(perceptsPack);
		unsigned preceptsCount = ser.GetVarUint();
		std::vector<std::string> precepts;
		for (unsigned prIndex = 0; prIndex < preceptsCount; prIndex++) {
			precepts.emplace_back(ser.GetVString());
		}
		item.SetPrecepts(precepts);
		if (preceptsCount) sendItemBack = true;
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
		if (!err.ok()) return err;
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
		if (!err.ok()) return err;
		qres.AddItem(item);
	}
	int32_t ptVers = -1;
	ResultFetchOpts opts;
	if (tmUpdated) {
		opts = ResultFetchOpts{.flags = kResultsWithItemID | kResultsWithPayloadTypes,
							   .ptVersions = {&ptVers, 1},
							   .fetchOffset = 0,
							   .fetchLimit = INT_MAX,
							   .withAggregations = true};
	} else {
		opts = ResultFetchOpts{
			.flags = kResultsWithItemID, .ptVersions = {}, .fetchOffset = 0, .fetchLimit = INT_MAX, .withAggregations = true};
	}
	if (sendItemBack) {
		if (format == FormatMsgPack) {
			opts.flags |= kResultsMsgPack;
		} else {
			opts.flags |= kResultsCJson;
		}
	}

	return sendResults(ctx, qres, RPCQrId(), opts);
}

Error RPCServer::DeleteQuery(cproto::Context &ctx, p_string queryBin, std::optional<int> flagsOpts) {
	Query query;
	Serializer ser(queryBin.data(), queryBin.size());
	try {
		query.Deserialize(ser);
	} catch (Error &err) {
		return err;
	}
	query.type_ = QueryDelete;

	QueryResults qres;
	auto err = getDB(ctx, kRoleDataWrite).Delete(query, qres);
	if (!err.ok()) {
		return err;
	}
	int flags = kResultsWithItemID;
	if (flagsOpts) {
		flags = *flagsOpts;
	}
	int32_t ptVersion = -1;
	ResultFetchOpts opts{.flags = flags, .ptVersions = {&ptVersion, 1}, .fetchOffset = 0, .fetchLimit = INT_MAX, .withAggregations = true};
	return sendResults(ctx, qres, RPCQrId(), opts);
}

Error RPCServer::UpdateQuery(cproto::Context &ctx, p_string queryBin, std::optional<int> flagsOpts) {
	Query query;
	Serializer ser(queryBin.data(), queryBin.size());
	try {
		query.Deserialize(ser);
	} catch (Error &err) {
		return err;
	}
	query.type_ = QueryUpdate;

	QueryResults qres;
	auto err = getDB(ctx, kRoleDataWrite).Update(query, qres);
	if (!err.ok()) {
		return err;
	}

	int32_t ptVersion = -1;
	int flags = kResultsWithItemID | kResultsWithPayloadTypes | kResultsCJson;
	if (flagsOpts) flags = *flagsOpts;
	ResultFetchOpts opts{.flags = flags, .ptVersions = {&ptVersion, 1}, .fetchOffset = 0, .fetchLimit = INT_MAX, .withAggregations = true};
	return sendResults(ctx, qres, RPCQrId(), opts);
}

Reindexer RPCServer::getDB(cproto::Context &ctx, UserRole role) {
	auto clientData = dynamic_cast<RPCClientData *>(ctx.GetClientData());
	if (rx_likely(clientData)) {
		Reindexer *db = nullptr;
		auto status = clientData->auth.GetDB(role, &db);
		if rx_unlikely (!status.ok()) {
			throw status;
		}
		if (rx_likely(db != nullptr)) {
			return db->NeedTraceActivity()
					   ? db->WithContextParams(ctx.call->execTimeout_, ctx.clientAddr, clientData->auth.Login(), clientData->connID)
					   : db->WithTimeout(ctx.call->execTimeout_);
		}
	}
	throw Error(errParams, "Database is not opened, you should open it first");
}

Error RPCServer::sendResults(cproto::Context &ctx, QueryResults &qres, RPCQrId id, const ResultFetchOpts &opts) {
	uint8_t serBuf[0x2000];
	WrResultSerializer rser(serBuf, opts);
	try {
		bool doClose = rser.PutResults(&qres);
		if (doClose && id.main >= 0) {
			freeQueryResults(ctx, id);
			id.main = -1;
			id.uid = RPCQrWatcher::kUninitialized;
		}
		std::string_view resSlice = rser.Slice();
		ctx.Return({cproto::Arg(p_string(&resSlice)), cproto::Arg(int(id.main)), cproto::Arg(int64_t(id.uid))});
	} catch (Error &err) {
		if (id.main >= 0) {
			try {
				freeQueryResults(ctx, id);
				id.main = -1;
				id.uid = RPCQrWatcher::kUninitialized;
			} catch (...) {
			}
		}
		return err;
	}

	return errOK;
}

Error RPCServer::processTxItem(DataFormat format, std::string_view itemData, Item &item, ItemModifyMode mode,
							   int stateToken) const noexcept {
	switch (format) {
		case FormatJson:
			return item.FromJSON(itemData, nullptr, mode == ModeDelete);
		case FormatCJson:
			if (item.GetStateToken() != stateToken) {
				return Error(errStateInvalidated, "stateToken mismatch:  %08X, need %08X. Can't process item", stateToken,
							 item.GetStateToken());
			} else {
				return item.FromCJSON(itemData, mode == ModeDelete);
			}
		case FormatMsgPack: {
			size_t offset = 0;
			return item.FromMsgPack(itemData, offset);
		}
		default:
			return Error(errNotValid, "Invalid source item format %d", format);
	}
}

RPCQrWatcher::Ref RPCServer::createQueryResults(cproto::Context &ctx, RPCQrId &id) {
	auto data = getClientDataSafe(ctx);

	assertrx(id.main < 0);

	RPCQrWatcher::Ref qres;
	qres = qrWatcher_.GetQueryResults(id);

	for (auto &qrId : data->results) {
		if (qrId.main < 0 || qrId.main == id.main) {
			qrId = id;
			return qres;
		}
	}

	if rx_unlikely (data->results.size() >= cproto::kMaxConcurentQueries) {
		for (unsigned idx = 0; idx < data->results.size(); ++idx) {
			RPCQrId tmpQrId{data->results[idx].main, data->results[idx].uid};
			assertrx(tmpQrId.main >= 0);
			if (!qrWatcher_.AreQueryResultsValid(tmpQrId)) {
				// Timed out query results were found
				data->results[idx] = id;
				return qres;
			}
		}
		qrWatcher_.FreeQueryResults(id, false);
		throw Error(errLogic, "Too many parallel queries");
	}
	data->results.emplace_back(id);

	return qres;
}

void RPCServer::freeQueryResults(cproto::Context &ctx, RPCQrId id) {
	auto data = getClientDataSafe(ctx);
	for (auto &qrId : data->results) {
		if (qrId.main == id.main) {
			if (qrId.uid != id.uid) {
				if (!qrWatcher_.AreQueryResultsValid(RPCQrId{qrId.main, qrId.uid})) {
					qrId.main = -1;	 // Qr is timed out
					continue;
				} else {
					throw Error(errLogic, "Invalid query uid: %d vs %d", qrId.uid, id.uid);
				}
			}
			qrId.main = -1;
			qrWatcher_.FreeQueryResults(id, true);
			return;
		}
	}
}

Transaction &RPCServer::getTx(cproto::Context &ctx, int64_t id) {
	auto data = getClientDataSafe(ctx);

	if (size_t(id) >= data->txs.size() || data->txs[id].IsFree()) {
		throw Error(errLogic, "Invalid tx id");
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
	assertrx(data->txStats);
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
	assertrx(data->txStats);
	data->txStats->txCount -= 1;
	data->txs[txId] = Transaction();
}

static h_vector<int32_t, 4> pack2vec(p_string pack) {
	// Get array of payload Type Versions
	Serializer ser(pack.data(), pack.size());
	h_vector<int32_t, 4> vec;
	int cnt = ser.GetVarUint();
	vec.reserve(cnt);
	for (int i = 0; i < cnt; i++) vec.emplace_back(ser.GetVarUint());
	return vec;
}

Error RPCServer::Select(cproto::Context &ctx, p_string queryBin, int flags, int limit, p_string ptVersionsPck) {
	Query query;
	Serializer ser(queryBin);
	try {
		query.Deserialize(ser);
	} catch (Error &err) {
		return err;
	}

	if (query.IsWALQuery()) {
		auto data = getClientDataSafe(ctx);
		query.Where(std::string("#slave_version"sv), CondEq, data->rxVersion.StrippedString());
	}

	RPCQrWatcher::Ref qres;
	RPCQrId id{-1, (flags & kResultsSupportIdleTimeout) ? RPCQrWatcher::kUninitialized : RPCQrWatcher::kDisabled};
	try {
		qres = createQueryResults(ctx, id);
	} catch (Error &e) {
		if (e.code() == errQrUIDMissmatch) {
			return e;
		}
		throw e;
	}

	auto ret = getDB(ctx, kRoleDataRead).Select(query, *qres);
	if (!ret.ok()) {
		freeQueryResults(ctx, id);
		return ret;
	}
	auto ptVersions = pack2vec(ptVersionsPck);
	ResultFetchOpts opts{
		.flags = flags, .ptVersions = ptVersions, .fetchOffset = 0, .fetchLimit = unsigned(limit), .withAggregations = true};

	return sendResults(ctx, *qres, id, opts);
}

Error RPCServer::SelectSQL(cproto::Context &ctx, p_string querySql, int flags, int limit, p_string ptVersionsPck) {
	RPCQrId id{-1, (flags & kResultsSupportIdleTimeout) ? RPCQrWatcher::kUninitialized : RPCQrWatcher::kDisabled};
	RPCQrWatcher::Ref qres;
	try {
		qres = createQueryResults(ctx, id);
	} catch (Error &e) {
		if (e.code() == errQrUIDMissmatch) {
			return e;
		}
		throw;
	}
	auto ret = execSqlQueryByType(querySql, *qres, ctx);
	if (!ret.ok()) {
		freeQueryResults(ctx, id);
		return ret;
	}
	auto ptVersions = pack2vec(ptVersionsPck);
	ResultFetchOpts opts{
		.flags = flags, .ptVersions = ptVersions, .fetchOffset = 0, .fetchLimit = unsigned(limit), .withAggregations = true};

	return sendResults(ctx, *qres, id, opts);
}

Error RPCServer::FetchResults(cproto::Context &ctx, int reqId, int flags, int offset, int limit, std::optional<int64_t> qrUID) {
	flags &= ~kResultsWithPayloadTypes;
	RPCQrId id{reqId, qrUID ? *qrUID : RPCQrWatcher::kDisabled};
	RPCQrWatcher::Ref qres;
	try {
		qres = qrWatcher_.GetQueryResults(id);
	} catch (Error &e) {
		if (e.code() == errQrUIDMissmatch) {
			return e;
		}
		throw;
	}

	ResultFetchOpts opts{
		.flags = flags, .ptVersions = {}, .fetchOffset = unsigned(offset), .fetchLimit = unsigned(limit), .withAggregations = false};
	return sendResults(ctx, *qres, id, opts);
}

Error RPCServer::CloseResults(cproto::Context &ctx, int reqId, std::optional<int64_t> qrUID, std::optional<bool> doNotReply) {
	if (doNotReply && *doNotReply) {
		ctx.respSent = true;
	}
	const RPCQrId id{reqId, qrUID ? *qrUID : RPCQrWatcher::kDisabled};
	try {
		freeQueryResults(ctx, id);
	} catch (Error &e) {
		return e;
	}
	return errOK;
}

Error RPCServer::GetSQLSuggestions(cproto::Context &ctx, p_string query, int pos) {
	std::vector<std::string> suggests;
	Error err = getDB(ctx, kRoleDataRead).GetSqlSuggestions(query, pos, suggests);

	if (err.ok()) {
		cproto::Args ret;
		ret.reserve(suggests.size());
		for (auto &suggest : suggests) ret.emplace_back(std::move(suggest));
		ctx.Return(ret);
	}
	return err;
}

Error RPCServer::Commit(cproto::Context &ctx, p_string ns) { return getDB(ctx, kRoleDataWrite).Commit(ns); }

Error RPCServer::GetMeta(cproto::Context &ctx, p_string ns, p_string key) {
	std::string data;
	auto err = getDB(ctx, kRoleDataRead).GetMeta(ns, key.toString(), data);
	if (!err.ok()) {
		return err;
	}

	ctx.Return({cproto::Arg(data)});
	return errOK;
}

Error RPCServer::PutMeta(cproto::Context &ctx, p_string ns, p_string key, p_string data) {
	return getDB(ctx, kRoleDataWrite).PutMeta(ns, key.toString(), data);
}

Error RPCServer::EnumMeta(cproto::Context &ctx, p_string ns) {
	std::vector<std::string> keys;
	auto err = getDB(ctx, kRoleDataWrite).EnumMeta(ns, keys);
	if (!err.ok()) {
		return err;
	}
	cproto::Args ret;
	for (auto &key : keys) {
		ret.emplace_back(std::move(key));
	}
	ctx.Return(ret);
	return errOK;
}

Error RPCServer::SubscribeUpdates(cproto::Context &ctx, int flag, std::optional<p_string> filterJson, std::optional<int> options) {
	UpdatesFilters filters;
	Error ret;
	if (filterJson) {
		filters.FromJSON(giftStr(*filterJson));
		if (!ret.ok()) {
			return ret;
		}
	}
	SubscriptionOpts opts;
	if (options) {
		opts.options = *options;
	}

	auto db = getDB(ctx, kRoleDataRead);
	auto clientData = getClientDataSafe(ctx);
	if (flag) {
		ret = db.SubscribeUpdates(&clientData->pusher, filters, opts);
	} else {
		ret = db.UnsubscribeUpdates(&clientData->pusher);
	}
	if (ret.ok()) clientData->subscribed = bool(flag);
	return ret;
}

bool RPCServer::Start(const std::string &addr, ev::dynamic_loop &loop) {
	dispatcher_.Register(cproto::kCmdPing, this, &RPCServer::Ping);
	dispatcher_.Register(cproto::kCmdLogin, this, &RPCServer::Login);
	dispatcher_.Register(cproto::kCmdOpenDatabase, this, &RPCServer::OpenDatabase);
	dispatcher_.Register(cproto::kCmdCloseDatabase, this, &RPCServer::CloseDatabase);
	dispatcher_.Register(cproto::kCmdDropDatabase, this, &RPCServer::DropDatabase);
	dispatcher_.Register(cproto::kCmdOpenNamespace, this, &RPCServer::OpenNamespace);
	dispatcher_.Register(cproto::kCmdDropNamespace, this, &RPCServer::DropNamespace);
	dispatcher_.Register(cproto::kCmdTruncateNamespace, this, &RPCServer::TruncateNamespace);
	dispatcher_.Register(cproto::kCmdRenameNamespace, this, &RPCServer::RenameNamespace);
	dispatcher_.Register(cproto::kCmdCloseNamespace, this, &RPCServer::CloseNamespace);
	dispatcher_.Register(cproto::kCmdEnumNamespaces, this, &RPCServer::EnumNamespaces);
	dispatcher_.Register(cproto::kCmdEnumDatabases, this, &RPCServer::EnumDatabases);

	dispatcher_.Register(cproto::kCmdAddIndex, this, &RPCServer::AddIndex);
	dispatcher_.Register(cproto::kCmdUpdateIndex, this, &RPCServer::UpdateIndex);
	dispatcher_.Register(cproto::kCmdDropIndex, this, &RPCServer::DropIndex);
	dispatcher_.Register(cproto::kCmdSetSchema, this, &RPCServer::SetSchema);
	dispatcher_.Register(cproto::kCmdCommit, this, &RPCServer::Commit);

	dispatcher_.Register(cproto::kCmdStartTransaction, this, &RPCServer::StartTransaction);
	dispatcher_.Register(cproto::kCmdAddTxItem, this, &RPCServer::AddTxItem);
	dispatcher_.Register(cproto::kCmdDeleteQueryTx, this, &RPCServer::DeleteQueryTx);
	dispatcher_.Register(cproto::kCmdUpdateQueryTx, this, &RPCServer::UpdateQueryTx);
	dispatcher_.Register(cproto::kCmdCommitTx, this, &RPCServer::CommitTx);
	dispatcher_.Register(cproto::kCmdRollbackTx, this, &RPCServer::RollbackTx);

	dispatcher_.Register(cproto::kCmdModifyItem, this, &RPCServer::ModifyItem);
	dispatcher_.Register(cproto::kCmdDeleteQuery, this, &RPCServer::DeleteQuery);
	dispatcher_.Register(cproto::kCmdUpdateQuery, this, &RPCServer::UpdateQuery);

	dispatcher_.Register(cproto::kCmdSelect, this, &RPCServer::Select);
	dispatcher_.Register(cproto::kCmdSelectSQL, this, &RPCServer::SelectSQL);
	dispatcher_.Register(cproto::kCmdFetchResults, this, &RPCServer::FetchResults);
	dispatcher_.Register(cproto::kCmdCloseResults, this, &RPCServer::CloseResults);

	dispatcher_.Register(cproto::kCmdGetSQLSuggestions, this, &RPCServer::GetSQLSuggestions);

	dispatcher_.Register(cproto::kCmdGetMeta, this, &RPCServer::GetMeta);
	dispatcher_.Register(cproto::kCmdPutMeta, this, &RPCServer::PutMeta);
	dispatcher_.Register(cproto::kCmdEnumMeta, this, &RPCServer::EnumMeta);
	dispatcher_.Register(cproto::kCmdSubscribeUpdates, this, &RPCServer::SubscribeUpdates);
	dispatcher_.Middleware(this, &RPCServer::CheckAuth);
	dispatcher_.OnClose(this, &RPCServer::OnClose);
	dispatcher_.OnResponse(this, &RPCServer::OnResponse);

	if (logger_) {
		dispatcher_.Logger(this, &RPCServer::Logger);
	}

	auto factory = cproto::ServerConnection::NewFactory(dispatcher_, serverConfig_.EnableConnectionsStats, serverConfig_.MaxUpdatesSize);
	if (serverConfig_.RPCThreadingMode == ServerConfig::kDedicatedThreading) {
		listener_.reset(new ForkedListener(loop, std::move(factory)));
	} else {
		listener_.reset(new Listener<ListenerType::Mixed>(loop, std::move(factory)));
	}

	assertrx(!qrWatcherThread_.joinable());
	auto thLoop = std::make_unique<ev::dynamic_loop>();
	qrWatcherTerminateAsync_.set([](ev::async &a) { a.loop.break_loop(); });
	qrWatcherTerminateAsync_.set(*thLoop);
	qrWatcherTerminateAsync_.start();
	qrWatcherThread_ = std::thread([this, thLoop = std::move(thLoop)]() {
		qrWatcher_.Register(*thLoop, logger_);	// -V522
		do {
			thLoop->run();
		} while (!terminate_);
		qrWatcherTerminateAsync_.stop();
		qrWatcherTerminateAsync_.reset();
		qrWatcher_.Stop();
	});

	return listener_->Bind(addr);
}

RPCClientData::~RPCClientData() {
	Reindexer *db = nullptr;
	auth.GetDB(kRoleNone, &db);
	if (subscribed && db) {
		db->UnsubscribeUpdates(&pusher);
	}
}

}  // namespace reindexer_server
