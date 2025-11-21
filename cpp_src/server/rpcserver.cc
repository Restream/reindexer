#include "rpcserver.h"
#include <sys/stat.h>
#include "cluster/clustercontrolrequest.h"
#include "cluster/config.h"
#include "cluster/sharding/shardingcontrolrequest.h"
#include "core/cjson/jsonbuilder.h"
#include "core/iclientsstats.h"
#include "core/namespace/namespacestat.h"
#include "core/namespace/snapshot/snapshot.h"
#include "debug/crashqueryreporter.h"
#include "events/subscriber_config.h"
#include "net/cproto/cproto.h"
#include "net/cproto/serverconnection.h"
#include "reindexer_version.h"
#include "tools/catch_and_return.h"

namespace reindexer_server {
using namespace std::string_view_literals;

const size_t kMaxTxCount = 1024;
static const reindexer::SemVersion kMinSubscriptionV4RxVersion("4.15.0");

RPCClientData::~RPCClientData() {
	Reindexer* db = nullptr;
	auto err = auth.GetDB<AuthContext::CalledFrom::Core>(kRoleNone, &db);
	if (subscribed && db && err.ok()) {
		err = db->UnsubscribeUpdates(pusher);
		(void)err;	// ignore
	}
}

RPCServer::RPCServer(DBManager& dbMgr, LoggerWrapper& logger, IClientsStats* clientsStats, const ServerConfig& scfg,
					 IStatsWatcher* statsCollector)
	: dbMgr_(dbMgr),
	  serverConfig_(scfg),
	  logger_(logger),
	  statsWatcher_(statsCollector),
	  clientsStats_(clientsStats),
	  startTs_(system_clock_w::now()),
	  qrWatcher_(serverConfig_.RPCQrIdleTimeout) {}

RPCServer::~RPCServer() {
	if (qrWatcherThread_.joinable()) {
		Stop();
	}
	listener_.reset();
}

Error RPCServer::Ping(cproto::Context&) {
	//
	return {};
}

static std::atomic<int> connCounter = {0};

Error RPCServer::Login(cproto::Context& ctx, p_string login, p_string password, p_string db, std::optional<bool> createDBIfMissing,
					   std::optional<bool> checkClusterID, std::optional<int> expectedClusterID, std::optional<p_string> clientRxVersion,
					   std::optional<p_string> appName, std::optional<int64_t> bindingCaps, std::optional<p_string> replToken) {
	if (ctx.GetClientData()) {
		return Error(errParams, "Already logged in");
	}

	auto clientData = std::make_unique<RPCClientData>();
	auto& clientDataRef = *clientData;

	clientData->connID = connCounter.fetch_add(1, std::memory_order_relaxed);
	clientData->auth = AuthContext(login.toString(), password.toString());
	clientData->txStats = std::make_shared<reindexer::TxStats>();
	clientData->caps = bindingCaps ? bindingCaps.value() : BindingCapabilities();

	auto dbName = db.toString();
	if (checkClusterID && *checkClusterID) {
		assertrx(expectedClusterID);
		clientData->auth.SetExpectedClusterID(*expectedClusterID);
	}
	auto status = dbMgr_.Login(dbName, clientData->auth);
	if (!status.ok()) {
		return status;
	}

	clientData->rxVersion = clientRxVersion ? SemVersion(std::string_view(clientRxVersion.value())) : SemVersion();
	clientData->pusher.SetWriter(ctx.writer);
	if (replToken) {
		clientData->replToken = make_key_string(std::move(*replToken));
	}

	if (clientsStats_) {
		reindexer::ClientConnectionStat conn;
		conn.connectionStat = ctx.writer->GetConnectionStat();
		conn.ip = std::string(ctx.clientAddr);
		conn.protocol = protocolName_;
		reindexer::deepCopy(conn.userName, clientData->auth.Login().Str());
		reindexer::deepCopy(conn.dbName, clientData->auth.DBName());
		conn.userRights = std::string(UserRoleName(clientData->auth.UserRights()));
		reindexer::deepCopy(conn.clientVersion, clientData->rxVersion.StrippedString());
		conn.appName = appName ? appName->toString() : std::string();
		conn.txStats = clientData->txStats;
		clientsStats_->AddConnection(clientData->connID, std::move(conn));
	}

	ctx.SetClientData(std::move(clientData));

	status = db.length() ? OpenDatabase(ctx, db, createDBIfMissing) : errOK;
	if (status.ok()) {
		const int64_t startTs = std::chrono::duration_cast<std::chrono::seconds>(startTs_.time_since_epoch()).count();
		constexpr std::string_view version = REINDEX_VERSION;
		ctx.Return({cproto::Arg(p_string(&version)), cproto::Arg(startTs)}, status);
	} else {
		std::cerr << status.what() << std::endl;
	}
	if (statsWatcher_) {
		statsWatcher_->OnClientConnected(clientDataRef.auth.DBName(), statsSourceName(), protocolName_);
	}

	return status;
}

static RPCClientData* getClientDataUnsafe(cproto::Context& ctx) noexcept { return dynamic_cast<RPCClientData*>(ctx.GetClientData()); }

static RPCClientData* getClientDataSafe(cproto::Context& ctx) noexcept {
	auto ret = dynamic_cast<RPCClientData*>(ctx.GetClientData());
	if (!ret) [[unlikely]] {
		std::abort();  // It has to be set by the middleware
	}
	return ret;
}

Error RPCServer::OpenDatabase(cproto::Context& ctx, p_string db, std::optional<bool> createDBIfMissing) {
	auto* clientData = getClientDataSafe(ctx);
	if (clientData->auth.HaveDB()) {
		return Error(errParams, "Database already opened");
	}
	auto status = dbMgr_.OpenDatabase(db.toString(), clientData->auth, createDBIfMissing && *createDBIfMissing);
	if (!status.ok()) {
		clientData->auth.ResetDB();
	}
	return status;
}

Error RPCServer::CloseDatabase(cproto::Context& ctx) {
	auto clientData = getClientDataSafe(ctx);
	cleanupTmpNamespaces(*clientData, ctx.clientAddr);
	clientData->auth.ResetDB();
	return {};
}
Error RPCServer::DropDatabase(cproto::Context& ctx) {
	auto clientData = getClientDataSafe(ctx);
	if (statsWatcher_) {
		// Avoid database access from the stats collecting thread during database drop
		auto statsSuspend = statsWatcher_->SuspendStatsThread();
		return dbMgr_.DropDatabase(clientData->auth);
	}
	return dbMgr_.DropDatabase(clientData->auth);
}

Error RPCServer::CheckAuth(cproto::Context& ctx) {
	cproto::ClientData* ptr = ctx.GetClientData();
	auto clientData = dynamic_cast<RPCClientData*>(ptr);

	if (ctx.call->cmd == cproto::kCmdLogin || ctx.call->cmd == cproto::kCmdPing) {
		return {};
	}

	if (!clientData) {
		return Error(errForbidden, "You should login");
	}

	return {};
}

void RPCServer::OnClose(cproto::Context& ctx, const Error& err) {
	(void)err;

	auto clientData = getClientDataUnsafe(ctx);
	if (clientData) {
		cleanupTmpNamespaces(*clientData);
		if (statsWatcher_) {
			statsWatcher_->OnClientDisconnected(clientData->auth.DBName(), statsSourceName(), protocolName_);
		}
		if (clientsStats_) {
			clientsStats_->DeleteConnection(clientData->connID);
		}
		for (auto& qrId : clientData->results) {
			if (qrId.main >= 0) {
				try {
					qrWatcher_.FreeQueryResults(qrId, false);
					// NOLINTBEGIN(bugprone-empty-catch)
				} catch (...) {
				}
				// NOLINTEND(bugprone-empty-catch)
			}
		}
	}
	logger_.info("RPC: Client disconnected");
}

void RPCServer::OnResponse(cproto::Context& ctx) {
	if (statsWatcher_) {
		auto clientData = getClientDataUnsafe(ctx);
		static const std::string kUnknownDbName("<unknown>");
		const std::string& dbName = (clientData != nullptr) ? clientData->auth.DBName() : kUnknownDbName;
		statsWatcher_->OnOutputTraffic(dbName, statsSourceName(), protocolName_, ctx.stat.sizeStat.respSizeBytes);
		if (ctx.stat.sizeStat.respSizeBytes) {
			// Don't update stats on responses like "updates push"
			statsWatcher_->OnInputTraffic(dbName, statsSourceName(), protocolName_, ctx.stat.sizeStat.reqSizeBytes);
		}
	}
}

Error RPCServer::execSqlQueryByType(std::string_view sqlQuery, QueryResults& res, int fetchLimit, cproto::Context& ctx) noexcept {
	try {
		const auto q = Query::FromSQL(sqlQuery);
		switch (q.Type()) {
			case QuerySelect:
				return getDB(ctx, kRoleDataRead).Select(q, res, unsigned(fetchLimit));
			case QueryDelete:
				return getDB(ctx, kRoleDataWrite).Delete(q, res);
			case QueryUpdate:
				return getDB(ctx, kRoleDataWrite).Update(q, res);
			case QueryTruncate:
				return getDB(ctx, kRoleDBAdmin).TruncateNamespace(q.NsName());
		}
		return Error(errParams, "unknown query type {}", int(q.Type()));
	}
	CATCH_AND_RETURN;
}

void RPCServer::Logger(cproto::Context& ctx, const Error& err, const cproto::Args& ret) {
	const auto clientData = getClientDataUnsafe(ctx);
	uint8_t buf[0x500];
	WrSerializer ser(buf);
	ser << "p='" << protocolName_ << '\'';
	if (clientData) {
		ser << "c='"sv << clientData->connID << "' db='"sv << clientData->auth.Login() << '@' << clientData->auth.DBName() << "' "sv;
	} else {
		ser << "- - "sv;
	}

	if (ctx.call) {
		ser << cproto::CmdName(ctx.call->cmd) << " "sv;
		ctx.call->args.Dump(ser, cproto::GetMaskArgs(ctx.call->cmd));
	} else {
		ser << '-';
	}

	ser << " -> "sv << (err.ok() ? "OK"sv : err.whatStr());
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

Error RPCServer::OpenNamespace(cproto::Context& ctx, p_string nsDefJson, std::optional<p_string> v) {
	NamespaceDef nsDef;
	NsReplicationOpts replOpts;
	Error err;
	if (v) {
		err = replOpts.FromJSON(giftStr(v.value()));
		if (!err.ok()) {
			return err;
		}
	}

	err = nsDef.FromJSON(giftStr(nsDefJson));
	if (!err.ok()) {
		return err;
	}
	if (!nsDef.indexes.empty()) {
		return getDB(ctx, kRoleDataRead).AddNamespace(nsDef, replOpts);
	}
	return getDB(ctx, kRoleDataRead).OpenNamespace(nsDef.name, nsDef.storage, replOpts);
}

Error RPCServer::DropNamespace(cproto::Context& ctx, p_string ns) {
	auto err = getDB(ctx, kRoleDBAdmin).DropNamespace(ns);
	if (err.ok()) {
		auto clientData = getClientDataSafe(ctx);
		clientData->tmpNss.erase(std::string_view(ns));
	}
	return err;
}

Error RPCServer::TruncateNamespace(cproto::Context& ctx, p_string ns) { return getDB(ctx, kRoleDBAdmin).TruncateNamespace(ns); }

Error RPCServer::RenameNamespace(cproto::Context& ctx, p_string srcNsName, p_string dstNsName) {
	auto err = getDB(ctx, kRoleDBAdmin).RenameNamespace(srcNsName, dstNsName.toString());
	if (err.ok()) {
		auto clientData = getClientDataSafe(ctx);
		clientData->tmpNss.erase(std::string_view(srcNsName));
	}
	return err;
}

Error RPCServer::CreateTemporaryNamespace(cproto::Context& ctx, p_string nsDefJson, int64_t v) {
	NamespaceDef nsDef;
	auto err = nsDef.FromJSON(giftStr(nsDefJson));
	if (!err.ok()) {
		return err;
	}
	std::string resultName;
	err = getDB(ctx, kRoleDataRead).CreateTemporaryNamespace(nsDef.name, resultName, nsDef.storage, lsn_t(v));
	if (err.ok()) {
		auto clientData = getClientDataSafe(ctx);
		clientData->tmpNss.emplace(resultName);
		ctx.Return({cproto::Arg(p_string(&resultName))});
	}
	return err;
}

Error RPCServer::CloseNamespace(cproto::Context&, p_string /*ns*/) {
	// Do not close.
	// TODO: add reference counters
	// return getDB(ctx, kRoleDataRead)->CloseNamespace(ns);
	return {};
}

Error RPCServer::EnumNamespaces(cproto::Context& ctx, std::optional<int> opts, std::optional<p_string> filter) {
	std::vector<NamespaceDef> nsDefs;
	EnumNamespacesOpts eopts;
	if (opts) {
		eopts.options_ = *opts;
	}
	if (filter) {
		eopts.filter_ = *filter;
	}

	auto err = getDB(ctx, kRoleDataRead).EnumNamespaces(nsDefs, eopts);
	if (!err.ok()) {
		return err;
	}
	WrSerializer ser;
	ser << "{\"items\":[";
	for (unsigned i = 0; i < nsDefs.size(); i++) {
		if (i != 0) {
			ser << ',';
		}
		nsDefs[i].GetJSON(ser);
	}
	ser << "]}";
	auto resSlice = ser.Slice();

	ctx.Return({cproto::Arg(p_string(&resSlice))});
	return errOK;
}

Error RPCServer::EnumDatabases(cproto::Context& ctx) {
	auto dbList = dbMgr_.EnumDatabases();

	WrSerializer ser;
	JsonBuilder jb(ser);
	std::span<const std::string> array(dbList.empty() ? nullptr : &dbList[0], dbList.size());
	jb.Array("databases"sv, array);
	jb.End();

	auto resSlice = ser.Slice();
	ctx.Return({cproto::Arg(p_string(&resSlice))});
	return errOK;
}

Error RPCServer::AddIndex(cproto::Context& ctx, p_string ns, p_string indexDefStr) {
	auto indexDef = IndexDef::FromJSON(giftStr(indexDefStr));
	if (!indexDef) {
		return indexDef.error();
	}
	return getDB(ctx, kRoleDBAdmin).AddIndex(ns, *indexDef);
}

Error RPCServer::UpdateIndex(cproto::Context& ctx, p_string ns, p_string indexDefStr) {
	auto indexDef = IndexDef::FromJSON(giftStr(indexDefStr));
	if (!indexDef) {
		return indexDef.error();
	}
	return getDB(ctx, kRoleDBAdmin).UpdateIndex(ns, *indexDef);
}

Error RPCServer::DropIndex(cproto::Context& ctx, p_string ns, p_string index) {
	return getDB(ctx, kRoleDBAdmin).DropIndex(ns, IndexDef{index.toString()});
}

Error RPCServer::SetSchema(cproto::Context& ctx, p_string ns, p_string schema) {
	return getDB(ctx, kRoleDBAdmin).SetSchema(ns, std::string_view(schema));
}

Error RPCServer::GetSchema(cproto::Context& ctx, p_string ns, int format) {
	std::string schema;
	auto err = getDB(ctx, kRoleDataRead).GetSchema(ns, format, schema);
	if (!err.ok()) {
		return err;
	}
	ctx.Return({cproto::Arg(schema)});
	return errOK;
}

Error RPCServer::StartTransaction(cproto::Context& ctx, p_string nsName) {
	int64_t id = -1;
	try {
		id = addTx(ctx, nsName);
	} catch (reindexer::Error& e) {
		return e;
	}
	ctx.Return({cproto::Arg(id)});
	return Error();
}

Error RPCServer::AddTxItem(cproto::Context& ctx, int format, p_string itemData, int mode, p_string perceptsPack, int stateToken,
						   int64_t txID) {
	Transaction& tr = getTx(ctx, txID);

	auto item = tr.NewItem();
	Error err;
	if (!item.Status().ok()) {
		return item.Status();
	}

	err = processTxItem(static_cast<DataFormat>(format), itemData, item, static_cast<ItemModifyMode>(mode), stateToken);
	if (err.code() == errTagsMissmatch) {
		item = getDB(ctx, kRoleDataWrite).NewItem(tr.GetNsName());
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
		const uint64_t preceptsCount = ser.GetVarUInt();
		std::vector<std::string> precepts;
		precepts.reserve(preceptsCount);
		for (unsigned prIndex = 0; prIndex < preceptsCount; ++prIndex) {
			precepts.emplace_back(ser.GetVString());
		}
		item.SetPrecepts(std::move(precepts));
	}
	return tr.Modify(std::move(item), ItemModifyMode(mode), ctx.call->lsn);
}

Error RPCServer::DeleteQueryTx(cproto::Context& ctx, p_string queryBin, int64_t txID) noexcept {
	try {
		Transaction& tr = getTx(ctx, txID);
		Serializer ser(queryBin.data(), queryBin.size());
		Query query = Query::Deserialize(ser);
		query.type_ = QueryDelete;
		return tr.Modify(std::move(query), ctx.call->lsn);
	}
	CATCH_AND_RETURN;
}

Error RPCServer::UpdateQueryTx(cproto::Context& ctx, p_string queryBin, int64_t txID) noexcept {
	try {
		Transaction& tr = getTx(ctx, txID);
		Serializer ser(queryBin.data(), queryBin.size());
		Query query = Query::Deserialize(ser);
		query.type_ = QueryUpdate;
		return tr.Modify(std::move(query), ctx.call->lsn);
	}
	CATCH_AND_RETURN;
}

Error RPCServer::PutMetaTx(cproto::Context& ctx, p_string key, p_string data, int64_t txID) noexcept {
	try {
		auto db = getDB(ctx, kRoleDataWrite);

		Transaction& tr = getTx(ctx, txID);
		return tr.PutMeta(key, data, ctx.call->lsn);
	}
	CATCH_AND_RETURN;
}

Error RPCServer::SetTagsMatcherTx(cproto::Context& ctx, int64_t statetoken, int64_t version, p_string data, int64_t txID) noexcept {
	try {
		auto db = getDB(ctx, kRoleDataWrite);

		Transaction& tr = getTx(ctx, txID);
		TagsMatcher tm;
		Serializer ser(data);
		tm.deserialize(ser, int(version), int(statetoken));
		return tr.SetTagsMatcher(std::move(tm), ctx.call->lsn);
	}
	CATCH_AND_RETURN;
}

Error RPCServer::CommitTx(cproto::Context& ctx, int64_t txId, std::optional<int> flagsOpts) {
	auto db = getDB(ctx, kRoleDataWrite);

	Transaction& tr = getTx(ctx, txId);
	QueryResults qres;

	auto err = db.CommitTransaction(tr, qres);
	if (err.ok()) {
		auto tmp = qres.GetNamespaces();
		int32_t ptVers = -1;
		ResultFetchOpts opts;
		int flags;
		if (flagsOpts) {
			// Transactions can not send items content
			flags = flagsOpts.value() & ~kResultsFormatMask;
		} else {
			flags = kResultsWithItemID;
			if (tr.IsTagsUpdated()) {
				flags |= kResultsWithPayloadTypes;
			}
		}
		if (flags & kResultsWithPayloadTypes) {
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

Error RPCServer::RollbackTx(cproto::Context& ctx, int64_t txId) {
	auto db = getDB(ctx, kRoleDataWrite);

	Error err;
	try {
		Transaction& tr = getTx(ctx, txId);
		err = db.RollBackTransaction(tr);
	} catch (Error& e) {
		return e;
	}

	clearTx(ctx, txId);
	return err;
}

Error RPCServer::ModifyItem(cproto::Context& ctx, p_string ns, int format, p_string itemData, int mode, p_string perceptsPack,
							int stateToken, int /*txID*/) {
	using std::chrono::milliseconds;
	using std::chrono::duration_cast;

	auto db = getDB(ctx, kRoleDataWrite);
	auto execTimeout = ctx.call->execTimeout;
	auto beginT = steady_clock_w::now_coarse();
	auto item = Item(db.NewItem(ns));
	if (execTimeout.count() > 0) {
		execTimeout -= duration_cast<milliseconds>(beginT - steady_clock_w::now_coarse());
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
			err = item.Unsafe().FromJSON(itemData, nullptr, false);	 // TODO: for mode == ModeDelete deserialize PK and sharding key only
			break;
		case FormatCJson:
			if (item.GetStateToken() != stateToken) {
				err = Error(errStateInvalidated, "stateToken mismatch:  {:#08x}, need {:#08x}. Can't process item", stateToken,
							item.GetStateToken());
			} else {
				// TODO: To delete an item with sharding you need
				// not only PK fields, but all the sharding keys
				err = item.Unsafe().FromCJSON(itemData, false);	 // TODO: for mode == ModeDelete deserialize PK and sharding key only
			}
			break;
		case FormatMsgPack: {
			size_t offset = 0;
			err = item.FromMsgPack(itemData, offset);
			break;
		}
		default:
			err = Error(errNotValid, "Invalid source item format {}", format);
	}
	if (!err.ok()) {
		return err;
	}
	tmUpdated = item.IsTagsUpdated();

	if (perceptsPack.length()) {
		Serializer ser(perceptsPack);
		const uint64_t preceptsCount = ser.GetVarUInt();
		std::vector<std::string> precepts;
		precepts.reserve(preceptsCount);
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
			default:
				return Error(errParams, "Unexpected ItemModifyMode: {}", mode);
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
			default:
				return Error(errParams, "Unexpected ItemModifyMode: {}", mode);
		}
		if (err.ok()) {
			if (item.GetID() != -1) {
				LocalQueryResults lqr;
				lqr.AddItemNoHold(item, lsn_t());
				qres.AddQr(std::move(lqr), item.GetShardID());
			}
		} else {
			return err;
		}
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

Error RPCServer::DeleteQuery(cproto::Context& ctx, p_string queryBin, std::optional<int> flagsOpts) noexcept {
	try {
		Serializer ser(queryBin.data(), queryBin.size());
		Query query = Query::Deserialize(ser);
		query.type_ = QueryDelete;

		ActiveQueryScope scope(query, QueryDelete);
		const int flags = flagsOpts ? flagsOpts.value() : kResultsWithItemID;
		QueryResults qres(flags);
		auto err = getDB(ctx, kRoleDataWrite).Delete(query, qres);
		if (!err.ok()) {
			return err;
		}
		int32_t ptVersion = -1;
		ResultFetchOpts opts{
			.flags = flags, .ptVersions = {&ptVersion, 1}, .fetchOffset = 0, .fetchLimit = INT_MAX, .withAggregations = true};
		return sendResults(ctx, qres, RPCQrId(), opts);
	}
	CATCH_AND_RETURN;
}

Error RPCServer::UpdateQuery(cproto::Context& ctx, p_string queryBin, std::optional<int> flagsOpts) noexcept {
	try {
		Serializer ser(queryBin.data(), queryBin.size());
		Query query = Query::Deserialize(ser);
		query.type_ = QueryUpdate;

		ActiveQueryScope scope(query, QueryUpdate);
		const int flags = flagsOpts ? flagsOpts.value() : (kResultsWithItemID | kResultsWithPayloadTypes | kResultsCJson);
		QueryResults qres(flags);
		auto err = getDB(ctx, kRoleDataWrite).Update(query, qres);
		if (!err.ok()) {
			return err;
		}

		int32_t ptVersion = -1;
		ResultFetchOpts opts{
			.flags = flags, .ptVersions = {&ptVersion, 1}, .fetchOffset = 0, .fetchLimit = INT_MAX, .withAggregations = true};
		return sendResults(ctx, qres, RPCQrId(), opts);
	}
	CATCH_AND_RETURN;
}

Reindexer RPCServer::getDB(cproto::Context& ctx, UserRole role) {
	auto clientData = getClientDataUnsafe(ctx);
	if (clientData) [[likely]] {
		Reindexer* db = nullptr;
		auto status = clientData->auth.GetDB<AuthContext::CalledFrom::RPCServer>(role, &db, ctx.call->lsn, ctx.call->emitterServerId,
																				 ctx.call->shardId);
		if (!status.ok()) [[unlikely]] {
			throw status;
		}

		if (db != nullptr) [[likely]] {
			NeedMaskingDSN needMaskingDSN{clientData->auth.UserRights() < kRoleDBAdmin};
			return db->NeedTraceActivity()
					   ? db->WithContextParams(ctx.call->execTimeout, ctx.call->lsn, ctx.call->emitterServerId, ctx.call->shardId,
											   ctx.call->shardingParallelExecution, clientData->replToken, needMaskingDSN, ctx.clientAddr,
											   clientData->auth.Login().Str(), clientData->connID)
					   : db->WithContextParams(ctx.call->execTimeout, ctx.call->lsn, ctx.call->emitterServerId, ctx.call->shardId,
											   ctx.call->shardingParallelExecution, clientData->replToken, needMaskingDSN);
		}
	}
	throw Error(errParams, "Database is not opened, you should open it first");
}

void RPCServer::cleanupTmpNamespaces(RPCClientData& clientData, std::string_view activity) {
	Reindexer* dbPtr = nullptr;
	auto status =
		clientData.auth.GetDB<AuthContext::CalledFrom::RPCServer>(kRoleDBAdmin, &dbPtr, lsn_t{0}, -1, ShardingKeyType::NotSetShard);
	if (!status.ok() || dbPtr == nullptr) {
		logger_.info("RPC: Unable to cleanup tmp namespaces:{}", status.what());
		return;
	}
	auto db = dbPtr->NeedTraceActivity()
				  ? dbPtr->WithActivityTracer(activity, std::string(clientData.auth.Login().Str()), clientData.connID)
				  : *dbPtr;

	for (auto& ns : clientData.tmpNss) {
		auto e = db.DropNamespace(ns);
		logger_.info("RPC: Dropping tmp ns {}:{}", ns, e.ok() ? "OK"sv : e.what());
	}
}

Error RPCServer::sendResults(cproto::Context& ctx, QueryResults& qres, RPCQrId id, const ResultFetchOpts& opts) {
	auto data = getClientDataSafe(ctx);
	uint8_t serBuf[0x2000];
	WrResultSerializer rser(serBuf, opts);
	try {
		bool doClose = false;
		if (qres.IsRawProxiedBufferAvailable(opts.flags) && rser.IsRawResultsSupported(data->caps, qres)) {
			doClose = rser.PutResultsRaw(qres);
		} else {
			doClose = rser.PutResults(qres, data->caps);
		}
		if (doClose && id.main >= 0) {
			freeQueryResults(ctx, id);
			id.main = -1;
			id.uid = RPCQrWatcher::kUninitialized;
		}
		std::string_view resSlice = rser.Slice();
		ctx.Return({cproto::Arg(p_string(&resSlice)), cproto::Arg(int(id.main)), cproto::Arg(int64_t(id.uid))});
	} catch (Error& err) {
		if (id.main >= 0) {
			try {
				freeQueryResults(ctx, id);
				id.main = -1;
				id.uid = RPCQrWatcher::kUninitialized;
				// NOLINTBEGIN(bugprone-empty-catch)
			} catch (...) {
			}
			// NOLINTEND(bugprone-empty-catch)
		}
		return err;
	}

	return Error();
}

Error RPCServer::processTxItem(DataFormat format, std::string_view itemData, Item& item, ItemModifyMode /*mode*/,
							   int stateToken) const noexcept {
	switch (format) {
		case FormatJson:
			return item.FromJSON(itemData, nullptr, false);	 // TODO: for mode == ModeDelete deserialize PK and sharding key only
		case FormatCJson:
			if (item.GetStateToken() != stateToken) {
				return Error(errStateInvalidated, "stateToken mismatch:  {:#08x}, need {:#08x}. Can't process tx item", stateToken,
							 item.GetStateToken());
			} else {
				return item.FromCJSON(itemData, false);	 // TODO: for mode == ModeDelete deserialize PK and sharding key only
			}
		case FormatMsgPack: {
			size_t offset = 0;
			return item.FromMsgPack(itemData, offset);
		}
		default:
			return Error(errNotValid, "Invalid source item format {}", int(format));
	}
}

RPCQrWatcher::Ref RPCServer::createQueryResults(cproto::Context& ctx, RPCQrId& id, int flags) {
	auto data = getClientDataSafe(ctx);

	assertrx(id.main < 0);

	RPCQrWatcher::Ref qres;
	qres = qrWatcher_.GetQueryResults(id, flags);

	for (auto& qrId : data->results) {
		if (qrId.main < 0 || qrId.main == id.main) {
			qrId = id;
			return qres;
		}
	}

	if (data->results.size() >= cproto::kMaxConcurrentQueries) [[unlikely]] {
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

void RPCServer::freeQueryResults(cproto::Context& ctx, RPCQrId id) {
	auto data = getClientDataSafe(ctx);
	for (auto& qrId : data->results) {
		if (qrId.main == id.main) {
			if (qrId.uid != id.uid) {
				if (!qrWatcher_.AreQueryResultsValid(RPCQrId{qrId.main, qrId.uid})) {
					qrId.main = -1;	 // Qr is timed out
					continue;
				} else {
					throw Error(errLogic, "Invalid query uid: {} vs {}", qrId.uid, id.uid);
				}
			}
			qrId.main = -1;
			qrWatcher_.FreeQueryResults(id, true);
			return;
		}
	}
}

Transaction& RPCServer::getTx(cproto::Context& ctx, int64_t id) {
	auto data = getClientDataSafe(ctx);

	if (size_t(id) >= data->txs.size() || data->txs[id].IsFree()) {
		throw Error(errLogic, "Invalid tx id {}", id);
	}
	return data->txs[id];
}

int64_t RPCServer::addTx(cproto::Context& ctx, std::string_view nsName) {
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

void RPCServer::clearTx(cproto::Context& ctx, uint64_t txId) {
	auto data = getClientDataSafe(ctx);
	if (txId >= data->txs.size()) {
		throw Error(errLogic, "Invalid tx id {}", txId);
	}
	assertrx(data->txStats);
	data->txStats->txCount -= 1;
	data->txs[txId] = Transaction();
}

Snapshot& RPCServer::getSnapshot(cproto::Context& ctx, int& id) {
	auto data = getClientDataSafe(ctx);

	if (id < 0) {
		for (id = 0; id < int(data->snapshots.size()); ++id) {
			if (!data->snapshots[id].second) {
				data->snapshots[id] = {Snapshot(), true};
				return data->snapshots[id].first;
			}
		}

		if (data->snapshots.size() >= cproto::kMaxConcurentSnapshots) {
			throw Error(errLogic, "Too many paralell snapshots");
		}
		id = data->snapshots.size();
		data->snapshots.emplace_back(std::make_pair(Snapshot(), true));
	}

	if (id >= int(data->snapshots.size())) {
		throw Error(errLogic, "Invalid snapshot id");
	}
	return data->snapshots[id].first;
}

Error RPCServer::fetchSnapshotRecords(cproto::Context& ctx, int id, int64_t offset, bool putHeaders) {
	cproto::Args args;
	WrSerializer ser;
	std::string_view resSlice;
	Snapshot* snapshot = nullptr;
	if (offset < 0) {
		freeSnapshot(ctx, id);
		return Error();
	}
	try {
		snapshot = &getSnapshot(ctx, id);
	} catch (Error& e) {
		return e;
	}

	if (putHeaders) {
		// Put header
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
	if (putHeaders) {
		// Put extra opts at the end for backwards compatibility
		const auto st = snapshot->ClusterOperationStat();
		args.emplace_back(st.leaderId);
		args.emplace_back(int(st.role));
	}
	if (++it == snapshot->end()) {
		freeSnapshot(ctx, id);
	}
	ctx.Return(args);

	return Error();
}

void RPCServer::freeSnapshot(cproto::Context& ctx, int id) {
	auto data = getClientDataSafe(ctx);
	if (id >= int(data->snapshots.size()) || id < 0) {
		throw Error(errLogic, "Invalid snapshot id");
	}
	data->snapshots[id] = {Snapshot(), false};
}

static h_vector<int32_t, 4> pack2vec(p_string pack) {
	// Get array of payload Type Versions
	Serializer ser(pack.data(), pack.size());
	h_vector<int32_t, 4> vec;
	int cnt = ser.GetVarUInt();
	vec.reserve(cnt);
	for (int i = 0; i < cnt; i++) {
		vec.emplace_back(ser.GetVarUInt());
	}
	return vec;
}

Error RPCServer::Select(cproto::Context& ctx, p_string queryBin, int flags, int limit, p_string ptVersionsPck) {
	Query query;
	Serializer ser(queryBin);
	try {
		query = Query::Deserialize(ser);
	} catch (Error& err) {
		return err;
	}

	ActiveQueryScope scope(query, QuerySelect);
	const auto data = getClientDataSafe(ctx);
	if (query.IsWALQuery()) {
		query.Where(std::string("#slave_version"sv), CondEq, data->rxVersion.StrippedString());
	}

	RPCQrWatcher::Ref qres;
	RPCQrId id{-1, data->caps.HasQrIdleTimeouts() ? RPCQrWatcher::kUninitialized : RPCQrWatcher::kDisabled};
	try {
		qres = createQueryResults(ctx, id, flags);
	} catch (Error& e) {
		if (e.code() == errQrUIDMissmatch) {
			return e;
		}
		throw e;
	}

	auto ret = getDB(ctx, kRoleDataRead).Select(query, *qres, unsigned(limit));
	if (!ret.ok()) {
		freeQueryResults(ctx, id);
		return ret;
	}
	auto ptVersions = pack2vec(ptVersionsPck);
	ResultFetchOpts opts{
		.flags = flags, .ptVersions = ptVersions, .fetchOffset = 0, .fetchLimit = unsigned(limit), .withAggregations = true};

	return sendResults(ctx, *qres, id, opts);
}

Error RPCServer::ExecSQL(cproto::Context& ctx, p_string querySql, int flags, int limit, p_string ptVersionsPck) {
	const auto data = getClientDataSafe(ctx);
	RPCQrId id{-1, data->caps.HasQrIdleTimeouts() ? RPCQrWatcher::kUninitialized : RPCQrWatcher::kDisabled};
	RPCQrWatcher::Ref qres;
	try {
		qres = createQueryResults(ctx, id, flags);
	} catch (Error& e) {
		if (e.code() == errQrUIDMissmatch) {
			return e;
		}
		throw;
	}

	ActiveQueryScope scope(querySql);
	auto ret = execSqlQueryByType(querySql, *qres, limit, ctx);
	if (!ret.ok()) {
		freeQueryResults(ctx, id);
		return ret;
	}
	auto ptVersions = pack2vec(ptVersionsPck);
	ResultFetchOpts opts{
		.flags = flags, .ptVersions = ptVersions, .fetchOffset = 0, .fetchLimit = unsigned(limit), .withAggregations = true};

	return sendResults(ctx, *qres, id, opts);
}

Error RPCServer::FetchResults(cproto::Context& ctx, int reqId, int flags, int offset, int limit, std::optional<int64_t> qrUID) {
	flags &= ~kResultsWithPayloadTypes;
	RPCQrId id{reqId, qrUID ? *qrUID : RPCQrWatcher::kDisabled};
	RPCQrWatcher::Ref qres;
	try {
		qres = qrWatcher_.GetQueryResults(id, 0);
	} catch (Error& e) {
		if (e.code() == errQrUIDMissmatch) {
			return e;
		}
		throw;
	}

	ResultFetchOpts opts{
		.flags = flags, .ptVersions = {}, .fetchOffset = unsigned(offset), .fetchLimit = unsigned(limit), .withAggregations = false};
	return sendResults(ctx, *qres, id, opts);
}

Error RPCServer::CloseResults(cproto::Context& ctx, int reqId, std::optional<int64_t> qrUID, std::optional<bool> doNotReply) {
	if (doNotReply && *doNotReply) {
		ctx.respSent = true;
	}
	const RPCQrId id{reqId, qrUID ? *qrUID : RPCQrWatcher::kDisabled};
	try {
		freeQueryResults(ctx, id);
	} catch (Error& e) {
		return e;
	}
	return Error();
}

Error RPCServer::GetSQLSuggestions(cproto::Context& ctx, p_string query, int pos) {
	std::vector<std::string> suggests;
	Error err = getDB(ctx, kRoleDataRead).GetSqlSuggestions(query, pos, suggests);

	if (err.ok()) {
		cproto::Args ret;
		ret.reserve(suggests.size());
		for (auto& suggest : suggests) {
			ret.emplace_back(std::move(suggest));
		}
		ctx.Return(ret);
	}
	return err;
}

Error RPCServer::GetReplState(cproto::Context& ctx, p_string ns) {
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

Error RPCServer::SetClusterOperationStatus(cproto::Context& ctx, p_string ns, p_string serStatus) {
	ClusterOperationStatus status;
	auto err = status.FromJSON(giftStr(serStatus));
	if (!err.ok()) {
		return err;
	}
	return getDB(ctx, kRoleDBAdmin).SetClusterOperationStatus(ns, status);
}

Error RPCServer::GetSnapshot(cproto::Context& ctx, p_string ns, p_string optsJson) {
	int id = -1;
	Snapshot* snapshot = nullptr;
	try {
		snapshot = &getSnapshot(ctx, id);
	} catch (Error& e) {
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

Error RPCServer::FetchSnapshot(cproto::Context& ctx, int id, int64_t offset) { return fetchSnapshotRecords(ctx, id, offset, false); }

Error RPCServer::ApplySnapshotChunk(cproto::Context& ctx, p_string ns, p_string rec) {
	SnapshotChunk ch;
	Serializer ser(rec);
	ch.Deserialize(ser);
	return getDB(ctx, kRoleDataWrite).ApplySnapshotChunk(ns, ch);
}

Error RPCServer::GetMeta(cproto::Context& ctx, p_string ns, p_string key, std::optional<int> options) {
	if (options && options.value() == 1) {
		std::vector<ShardedMeta> data;
		auto err = getDB(ctx, kRoleDataRead).GetMeta(ns, key.toString(), data);
		if (!err.ok()) {
			return err;
		}
		cproto::Args ret;
		WrSerializer wser;
		for (auto& d : data) {
			wser.Reset();
			d.GetJSON(wser);
			ret.push_back(cproto::Arg(wser.Slice()));
		}
		ctx.Return(ret);
	} else {
		std::string data;
		auto err = getDB(ctx, kRoleDataRead).GetMeta(ns, key.toString(), data);
		if (!err.ok()) {
			return err;
		}
		ctx.Return({cproto::Arg(data)});
	}

	return Error();
}

Error RPCServer::PutMeta(cproto::Context& ctx, p_string ns, p_string key, p_string data) {
	return getDB(ctx, kRoleDataWrite).PutMeta(ns, key.toString(), data);
}

Error RPCServer::EnumMeta(cproto::Context& ctx, p_string ns) {
	std::vector<std::string> keys;
	auto err = getDB(ctx, kRoleDataWrite).EnumMeta(ns, keys);
	if (!err.ok()) {
		return err;
	}
	cproto::Args ret;
	for (auto& key : keys) {
		ret.emplace_back(std::move(key));
	}
	ctx.Return(ret);
	return Error();
}

Error RPCServer::DeleteMeta(cproto::Context& ctx, p_string ns, p_string key) {
	return getDB(ctx, kRoleDataWrite).DeleteMeta(ns, key.toString());
}

Error RPCServer::SubscribeUpdates([[maybe_unused]] cproto::Context& ctx, [[maybe_unused]] int flag,
								  [[maybe_unused]] std::optional<p_string> filterV3Json, [[maybe_unused]] std::optional<int> options,
								  std::optional<p_string> subscriptionOpts) {
	auto clientData = getClientDataSafe(ctx);
	if (subscriptionOpts.has_value()) {
		if (clientData->rxVersion <= kMinSubscriptionV4RxVersion) {
			return Error(errForbidden, "Events subscription is available for v4.16.0+");
		}
		if (filterV3Json.has_value() && !std::string_view(filterV3Json.value()).empty()) {
			return Error(errParams, "Unable to mix v3 and v4 subscription filters");
		}
		if (options.has_value() && options.value() != 0) {
			return Error(errParams, "Unable to mix v3 and v4 subscription options");
		}
		EventSubscriberConfig cfg;
		auto err = cfg.FromJSON(giftStr(*subscriptionOpts));
		if (!err.ok()) {
			return err;
		}
		const auto streamsCnt = cfg.ActiveStreams();
		auto db = getDB(ctx, kRoleDataRead);
		if (flag) {
			err = db.SubscribeUpdates(clientData->pusher, std::move(cfg));
		} else {
			err = db.UnsubscribeUpdates(clientData->pusher);
		}
		if (err.ok()) {
			clientData->subscribed = flag && streamsCnt;
		}
		return err;
	} else {
		return Error(errForbidden, "Updates subscription from V3 is no longer supported");
	}
}

Error RPCServer::SuggestLeader(cproto::Context& ctx, p_string suggestion) {
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

Error RPCServer::ShardingControlRequest(cproto::Context& ctx, p_string data) noexcept {
	try {
		sharding::ShardingControlRequestData req;
		Error err = req.FromJSON(giftStr(data));
		if (!err.ok()) {
			return err;
		}

		sharding::ShardingControlResponseData res;
		err = getDB(ctx, kRoleDBAdmin).ShardingControlRequest(req, res);
		if (err.ok()) {
			WrSerializer ser;
			if (res.type == sharding::ControlCmdType::GetNodeConfig) {
				std::get<sharding::GetNodeConfigCommand>(res.data).masking =
					getClientDataSafe(ctx)->auth.UserRights() < UserRole::kRoleDBAdmin;
			}
			res.GetJSON(ser);
			auto slice = ser.Slice();
			ctx.Return({cproto::Arg(p_string(&slice))});
		}
		return err;
	}
	CATCH_AND_RETURN
}

Error RPCServer::LeadersPing(cproto::Context& ctx, p_string leader) {
	cluster::NodeData l;
	auto err = l.FromJSON(giftStr(leader));
	if (err.ok()) {
		err = getDB(ctx, kRoleDataWrite).LeadersPing(l);
	}
	return err;
}

Error RPCServer::GetRaftInfo(cproto::Context& ctx) {
	cluster::RaftInfo info;
	auto err = getDB(ctx, kRoleDataRead).GetRaftInfo(info);
	if (err.ok()) {
		WrSerializer ser;
		info.GetJSON(ser);
		auto serSlice = ser.Slice();
		ctx.Return({cproto::Arg(p_string(&serSlice))});
	}
	return err;
}

Error RPCServer::ClusterControlRequest(cproto::Context& ctx, p_string data) {
	ClusterControlRequestData req;
	Error err = req.FromJSON(giftStr(data));
	if (!err.ok()) {
		return err;
	}
	return getDB(ctx, kRoleDBAdmin).ClusterControlRequest(req);
}

Error RPCServer::SetTagsMatcher(cproto::Context& ctx, p_string ns, int64_t statetoken, int64_t version, p_string data) {
	TagsMatcher tm;
	Serializer ser(data);
	try {
		tm.deserialize(ser, int(version), int(statetoken));
	} catch (Error& err) {
		return err;
	}
	return getDB(ctx, kRoleDataWrite).SetTagsMatcher(ns, std::move(tm));
}

void RPCServer::Start(const std::string& addr, ev::dynamic_loop& loop, RPCSocketT sockDomain, std::string_view threadingMode) {
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
	dispatcher_.Register(cproto::kCmdGetSchema, this, &RPCServer::GetSchema);
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
	dispatcher_.Register(cproto::kCmdExecSQL, this, &RPCServer::ExecSQL);
	dispatcher_.Register(cproto::kCmdFetchResults, this, &RPCServer::FetchResults);
	dispatcher_.Register(cproto::kCmdCloseResults, this, &RPCServer::CloseResults);
	dispatcher_.Register(cproto::kCmdGetSQLSuggestions, this, &RPCServer::GetSQLSuggestions);
	dispatcher_.Register(cproto::kCmdGetMeta, this, &RPCServer::GetMeta);
	dispatcher_.Register(cproto::kCmdPutMeta, this, &RPCServer::PutMeta);
	dispatcher_.Register(cproto::kCmdEnumMeta, this, &RPCServer::EnumMeta);
	dispatcher_.Register(cproto::kCmdDeleteMeta, this, &RPCServer::DeleteMeta);
	dispatcher_.Register(cproto::kCmdSubscribeUpdates, this, &RPCServer::SubscribeUpdates);
	dispatcher_.Register(cproto::kCmdGetReplState, this, &RPCServer::GetReplState);
	dispatcher_.Register(cproto::kCmdCreateTmpNamespace, this, &RPCServer::CreateTemporaryNamespace);
	dispatcher_.Register(cproto::kCmdSetClusterOperationStatus, this, &RPCServer::SetClusterOperationStatus);
	dispatcher_.Register(cproto::kCmdGetSnapshot, this, &RPCServer::GetSnapshot);
	dispatcher_.Register(cproto::kCmdFetchSnapshot, this, &RPCServer::FetchSnapshot);
	dispatcher_.Register(cproto::kCmdApplySnapshotCh, this, &RPCServer::ApplySnapshotChunk);
	dispatcher_.Register(cproto::kCmdPutTxMeta, this, &RPCServer::PutMetaTx);
	dispatcher_.Register(cproto::kCmdSetTagsMatcherTx, this, &RPCServer::SetTagsMatcherTx);
	dispatcher_.Register(cproto::kCmdSuggestLeader, this, &RPCServer::SuggestLeader);
	dispatcher_.Register(cproto::kCmdLeadersPing, this, &RPCServer::LeadersPing);
	dispatcher_.Register(cproto::kCmdGetRaftInfo, this, &RPCServer::GetRaftInfo);
	dispatcher_.Register(cproto::kCmdClusterControlRequest, this, &RPCServer::ClusterControlRequest);
	dispatcher_.Register(cproto::kCmdSetTagsMatcher, this, &RPCServer::SetTagsMatcher);
	dispatcher_.Register(cproto::kShardingControlRequest, this, &RPCServer::ShardingControlRequest);

	dispatcher_.Middleware(this, &RPCServer::CheckAuth);
	dispatcher_.OnClose(this, &RPCServer::OnClose);
	dispatcher_.OnResponse(this, &RPCServer::OnResponse);

	if (logger_) {
		dispatcher_.Logger(this, &RPCServer::Logger);
	}

	protocolName_ = (sockDomain == RPCSocketT::TCP) ? kTcpProtocolName : kUnixProtocolName;
	auto factory = cproto::ServerConnection::NewFactory(dispatcher_, serverConfig_.EnableConnectionsStats);
	auto sslCtx =
		serverConfig_.RPCsAddr == addr ? openssl::create_server_context(serverConfig_.SslCertPath, serverConfig_.SslKeyPath) : nullptr;
	if (threadingMode == ServerConfig::kDedicatedThreading) {
		listener_ = std::make_unique<ForkedListener>(loop, std::move(factory), std::move(sslCtx));
	} else {
		listener_ = std::make_unique<Listener<ListenerType::Mixed>>(loop, std::move(factory), std::move(sslCtx));
	}

	assertrx(!qrWatcherThread_.joinable());
	auto thLoop = std::make_unique<ev::dynamic_loop>();
	qrWatcherTerminateAsync_.set([](ev::async& a) { a.loop.break_loop(); });
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

	listener_->Bind(addr, (sockDomain == RPCSocketT::TCP) ? socket_domain::tcp : socket_domain::unx);
}

}  // namespace reindexer_server
