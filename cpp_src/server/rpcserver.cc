#include "rpcserver.h"
#include <sys/stat.h>
#include <sstream>
#include "core/cjson/jsonbuilder.h"
#include "core/transactionimpl.h"
#include "net/cproto/cproto.h"
#include "net/cproto/serverconnection.h"
#include "net/listener.h"
#include "reindexer_version.h"

namespace reindexer_server {

RPCServer::RPCServer(DBManager &dbMgr, LoggerWrapper logger, bool allocDebug, IStatsWatcher *statsCollector)
	: dbMgr_(dbMgr), logger_(logger), allocDebug_(allocDebug), statsWatcher_(statsCollector), startTs_(std::chrono::system_clock::now()) {}

RPCServer::~RPCServer() {}

Error RPCServer::Ping(cproto::Context &) {
	//
	return 0;
}

static std::atomic<int> connCounter;

Error RPCServer::Login(cproto::Context &ctx, p_string login, p_string password, p_string db, cproto::optional<bool> createDBIfMissing,
					   cproto::optional<bool> checkClusterID, cproto::optional<int> expectedClusterID) {
	if (ctx.GetClientData()) {
		return Error(errParams, "Already logged in");
	}

	std::unique_ptr<RPCClientData> clientData(new RPCClientData);

	clientData->connID = connCounter.fetch_add(1, std::memory_order_relaxed);
	clientData->pusher.SetWriter(ctx.writer);
	clientData->subscribed = false;

	clientData->auth = AuthContext(login.toString(), password.toString());

	auto dbName = db.toString();
	if (checkClusterID.hasValue() && checkClusterID.value()) {
		assert(expectedClusterID.hasValue());
		clientData->auth.SetExpectedClusterID(expectedClusterID.value());
	}
	auto status = dbMgr_.Login(dbName, clientData->auth);
	if (!status.ok()) {
		return status;
	}
	ctx.SetClientData(std::move(clientData));
	if (statsWatcher_) {
		statsWatcher_->OnClientConnected(dbName, statsSourceName());
	}
	int64_t startTs = std::chrono::duration_cast<std::chrono::seconds>(startTs_.time_since_epoch()).count();
	static string_view version = REINDEX_VERSION;

	status = db.length() ? OpenDatabase(ctx, db, createDBIfMissing) : errOK;
	if (status.ok()) {
		ctx.Return({cproto::Arg(p_string(&version)), cproto::Arg(startTs)}, status);
	}

	return status;
}

Error RPCServer::OpenDatabase(cproto::Context &ctx, p_string db, cproto::optional<bool> createDBIfMissing) {
	auto clientData = dynamic_cast<RPCClientData *>(ctx.GetClientData());
	if (clientData->auth.HaveDB()) {  // -V522 this thing is handled in midleware (within CheckAuth)
		return Error(errParams, "Database already opened");
	}
	auto status = dbMgr_.OpenDatabase(db.toString(), clientData->auth, createDBIfMissing.hasValue() && createDBIfMissing.value());
	if (!status.ok()) {
		clientData->auth.ResetDB();
	}
	return status;
}

Error RPCServer::CloseDatabase(cproto::Context &ctx) {
	auto clientData = dynamic_cast<RPCClientData *>(ctx.GetClientData());
	clientData->auth.ResetDB();	 // -V522 this thing is handled in midleware (within CheckAuth)
	return errOK;
}
Error RPCServer::DropDatabase(cproto::Context &ctx) {
	auto clientData = dynamic_cast<RPCClientData *>(ctx.GetClientData());
	return dbMgr_.DropDatabase(clientData->auth);  // -V522 this thing is handled in midleware (within CheckAuth)
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

	if (statsWatcher_) {
		auto clientData = dynamic_cast<RPCClientData *>(ctx.GetClientData());
		if (clientData) {
			statsWatcher_->OnClientDisconnected(clientData->auth.DBName(), statsSourceName());
		}
	}
	logger_.info("RPC: Client disconnected");
}

void RPCServer::OnResponse(cproto::Context &ctx) {
	if (statsWatcher_) {
		auto clientData = dynamic_cast<RPCClientData *>(ctx.GetClientData());
		auto dbName = (clientData != nullptr) ? clientData->auth.DBName() : "<unknown>";
		statsWatcher_->OnOutputTraffic(dbName, statsSourceName(), ctx.stat.sizeStat.respSizeBytes);
		if (ctx.stat.sizeStat.respSizeBytes) {
			// Don't update stats on responses like "updates push"
			statsWatcher_->OnInputTraffic(dbName, statsSourceName(), ctx.stat.sizeStat.reqSizeBytes);
		}
	}
}

void RPCServer::Logger(cproto::Context &ctx, const Error &err, const cproto::Args &ret) {
	auto clientData = dynamic_cast<RPCClientData *>(ctx.GetClientData());
	WrSerializer ser;

	if (clientData) {
		ser << "c='"_sv << clientData->connID << "' db='"_sv << clientData->auth.Login() << "@"_sv << clientData->auth.DBName() << "' "_sv;
	} else {
		ser << "- - "_sv;
	}

	if (ctx.call) {
		ser << cproto::CmdName(ctx.call->cmd) << " "_sv;
		ctx.call->args.Dump(ser);
	} else {
		ser << '-';
	}

	ser << " -> "_sv << (err.ok() ? "OK"_sv : err.what());
	if (ret.size()) {
		ser << ' ';
		ret.Dump(ser);
	}

	HandlerStat statDiff = HandlerStat() - ctx.stat.allocStat;
	ser << ' ' << statDiff.GetTimeElapsed() << "us"_sv;

	if (allocDebug_) {
		ser << " |  allocs: "_sv << statDiff.GetAllocsCnt() << ", allocated: " << statDiff.GetAllocsBytes() << " byte(s)";
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

Error RPCServer::CloseNamespace(cproto::Context &ctx, p_string ns) {
	// Do not close.
	// TODO: add reference counters
	// return getDB(ctx, kRoleDataRead)->CloseNamespace(ns);
	return getDB(ctx, kRoleDataRead).Commit(ns);
}

Error RPCServer::EnumNamespaces(cproto::Context &ctx) {
	vector<NamespaceDef> nsDefs;
	auto err = getDB(ctx, kRoleDataRead).EnumNamespaces(nsDefs, true);
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
	jb.Array("databases"_sv, array);
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

Error RPCServer::StartTransaction(cproto::Context &ctx, p_string nsName) {
	auto db = getDB(ctx, kRoleDataWrite);
	auto tr = db.NewTransaction(nsName);
	if (!tr.Status().ok()) {
		return tr.Status();
	}

	auto id = addTx(ctx, std::move(tr));

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
		unsigned preceptsCount = ser.GetVarUint();
		vector<string> precepts;
		for (unsigned prIndex = 0; prIndex < preceptsCount; prIndex++) {
			string precept(ser.GetVString());
			precepts.push_back(precept);
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
	query.Deserialize(ser);
	query.type_ = QueryDelete;
	tr.Modify(std::move(query));
	return errOK;
}

Error RPCServer::UpdateQueryTx(cproto::Context &ctx, p_string queryBin, int64_t txID) {
	auto db = getDB(ctx, kRoleDataWrite);

	Transaction &tr = getTx(ctx, txID);
	Query query;
	Serializer ser(queryBin.data(), queryBin.size());
	query.Deserialize(ser);
	query.type_ = QueryUpdate;
	tr.Modify(std::move(query));
	return errOK;
}

Error RPCServer::CommitTx(cproto::Context &ctx, int64_t txId) {
	auto db = getDB(ctx, kRoleDataWrite);

	Transaction &tr = getTx(ctx, txId);
	auto err = db.CommitTransaction(tr);
	if (err.ok()) {
		QueryResults qres;
		bool tmUpdated = false;

		for (auto &step : tr.GetSteps()) {
			if (!step.query_) {
				qres.AddItem(step.item_);
				if (!tmUpdated) tmUpdated = step.item_.IsTagsUpdated();
			}
		}
		int32_t ptVers = -1;
		ResultFetchOpts opts;
		if (tmUpdated) {
			opts = ResultFetchOpts{kResultsWithItemID | kResultsWithPayloadTypes, span<int32_t>(&ptVers, 1), 0, INT_MAX};
		} else {
			opts = ResultFetchOpts{kResultsWithItemID, {}, 0, INT_MAX};
		}

		err = sendResults(ctx, qres, -1, opts);
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
		default:
			err = Error(-1, "Invalid source item format %d", format);
	}
	if (!err.ok()) {
		return err;
	}
	tmUpdated = item.IsTagsUpdated();

	if (perceptsPack.length()) {
		Serializer ser(perceptsPack);
		unsigned preceptsCount = ser.GetVarUint();
		vector<string> precepts;
		for (unsigned prIndex = 0; prIndex < preceptsCount; prIndex++) {
			string precept(ser.GetVString());
			precepts.push_back(precept);
		}
		item.SetPrecepts(precepts);
		if (preceptsCount) sendItemBack = true;
	}
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
	if (!err.ok()) {
		return err;
	}
	QueryResults qres;
	qres.AddItem(item, sendItemBack);
	int32_t ptVers = -1;
	ResultFetchOpts opts;
	if (tmUpdated) {
		opts = ResultFetchOpts{kResultsWithItemID | kResultsWithPayloadTypes, span<int32_t>(&ptVers, 1), 0, INT_MAX};
	} else {
		opts = ResultFetchOpts{kResultsWithItemID, {}, 0, INT_MAX};
	}
	if (sendItemBack) opts.flags |= kResultsCJson;

	return sendResults(ctx, qres, -1, opts);
}

Error RPCServer::DeleteQuery(cproto::Context &ctx, p_string queryBin) {
	Query query;
	Serializer ser(queryBin.data(), queryBin.size());
	query.Deserialize(ser);
	query.type_ = QueryDelete;

	QueryResults qres;
	auto err = getDB(ctx, kRoleDataWrite).Delete(query, qres);
	if (!err.ok()) {
		return err;
	}
	ResultFetchOpts opts{0, {}, 0, INT_MAX};
	return sendResults(ctx, qres, -1, opts);
}

Error RPCServer::UpdateQuery(cproto::Context &ctx, p_string queryBin) {
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
	ResultFetchOpts opts{kResultsCJson | kResultsWithPayloadTypes, {&ptVersion, 1}, 0, INT_MAX};
	return sendResults(ctx, qres, -1, opts);
}

Reindexer RPCServer::getDB(cproto::Context &ctx, UserRole role) {
	auto rawClientData = ctx.GetClientData();
	if (rawClientData) {
		auto clientData = dynamic_cast<RPCClientData *>(rawClientData);
		if (clientData) {
			Reindexer *db = nullptr;
			auto status = clientData->auth.GetDB(role, &db);
			if (!status.ok()) {
				throw status;
			}
			if (db != nullptr) {
				return db->NeedTraceActivity()
						   ? db->WithTimeout(ctx.call->execTimeout_).WithActivityTracer(ctx.clientAddr, clientData->auth.Login())
						   : db->WithTimeout(ctx.call->execTimeout_);
			}
		}
	}
	throw Error(errParams, "Database is not opened, you should open it first");
}

Error RPCServer::sendResults(cproto::Context &ctx, QueryResults &qres, int reqId, const ResultFetchOpts &opts) {
	WrResultSerializer rser(opts);

	bool doClose = rser.PutResults(&qres);

	if (doClose && reqId >= 0) {
		freeQueryResults(ctx, reqId);
		reqId = -1;
	}

	string_view resSlice = rser.Slice();
	ctx.Return({cproto::Arg(p_string(&resSlice)), cproto::Arg(int(reqId))});
	return errOK;
}

Error RPCServer::processTxItem(DataFormat format, string_view itemData, Item &item, ItemModifyMode mode, int stateToken) const noexcept {
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
		default:
			return Error(-1, "Invalid source item format %d", format);
	}
}

QueryResults &RPCServer::getQueryResults(cproto::Context &ctx, int &id) {
	auto data = dynamic_cast<RPCClientData *>(ctx.GetClientData());

	if (id < 0) {
		for (id = 0; id < int(data->results.size()); id++) {
			if (!data->results[id].second) {
				data->results[id] = {QueryResults(), true};
				return data->results[id].first;
			}
		}

		if (data->results.size() > cproto::kMaxConcurentQueries) throw Error(errLogic, "Too many paralell queries");
		id = data->results.size();
		data->results.push_back({QueryResults(), true});
	}

	if (id >= int(data->results.size())) {
		throw Error(errLogic, "Invalid query id");
	}
	return data->results[id].first;
}

Transaction &RPCServer::getTx(cproto::Context &ctx, int64_t id) {
	auto data = dynamic_cast<RPCClientData *>(ctx.GetClientData());

	if (size_t(id) >= data->txs.size()) {
		throw Error(errLogic, "Invalid tx id");
	}
	return data->txs[id];
}

int64_t RPCServer::addTx(cproto::Context &ctx, Transaction &&tr) {
	auto data = dynamic_cast<RPCClientData *>(ctx.GetClientData());
	for (size_t i = 0; i < data->txs.size(); ++i) {	 // -V522 this thing is handled in midleware (within CheckAuth)
		if (data->txs[i].IsFree()) {
			data->txs[i] = std::move(tr);
			return i;
		}
	}
	data->txs.emplace_back(std::move(tr));
	return int64_t(data->txs.size() - 1);
}
void RPCServer::clearTx(cproto::Context &ctx, uint64_t txId) {
	auto data = dynamic_cast<RPCClientData *>(ctx.GetClientData());
	if (txId >= data->txs.size()) {	 // -V522 this thing is handled in midleware (within CheckAuth)
		throw Error(errLogic, "Invalid tx id");
	}
	data->txs[txId] = Transaction();
}

void RPCServer::freeQueryResults(cproto::Context &ctx, int id) {
	auto data = dynamic_cast<RPCClientData *>(ctx.GetClientData());
	if (id >= int(data->results.size()) || id < 0) {  // -V522 this thing is handled in midleware (within CheckAuth)
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
	return errOK;
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

Error RPCServer::Commit(cproto::Context &ctx, p_string ns) { return getDB(ctx, kRoleDataWrite).Commit(ns); }

Error RPCServer::GetMeta(cproto::Context &ctx, p_string ns, p_string key) {
	string data;
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
	return errOK;
}

Error RPCServer::SubscribeUpdates(cproto::Context &ctx, int flag) {
	auto db = getDB(ctx, kRoleDataRead);
	auto clientData = dynamic_cast<RPCClientData *>(ctx.GetClientData());
	auto ret = db.SubscribeUpdates(&clientData->pusher, flag);	// -V522 this thing is handled in midleware (within CheckAuth)
	if (ret.ok()) clientData->subscribed = bool(flag);
	return ret;
}

bool RPCServer::Start(const string &addr, ev::dynamic_loop &loop) {
	dispatcher_.Register(cproto::kCmdPing, this, &RPCServer::Ping);
	dispatcher_.Register(cproto::kCmdLogin, this, &RPCServer::Login, true);
	dispatcher_.Register(cproto::kCmdOpenDatabase, this, &RPCServer::OpenDatabase);
	dispatcher_.Register(cproto::kCmdCloseDatabase, this, &RPCServer::CloseDatabase);
	dispatcher_.Register(cproto::kCmdDropDatabase, this, &RPCServer::DropDatabase);
	dispatcher_.Register(cproto::kCmdOpenNamespace, this, &RPCServer::OpenNamespace);
	dispatcher_.Register(cproto::kCmdDropNamespace, this, &RPCServer::DropNamespace);
	dispatcher_.Register(cproto::kCmdTruncateNamespace, this, &RPCServer::TruncateNamespace);
	dispatcher_.Register(cproto::kCmdCloseNamespace, this, &RPCServer::CloseNamespace);
	dispatcher_.Register(cproto::kCmdEnumNamespaces, this, &RPCServer::EnumNamespaces);
	dispatcher_.Register(cproto::kCmdEnumDatabases, this, &RPCServer::EnumDatabases);

	dispatcher_.Register(cproto::kCmdAddIndex, this, &RPCServer::AddIndex);
	dispatcher_.Register(cproto::kCmdUpdateIndex, this, &RPCServer::UpdateIndex);
	dispatcher_.Register(cproto::kCmdDropIndex, this, &RPCServer::DropIndex);
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

	listener_.reset(new Listener(loop, cproto::ServerConnection::NewFactory(dispatcher_)));
	return listener_->Bind(addr);
}

RPCClientData::~RPCClientData() {
	Reindexer *db = nullptr;
	auth.GetDB(kRoleNone, &db);
	if (subscribed && db) {
		db->SubscribeUpdates(&pusher, false);
	}
}

}  // namespace reindexer_server
