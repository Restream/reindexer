#include "rpcserver.h"
#include <sys/stat.h>
#include <sstream>
#include "net/cproto/cproto.h"
#include "net/cproto/serverconnection.h"
#include "net/listener.h"

namespace reindexer_server {

RPCServer::RPCServer(DBManager &dbMgr) : dbMgr_(dbMgr) {}
RPCServer::RPCServer(DBManager &dbMgr, LoggerWrapper logger, bool allocDebug) : dbMgr_(dbMgr), logger_(logger), allocDebug_(allocDebug) {}
RPCServer::~RPCServer() {}

Error RPCServer::Ping(cproto::Context &) {
	//
	return 0;
}

static std::atomic<int> connCounter;

Error RPCServer::Login(cproto::Context &ctx, p_string login, p_string password, p_string db) {
	if (ctx.GetClientData()) {
		return Error(errParams, "Already logged in");
	}

	auto clientData = std::make_shared<RPCClientData>();

	clientData->connID = connCounter.load();
	connCounter++;

	clientData->auth = AuthContext(login.toString(), password.toString());

	auto status = dbMgr_.Login(db.toString(), clientData->auth);
	if (!status.ok()) {
		return status;
	}
	ctx.SetClientData(clientData);

	return db.length() ? OpenDatabase(ctx, db) : 0;
}

Error RPCServer::OpenDatabase(cproto::Context &ctx, p_string db) {
	auto clientData = dynamic_cast<RPCClientData *>(ctx.GetClientData().get());
	if (clientData->auth.HaveDB()) {
		return Error(errParams, "Database already opened");
	}
	return dbMgr_.OpenDatabase(db.toString(), clientData->auth, true);
}

Error RPCServer::CloseDatabase(cproto::Context &ctx) {
	auto clientData = dynamic_cast<RPCClientData *>(ctx.GetClientData().get());
	clientData->auth.ResetDB();
	return 0;
}
Error RPCServer::DropDatabase(cproto::Context &ctx) {
	auto clientData = dynamic_cast<RPCClientData *>(ctx.GetClientData().get());
	return dbMgr_.DropDatabase(clientData->auth);
}

Error RPCServer::CheckAuth(cproto::Context &ctx) {
	auto clientData = dynamic_cast<RPCClientData *>(ctx.GetClientData().get());

	if (ctx.call->cmd == cproto::kCmdLogin || ctx.call->cmd == cproto::kCmdPing) {
		return 0;
	}

	if (!clientData) {
		return Error(errForbidden, "You should login");
	}

	return 0;
}

void RPCServer::OnClose(cproto::Context &ctx, const Error &err) {
	(void)ctx;
	(void)err;

	logger_.info("RPC: Client disconnected");
}

void RPCServer::Logger(cproto::Context &ctx, const Error &err, const cproto::Args &ret) {
	auto clientData = dynamic_cast<RPCClientData *>(ctx.GetClientData().get());
	WrSerializer ser;

	if (clientData) {
		ser.Printf("c='%d' db='%s@%s' ", clientData->connID, clientData->auth.Login().c_str(), clientData->auth.DBName().c_str());
	} else {
		ser.PutChars("- - ");
	}

	if (ctx.call) {
		ser.Printf("%s ", cproto::CmdName(ctx.call->cmd));
		ctx.call->args.Dump(ser);
	} else {
		ser.PutChars("-");
	}

	ser.Printf(" -> %s", err.ok() ? "OK" : err.what().c_str());
	if (ret.size()) {
		ser.PutChars(" ");
		ret.Dump(ser);
	}

	if (allocDebug_) {
		Stat statDiff = Stat() - ctx.stat;

		ser.Printf(" | elapsed: %dus, allocs: %d, allocated: %d byte(s)", int(statDiff.GetTimeElapsed()), int(statDiff.GetAllocsCnt()),
				   int(statDiff.GetAllocsBytes()));
	}

	ser.PutChar(0);

	logger_.info("{0}", reinterpret_cast<char *>(ser.Buf()));
}

Error RPCServer::OpenNamespace(cproto::Context &ctx, p_string ns) {
	NamespaceDef nsDef;
	p_string nsDefJson("");

	if (ctx.call->args.size() > 1) {
		nsDefJson = p_string(ctx.call->args[1]);
	}

	string json = (nsDefJson.length() ? nsDefJson : ns).toString();

	if (json[0] == '{') {
		nsDef.FromJSON(const_cast<char *>(json.c_str()));
		if (!nsDef.indexes.empty()) {
			return getDB(ctx, kRoleDataRead)->AddNamespace(nsDef);
		}
		return getDB(ctx, kRoleDataRead)->OpenNamespace(nsDef.name, nsDef.storage, nsDef.cacheMode);
	}
	// tmp fix for compat
	return getDB(ctx, kRoleDataRead)->OpenNamespace(ns.toString());
}

Error RPCServer::DropNamespace(cproto::Context &ctx, p_string ns) {
	//
	return getDB(ctx, kRoleDBAdmin)->DropNamespace(ns.toString());
}

Error RPCServer::CloseNamespace(cproto::Context &ctx, p_string ns) {
	//
	return getDB(ctx, kRoleDataRead)->CloseNamespace(ns.toString());
}

Error RPCServer::EnumNamespaces(cproto::Context &ctx) {
	vector<NamespaceDef> nsDefs;
	auto err = getDB(ctx, kRoleDataRead)->EnumNamespaces(nsDefs, true);
	if (!err.ok()) {
		return err;
	}
	WrSerializer ser;
	ser.PutChars("{\"items\":[");
	for (unsigned i = 0; i < nsDefs.size(); i++) {
		if (i != 0) ser.PutChar(',');
		nsDefs[i].GetJSON(ser);
	}
	ser.PutChars("]}");
	auto resSlice = ser.Slice();

	ctx.Return({cproto::Arg(p_string(&resSlice))});
	return 0;
}

Error RPCServer::AddIndex(cproto::Context &ctx, p_string ns, p_string indexDef) {
	IndexDef iDef;
	auto err = iDef.FromJSON(const_cast<char *>(indexDef.toString().c_str()));
	if (!err.ok()) {
		return err;
	}
	return getDB(ctx, kRoleDBAdmin)->AddIndex(ns.toString(), iDef);
}

Error RPCServer::DropIndex(cproto::Context &ctx, p_string ns, p_string index) {
	return getDB(ctx, kRoleDBAdmin)->DropIndex(ns.toString(), index.toString());
}

Error RPCServer::ConfigureIndex(cproto::Context &ctx, p_string ns, p_string index, p_string config) {
	return getDB(ctx, kRoleDBAdmin)->ConfigureIndex(ns.toString(), index.toString(), config.toString());
}

Error RPCServer::ModifyItem(cproto::Context &ctx, p_string itemPack, int mode) {
	auto db = getDB(ctx, kRoleDataWrite);
	Serializer ser(itemPack.data(), itemPack.size());
	string ns = ser.GetVString().ToString();
	int format = ser.GetVarUint();
	auto item = Item(db->NewItem(ns));
	bool tmUpdated = false;
	Error err;
	if (!item.Status().ok()) {
		return item.Status();
	}
	switch (format) {
		case FormatJson:
			err = item.Unsafe().FromJSON(ser.GetSlice(), nullptr, mode == ModeDelete);
			break;
		case FormatCJson:
			err = item.Unsafe().FromCJSON(ser.GetSlice(), mode == ModeDelete);
			break;
		default:
			err = Error(-1, "Invalid source item format %d", format);
	}
	if (!err.ok()) {
		return err;
	}
	tmUpdated = item.IsTagsUpdated();
	unsigned preceptsCount = ser.GetVarUint();
	vector<string> precepts;
	for (unsigned prIndex = 0; prIndex < preceptsCount; prIndex++) {
		string precept = ser.GetVString().ToString();
		precepts.push_back(precept);
	}
	item.SetPrecepts(precepts);

	switch (mode) {
		case ModeUpsert:
			err = db->Upsert(ns, item);
			break;
		case ModeInsert:
			err = db->Insert(ns, item);
			break;
		case ModeUpdate:
			err = db->Update(ns, item);
			break;
		case ModeDelete:
			err = db->Delete(ns, item);
			break;
	}
	if (!err.ok()) {
		return err;
	}
	QueryResults qres;
	qres.AddItem(item);
	int32_t ptVers = -1;
	ResultFetchOpts opts;
	if (tmUpdated) {
		opts = ResultFetchOpts{kResultsWithPayloadTypes, &ptVers, 1, 0, INT_MAX, 0};
	} else {
		opts = ResultFetchOpts{0, nullptr, 0, 0, INT_MAX, 0};
	}

	return sendResults(ctx, qres, -1, opts);
}

Error RPCServer::DeleteQuery(cproto::Context &ctx, p_string queryBin) {
	Query query;
	Serializer ser(queryBin.data(), queryBin.size());
	query.Deserialize(ser);

	QueryResults qres;
	auto err = getDB(ctx, kRoleDataWrite)->Delete(query, qres);
	if (!err.ok()) {
		return err;
	}
	ResultFetchOpts opts{0, nullptr, 0, 0, INT_MAX, 0};
	return sendResults(ctx, qres, -1, opts);
}

shared_ptr<Reindexer> RPCServer::getDB(cproto::Context &ctx, UserRole role) {
	auto clientData = ctx.GetClientData().get();
	if (clientData) {
		shared_ptr<Reindexer> db;
		auto status = dynamic_cast<RPCClientData *>(clientData)->auth.GetDB(role, &db);
		if (!status.ok()) {
			throw status;
		}
		if (db != nullptr) return db;
	}
	throw Error(errParams, "Database is not openeded, you should open it first");
}

Error RPCServer::sendResults(cproto::Context &ctx, QueryResults &qres, int reqId, const ResultFetchOpts &opts) {
	WrResultSerializer rser(true, opts);

	bool doClose = rser.PutResults(&qres);

	if (doClose && reqId >= 0) {
		freeQueryResults(ctx, reqId);
		reqId = -1;
	}

	string_view resSlice = rser.Slice();
	ctx.Return({cproto::Arg(p_string(&resSlice)), cproto::Arg(int(reqId))});
	return 0;
}

QueryResults &RPCServer::getQueryResults(cproto::Context &ctx, int &id) {
	auto data = dynamic_cast<RPCClientData *>(ctx.GetClientData().get());

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

void RPCServer::freeQueryResults(cproto::Context &ctx, int id) {
	auto data = dynamic_cast<RPCClientData *>(ctx.GetClientData().get());
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

Error RPCServer::Select(cproto::Context &ctx, p_string queryBin, int flags, int limit, int64_t fetchDataMask, p_string ptVersionsPck) {
	Query query;
	Serializer ser(queryBin.data(), queryBin.size());
	query.Deserialize(ser);

	int id = -1;
	QueryResults &qres = getQueryResults(ctx, id);

	auto ret = getDB(ctx, kRoleDataRead)->Select(query, qres);
	if (!ret.ok()) {
		freeQueryResults(ctx, id);
		return ret;
	}
	auto ptVersions = pack2vec(ptVersionsPck);
	ResultFetchOpts opts{flags, ptVersions.data(), int(ptVersions.size()), 0, unsigned(limit), fetchDataMask};

	return fetchResults(ctx, id, opts);
}

Error RPCServer::SelectSQL(cproto::Context &ctx, p_string querySql, int flags, int limit, int64_t fetchDataMask, p_string ptVersionsPck) {
	int id = -1;
	QueryResults &qres = getQueryResults(ctx, id);
	auto ret = getDB(ctx, kRoleDataRead)->Select(querySql.toString(), qres);
	if (!ret.ok()) {
		freeQueryResults(ctx, id);
		return ret;
	}
	auto ptVersions = pack2vec(ptVersionsPck);
	ResultFetchOpts opts{flags, ptVersions.data(), int(ptVersions.size()), 0, unsigned(limit), fetchDataMask};

	return fetchResults(ctx, id, opts);
}

Error RPCServer::FetchResults(cproto::Context &ctx, int reqId, int flags, int offset, int limit, int64_t fetchDataMask) {
	flags &= ~kResultsWithPayloadTypes;

	ResultFetchOpts opts = {flags, nullptr, 0, unsigned(offset), unsigned(limit), fetchDataMask};
	return fetchResults(ctx, reqId, opts);
}

Error RPCServer::CloseResults(cproto::Context &ctx, int reqId) {
	freeQueryResults(ctx, reqId);
	return errOK;
}

Error RPCServer::fetchResults(cproto::Context &ctx, int reqId, const ResultFetchOpts &opts) {
	cproto::Args ret;
	QueryResults &qres = getQueryResults(ctx, reqId);

	return sendResults(ctx, qres, reqId, opts);
}

Error RPCServer::Commit(cproto::Context &ctx, p_string ns) {
	//
	return getDB(ctx, kRoleDataWrite)->Commit(ns.toString());
}

Error RPCServer::GetMeta(cproto::Context &ctx, p_string ns, p_string key) {
	string data;
	auto err = getDB(ctx, kRoleDataRead)->GetMeta(ns.toString(), key.toString(), data);
	if (!err.ok()) {
		return err;
	}

	WrSerializer wrSer;
	wrSer.PutVString(data);
	string_view slData = wrSer.Slice();
	ctx.Return({cproto::Arg(p_string(&slData))});
	return 0;
}

Error RPCServer::PutMeta(cproto::Context &ctx, p_string ns, p_string key, p_string data) {
	return getDB(ctx, kRoleDataWrite)->PutMeta(ns.toString(), key.toString(), data);
}

Error RPCServer::EnumMeta(cproto::Context &ctx, p_string ns) {
	vector<string> keys;
	return getDB(ctx, kRoleDataWrite)->EnumMeta(ns.toString(), keys);
}

bool RPCServer::Start(const string &addr, ev::dynamic_loop &loop) {
	dispatcher.Register(cproto::kCmdPing, this, &RPCServer::Ping);
	dispatcher.Register(cproto::kCmdLogin, this, &RPCServer::Login);
	dispatcher.Register(cproto::kCmdOpenDatabase, this, &RPCServer::OpenDatabase);
	dispatcher.Register(cproto::kCmdCloseDatabase, this, &RPCServer::CloseDatabase);
	dispatcher.Register(cproto::kCmdDropDatabase, this, &RPCServer::DropDatabase);
	dispatcher.Register(cproto::kCmdOpenNamespace, this, &RPCServer::OpenNamespace);
	dispatcher.Register(cproto::kCmdDropNamespace, this, &RPCServer::DropNamespace);
	dispatcher.Register(cproto::kCmdCloseNamespace, this, &RPCServer::CloseNamespace);
	dispatcher.Register(cproto::kCmdEnumNamespaces, this, &RPCServer::EnumNamespaces);

	dispatcher.Register(cproto::kCmdAddIndex, this, &RPCServer::AddIndex);
	dispatcher.Register(cproto::kCmdDropIndex, this, &RPCServer::DropIndex);
	dispatcher.Register(cproto::kCmdConfigureIndex, this, &RPCServer::ConfigureIndex);
	dispatcher.Register(cproto::kCmdCommit, this, &RPCServer::Commit);

	dispatcher.Register(cproto::kCmdModifyItem, this, &RPCServer::ModifyItem);
	dispatcher.Register(cproto::kCmdDeleteQuery, this, &RPCServer::DeleteQuery);

	dispatcher.Register(cproto::kCmdSelect, this, &RPCServer::Select);
	dispatcher.Register(cproto::kCmdSelectSQL, this, &RPCServer::SelectSQL);
	dispatcher.Register(cproto::kCmdFetchResults, this, &RPCServer::FetchResults);
	dispatcher.Register(cproto::kCmdCloseResults, this, &RPCServer::CloseResults);

	dispatcher.Register(cproto::kCmdGetMeta, this, &RPCServer::GetMeta);
	dispatcher.Register(cproto::kCmdPutMeta, this, &RPCServer::PutMeta);
	dispatcher.Register(cproto::kCmdEnumMeta, this, &RPCServer::EnumMeta);
	dispatcher.Middleware(this, &RPCServer::CheckAuth);
	dispatcher.OnClose(this, &RPCServer::OnClose);

	if (logger_) {
		dispatcher.Logger(this, &RPCServer::Logger);
	}

	listener_.reset(new Listener(loop, cproto::ServerConnection::NewFactory(dispatcher)));
	return listener_->Bind(addr);
}

}  // namespace reindexer_server
