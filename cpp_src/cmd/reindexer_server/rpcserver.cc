
#include "rpcserver.h"
#include <sys/stat.h>
#include <unistd.h>
#include <sstream>
#include "net/cproto/connection.h"
#include "net/cproto/cproto.h"

#include "net/listener.h"

namespace reindexer_server {

enum { ModeUpdate, ModeInsert, ModeUpsert, ModeDelete };

RPCServer::RPCServer(shared_ptr<reindexer::Reindexer> db) : db_(db) {}
RPCServer::~RPCServer() {}

Error RPCServer::Ping(cproto::Context &) {
	//
	return 0;
}

Error RPCServer::OpenNamespace(cproto::Context &, p_string ns, p_string nsDef) {
	(void)nsDef;
	// TODO
	return db_->OpenNamespace(ns.toString());
}

Error RPCServer::DropNamespace(cproto::Context &, p_string ns) {
	//
	return db_->DropNamespace(ns.toString());
}

Error RPCServer::CloseNamespace(cproto::Context &, p_string ns) {
	//
	return db_->CloseNamespace(ns.toString());
}

Error RPCServer::RenameNamespace(cproto::Context &, p_string src, p_string dst) {
	//
	return db_->RenameNamespace(src.toString(), dst.toString());
}

Error RPCServer::CloneNamespace(cproto::Context &, p_string src, p_string dst) {
	//
	return db_->CloneNamespace(src.toString(), dst.toString());
}
Error RPCServer::EnumNamespaces(cproto::Context &) {
	vector<NamespaceDef> nsDefs;
	auto err = db_->EnumNamespaces(nsDefs, true);
	if (!err.ok()) {
		return err;
	}
	// TODO
	return 0;
}

Error RPCServer::AddIndex(cproto::Context &, p_string ns, p_string indexDef) {
	IndexDef iDef;
	auto err = iDef.Parse(const_cast<char *>(indexDef.toString().c_str()));
	if (!err.ok()) {
		return err;
	}
	return db_->AddIndex(ns.toString(), iDef);
}

Error RPCServer::ConfigureIndex(cproto::Context &, p_string ns, p_string index, p_string config) {
	return db_->ConfigureIndex(ns.toString(), index.toString(), config.toString());
}

Error RPCServer::ModifyItem(cproto::Context &ctx, p_string itemPack, int mode) {
	Serializer ser(itemPack.data(), itemPack.size());
	string ns = ser.GetVString().ToString();
	int format = ser.GetVarUint();
	auto item = unique_ptr<Item>(db_->NewItem(ns));
	Error err;
	if (!item->Status().ok()) {
		return item->Status();
	}
	switch (format) {
		case FormatJson:
			err = item->FromJSON(ser.GetSlice());
			break;
		case FormatCJson:
			err = item->FromCJSON(ser.GetSlice());
			break;
		default:
			err = Error(-1, "Invalid source item format %d", format);
	}
	if (!err.ok()) {
		return err;
	}
	unsigned preceptsCount = ser.GetVarUint();
	vector<string> precepts;
	for (unsigned prIndex = 0; prIndex < preceptsCount; prIndex++) {
		string precept = ser.GetVString().ToString();
		precepts.push_back(precept);
	}
	item->SetPrecepts(precepts);

	switch (mode) {
		case ModeUpsert:
			err = db_->Upsert(ns, item.get());
			break;
		case ModeInsert:
			err = db_->Insert(ns, item.get());
			break;
		case ModeUpdate:
			err = db_->Update(ns, item.get());
			break;
		case ModeDelete:
			err = db_->Delete(ns, item.get());
			break;
	}
	if (!err.ok()) {
		return err;
	}
	QueryResults qres;
	if (item->GetRef().id != -1) {
		qres.push_back(item->GetRef());
	}

	ResultFetchOpts opts{0, nullptr, 0, INT_MAX, 0};
	return sendResults(ctx, qres, -1, opts);
}

Error RPCServer::DeleteQuery(cproto::Context &ctx, p_string queryBin) {
	Query query;
	Serializer ser(queryBin.data(), queryBin.size());
	query.Deserialize(ser);

	QueryResults qres;
	auto err = db_->Delete(query, qres);
	if (!err.ok()) {
		return err;
	}
	ResultFetchOpts opts{0, nullptr, 0, INT_MAX, 0};
	return sendResults(ctx, qres, -1, opts);
}

Error RPCServer::sendResults(cproto::Context &ctx, QueryResults &qres, int reqId, const ResultFetchOpts &opts) {
	ResultSerializer rser(true, opts);
	rser.PutResults(&qres);
	Slice resSlice(reinterpret_cast<char *>(rser.Buf()), rser.Len());
	ctx.Return({cproto::Arg(p_string(&resSlice)), cproto::Arg(int(reqId))});
	return 0;
}

QueryResults &getQueryResults(cproto::Context &ctx, int &id) {
	auto clientData = ctx.GetClientData();
	if (!clientData) {
		clientData = std::make_shared<ReindexerClientData>();
		ctx.SetClientData(clientData);
	}
	auto data = dynamic_cast<ReindexerClientData *>(clientData.get());

	if (id < 0) {
		if (data->results.size() > cproto::kMaxConcurentQueries) throw Error(errLogic, "Too many paralell queries");
		id = data->results.size();
		data->results.push_back(QueryResults());
	}

	if (id >= int(data->results.size())) {
		throw Error(errLogic, "Invalid query id");
	}
	return data->results[id];
}

void freeQueryResults(cproto::Context &ctx, int id) {
	auto data = dynamic_cast<ReindexerClientData *>(ctx.GetClientData().get());
	assert(id < int(data->results.size()));
	data->results.erase(data->results.begin() + id);
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

	auto ret = db_->Select(query, qres);
	if (!ret.ok()) {
		freeQueryResults(ctx, id);
		return ret;
	}
	auto ptVersions = pack2vec(ptVersionsPck);
	ResultFetchOpts opts{flags, ptVersions.data(), 0, unsigned(limit), fetchDataMask};

	return fetchResults(ctx, id, opts);
}

Error RPCServer::SelectSQL(cproto::Context &ctx, p_string querySql, int flags, int limit, int64_t fetchDataMask, p_string ptVersionsPck) {
	int id = -1;
	QueryResults &qres = getQueryResults(ctx, id);
	auto ret = db_->Select(querySql.toString(), qres);
	if (!ret.ok()) {
		freeQueryResults(ctx, id);
		return ret;
	}
	auto ptVersions = pack2vec(ptVersionsPck);
	ResultFetchOpts opts{flags, ptVersions.data(), 0, unsigned(limit), fetchDataMask};

	return fetchResults(ctx, id, opts);
}

Error RPCServer::FetchResults(cproto::Context &ctx, int reqId, int flags, int offset, int limit, int64_t fetchDataMask) {
	ResultFetchOpts opts = {flags, nullptr, unsigned(offset), unsigned(limit), fetchDataMask};
	return fetchResults(ctx, reqId, opts);
}

Error RPCServer::fetchResults(cproto::Context &ctx, int reqId, const ResultFetchOpts &opts) {
	cproto::Args ret;
	QueryResults &qres = getQueryResults(ctx, reqId);

	sendResults(ctx, qres, reqId, opts);

	if (opts.flags & kResultsClose) {
		freeQueryResults(ctx, reqId);
	}
	return 0;
}

Error RPCServer::Commit(cproto::Context &, p_string ns) {
	//
	return db_->Commit(ns.toString());
}

Error RPCServer::GetMeta(cproto::Context &ctx, p_string ns, p_string key) {
	string data;
	auto err = db_->GetMeta(ns.toString(), key.toString(), data);
	if (!err.ok()) {
		return err;
	}

	WrSerializer wrSer;
	wrSer.PutVString(data);
	Slice slData(reinterpret_cast<char *>(wrSer.Buf()), wrSer.Len());
	ctx.Return({cproto::Arg(p_string(&slData))});
	return 0;
}

Error RPCServer::PutMeta(cproto::Context &, p_string ns, p_string key, p_string data) {
	return db_->PutMeta(ns.toString(), key.toString(), data);
}

Error RPCServer::EnumMeta(cproto::Context &, p_string ns) {
	//
	vector<string> keys;
	return db_->EnumMeta(ns.toString(), keys);
}

bool RPCServer::Start(int port) {
	ev::dynamic_loop loop;
	cproto::Dispatcher dispatcher;

	dispatcher.Register(cproto::kCmdPing, this, &RPCServer::Ping);
	dispatcher.Register(cproto::kCmdOpenNamespace, this, &RPCServer::OpenNamespace);
	dispatcher.Register(cproto::kCmdDropNamespace, this, &RPCServer::DropNamespace);
	dispatcher.Register(cproto::kCmdCloseNamespace, this, &RPCServer::CloseNamespace);
	dispatcher.Register(cproto::kCmdRenameNamespace, this, &RPCServer::RenameNamespace);
	dispatcher.Register(cproto::kCmdCloneNamespace, this, &RPCServer::CloneNamespace);
	dispatcher.Register(cproto::kCmdEnumNamespaces, this, &RPCServer::EnumNamespaces);

	dispatcher.Register(cproto::kCmdAddIndex, this, &RPCServer::AddIndex);
	dispatcher.Register(cproto::kCmdConfigureIndex, this, &RPCServer::ConfigureIndex);

	dispatcher.Register(cproto::kCmdCommit, this, &RPCServer::Commit);
	dispatcher.Register(cproto::kCmdModifyItem, this, &RPCServer::ModifyItem);
	dispatcher.Register(cproto::kCmdDeleteQuery, this, &RPCServer::DeleteQuery);

	dispatcher.Register(cproto::kCmdSelect, this, &RPCServer::Select);
	dispatcher.Register(cproto::kCmdSelectSQL, this, &RPCServer::SelectSQL);
	dispatcher.Register(cproto::kCmdFetchResults, this, &RPCServer::FetchResults);

	dispatcher.Register(cproto::kCmdGetMeta, this, &RPCServer::GetMeta);
	dispatcher.Register(cproto::kCmdPutMeta, this, &RPCServer::PutMeta);
	dispatcher.Register(cproto::kCmdEnumMeta, this, &RPCServer::EnumMeta);

	Listener listener(loop, cproto::Connection::NewFactory(dispatcher));

	if (!listener.Bind(port)) {
		printf("Can't listen on %d port\n", port);
		return false;
	}

	//	listener.Fork(7);
	listener.Run();
	printf("listener::Run exited\n");

	return true;
}

}  // namespace reindexer_server
