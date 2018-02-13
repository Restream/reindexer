#pragma once

#include "core/cbinding/resultserializer.h"
#include "core/keyvalue/keyref.h"
#include "core/reindexer.h"
#include "net/cproto/dispatcher.h"

namespace reindexer_server {

using std::string;
using namespace reindexer::net;
using namespace reindexer;

class ReindexerClientData : public cproto::ClientData {
public:
	h_vector<QueryResults, 1> results;
};

class RPCServer {
public:
	RPCServer(shared_ptr<reindexer::Reindexer> db);
	~RPCServer();

	bool Start(int port);

	Error Ping(cproto::Context &ctx);
	Error OpenNamespace(cproto::Context &ctx, p_string ns, p_string def);
	Error DropNamespace(cproto::Context &ctx, p_string ns);
	Error CloseNamespace(cproto::Context &ctx, p_string ns);
	Error RenameNamespace(cproto::Context &ctx, p_string src, p_string dst);
	Error CloneNamespace(cproto::Context &ctx, p_string src, p_string dst);
	Error EnumNamespaces(cproto::Context &ctx);

	Error AddIndex(cproto::Context &ctx, p_string ns, p_string indexDef);
	Error ConfigureIndex(cproto::Context &ctx, p_string ns, p_string index, p_string config);

	Error Commit(cproto::Context &ctx, p_string ns);

	Error ModifyItem(cproto::Context &ctx, p_string itemPack, int mode);
	Error DeleteQuery(cproto::Context &ctx, p_string query);

	Error Select(cproto::Context &ctx, p_string query, int flags, int limit, int64_t fetchDataMask, p_string ptVersions);
	Error SelectSQL(cproto::Context &ctx, p_string query, int flags, int limit, int64_t fetchDataMask, p_string ptVersions);
	Error FetchResults(cproto::Context &ctx, int reqId, int flags, int offset, int limit, int64_t fetchDataMask);

	Error GetMeta(cproto::Context &ctx, p_string ns, p_string key);
	Error PutMeta(cproto::Context &ctx, p_string ns, p_string key, p_string data);
	Error EnumMeta(cproto::Context &ctx, p_string ns);

protected:
	Error sendResults(cproto::Context &ctx, QueryResults &qr, int reqId, const ResultFetchOpts &opts);
	Error fetchResults(cproto::Context &ctx, int reqId, const ResultFetchOpts &opts);

	shared_ptr<reindexer::Reindexer> db_;
};

}  // namespace reindexer_server
