#include "httpserver.h"
#include <sys/stat.h>
#include <unistd.h>
#include <sstream>
#include "base64/base64.h"
#include "core/type_consts.h"
#include "gason/gason.h"
#include "loggerwrapper.h"
#include "net/http/connection.h"
#include "net/listener.h"
#include "tools/fsops.h"
#include "tools/serializer.h"
#include "tools/stringstools.h"

using std::string;
using std::stringstream;
using std::to_string;

namespace reindexer_server {

HTTPServer::HTTPServer(DBManager &dbMgr, const string &webRoot) : dbMgr_(dbMgr), webRoot_(reindexer::JoinPath(webRoot, "")) {}
HTTPServer::HTTPServer(DBManager &dbMgr, const string &webRoot, LoggerWrapper logger, bool allocDebug)
	: dbMgr_(dbMgr), webRoot_(reindexer::JoinPath(webRoot, "")), logger_(logger), allocDebug_(allocDebug) {}
HTTPServer::~HTTPServer() {}

enum { ModeUpdate, ModeInsert, ModeUpsert, ModeDelete };

int HTTPServer::GetQuery(http::Context &ctx) {
	shared_ptr<Reindexer> db = getDB(ctx, kRoleDataRead);
	reindexer::QueryResults res;
	const char *sqlQuery = nullptr;

	for (auto p : ctx.request->params) {
		if (!strcmp(p.name, "q")) sqlQuery = p.val;
	}

	if (!sqlQuery) {
		return jsonStatus(ctx, false, http::StatusBadRequest, "Missed `q` parameter");
	}

	auto ret = db->Select(sqlQuery, res);

	if (!ret.ok()) {
		return jsonStatus(ctx, false, http::StatusInternalServerError, ret.what());
	}
	return queryResults(ctx, res, "items");
}

int HTTPServer::PostQuery(http::Context &ctx) {
	shared_ptr<Reindexer> db = getDB(ctx, kRoleDataRead);
	reindexer::QueryResults res;
	string dsl = ctx.body->Read();

	reindexer::Query q;
	auto status = q.ParseJson(dsl);
	if (!status.ok()) {
		return jsonStatus(ctx, false, http::StatusInternalServerError, status.what());
	}

	status = db->Select(q, res);
	if (!status.ok()) {
		return jsonStatus(ctx, false, http::StatusInternalServerError, status.what());
	}
	return queryResults(ctx, res, "items");
}

int HTTPServer::GetDatabases(http::Context &ctx) {
	ctx.writer->SetHeader(http::Header{"Content-Type", "application/json; charset=utf-8"});
	ctx.writer->SetRespCode(http::StatusOK);

	auto dbs = dbMgr_.EnumDatabases();

	ctx.writer->Write("{");
	ctx.writer->Write("\"items\":[");
	for (auto &db : dbs) {
		ctx.writer->Write("\"");
		ctx.writer->Write(db.c_str(), db.length());
		ctx.writer->Write("\"");
		if (db != dbs.back()) ctx.writer->Write(",");
	}

	ctx.writer->Write("],");
	ctx.writer->Write("\"total_items\":");

	auto total = to_string(dbs.size());
	ctx.writer->Write(total.c_str(), total.length());
	ctx.writer->Write("}");

	return 0;
}

int HTTPServer::PostDatabase(http::Context &ctx) {
	string json = ctx.body->Read();
	string newDbName = getNameFromJson(json);

	auto dbs = dbMgr_.EnumDatabases();
	for (auto &db : dbs) {
		if (db == newDbName) {
			return jsonStatus(ctx, false, http::StatusBadRequest, "Database already exists");
		}
	}

	AuthContext dummyCtx;
	AuthContext *actx = &dummyCtx;
	if (!dbMgr_.IsNoSecurity()) {
		auto clientData = dynamic_cast<HTTPClientData *>(ctx.clientData.get());
		assert(clientData);
		actx = &clientData->auth;
	}

	auto status = dbMgr_.OpenDatabase(newDbName, *actx, true);
	if (!status.ok()) {
		return jsonStatus(ctx, false, http::StatusInternalServerError, status.what());
	}

	return jsonStatus(ctx);
}

int HTTPServer::DeleteDatabase(http::Context &ctx) {
	shared_ptr<Reindexer> db = getDB(ctx, kRoleDBAdmin);
	const char *dbName = ctx.request->urlParams[0];
	AuthContext actx;

	auto status = dbMgr_.Login(dbName, actx);
	if (!status.ok()) {
		return jsonStatus(ctx, false, http::StatusUnauthorized, status.what());
	}

	status = dbMgr_.DropDatabase(actx);
	if (!status.ok()) {
		return jsonStatus(ctx, false, http::StatusInternalServerError, status.what());
	}

	return jsonStatus(ctx);
}

int HTTPServer::GetNamespaces(http::Context &ctx) {
	shared_ptr<Reindexer> db = getDB(ctx, kRoleDataRead);

	const char *sortOrder = "";
	for (auto p : ctx.request->params) {
		if (!strcmp(p.name, "sort_order")) sortOrder = p.val;
	}

	vector<reindexer::NamespaceDef> nsDefs;
	db->EnumNamespaces(nsDefs, false);

	int sortDirection = 0;
	if (!strcmp(sortOrder, "asc")) {
		sortDirection = 1;
	} else if (!strcmp(sortOrder, "desc")) {
		sortDirection = -1;
	}

	if (sortDirection) {
		std::sort(nsDefs.begin(), nsDefs.end(), [sortDirection](const NamespaceDef &lhs, const NamespaceDef &rhs) {
			return sortDirection > 0 ? !(collateCompare(lhs.name, rhs.name, CollateASCII) > 0)
									 : !(collateCompare(lhs.name, rhs.name, CollateASCII) < 0);
		});
	}

	ctx.writer->SetHeader(http::Header{"Content-Type", "application/json; charset=utf-8"});
	ctx.writer->SetRespCode(http::StatusOK);

	ctx.writer->Write("{");
	ctx.writer->Write("\"items\":[");
	for (auto &nsDef : nsDefs) {
		ctx.writer->Write("{\"name\":\"");
		ctx.writer->Write(nsDef.name.c_str(), nsDef.name.length());
		ctx.writer->Write("\",");
		string isStorageEnabled = nsDef.storage.IsEnabled() ? "true" : "false";
		ctx.writer->Write("\"storage_enabled\":");
		ctx.writer->Write(isStorageEnabled.c_str(), isStorageEnabled.length());
		ctx.writer->Write("}");
		if (&nsDef != &nsDefs.back()) ctx.writer->Write(",");
	}

	ctx.writer->Write("],");
	ctx.writer->Write("\"total_items\":");

	auto total = to_string(nsDefs.size());
	ctx.writer->Write(total.c_str(), total.length());
	ctx.writer->Write("}");

	return 0;
}

int HTTPServer::GetNamespace(http::Context &ctx) {
	shared_ptr<Reindexer> db = getDB(ctx, kRoleDataRead);

	const char *nsName = ctx.request->urlParams[1];

	if (!*nsName) {
		return jsonStatus(ctx, false, http::StatusBadRequest, "Namespace is not specified");
	}

	vector<reindexer::NamespaceDef> nsDefs;
	db->EnumNamespaces(nsDefs, false);
	auto nsDefIt = std::find_if(nsDefs.begin(), nsDefs.end(), [&](const NamespaceDef &nsDef) { return nsDef.name == nsName; });

	if (nsDefIt == nsDefs.end()) {
		return jsonStatus(ctx, false, http::StatusNotFound, "Namespace is not found");
	}

	ctx.writer->SetHeader(http::Header{"Content-Type", "application/json; charset=utf-8"});
	ctx.writer->SetRespCode(http::StatusOK);

	reindexer::WrSerializer wrSer(true);

	nsDefIt->GetJSON(wrSer);
	ctx.writer->Write(wrSer.Buf(), wrSer.Len());

	return 0;
}

int HTTPServer::PostNamespace(http::Context &ctx) {
	shared_ptr<Reindexer> db = getDB(ctx, kRoleDBAdmin);

	string nsdefJson = ctx.body->Read();
	reindexer::NamespaceDef nsdef("");

	auto status = nsdef.FromJSON(const_cast<char *>(nsdefJson.c_str()));
	if (!status.ok()) {
		return jsonStatus(ctx, false, http::StatusInternalServerError, status.what());
	}

	status = db->AddNamespace(nsdef);
	if (!status.ok()) {
		return jsonStatus(ctx, false, http::StatusInternalServerError, status.what());
	}

	return jsonStatus(ctx);
}

int HTTPServer::DeleteNamespace(http::Context &ctx) {
	shared_ptr<Reindexer> db = getDB(ctx, kRoleDBAdmin);

	const char *nsName = ctx.request->urlParams[1];

	if (!*nsName) {
		return jsonStatus(ctx, false, http::StatusBadRequest, "Namespace is not specified");
	}

	auto status = db->DropNamespace(nsName);
	if (!status.ok()) {
		return jsonStatus(ctx, false, http::StatusInternalServerError, status.what());
	}

	return jsonStatus(ctx);
}

int HTTPServer::GetItems(http::Context &ctx) {
	shared_ptr<Reindexer> db = getDB(ctx, kRoleDataRead);

	const char *nsName = ctx.request->urlParams[1];

	const char *limitParam = nullptr;
	const char *offsetParam = nullptr;
	const char *sortField = nullptr;
	const char *sortOrder = "asc";

	for (auto p : ctx.request->params) {
		if (!strcmp(p.name, "limit")) limitParam = p.val;
		if (!strcmp(p.name, "offset")) offsetParam = p.val;
		if (!strcmp(p.name, "sort_field")) sortField = p.val;
		if (!strcmp(p.name, "sort_order")) sortOrder = p.val;
	}

	if (!*nsName) {
		return jsonStatus(ctx, false, http::StatusBadRequest, "Namespace is not specified");
	}

	int limit = limit_default;
	int offset = offset_default;
	if (limitParam != nullptr) {
		limit = atoi(limitParam);
		if (limit < 0) limit = limit_default;
		if (limit > limit_max) limit = limit_max;
	}
	if (offsetParam != nullptr) {
		offset = atoi(offsetParam);
		if (offset < 0) offset = 0;
	}

	reindexer::Query query(nsName, offset, limit, ModeAccurateTotal);

	if (sortField) {
		bool isSortDesc = !strcmp(sortOrder, "desc");
		query.Sort(sortField, isSortDesc);
	}

	reindexer::QueryResults res;
	auto status = db->Select(query, res);
	if (!status.ok()) {
		return jsonStatus(ctx, false, http::StatusInternalServerError, status.what());
	}

	ctx.writer->SetHeader(http::Header{"Content-Type", "application/json; charset=utf-8"});
	ctx.writer->SetRespCode(http::StatusOK);

	ctx.writer->Write("{\"items\":[");
	reindexer::WrSerializer wrSer(true);
	for (size_t i = 0; i < res.size(); i++) {
		wrSer.Reset();
		res.GetJSON(i, wrSer, false);
		ctx.writer->Write(wrSer.Buf(), wrSer.Len());
		if (i != res.size() - 1) ctx.writer->Write(",");
	}
	ctx.writer->Write("],");
	ctx.writer->Write("\"total_items\":");
	string total = to_string(res.totalCount);
	ctx.writer->Write(total.c_str(), total.length());
	ctx.writer->Write("}");

	return 0;
}

int HTTPServer::DeleteItems(http::Context &ctx) { return modifyItem(ctx, ModeDelete); }
int HTTPServer::PutItems(http::Context &ctx) { return modifyItem(ctx, ModeUpdate); }
int HTTPServer::PostItems(http::Context &ctx) { return modifyItem(ctx, ModeInsert); }

int HTTPServer::GetIndexes(http::Context &ctx) {
	shared_ptr<Reindexer> db = getDB(ctx, kRoleDataRead);

	const char *nsName = ctx.request->urlParams[1];

	if (!*nsName) {
		return jsonStatus(ctx, false, http::StatusBadRequest, "Namespace is not specified");
	}

	vector<reindexer::NamespaceDef> nsDefs;
	db->EnumNamespaces(nsDefs, false);
	auto nsDefIt = std::find_if(nsDefs.begin(), nsDefs.end(), [&](const NamespaceDef &nsDef) { return nsDef.name == nsName; });

	if (nsDefIt == nsDefs.end()) {
		return jsonStatus(ctx, false, http::StatusNotFound, "Namespace is not found");
	}

	ctx.writer->SetHeader(http::Header{"Content-Type", "application/json; charset=utf-8"});
	ctx.writer->SetRespCode(http::StatusOK);

	ctx.writer->Write("{");

	reindexer::WrSerializer wrSer(true);

	wrSer.PutChars("\"indexes\":[");
	for (size_t i = 0; i < nsDefIt->indexes.size(); i++) {
		if (i != 0) wrSer.PutChar(',');
		nsDefIt->indexes[i].GetJSON(wrSer);
	}
	wrSer.PutChars("]");

	ctx.writer->Write(wrSer.Buf(), wrSer.Len());

	ctx.writer->Write(",\"total_items\":");
	string total = to_string(nsDefIt->indexes.size());
	ctx.writer->Write(total.c_str(), total.length());
	ctx.writer->Write("}");

	return 0;
}

int HTTPServer::PostIndex(http::Context &ctx) {
	shared_ptr<Reindexer> db = getDB(ctx, kRoleDBAdmin);

	const char *nsName = ctx.request->urlParams[1];

	if (!*nsName) {
		return jsonStatus(ctx, false, http::StatusBadRequest, "Namespace is not specified");
	}

	string json = ctx.body->Read();
	string newIdxName = getNameFromJson(json);

	vector<reindexer::NamespaceDef> nsDefs;
	db->EnumNamespaces(nsDefs, false);
	auto nsDefIt = std::find_if(nsDefs.begin(), nsDefs.end(), [&](const NamespaceDef &nsDef) { return nsDef.name == nsName; });

	reindexer::IndexDef idxDef;
	idxDef.FromJSON(&json[0]);

	if (nsDefIt != nsDefs.end()) {
		auto &indexes = nsDefIt->indexes;
		if (std::find_if(indexes.begin(), indexes.end(), [&](const IndexDef &idx) { return idx.name == newIdxName; }) == indexes.end()) {
			return jsonStatus(ctx, false, http::StatusBadRequest, "Index already exists");
		}
	}

	auto status = db->AddIndex(nsName, idxDef);
	if (!status.ok()) {
		return jsonStatus(ctx, false, http::StatusInternalServerError, status.what());
	}

	return jsonStatus(ctx);
}

int HTTPServer::DeleteIndex(http::Context &ctx) {
	shared_ptr<Reindexer> db = getDB(ctx, kRoleDBAdmin);

	const char *nsName = ctx.request->urlParams[1];
	const char *idxName = ctx.request->urlParams[2];

	if (!*nsName) {
		return jsonStatus(ctx, false, http::StatusBadRequest, "Namespace is not specified");
	}

	if (!*idxName) {
		return jsonStatus(ctx, false, http::StatusBadRequest, "Index is not specified");
	}

	auto status = db->DropIndex(nsName, idxName);
	if (!status.ok()) {
		return jsonStatus(ctx, false, http::StatusInternalServerError, status.what());
	}

	return jsonStatus(ctx);
}

int HTTPServer::Check(http::Context &ctx) { return ctx.String(http::StatusOK, "Hello world"); }
int HTTPServer::DocHandler(http::Context &ctx) {
	string path = ctx.request->path + 1;
	string target = webRoot_ + path;

	struct stat stat;
	int res = lstat(target.c_str(), &stat);
	if (res >= 0) {
		if (S_ISDIR(stat.st_mode)) {
			if (!path.empty() && path.back() != '/') {
				return ctx.Redirect((path += '/').c_str());
			}

			target += "index.html";
		}

		return ctx.File(http::StatusOK, target.c_str());
	} else {
		target += "/index.html";
	}

	char *targetPtr = &target[0];
	char *ptr1;
	char *ptr2;

	for (;;) {
		if (strncmp(targetPtr, webRoot_.c_str(), webRoot_.length())) {
			break;
		}

		int res = lstat(targetPtr, &stat);
		if (res >= 0) {
			return ctx.File(http::StatusOK, targetPtr);
		}

		ptr1 = strrchr(targetPtr, '/');
		if (!ptr1 || ptr1 == targetPtr) {
			break;
		}

		*ptr1 = '\0';
		ptr2 = strrchr(targetPtr, '/');

		if (!ptr2) {
			ptr2 = targetPtr;
		}

		size_t len = strlen(ptr1 + 1) + 1;
		memmove(ptr2 + 1, ptr1 + 1, len);
	}

	return ctx.File(http::StatusOK, target.c_str());
}

int HTTPServer::NotFoundHandler(http::Context &ctx) { return jsonStatus(ctx, false, http::StatusNotFound, "Not found"); }

bool HTTPServer::Start(const string &addr, ev::dynamic_loop &loop) {
	router_.NotFound<HTTPServer, &HTTPServer::NotFoundHandler>(this);

	router_.GET<HTTPServer, &HTTPServer::DocHandler>("/swagger", this);
	router_.GET<HTTPServer, &HTTPServer::DocHandler>("/swagger/*", this);
	router_.GET<HTTPServer, &HTTPServer::DocHandler>("/face", this);
	router_.GET<HTTPServer, &HTTPServer::DocHandler>("/face/*", this);
	router_.GET<HTTPServer, &HTTPServer::DocHandler>("/facestaging", this);
	router_.GET<HTTPServer, &HTTPServer::DocHandler>("/facestaging/*", this);

	router_.GET<HTTPServer, &HTTPServer::Check>("/api/v1/check", this);

	router_.GET<HTTPServer, &HTTPServer::GetQuery>("/api/v1/db/:db/query", this);
	router_.POST<HTTPServer, &HTTPServer::PostQuery>("/api/v1/:db/query", this);

	router_.GET<HTTPServer, &HTTPServer::GetDatabases>("/api/v1/db", this);
	router_.POST<HTTPServer, &HTTPServer::PostDatabase>("/api/v1/db", this);
	router_.DELETE<HTTPServer, &HTTPServer::DeleteDatabase>("/api/v1/db/:db", this);

	router_.GET<HTTPServer, &HTTPServer::GetNamespaces>("/api/v1/db/:db/namespaces", this);
	router_.GET<HTTPServer, &HTTPServer::GetNamespace>("/api/v1/db/:db/namespaces/:ns", this);
	router_.POST<HTTPServer, &HTTPServer::PostNamespace>("/api/v1/db/:db/namespaces", this);
	router_.DELETE<HTTPServer, &HTTPServer::DeleteNamespace>("/api/v1/db/:db/namespaces/:ns", this);

	router_.GET<HTTPServer, &HTTPServer::GetItems>("/api/v1/db/:db/namespaces/:ns/items", this);
	router_.PUT<HTTPServer, &HTTPServer::PutItems>("/api/v1/db/:db/namespaces/:ns/items", this);
	router_.POST<HTTPServer, &HTTPServer::PostItems>("/api/v1/db/:db/namespaces/:ns/items", this);
	router_.DELETE<HTTPServer, &HTTPServer::DeleteItems>("/api/v1/db/:db/namespaces/:ns/items", this);

	router_.GET<HTTPServer, &HTTPServer::GetIndexes>("/api/v1/db/:db/namespaces/:ns/indexes", this);
	router_.POST<HTTPServer, &HTTPServer::PostIndex>("/api/v1/db/:db/namespaces/:ns/indexes", this);
	router_.DELETE<HTTPServer, &HTTPServer::DeleteIndex>("/api/v1/db/:db/namespaces/:ns/indexes/:idx", this);

	router_.Middleware<HTTPServer, &HTTPServer::CheckAuth>(this);

	if (logger_) {
		router_.Logger<HTTPServer, &HTTPServer::Logger>(this);
	}

	pprof_.Attach(router_);

	listener_.reset(new Listener(loop, http::Connection::NewFactory(router_)));

	return listener_->Bind(addr);
}

int HTTPServer::modifyItem(http::Context &ctx, int mode) {
	shared_ptr<Reindexer> db = getDB(ctx, kRoleDataWrite);

	const char *nsName = ctx.request->urlParams[1];

	string itemJson = ctx.body->Read();

	if (!*nsName) {
		return jsonStatus(ctx, false, http::StatusBadRequest, "Namespace is not specified");
	}

	char *jsonPtr = &itemJson[0];
	size_t jsonLeft = itemJson.size();
	int cnt = 0;
	while (jsonPtr && *jsonPtr) {
		Item item = db->NewItem(nsName);
		if (!item.Status().ok()) {
			return jsonStatus(ctx, false, http::StatusInternalServerError, item.Status().what());
		}
		char *prevPtr = 0;
		auto status = item.Unsafe().FromJSON(reindexer::Slice(jsonPtr, jsonLeft), &jsonPtr);
		jsonLeft -= (jsonPtr - prevPtr);

		if (!status.ok()) {
			return jsonStatus(ctx, false, http::StatusInternalServerError, status.what());
		}

		switch (mode) {
			case ModeUpsert:
				status = db->Upsert(nsName, item);
				break;
			case ModeDelete:
				status = db->Delete(nsName, item);
				break;
			case ModeInsert:
				status = db->Insert(nsName, item);
				break;
			case ModeUpdate:
				status = db->Update(nsName, item);
				break;
		}

		if (!status.ok()) {
			return jsonStatus(ctx, false, http::StatusInternalServerError, status.what());
		}
		cnt += item.GetID() == -1 ? 0 : 1;
	}
	db->Commit(nsName);

	return jsonStatus(ctx);
}

int HTTPServer::queryResults(http::Context &ctx, reindexer::QueryResults &res, const char *name) {
	ctx.writer->SetHeader(http::Header{"Content-Type", "application/json; charset=utf-8"});
	ctx.writer->SetRespCode(http::StatusOK);
	reindexer::WrSerializer wrSer(true);
	ctx.writer->Write("{\"");
	ctx.writer->Write(name, strlen(name));
	ctx.writer->Write("\":[");
	for (size_t i = 0; i < res.size(); i++) {
		wrSer.Reset();
		if (i != 0) ctx.writer->Write(",");
		res.GetJSON(i, wrSer, false);
		ctx.writer->Write(wrSer.Buf(), wrSer.Len());
	}
	ctx.writer->Write("]}");
	return 0;
}

int HTTPServer::jsonStatus(http::Context &ctx, bool isSuccess, int respcode, const string &description) {
	ctx.writer->SetHeader(http::Header{"Content-Type", "application/json; charset=utf-8"});
	ctx.writer->SetRespCode(respcode);
	ctx.writer->Write("{");

	if (isSuccess) {
		ctx.writer->Write("\"success\":true");
	} else {
		ctx.writer->Write("\"success\":false,");
		ctx.writer->Write("\"response_code\":");
		string rcode = to_string(respcode);
		ctx.writer->Write(rcode.c_str(), rcode.length());
		ctx.writer->Write(",\"description\":\"");
		ctx.writer->Write(description.c_str(), description.length());
		ctx.writer->Write("\"");
	}

	ctx.writer->Write("}");

	return 0;
}

shared_ptr<Reindexer> HTTPServer::getDB(http::Context &ctx, UserRole role) {
	(void)ctx;
	shared_ptr<Reindexer> db;

	const char *dbName = ctx.request->urlParams[0];

	AuthContext dummyCtx;

	AuthContext *actx = &dummyCtx;
	if (!dbMgr_.IsNoSecurity()) {
		auto clientData = dynamic_cast<HTTPClientData *>(ctx.clientData.get());
		assert(clientData);
		actx = &clientData->auth;
	}

	auto status = dbMgr_.OpenDatabase(dbName, *actx, false);
	if (!status.ok()) {
		throw status;
	}

	status = actx->GetDB(role, &db);
	if (!status.ok()) {
		throw status;
	}
	assert(db);
	return db;
}

string HTTPServer::getNameFromJson(string json) {
	JsonAllocator jalloc;
	JsonValue jvalue;
	char *endp;

	int status = jsonParse(&json[0], &endp, &jvalue, jalloc);
	if (status != JSON_OK) {
		throw Error(http::StatusBadRequest, jsonStrError(status));
	}

	string dbName;
	for (auto elem : jvalue) {
		if (elem->value.getTag() == JSON_STRING && !strcmp(elem->key, "name")) {
			dbName = elem->value.toString();
			break;
		}
	}

	return dbName;
}

int HTTPServer::CheckAuth(http::Context &ctx) {
	(void)ctx;
	if (dbMgr_.IsNoSecurity()) {
		return 0;
	}

	const char *authHeader = nullptr;
	for (auto &p : ctx.request->headers) {
		if (!strcmp(p.name, "authorization")) authHeader = p.val;
	}

	size_t authHdrLen = authHeader != nullptr ? strlen(authHeader) : 0;
	if (!authHeader || authHdrLen < 6) {
		ctx.writer->SetHeader({"WWW-Authenticate", "Basic realm=\"reindexer\""});
		ctx.String(http::StatusUnauthorized, "Forbidden");
		return -1;
	}

	char *credBuf = reinterpret_cast<char *>(alloca(authHdrLen));
	Base64decode(credBuf, authHeader + 6);
	char *password = strchr(credBuf, ':');
	if (password != nullptr) *password++ = 0;

	AuthContext auth(credBuf, password ? password : "");
	auto status = dbMgr_.Login("", auth);
	if (!status.ok()) {
		ctx.writer->SetHeader({"WWW-Authenticate", "Basic realm=\"reindexer\""});
		ctx.String(http::StatusUnauthorized, status.what());
		return -1;
	}

	auto clientData = std::make_shared<HTTPClientData>();
	ctx.clientData = clientData;
	clientData->auth = auth;
	return 0;
}

void HTTPServer::Logger(http::Context &ctx) {
	if (allocDebug_) {
		Stat statDiff = Stat() - ctx.stat;

		logger_.info("{0} {1} {2} {3} | elapsed: {4}us, allocs: {5}, allocated: {6} byte(s)", ctx.request->method, ctx.request->uri,
					 ctx.writer->RespCode(), ctx.writer->Written(), statDiff.GetTimeElapsed(), statDiff.GetAllocsCnt(),
					 statDiff.GetAllocsBytes());
	} else {
		logger_.info("{0} {1} {2} {3}", ctx.request->method, ctx.request->uri, ctx.writer->RespCode(), ctx.writer->Written());
	}
}

}  // namespace reindexer_server
