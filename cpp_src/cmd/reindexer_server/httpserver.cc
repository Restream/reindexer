#include "httpserver.h"
#include <sys/stat.h>
#include <sstream>
#include "base64/base64.h"
#include "core/type_consts.h"
#include "gason/gason.h"
#include "loggerwrapper.h"
#include "net/http/serverconnection.h"
#include "net/listener.h"
#include "tools/fsops.h"
#include "tools/serializer.h"
#include "tools/stringstools.h"

using std::string;
using std::stringstream;
using std::to_string;

namespace reindexer_server {

HTTPServer::HTTPServer(DBManager &dbMgr, const string &webRoot) : dbMgr_(dbMgr), webRoot_(reindexer::fs::JoinPath(webRoot, "")) {}
HTTPServer::HTTPServer(DBManager &dbMgr, const string &webRoot, LoggerWrapper logger, bool allocDebug, bool enablePprof)
	: dbMgr_(dbMgr), webRoot_(reindexer::fs::JoinPath(webRoot, "")), logger_(logger), allocDebug_(allocDebug), enablePprof_(enablePprof) {}
HTTPServer::~HTTPServer() {}

enum { ModeUpdate, ModeInsert, ModeUpsert, ModeDelete };

int HTTPServer::GetSQLQuery(http::Context &ctx) {
	shared_ptr<Reindexer> db = getDB(ctx, kRoleDataRead);
	reindexer::QueryResults res;
	string sqlQuery = urldecode2(ctx.request->params.Get("q"));

	if (sqlQuery.empty()) {
		return jsonStatus(ctx, false, http::StatusBadRequest, "Missed `q` parameter");
	}

	auto ret = db->Select(sqlQuery, res);

	if (!ret.ok()) {
		return jsonStatus(ctx, false, http::StatusInternalServerError, ret.what());
	}
	return queryResults(ctx, res, "items");
}

int HTTPServer::PostSQLQuery(http::Context &ctx) {
	shared_ptr<Reindexer> db = getDB(ctx, kRoleDataRead);
	reindexer::QueryResults res;
	string sqlQuery = ctx.body->Read();

	if (!sqlQuery.length()) {
		return jsonStatus(ctx, false, http::StatusBadRequest, "Query is empty");
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

	string_view sortOrder = ctx.request->params.Get("sort_order");

	auto dbs = dbMgr_.EnumDatabases();

	int sortDirection = 0;
	if (sortOrder == "asc") {
		sortDirection = 1;
	} else if (sortOrder == "desc") {
		sortDirection = -1;
	} else if (sortOrder.length()) {
		return jsonStatus(ctx, false, http::StatusBadRequest, "Invalid `sort_order` parameter");
	}

	if (sortDirection) {
		std::sort(dbs.begin(), dbs.end(), [sortDirection](const string &lhs, const string &rhs) {
			if (sortDirection > 0)
				return collateCompare(lhs, rhs, CollateOpts(CollateASCII)) < 0;
			else
				return collateCompare(lhs, rhs, CollateOpts(CollateASCII)) > 0;
		});
	}

	ctx.writer->Write("{"_sv);
	ctx.writer->Write("\"items\":["_sv);
	for (auto &db : dbs) {
		ctx.writer->Write("\""_sv);
		ctx.writer->Write(db);
		ctx.writer->Write("\""_sv);
		if (db != dbs.back()) ctx.writer->Write(","_sv);
	}

	ctx.writer->Write("],"_sv);
	ctx.writer->Write("\"total_items\":"_sv);

	auto total = to_string(dbs.size());
	ctx.writer->Write(total);
	ctx.writer->Write("}"_sv);

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
	string dbName = ctx.request->urlParams[0].ToString();
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

	string_view sortOrder = ctx.request->params.Get("sort_order");

	vector<reindexer::NamespaceDef> nsDefs;
	db->EnumNamespaces(nsDefs, false);

	int sortDirection = 0;
	if (sortOrder == "asc") {
		sortDirection = 1;
	} else if (sortOrder == "desc") {
		sortDirection = -1;
	} else if (sortOrder.length()) {
		return jsonStatus(ctx, false, http::StatusBadRequest, "Invalid `sort_order` parameter");
	}

	if (sortDirection) {
		std::sort(nsDefs.begin(), nsDefs.end(), [sortDirection](const NamespaceDef &lhs, const NamespaceDef &rhs) {
			if (sortDirection > 0)
				return collateCompare(lhs.name, rhs.name, CollateOpts(CollateASCII)) < 0;
			else
				return collateCompare(lhs.name, rhs.name, CollateOpts(CollateASCII)) > 0;
		});
	}

	ctx.writer->SetHeader(http::Header{"Content-Type", "application/json; charset=utf-8"});
	ctx.writer->SetRespCode(http::StatusOK);

	ctx.writer->Write("{"_sv);
	ctx.writer->Write("\"items\":["_sv);
	for (auto &nsDef : nsDefs) {
		ctx.writer->Write("{\"name\":\""_sv);
		ctx.writer->Write(nsDef.name.c_str(), nsDef.name.length());
		ctx.writer->Write("\",");
		string_view isStorageEnabled = nsDef.storage.IsEnabled() ? "true"_sv : "false"_sv;
		ctx.writer->Write("\"storage_enabled\":"_sv);
		ctx.writer->Write(isStorageEnabled);
		ctx.writer->Write("}"_sv);
		if (&nsDef != &nsDefs.back()) ctx.writer->Write(","_sv);
	}

	ctx.writer->Write("],"_sv);
	ctx.writer->Write("\"total_items\":"_sv);

	auto total = to_string(nsDefs.size());
	ctx.writer->Write(total);
	ctx.writer->Write("}"_sv);

	return 0;
}

int HTTPServer::GetNamespace(http::Context &ctx) {
	shared_ptr<Reindexer> db = getDB(ctx, kRoleDataRead);

	string nsName = ctx.request->urlParams[1].ToString();

	if (!nsName.length()) {
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

	string nsName = ctx.request->urlParams[1].ToString();

	if (nsName.empty()) {
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

	string nsName = ctx.request->urlParams[1].ToString();

	string_view limitParam = ctx.request->params.Get("limit");
	string_view offsetParam = ctx.request->params.Get("offset");
	string_view sortField = ctx.request->params.Get("sort_field");
	string_view sortOrder = ctx.request->params.Get("sort_order");

	if (nsName.empty()) {
		return jsonStatus(ctx, false, http::StatusBadRequest, "Namespace is not specified");
	}

	int limit = limit_default;
	int offset = offset_default;
	if (limitParam.length()) {
		limit = stoi(limitParam);
		if (limit < 0) limit = limit_default;
		if (limit > limit_max) limit = limit_max;
	}
	if (offsetParam.length()) {
		offset = stoi(offsetParam);
		if (offset < 0) offset = 0;
	}

	reindexer::Query query(nsName, offset, limit, ModeAccurateTotal);

	if (sortField.length()) {
		bool isSortDesc = sortOrder == "desc";
		query.Sort(sortField.ToString(), isSortDesc);
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
		if (i != res.size() - 1) ctx.writer->Write(',');
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

	string nsName = ctx.request->urlParams[1].ToString();

	if (!nsName.length()) {
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

	ctx.writer->Write('{');

	reindexer::WrSerializer wrSer(true);

	wrSer.PutChars("\"items\":[");
	for (size_t i = 0; i < nsDefIt->indexes.size(); i++) {
		if (i != 0) wrSer.PutChar(',');
		nsDefIt->indexes[i].GetJSON(wrSer);
	}
	wrSer.PutChars("]");

	ctx.writer->Write(wrSer.Buf(), wrSer.Len());

	ctx.writer->Write(",\"total_items\":");
	string total = to_string(nsDefIt->indexes.size());
	ctx.writer->Write(total.c_str(), total.length());
	ctx.writer->Write('}');

	return 0;
}

int HTTPServer::PostIndex(http::Context &ctx) {
	shared_ptr<Reindexer> db = getDB(ctx, kRoleDBAdmin);

	string nsName = ctx.request->urlParams[1].ToString();

	if (!nsName.length()) {
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
		auto foundIndexIt =
			std::find_if(indexes.begin(), indexes.end(), [&newIdxName](const IndexDef &idx) { return idx.name == newIdxName; });
		if (foundIndexIt != indexes.end()) {
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

	string nsName = ctx.request->urlParams[1].ToString();
	string idxName = ctx.request->urlParams[2].ToString();

	if (nsName.empty()) {
		return jsonStatus(ctx, false, http::StatusBadRequest, "Namespace is not specified");
	}

	if (idxName.empty()) {
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
	string path = ctx.request->path.substr(1).ToString();
	string target = webRoot_ + path;

	switch (fs::Stat(target)) {
		case fs::StatError:
			target += "/index.html";
			break;
		case fs::StatDir:
			if (!path.empty() && path.back() != '/') {
				return ctx.Redirect((path += '/').c_str());
			}

			target += "index.html";
			return ctx.File(http::StatusOK, target.c_str());
		case fs::StatFile:
			return ctx.File(http::StatusOK, target.c_str());
	}

	char *targetPtr = &target[0];
	char *ptr1;
	char *ptr2;

	for (;;) {
		if (strncmp(targetPtr, webRoot_.c_str(), webRoot_.length())) {
			break;
		}

		if (fs::Stat(targetPtr) == fs::StatFile) {
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

	router_.GET<HTTPServer, &HTTPServer::GetSQLQuery>("/api/v1/db/:db/query", this);
	router_.POST<HTTPServer, &HTTPServer::PostQuery>("/api/v1/db/:db/query", this);
	router_.POST<HTTPServer, &HTTPServer::PostSQLQuery>("/api/v1/db/:db/sqlquery", this);

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

	if (enablePprof_) {
		pprof_.Attach(router_);
	}
	listener_.reset(new Listener(loop, http::ServerConnection::NewFactory(router_)));

	return listener_->Bind(addr);
}

int HTTPServer::modifyItem(http::Context &ctx, int mode) {
	shared_ptr<Reindexer> db = getDB(ctx, kRoleDataWrite);

	string nsName = ctx.request->urlParams[1].ToString();

	string itemJson = ctx.body->Read();

	if (nsName.empty()) {
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
		auto status = item.Unsafe().FromJSON(reindexer::string_view(jsonPtr, jsonLeft), &jsonPtr);
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
	ctx.writer->SetHeader(http::Header{"Content-Type"_sv, "application/json; charset=utf-8"_sv});
	ctx.writer->SetRespCode(http::StatusOK);
	reindexer::WrSerializer wrSer(true);
	ctx.writer->Write('{');
	if (res.totalCount) {
		ctx.writer->Write("\"total_items\":"_sv);
		string total = to_string(res.totalCount);
		ctx.writer->Write(total.c_str(), total.length());
		ctx.writer->Write(",");
	}
	if (!res.aggregationResults.empty()) {
		ctx.writer->Write("\"aggregations\": ["_sv);
		for (unsigned i = 0; i < res.aggregationResults.size(); i++) {
			if (i) ctx.writer->Write(',');
			string agg = to_string(res.aggregationResults[i]);
			ctx.writer->Write(agg.c_str(), agg.length());
		}
		ctx.writer->Write("],"_sv);
	}

	ctx.writer->Write('\"');
	ctx.writer->Write(name, strlen(name));
	ctx.writer->Write("\":["_sv);
	for (size_t i = 0; i < res.size(); i++) {
		wrSer.Reset();
		if (i != 0) ctx.writer->Write(',');
		res.GetJSON(i, wrSer, false);
		ctx.writer->Write(wrSer.Buf(), wrSer.Len());
	}
	ctx.writer->Write("]}"_sv);
	return 0;
}

int HTTPServer::jsonStatus(http::Context &ctx, bool isSuccess, int respcode, const string_view &description) {
	ctx.writer->SetHeader(http::Header{"Content-Type"_sv, "application/json; charset=utf-8"_sv});
	ctx.writer->SetRespCode(respcode);
	ctx.writer->Write('{');

	if (isSuccess) {
		ctx.writer->Write("\"success\":true"_sv);
	} else {
		ctx.writer->Write("\"success\":false,"_sv);
		ctx.writer->Write("\"response_code\":"_sv);
		string rcode = to_string(respcode);
		ctx.writer->Write(rcode);
		ctx.writer->Write(",\"description\":\""_sv);
		ctx.writer->Write(description);
		ctx.writer->Write('\"');
	}

	ctx.writer->Write('}');

	return 0;
}

shared_ptr<Reindexer> HTTPServer::getDB(http::Context &ctx, UserRole role) {
	(void)ctx;
	shared_ptr<Reindexer> db;

	string dbName = ctx.request->urlParams[0].ToString();

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
		throw Error(http::StatusBadRequest, "%s", jsonStrError(status));
	}

	if (jvalue.getTag() != JSON_OBJECT) {
		throw Error(http::StatusBadRequest, "Json is malformed: %d", jvalue.getTag());
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

	string_view authHeader = ctx.request->headers.Get("authorization");

	if (authHeader.length() < 6) {
		ctx.writer->SetHeader({"WWW-Authenticate"_sv, "Basic realm=\"reindexer\""_sv});
		ctx.String(http::StatusUnauthorized, "Forbidden");
		return -1;
	}

	char *credBuf = reinterpret_cast<char *>(alloca(authHeader.length()));
	Base64decode(credBuf, authHeader.data() + 6);
	char *password = strchr(credBuf, ':');
	if (password != nullptr) *password++ = 0;

	AuthContext auth(credBuf, password ? password : "");
	auto status = dbMgr_.Login("", auth);
	if (!status.ok()) {
		ctx.writer->SetHeader({"WWW-Authenticate"_sv, "Basic realm=\"reindexer\""_sv});
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
