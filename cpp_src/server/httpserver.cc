#include "httpserver.h"
#include <sys/stat.h>
#include <sstream>
#include "base64/base64.h"
#include "core/cjson/jsonbuilder.h"
#include "core/type_consts.h"
#include "gason/gason.h"
#include "loggerwrapper.h"
#include "net/http/serverconnection.h"
#include "net/listener.h"
#include "reindexer_version.h"
#include "resources_wrapper.h"
#include "tools/fsops.h"
#include "tools/serializer.h"
#include "tools/stringstools.h"
#if REINDEX_WITH_GPERFTOOLS
#include <gperftools/malloc_extension_c.h>
#endif

using std::string;
using std::stringstream;
using std::to_string;

namespace reindexer_server {

HTTPServer::HTTPServer(DBManager &dbMgr, const string &webRoot, LoggerWrapper logger, bool allocDebug, bool enablePprof)
	: dbMgr_(dbMgr),
	  webRoot_(reindexer::fs::JoinPath(webRoot, "")),
	  logger_(logger),
	  allocDebug_(allocDebug),
	  enablePprof_(enablePprof),
	  startTs_(std::chrono::system_clock::now()) {}
HTTPServer::~HTTPServer() {}

enum { ModeUpdate, ModeInsert, ModeUpsert, ModeDelete };

int HTTPServer::GetSQLQuery(http::Context &ctx) {
	shared_ptr<Reindexer> db = getDB(ctx, kRoleDataRead);
	reindexer::QueryResults res;
	string sqlQuery = urldecode2(ctx.request->params.Get("q"));

	string_view limitParam = ctx.request->params.Get("limit");
	string_view offsetParam = ctx.request->params.Get("offset");

	unsigned limit = prepareLimit(limitParam);
	unsigned offset = prepareOffset(offsetParam);

	if (sqlQuery.empty()) {
		return jsonStatus(ctx, http::HttpStatus(http::StatusBadRequest, "Missed `q` parameter"));
	}

	auto ret = db->Select(sqlQuery, res);
	if (!ret.ok()) {
		http::HttpStatus httpStatus(http::StatusInternalServerError, ret.what());

		return jsonStatus(ctx, httpStatus);
	}

	return queryResults(ctx, res, true, limit, offset);
}

int HTTPServer::PostSQLQuery(http::Context &ctx) {
	shared_ptr<Reindexer> db = getDB(ctx, kRoleDataRead);
	reindexer::QueryResults res;

	string sqlQuery = ctx.body->Read();
	if (!sqlQuery.length()) {
		return jsonStatus(ctx, http::HttpStatus(http::StatusBadRequest, "Query is empty"));
	}

	auto ret = db->Select(sqlQuery, res);
	if (!ret.ok()) {
		return jsonStatus(ctx, http::HttpStatus(http::StatusBadRequest, ret.what()));
	}
	return queryResults(ctx, res, true);
}

int HTTPServer::PostQuery(http::Context &ctx) {
	shared_ptr<Reindexer> db = getDB(ctx, kRoleDataRead);
	reindexer::QueryResults res;
	string dsl = ctx.body->Read();

	reindexer::Query q;
	auto status = q.ParseJson(dsl);
	if (!status.ok()) {
		return jsonStatus(ctx, http::HttpStatus(status));
	}

	status = db->Select(q, res);
	if (!status.ok()) {
		return jsonStatus(ctx, http::HttpStatus(status));
	}
	return queryResults(ctx, res, true);
}

int HTTPServer::DeleteQuery(http::Context &ctx) {
	shared_ptr<Reindexer> db = getDB(ctx, kRoleDataWrite);
	reindexer::QueryResults res;
	string dsl = ctx.body->Read();

	reindexer::Query q;
	auto status = q.ParseJson(dsl);
	if (!status.ok()) {
		return jsonStatus(ctx, http::HttpStatus(status));
	}

	status = db->Delete(q, res);
	if (!status.ok()) {
		return jsonStatus(ctx, http::HttpStatus(status));
	}
	WrSerializer ser(ctx.writer->GetChunk());
	reindexer::JsonBuilder builder(ser);
	builder.Put("updated", res.Count());
	builder.End();

	return ctx.JSON(http::StatusOK, ser.DetachChunk());
}

int HTTPServer::GetDatabases(http::Context &ctx) {
	string_view sortOrder = ctx.request->params.Get("sort_order");

	auto dbs = dbMgr_.EnumDatabases();

	int sortDirection = 0;
	if (sortOrder == "asc") {
		sortDirection = 1;
	} else if (sortOrder == "desc") {
		sortDirection = -1;
	} else if (sortOrder.length()) {
		return jsonStatus(ctx, http::HttpStatus(http::StatusBadRequest, "Invalid `sort_order` parameter"));
	}

	if (sortDirection) {
		std::sort(dbs.begin(), dbs.end(), [sortDirection](const string &lhs, const string &rhs) {
			if (sortDirection > 0)
				return collateCompare(lhs, rhs, CollateOpts(CollateASCII)) < 0;
			else
				return collateCompare(lhs, rhs, CollateOpts(CollateASCII)) > 0;
		});
	}

	WrSerializer ser(ctx.writer->GetChunk());
	{
		JsonBuilder builder(ser);
		builder.Put("total_items", dbs.size());
		auto arrNode = builder.Array("items");
		for (auto &db : dbs) arrNode.Put(nullptr, db);
	}

	return ctx.JSON(http::StatusOK, ser.DetachChunk());
}

int HTTPServer::PostDatabase(http::Context &ctx) {
	string json = ctx.body->Read();
	string newDbName = getNameFromJson(json);

	auto dbs = dbMgr_.EnumDatabases();
	for (auto &db : dbs) {
		if (db == newDbName) {
			return jsonStatus(ctx, http::HttpStatus(http::StatusBadRequest, "Database already exists"));
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
		return jsonStatus(ctx, http::HttpStatus(status));
	}

	return jsonStatus(ctx);
}

int HTTPServer::DeleteDatabase(http::Context &ctx) {
	string dbName = urldecode2(ctx.request->urlParams[0].ToString());

	AuthContext dummyCtx;
	AuthContext *actx = &dummyCtx;
	if (!dbMgr_.IsNoSecurity()) {
		auto clientData = dynamic_cast<HTTPClientData *>(ctx.clientData.get());
		assert(clientData);
		actx = &clientData->auth;
	}

	auto status = dbMgr_.Login(dbName, *actx);
	if (!status.ok()) {
		return jsonStatus(ctx, http::HttpStatus(http::StatusUnauthorized, status.what()));
	}

	status = dbMgr_.DropDatabase(*actx);
	if (!status.ok()) {
		return jsonStatus(ctx, http::HttpStatus(status));
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
		return jsonStatus(ctx, http::HttpStatus(http::StatusBadRequest, "Invalid `sort_order` parameter"));
	}

	if (sortDirection) {
		std::sort(nsDefs.begin(), nsDefs.end(), [sortDirection](const NamespaceDef &lhs, const NamespaceDef &rhs) {
			if (sortDirection > 0)
				return collateCompare(lhs.name, rhs.name, CollateOpts(CollateASCII)) < 0;
			else
				return collateCompare(lhs.name, rhs.name, CollateOpts(CollateASCII)) > 0;
		});
	}

	WrSerializer ser(ctx.writer->GetChunk());
	{
		JsonBuilder builder(ser);
		builder.Put("total_items", nsDefs.size());
		auto arrNode = builder.Array("items");
		for (auto &nsDef : nsDefs) {
			auto objNode = arrNode.Object(nullptr);
			objNode.Put("name", nsDef.name);
			objNode.Put("storage_enabled", nsDef.storage.IsEnabled());
		}
	}
	return ctx.JSON(http::StatusOK, ser.DetachChunk());
}

int HTTPServer::GetNamespace(http::Context &ctx) {
	shared_ptr<Reindexer> db = getDB(ctx, kRoleDataRead);

	string nsName = urldecode2(ctx.request->urlParams[1]);

	if (!nsName.length()) {
		return jsonStatus(ctx, http::HttpStatus(http::StatusBadRequest, "Namespace is not specified"));
	}

	vector<reindexer::NamespaceDef> nsDefs;
	db->EnumNamespaces(nsDefs, false);
	auto nsDefIt = std::find_if(nsDefs.begin(), nsDefs.end(), [&](const NamespaceDef &nsDef) { return nsDef.name == nsName; });

	if (nsDefIt == nsDefs.end()) {
		return jsonStatus(ctx, http::HttpStatus(http::StatusNotFound, "Namespace is not found"));
	}

	WrSerializer wrSer(ctx.writer->GetChunk());
	nsDefIt->GetJSON(wrSer);
	return ctx.JSON(http::StatusOK, wrSer.DetachChunk());
}

int HTTPServer::PostNamespace(http::Context &ctx) {
	shared_ptr<Reindexer> db = getDB(ctx, kRoleDBAdmin);
	string nsdefJson = ctx.body->Read();
	reindexer::NamespaceDef nsdef("");

	auto status = nsdef.FromJSON(const_cast<char *>(nsdefJson.c_str()));
	if (!status.ok()) {
		return jsonStatus(ctx, http::HttpStatus(status));
	}

	status = db->AddNamespace(nsdef);
	if (!status.ok()) {
		return jsonStatus(ctx, http::HttpStatus(status));
	}

	return jsonStatus(ctx);
}

int HTTPServer::DeleteNamespace(http::Context &ctx) {
	shared_ptr<Reindexer> db = getDB(ctx, kRoleDBAdmin);
	string nsName = urldecode2(ctx.request->urlParams[1]);

	if (nsName.empty()) {
		return jsonStatus(ctx, http::HttpStatus(http::StatusBadRequest, "Namespace is not specified"));
	}

	auto status = db->DropNamespace(nsName);
	if (!status.ok()) {
		http::HttpStatus httpStatus(status);

		return jsonStatus(ctx, httpStatus);
	}

	return jsonStatus(ctx);
}

int HTTPServer::GetItems(http::Context &ctx) {
	shared_ptr<Reindexer> db = getDB(ctx, kRoleDataRead);

	string nsName = urldecode2(ctx.request->urlParams[1]);

	string_view limitParam = ctx.request->params.Get("limit");
	string_view offsetParam = ctx.request->params.Get("offset");
	string_view sortField = ctx.request->params.Get("sort_field");
	string_view sortOrder = ctx.request->params.Get("sort_order");

	string filterParam = urldecode2(ctx.request->params.Get("filter"));
	string fields = urldecode2(ctx.request->params.Get("fields"));

	if (nsName.empty()) {
		return jsonStatus(ctx, http::HttpStatus(http::StatusBadRequest, "Namespace is not specified"));
	}
	if (fields.empty()) {
		fields = "*";
	}

	unsigned limit = prepareLimit(limitParam, kDefaultItemsLimit);
	unsigned offset = prepareOffset(offsetParam);

	reindexer::WrSerializer querySer;
	querySer << "SELECT " << fields << " FROM " << nsName;
	if (filterParam.length()) {
		querySer << " WHERE " << filterParam;
	}
	if (sortField.length()) {
		querySer << " ORDER BY " << sortField;

		if (sortOrder == "desc") {
			querySer << " DESC";
		} else if ((sortOrder.size() > 0) && (sortOrder != "asc")) {
			http::HttpStatus httpStatus(http::StatusBadRequest, "Invalid `sort_order` parameter");
			return jsonStatus(ctx, httpStatus);
		}
	}
	querySer << " LIMIT " << limit << " OFFSET " << offset;

	reindexer::Query q;

	q.FromSQL(querySer.Slice());
	q.ReqTotal();

	reindexer::QueryResults res;
	auto ret = db->Select(q, res);
	if (!ret.ok()) {
		http::HttpStatus httpStatus(http::StatusInternalServerError, ret.what());

		return jsonStatus(ctx, httpStatus);
	}

	return queryResults(ctx, res);
}

int HTTPServer::DeleteItems(http::Context &ctx) { return modifyItem(ctx, ModeDelete); }

int HTTPServer::PutItems(http::Context &ctx) { return modifyItem(ctx, ModeUpdate); }
int HTTPServer::PostItems(http::Context &ctx) { return modifyItem(ctx, ModeInsert); }

int HTTPServer::GetIndexes(http::Context &ctx) {
	shared_ptr<Reindexer> db = getDB(ctx, kRoleDataRead);

	string nsName = urldecode2(ctx.request->urlParams[1]);

	if (!nsName.length()) {
		return jsonStatus(ctx, http::HttpStatus(http::StatusBadRequest, "Namespace is not specified"));
	}

	vector<reindexer::NamespaceDef> nsDefs;
	db->EnumNamespaces(nsDefs, false);
	auto nsDefIt = std::find_if(nsDefs.begin(), nsDefs.end(), [&](const NamespaceDef &nsDef) { return nsDef.name == nsName; });

	if (nsDefIt == nsDefs.end()) {
		return jsonStatus(ctx, http::HttpStatus(http::StatusNotFound, "Namespace is not found"));
	}

	WrSerializer ser(ctx.writer->GetChunk());
	{
		JsonBuilder builder(ser);
		builder.Put("total_items", nsDefIt->indexes.size());
		auto arrNode = builder.Array("items");
		for (auto &idxDef : nsDefIt->indexes) {
			arrNode.Raw(nullptr, "");
			idxDef.GetJSON(ser);
		}
	}
	return ctx.JSON(http::StatusOK, ser.DetachChunk());
}

int HTTPServer::PostIndex(http::Context &ctx) {
	shared_ptr<Reindexer> db = getDB(ctx, kRoleDBAdmin);

	string nsName = urldecode2(ctx.request->urlParams[1]);
	if (!nsName.length()) {
		return jsonStatus(ctx, http::HttpStatus(http::StatusBadRequest, "Namespace is not specified"));
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
			std::find_if(indexes.begin(), indexes.end(), [&newIdxName](const IndexDef &idx) { return idx.name_ == newIdxName; });
		if (foundIndexIt != indexes.end()) {
			return jsonStatus(ctx, http::HttpStatus(http::StatusBadRequest, "Index already exists"));
		}
	}

	auto status = db->AddIndex(nsName, idxDef);
	if (!status.ok()) {
		http::HttpStatus httpStatus(status);

		return jsonStatus(ctx, httpStatus);
	}

	return jsonStatus(ctx);
}

int HTTPServer::PutIndex(http::Context &ctx) {
	shared_ptr<Reindexer> db = getDB(ctx, kRoleDBAdmin);

	string nsName = urldecode2(ctx.request->urlParams[1]);
	if (!nsName.length()) {
		return jsonStatus(ctx, http::HttpStatus(http::StatusBadRequest, "Namespace is not specified"));
	}

	string json = ctx.body->Read();

	reindexer::IndexDef idxDef;
	idxDef.FromJSON(&json[0]);

	auto status = db->UpdateIndex(nsName, idxDef);
	if (!status.ok()) {
		return jsonStatus(ctx, http::HttpStatus(status));
	}

	return jsonStatus(ctx);
}

int HTTPServer::DeleteIndex(http::Context &ctx) {
	shared_ptr<Reindexer> db = getDB(ctx, kRoleDBAdmin);

	string nsName = urldecode2(ctx.request->urlParams[1]);
	string idxName = urldecode2(ctx.request->urlParams[2]);

	if (nsName.empty()) {
		return jsonStatus(ctx, http::HttpStatus(http::StatusBadRequest, "Namespace is not specified"));
	}

	if (idxName.empty()) {
		return jsonStatus(ctx, http::HttpStatus(http::StatusBadRequest, "Index is not specified"));
	}

	auto status = db->DropIndex(nsName, idxName);
	if (!status.ok()) {
		return jsonStatus(ctx, http::HttpStatus(status));
	}

	return jsonStatus(ctx);
}

int HTTPServer::Check(http::Context &ctx) {
	WrSerializer ser(ctx.writer->GetChunk());
	{
		JsonBuilder builder(ser);
		builder.Put("version", REINDEX_VERSION);

		size_t startTs = std::chrono::duration_cast<std::chrono::seconds>(startTs_.time_since_epoch()).count();
		size_t uptime = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now() - startTs_).count();
		builder.Put("start_time", startTs);
		builder.Put("uptime", uptime);

#ifdef REINDEX_WITH_GPERFTOOLS
		size_t val = 0;
		MallocExtension_GetNumericProperty("generic.current_allocated_bytes", &val);
		builder.Put("current_allocated_bytes", val);

		MallocExtension_GetNumericProperty("generic.heap_size", &val);
		builder.Put("heap_size", val);

		MallocExtension_GetNumericProperty("tcmalloc.pageheap_free_bytes", &val);
		builder.Put("pageheap_free", val);

		MallocExtension_GetNumericProperty("tcmalloc.pageheap_unmapped_bytes", &val);
		builder.Put("pageheap_unmapped", val);
#endif
	}

	return ctx.JSON(http::StatusOK, ser.DetachChunk());
}
int HTTPServer::DocHandler(http::Context &ctx) {
	string path = ctx.request->path.substr(1).ToString();

	bool endsWithSlash = (path.length() > 0 && path.back() == '/');
	if (endsWithSlash) {
		path.pop_back();
	}

	if (path == "" || path == "/") {
		return ctx.Redirect("face/");
	}

	string fsPath = webRoot_ + path;

	if (web::stat(fsPath) == fs::StatFile) {
		return web::file(ctx, http::StatusOK, fsPath);
	}

	if (web::stat(fsPath) == fs::StatDir && !endsWithSlash) {
		return ctx.Redirect(path + "/");
	}

	for (; fsPath.length() > webRoot_.length();) {
		string file = fs::JoinPath(fsPath, "index.html");
		if (web::stat(file) == fs::StatFile) {
			return web::file(ctx, http::StatusOK, file);
		}

		auto pos = fsPath.find_last_of('/');
		if (pos == string::npos) break;

		fsPath = fsPath.erase(pos);
	}

	return NotFoundHandler(ctx);
}

int HTTPServer::NotFoundHandler(http::Context &ctx) {
	http::HttpStatus httpStatus(http::StatusNotFound, "Not found");

	return jsonStatus(ctx, httpStatus);
}

bool HTTPServer::Start(const string &addr, ev::dynamic_loop &loop) {
	router_.NotFound<HTTPServer, &HTTPServer::NotFoundHandler>(this);

	router_.GET<HTTPServer, &HTTPServer::DocHandler>("/", this);
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
	router_.DELETE<HTTPServer, &HTTPServer::DeleteQuery>("/api/v1/db/:db/query", this);

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
	router_.PUT<HTTPServer, &HTTPServer::PutIndex>("/api/v1/db/:db/namespaces/:ns/indexes", this);
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
	string nsName = urldecode2(ctx.request->urlParams[1]);
	string itemJson = ctx.body->Read();

	if (nsName.empty()) {
		return jsonStatus(ctx, http::HttpStatus(http::StatusBadRequest, "Namespace is not specified"));
	}

	char *jsonPtr = &itemJson[0];
	size_t jsonLeft = itemJson.size();
	int cnt = 0;
	while (jsonPtr && *jsonPtr) {
		Item item = db->NewItem(nsName);
		if (!item.Status().ok()) {
			http::HttpStatus httpStatus(item.Status());

			return jsonStatus(ctx, httpStatus);
		}
		char *prevPtr = 0;

		auto status = item.Unsafe().FromJSON(reindexer::string_view(jsonPtr, jsonLeft), &jsonPtr, mode == ModeDelete);
		jsonLeft -= (jsonPtr - prevPtr);

		if (!status.ok()) {
			http::HttpStatus httpStatus(status);

			return jsonStatus(ctx, httpStatus);
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
			http::HttpStatus httpStatus(status);

			return jsonStatus(ctx, httpStatus);
		}
		cnt += item.GetID() == -1 ? 0 : 1;
	}
	db->Commit(nsName);

	WrSerializer ser(ctx.writer->GetChunk());
	JsonBuilder builder(ser);
	builder.Put("updated", cnt);
	builder.Put("success", true);
	builder.End();

	return ctx.JSON(http::StatusOK, ser.DetachChunk());
}

int HTTPServer::queryResults(http::Context &ctx, reindexer::QueryResults &res, bool isQueryResults, unsigned limit, unsigned offset) {
	WrSerializer wrSer(ctx.writer->GetChunk());
	JsonBuilder builder(wrSer);

	auto iarray = builder.Array("items");

	for (size_t i = offset; i < res.Count() && i < offset + limit; i++) {
		iarray.Raw(nullptr, "");
		res[i].GetJSON(wrSer, false);
		if (i == offset) wrSer.Reserve(wrSer.Len() * (std::min(limit, unsigned(res.Count() - offset)) + 1));
	}
	iarray.End();

	if (!res.aggregationResults.empty()) {
		auto arrNode = builder.Array("aggregations");
		for (unsigned i = 0; i < res.aggregationResults.size(); i++) {
			arrNode.Raw(nullptr, "");
			res.aggregationResults[i].GetJSON(wrSer);
		}
	}

	if (!res.GetExplainResults().empty()) {
		builder.Raw("explain", res.GetExplainResults());
	}

	unsigned totalItems = isQueryResults ? res.Count() : static_cast<unsigned>(res.totalCount);

	if (!isQueryResults || limit != kDefaultLimit) {
		builder.Put("total_items", totalItems);
	}

	if (isQueryResults && res.totalCount) {
		builder.Put("query_total_items", res.totalCount);
	}
	builder.End();

	return ctx.JSON(http::StatusOK, wrSer.DetachChunk());
}

int HTTPServer::jsonStatus(http::Context &ctx, http::HttpStatus status) {
	WrSerializer ser(ctx.writer->GetChunk());
	JsonBuilder builder(ser);
	builder.Put("success", bool(status.code == http::StatusOK));
	builder.Put("response_code", int(status.code));
	builder.Put("description", status.what);
	builder.End();

	return ctx.JSON(status.code, ser.DetachChunk());
}

unsigned HTTPServer::prepareLimit(const string_view &limitParam, int limitDefault) {
	int limit = limitDefault;

	if (limitParam.length()) {
		limit = stoi(limitParam);
		if (limit < 0) limit = 0;
	}

	return static_cast<unsigned>(limit);
}

unsigned HTTPServer::prepareOffset(const string_view &offsetParam, int offsetDefault) {
	int offset = offsetDefault;

	if (offsetParam.length()) {
		offset = stoi(offsetParam);
		if (offset < 0) offset = 0;
	}

	return static_cast<unsigned>(offset);
}

shared_ptr<Reindexer> HTTPServer::getDB(http::Context &ctx, UserRole role) {
	(void)ctx;
	shared_ptr<Reindexer> db;

	string dbName = urldecode2(ctx.request->urlParams[0].ToString());

	AuthContext dummyCtx;

	AuthContext *actx = &dummyCtx;
	if (!dbMgr_.IsNoSecurity()) {
		auto clientData = dynamic_cast<HTTPClientData *>(ctx.clientData.get());
		assert(clientData);
		actx = &clientData->auth;
	}

	auto status = dbMgr_.OpenDatabase(dbName, *actx, false);
	if (!status.ok()) {
		throw http::HttpStatus(status);
	}

	status = actx->GetDB(role, &db);
	if (!status.ok()) {
		throw http::HttpStatus(status);
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
