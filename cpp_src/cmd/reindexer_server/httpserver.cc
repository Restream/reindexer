
#include "httpserver.h"
#include <sys/stat.h>
#include <unistd.h>
#include <sstream>
#include "net/http/connection.h"
#include "net/listener.h"
#include "tools/fsops.h"
#include "tools/serializer.h"
#include "tools/stringstools.h"

using std::string;
using std::stringstream;
using std::to_string;

namespace reindexer_server {

HTTPServer::HTTPServer(shared_ptr<reindexer::Reindexer> db, const string &webRoot) : db_(db), webRoot_(reindexer::JoinPath(webRoot, "")) {}
HTTPServer::~HTTPServer() {}

enum { ModeUpdate, ModeInsert, ModeUpsert, ModeDelete };

int HTTPServer::GetQuery(http::Context &ctx) {
	reindexer::QueryResults res;
	const char *sqlQuery = nullptr;

	for (auto p : ctx.request->params) {
		if (!strcmp(p.name, "q")) sqlQuery = p.val;
	}

	if (!sqlQuery) {
		return jsonStatus(ctx, false, http::StatusBadRequest, "Missed `q` parameter");
	}

	auto ret = db_->Select(sqlQuery, res);

	if (!ret.ok()) {
		return jsonStatus(ctx, false, http::StatusInternalServerError, ret.what());
	}
	return queryResults(ctx, res, "items");
}

int HTTPServer::PostQuery(http::Context &ctx) {
	reindexer::QueryResults res;
	string dsl = ctx.body->Read();

	reindexer::Query q;
	auto status = q.ParseJson(dsl);
	if (!status.ok()) {
		return jsonStatus(ctx, false, http::StatusInternalServerError, status.what());
	}

	status = db_->Select(q, res);
	if (!status.ok()) {
		return jsonStatus(ctx, false, http::StatusInternalServerError, status.what());
	}
	return queryResults(ctx, res, "items");
}

int HTTPServer::GetNamespaces(http::Context &ctx) {
	ctx.writer->SetHeader(http::Header{"Content-Type", "application/json; charset=utf-8"});
	ctx.writer->SetRespCode(http::StatusOK);

	vector<reindexer::NamespaceDef> nsDefs;
	db_->EnumNamespaces(nsDefs, false);

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
	if (ctx.request->urlParams[0] == NULL) {
		return jsonStatus(ctx, false, http::StatusBadRequest, "Namespace is not specified");
	}

	ctx.writer->SetHeader(http::Header{"Content-Type", "application/json; charset=utf-8"});
	ctx.writer->SetRespCode(http::StatusOK);

	vector<reindexer::NamespaceDef> nsDefs;
	db_->EnumNamespaces(nsDefs, false);

	reindexer::NamespaceDef *nsDefPtr = nullptr;
	for (auto &nsdef : nsDefs) {
		if (nsdef.name == ctx.request->urlParams[0]) {
			nsDefPtr = &nsdef;
		}
	}

	if (!nsDefPtr) {
		return jsonStatus(ctx, false, http::StatusNotFound, "Namespace is not found");
	}

	reindexer::NamespaceDef &nsDef = *nsDefPtr;

	reindexer::WrSerializer wrSer(true);
	nsDef.Print(wrSer);
	ctx.writer->Write(wrSer.Buf(), wrSer.Len());

	return 0;
}

int HTTPServer::PostNamespace(http::Context &ctx) {
	string nsdefJson = ctx.body->Read();
	reindexer::NamespaceDef nsdef("");

	auto status = nsdef.Parse(const_cast<char *>(nsdefJson.c_str()));
	if (!status.ok()) {
		return jsonStatus(ctx, false, http::StatusInternalServerError, status.what());
	}

	status = db_->AddNamespace(nsdef);
	if (!status.ok()) {
		return jsonStatus(ctx, false, http::StatusInternalServerError, status.what());
	}

	return jsonStatus(ctx);
}

int HTTPServer::DeleteNamespace(http::Context &ctx) {
	if (ctx.request->urlParams[0] == NULL) {
		return jsonStatus(ctx, false, http::StatusNotFound, "Namespace is not found");
	}

	auto status = db_->DropNamespace(ctx.request->urlParams[0]);
	if (!status.ok()) {
		return jsonStatus(ctx, false, http::StatusInternalServerError, status.what());
	}

	return jsonStatus(ctx);
}

int HTTPServer::GetItems(http::Context &ctx) {
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

	if (ctx.request->urlParams[0] == NULL) {
		return jsonStatus(ctx, false, http::StatusBadRequest, "Namespace is required");
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

	auto query = reindexer::Query(ctx.request->urlParams[0], offset, limit, ModeAccurateTotal);
	if (sortField) {
		bool isSortDesc = !strcmp(sortOrder, "desc");
		query = query.Sort(sortField, isSortDesc);
	}

	reindexer::QueryResults res;
	auto status = db_->Select(query, res);
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
	if (ctx.request->urlParams[0] == NULL) {
		return jsonStatus(ctx, false, http::StatusBadRequest, "Namespace is not specified");
	}

	ctx.writer->SetHeader(http::Header{"Content-Type", "application/json; charset=utf-8"});
	ctx.writer->SetRespCode(http::StatusOK);

	vector<reindexer::NamespaceDef> nsDefs;
	db_->EnumNamespaces(nsDefs, false);

	reindexer::NamespaceDef *nsDefPtr = nullptr;
	for (auto &nsdef : nsDefs) {
		if (nsdef.name == ctx.request->urlParams[0]) {
			nsDefPtr = &nsdef;
		}
	}

	if (!nsDefPtr) {
		return jsonStatus(ctx, false, http::StatusNotFound, "Namespace is not found");
	}

	reindexer::NamespaceDef &nsDef = *nsDefPtr;

	ctx.writer->Write("{");

	reindexer::WrSerializer wrSer(true);
	nsDef.PrintIndexes(wrSer, "indexes");
	ctx.writer->Write(wrSer.Buf(), wrSer.Len());

	ctx.writer->Write(",\"total_items\":");
	string total = to_string(nsDef.indexes.size());
	ctx.writer->Write(total.c_str(), total.length());
	ctx.writer->Write("}");

	return 0;
}

int HTTPServer::PostIndex(http::Context &ctx) {
	if (ctx.request->urlParams[0] == NULL) {
		return jsonStatus(ctx, false, http::StatusNotFound, "Namespace is not found");
	}

	string json = ctx.body->Read();
	reindexer::IndexDef idxDef;
	idxDef.Parse(&json[0]);

	auto status = db_->AddIndex(ctx.request->urlParams[0], idxDef);
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

bool HTTPServer::Start(int port) {
	ev::dynamic_loop loop;
	http::Router router;

	router.NotFound<HTTPServer, &HTTPServer::NotFoundHandler>(this);

	router.GET<HTTPServer, &HTTPServer::DocHandler>("/swagger", this);
	router.GET<HTTPServer, &HTTPServer::DocHandler>("/swagger/*", this);
	router.GET<HTTPServer, &HTTPServer::DocHandler>("/face", this);
	router.GET<HTTPServer, &HTTPServer::DocHandler>("/face/*", this);

	router.GET<HTTPServer, &HTTPServer::Check>("/api/v1/check", this);
	router.GET<HTTPServer, &HTTPServer::GetQuery>("/api/v1/query", this);
	router.POST<HTTPServer, &HTTPServer::PostQuery>("/api/v1/query", this);

	router.GET<HTTPServer, &HTTPServer::GetNamespaces>("/api/v1/namespaces", this);
	router.GET<HTTPServer, &HTTPServer::GetNamespace>("/api/v1/namespaces/:name", this);
	router.POST<HTTPServer, &HTTPServer::PostNamespace>("/api/v1/namespaces", this);
	router.DELETE<HTTPServer, &HTTPServer::DeleteNamespace>("/api/v1/namespaces/:name", this);

	router.GET<HTTPServer, &HTTPServer::GetItems>("/api/v1/namespaces/:namespace/items", this);
	router.PUT<HTTPServer, &HTTPServer::PutItems>("/api/v1/namespaces/:namespace/items", this);
	router.POST<HTTPServer, &HTTPServer::PostItems>("/api/v1/namespaces/:namespace/items", this);
	router.DELETE<HTTPServer, &HTTPServer::DeleteItems>("/api/v1/namespaces/:namespace/items", this);

	router.GET<HTTPServer, &HTTPServer::GetIndexes>("/api/v1/namespaces/:namespace/indexes", this);
	router.POST<HTTPServer, &HTTPServer::PostIndex>("/api/v1/namespaces/:namespace/indexes", this);

	pprof.Attach(router);

	Listener listener(loop, http::Connection::NewFactory(router));

	if (!listener.Bind(port)) {
		printf("Can't listen on %d port\n", port);
		return false;
	}

	listener.Run();
	printf("listener::Run exited\n");

	return true;
}

int HTTPServer::modifyItem(http::Context &ctx, int mode) {
	string itemJson = ctx.body->Read();

	if (ctx.request->urlParams[0] == NULL) {
		return jsonStatus(ctx, false, http::StatusBadRequest, "Namespace is required");
	}

	char *jsonPtr = &itemJson[0];
	size_t jsonLeft = itemJson.size();
	int cnt = 0;
	while (jsonPtr && *jsonPtr) {
		auto item = std::unique_ptr<reindexer::Item>(db_->NewItem(ctx.request->urlParams[0]));
		if (!item->Status().ok()) {
			return jsonStatus(ctx, false, http::StatusInternalServerError, item->Status().what());
		}
		char *prevPtr = 0;
		auto status = item->FromJSON(reindexer::Slice(jsonPtr, jsonLeft), &jsonPtr);
		jsonLeft -= (jsonPtr - prevPtr);

		if (!status.ok()) {
			return jsonStatus(ctx, false, http::StatusInternalServerError, status.what());
		}

		switch (mode) {
			case ModeUpsert:
				status = db_->Upsert(ctx.request->urlParams[0], item.get());
				break;
			case ModeDelete:
				status = db_->Delete(ctx.request->urlParams[0], item.get());
				break;
			case ModeInsert:
				status = db_->Insert(ctx.request->urlParams[0], item.get());
				break;
			case ModeUpdate:
				status = db_->Update(ctx.request->urlParams[0], item.get());
				break;
		}

		if (!status.ok()) {
			return jsonStatus(ctx, false, http::StatusInternalServerError, status.what());
		}
		cnt += item->GetRef().id == -1 ? 0 : 1;
	}
	db_->Commit(ctx.request->urlParams[0]);

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

}  // namespace reindexer_server
