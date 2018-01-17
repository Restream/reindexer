
#include "server.h"
#include <sys/stat.h>
#include <unistd.h>
#include "http/listener.h"
#include "tools/fsops.h"
#include "tools/serializer.h"

using std::string;

namespace reindexer_server {

Server::Server(shared_ptr<reindexer::Reindexer> db, const string &webRoot) : db_(db), webRoot_(webRoot) {}
Server::~Server() {}

int Server::GetQuery(http::Context &ctx) {
	reindexer::QueryResults res;
	const char *sqlQuery = nullptr;

	for (auto p : ctx.request->params) {
		if (!strcmp(p.name, "q")) sqlQuery = p.val;
	}

	if (!sqlQuery) {
		return ctx.String(http::StatusBadRequest, "Missed `q` parameter");
	}

	auto ret = db_->Select(sqlQuery, res);

	if (!ret.ok()) {
		return ctx.String(http::StatusInternalServerError, ret.what());
	}
	return queryResults(ctx, res, "items");
}

int Server::PostQuery(http::Context &ctx) {
	reindexer::QueryResults res;
	string dsl = ctx.body->Read();

	reindexer::Query q;
	auto status = q.ParseJson(dsl);
	if (!status.ok()) {
		return ctx.String(http::StatusInternalServerError, status.what());
	}

	status = db_->Select(q, res);
	if (!status.ok()) {
		return ctx.String(http::StatusInternalServerError, status.what());
	}
	return queryResults(ctx, res, "items");
}

enum { ModeUpdate, ModeInsert, ModeUpsert, ModeDelete };

int Server::DeleteItems(http::Context &ctx) { return modifyItem(ctx, ModeDelete); }
int Server::PutItems(http::Context &ctx) { return modifyItem(ctx, ModeUpdate); }
int Server::PostItems(http::Context &ctx) { return modifyItem(ctx, ModeInsert); }

int Server::PostNamespaces(http::Context &ctx) {
	string nsdefJson = ctx.body->Read();

	reindexer::NamespaceDef nsdef("");

	auto status = nsdef.Parse(&nsdefJson[0]);
	if (!status.ok()) {
		return ctx.String(http::StatusInternalServerError, status.what());
	}

	status = db_->OpenNamespace(nsdef);
	if (!status.ok()) {
		return ctx.String(http::StatusInternalServerError, status.what());
	}

	return ctx.JSON(http::StatusOK, "{\"status\":\"ok\"}");
}

int Server::GetNamespaces(http::Context &ctx) {
	reindexer::QueryResults res;
	auto status = db_->Select("DESCRIBE *", res);

	if (!status.ok()) {
		return ctx.String(http::StatusInternalServerError, status.what());
	}
	return queryResults(ctx, res, "namespaces");
}

int Server::DeleteNamespaces(http::Context &ctx) {
	const char *ns = nullptr;
	int drop = 0;
	for (auto p : ctx.request->params) {
		if (!strcmp(p.name, "ns")) ns = p.val;
	}
	for (auto p : ctx.request->params) {
		if (!strcmp(p.name, "drop")) drop = atoi(p.val);
	}

	if (!ns) {
		return ctx.String(http::StatusBadRequest, "Missed `ns` parameter");
	}

	auto status = drop ? db_->DropNamespace(ns) : db_->CloseNamespace(ns);

	if (!status.ok()) {
		return ctx.String(http::StatusBadRequest, status.what());
	}

	return ctx.JSON(http::StatusOK, "{\"status\":\"ok\"}");
}

int Server::Check(http::Context &ctx) { return ctx.String(http::StatusOK, "Hello world"); }
int Server::DocHandler(http::Context &ctx) {
	string path = ctx.request->path + 1;
	string target = webRoot_ + path;

	struct stat stat;
	int res = lstat(target.c_str(), &stat);
	if (res >= 0 && S_ISDIR(stat.st_mode)) {
		if (path.back() != '/') {
			return ctx.Redirect((path += '/').c_str());
		}

		target += "index.html";
	}

	return ctx.File(http::StatusOK, target.c_str());
}

bool Server::Start(int port) {
	ev::dynamic_loop loop;
	http::Router router;

	router.GET<Server, &Server::Check>("/check", this);
	router.GET<Server, &Server::GetQuery>("/query", this);
	router.POST<Server, &Server::PostQuery>("/query", this);

	router.PUT<Server, &Server::PutItems>("/items", this);
	router.POST<Server, &Server::PostItems>("/items", this);
	router.DELETE<Server, &Server::DeleteItems>("/items", this);

	router.GET<Server, &Server::GetNamespaces>("/namespaces", this);
	router.POST<Server, &Server::PostNamespaces>("/namespaces", this);
	router.DELETE<Server, &Server::DeleteNamespaces>("/namespaces", this);

	router.GET<Server, &Server::DocHandler>("/doc", this);
	router.GET<Server, &Server::DocHandler>("/face", this);

	pprof.Attach(router);

	http::Listener listener(loop, router);

	if (!listener.Bind(port)) {
		printf("Can't listen on %d port\n", port);
		return false;
	}

	listener.Run();
	printf("listener::Run exited\n");

	return true;
}

int Server::modifyItem(http::Context &ctx, int mode) {
	string itemJson = ctx.body->Read();

	const char *ns = nullptr;
	for (auto p : ctx.request->params) {
		if (!strcmp(p.name, "ns")) ns = p.val;
	}

	if (!ns) {
		return ctx.String(http::StatusBadRequest, "Missed `ns` parameter");
	}

	char *jsonPtr = &itemJson[0];
	size_t jsonLeft = itemJson.size();
	int cnt = 0;
	while (jsonPtr && *jsonPtr) {
		auto item = std::unique_ptr<reindexer::Item>(db_->NewItem(ns));
		if (!item->Status().ok()) {
			return ctx.String(http::StatusBadRequest, item->Status().what());
		}
		char *prevPtr = 0;
		auto status = item->FromJSON(reindexer::Slice(jsonPtr, jsonLeft), &jsonPtr);
		jsonLeft -= (jsonPtr - prevPtr);

		if (!status.ok()) {
			return ctx.String(http::StatusBadRequest, status.what());
		}

		switch (mode) {
			case ModeUpsert:
				status = db_->Upsert(ns, item.get());
				break;
			case ModeDelete:
				status = db_->Delete(ns, item.get());
				break;
			case ModeInsert:
				status = db_->Insert(ns, item.get());
				break;
			case ModeUpdate:
				status = db_->Update(ns, item.get());
				break;
		}

		if (!status.ok()) {
			return ctx.String(http::StatusBadRequest, status.what().c_str());
		}
		cnt += item->GetRef().id == -1 ? 0 : 1;
	}
	db_->Commit(ns);

	return ctx.Printf(http::StatusOK, "application/json; charset=utf-8", "{\"status\":\"ok\",\"updated\":%d}", cnt);
}

int Server::queryResults(http::Context &ctx, reindexer::QueryResults &res, const char *name) {
	ctx.writer->SetHeader(http::Header{"Content-Type", "application/json; charset=utf-8"});
	ctx.writer->SetRespCode(http::StatusOK);
	reindexer::WrSerializer wrSer(true);
	ctx.writer->Write("{\"", 2);
	ctx.writer->Write(name, strlen(name));
	ctx.writer->Write("\":[", 3);
	for (size_t i = 0; i < res.size(); i++) {
		wrSer.Reset();
		if (i != 0) ctx.writer->Write(",", 1);
		res.GetJSON(i, wrSer, false);
		ctx.writer->Write(wrSer.Buf(), wrSer.Len());
	}
	ctx.writer->Write("]}", 2);
	return 0;
}

}  // namespace reindexer_server
