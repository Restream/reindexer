#pragma once

#include <memory>
#include "core/reindexer.h"
#include "net/cproto/dispatcher.h"
#include "net/listener.h"
#include "server/dbmanager.h"

using namespace reindexer_server;
using namespace reindexer::net;
using namespace reindexer;

struct RPCClientData : public cproto::ClientData {
	AuthContext auth;
	int connID;
};

class RPCServerFake {
public:
	RPCServerFake();
	~RPCServerFake();

	bool Start(const string &addr, ev::dynamic_loop &loop);
	void Stop() { listener_->Stop(); }

	Error Ping(cproto::Context &ctx);
	Error Login(cproto::Context &ctx, p_string login, p_string password, p_string db);
	Error OpenNamespace(cproto::Context &ctx, p_string ns);
	Error DropNamespace(cproto::Context &ctx, p_string ns);

	Error CheckAuth(cproto::Context &ctx);

protected:
	cproto::Dispatcher dispatcher_;
	std::unique_ptr<Listener> listener_;

	std::chrono::system_clock::time_point startTs_;
};
