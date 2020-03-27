#pragma once

#include <memory>
#include "core/reindexer.h"
#include "net/cproto/dispatcher.h"
#include "net/listener.h"
#include "server/dbmanager.h"

using namespace reindexer_server;
using namespace reindexer::net;
using namespace reindexer;

struct RPCServerConfig {
	std::chrono::milliseconds loginDelay = std::chrono::milliseconds(2000);
	std::chrono::milliseconds openNsDelay = std::chrono::milliseconds(2000);
	std::chrono::milliseconds selectDelay = std::chrono::milliseconds(2000);
};

enum RPCServerStatus { Init, Connected, Stopped };

struct RPCClientData : public cproto::ClientData {
	AuthContext auth;
	int connID;
};

class RPCServerFake {
public:
	RPCServerFake(const RPCServerConfig &conf);
	~RPCServerFake();

	bool Start(const string &addr, ev::dynamic_loop &loop, Error loginError);
	void Stop();

	Error Ping(cproto::Context &ctx);
	Error Login(cproto::Context &ctx, p_string login, p_string password, p_string db);
	Error Select(cproto::Context &ctx, p_string query, int flags, int limit, p_string ptVersions);
	Error OpenNamespace(cproto::Context &ctx, p_string ns);
	Error DropNamespace(cproto::Context &ctx, p_string ns);
	RPCServerStatus Status() const;

	Error CheckAuth(cproto::Context &ctx);

protected:
	cproto::Dispatcher dispatcher_;
	std::unique_ptr<Listener> listener_;

	std::chrono::system_clock::time_point startTs_;
	RPCServerConfig conf_;
	string dsn_;
	std::atomic<RPCServerStatus> state_;
	Error loginError_;
};
