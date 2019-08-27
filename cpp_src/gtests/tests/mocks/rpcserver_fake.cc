#include "rpcserver_fake.h"
#include <thread>
#include "net/cproto/cproto.h"
#include "net/cproto/serverconnection.h"
#include "net/listener.h"
#include "reindexer_version.h"

using std::shared_ptr;

RPCServerFake::RPCServerFake() : startTs_(std::chrono::system_clock::now()) {}

RPCServerFake::~RPCServerFake() {}

Error RPCServerFake::Ping(cproto::Context &) {
	//
	return 0;
}

Error RPCServerFake::Login(cproto::Context &ctx, p_string /*login*/, p_string /*password*/, p_string /*db*/) {
	std::this_thread::sleep_for(std::chrono::seconds(2));
	if (ctx.GetClientData()) {
		return Error(errParams, "Already logged in");
	}

	ctx.SetClientData(std::make_shared<RPCClientData>());
	int64_t startTs = std::chrono::duration_cast<std::chrono::seconds>(startTs_.time_since_epoch()).count();

	ctx.Return({cproto::Arg(p_string(REINDEX_VERSION)), cproto::Arg(startTs)});

	return 0;
}

Error RPCServerFake::CheckAuth(cproto::Context &ctx) {
	auto clientData = dynamic_cast<RPCClientData *>(ctx.GetClientData().get());

	if (ctx.call->cmd == cproto::kCmdLogin || ctx.call->cmd == cproto::kCmdPing) {
		return 0;
	}

	if (!clientData) {
		return Error(errForbidden, "You should login");
	}

	return 0;
}

Error RPCServerFake::OpenNamespace(cproto::Context &, p_string) {
	std::this_thread::sleep_for(std::chrono::seconds(2));
	return 0;
}

Error RPCServerFake::DropNamespace(cproto::Context &, p_string) { return Error(errOK); }

bool RPCServerFake::Start(const string &addr, ev::dynamic_loop &loop) {
	dispatcher_.Register(cproto::kCmdPing, this, &RPCServerFake::Ping);
	dispatcher_.Register(cproto::kCmdLogin, this, &RPCServerFake::Login);
	dispatcher_.Register(cproto::kCmdOpenNamespace, this, &RPCServerFake::OpenNamespace);
	dispatcher_.Register(cproto::kCmdDropNamespace, this, &RPCServerFake::DropNamespace);

	dispatcher_.Middleware(this, &RPCServerFake::CheckAuth);

	listener_.reset(new Listener(loop, cproto::ServerConnection::NewFactory(dispatcher_)));
	return listener_->Bind(addr);
}
