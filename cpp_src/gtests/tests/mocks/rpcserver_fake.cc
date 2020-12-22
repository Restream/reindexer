#include "rpcserver_fake.h"
#include <thread>
#include "net/cproto/cproto.h"
#include "net/cproto/serverconnection.h"
#include "net/listener.h"
#include "reindexer_version.h"

using std::shared_ptr;

RPCServerFake::RPCServerFake(const RPCServerConfig &conf) : startTs_(std::chrono::system_clock::now()), conf_(conf), state_(Init) {}

RPCServerFake::~RPCServerFake() {}

Error RPCServerFake::Ping(cproto::Context &) {
	//
	return 0;
}

Error RPCServerFake::Login(cproto::Context &ctx, p_string /*login*/, p_string /*password*/, p_string /*db*/) {
	if (loginError_.ok()) {
		std::this_thread::sleep_for(conf_.loginDelay);
	}
	if (ctx.GetClientData()) {
		return Error(errParams, "Already logged in");
	}

	ctx.SetClientData(std::unique_ptr<RPCClientData>(new RPCClientData));
	int64_t startTs = std::chrono::duration_cast<std::chrono::seconds>(startTs_.time_since_epoch()).count();

	if (loginError_.ok()) {
		state_ = Connected;
		ctx.Return({cproto::Arg(p_string(REINDEX_VERSION)), cproto::Arg(startTs)});
	}

	return loginError_.code();
}

Error RPCServerFake::CheckAuth(cproto::Context &ctx) {
	auto clientData = dynamic_cast<RPCClientData *>(ctx.GetClientData());

	if (ctx.call->cmd == cproto::kCmdLogin || ctx.call->cmd == cproto::kCmdPing) {
		return 0;
	}

	if (!clientData) {
		return Error(errForbidden, "You should login");
	}

	return 0;
}

Error RPCServerFake::OpenNamespace(cproto::Context &, p_string) {
	std::this_thread::sleep_for(conf_.openNsDelay);
	return 0;
}

Error RPCServerFake::DropNamespace(cproto::Context &, p_string) { return Error(errOK); }

void RPCServerFake::Stop() {
	listener_->Stop();
	state_ = Stopped;
}

Error RPCServerFake::Select(cproto::Context & /*ctx*/, p_string /*query*/, int /*flags*/, int /*limit*/, p_string /*ptVersions*/) {
	std::this_thread::sleep_for(conf_.selectDelay);
	return 0;
}

bool RPCServerFake::Start(const string &addr, ev::dynamic_loop &loop, Error loginError) {
#ifndef _WIN32
	signal(SIGPIPE, SIG_IGN);
#endif

	dsn_ = addr;
	loginError_ = loginError;
	dispatcher_.Register(cproto::kCmdPing, this, &RPCServerFake::Ping);
	dispatcher_.Register(cproto::kCmdLogin, this, &RPCServerFake::Login);
	dispatcher_.Register(cproto::kCmdOpenNamespace, this, &RPCServerFake::OpenNamespace);
	dispatcher_.Register(cproto::kCmdDropNamespace, this, &RPCServerFake::DropNamespace);
	dispatcher_.Register(cproto::kCmdSelect, this, &RPCServerFake::Select);

	dispatcher_.Middleware(this, &RPCServerFake::CheckAuth);

	listener_.reset(new Listener(loop, cproto::ServerConnection::NewFactory(dispatcher_, false)));
	return listener_->Bind(addr);
}

RPCServerStatus RPCServerFake::Status() const { return state_; }
