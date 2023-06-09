#include "rpcserver_fake.h"
#include <thread>
#include "net/cproto/cproto.h"
#include "net/cproto/serverconnection.h"
#include "net/listener.h"
#include "reindexer_version.h"

RPCServerFake::RPCServerFake(const RPCServerConfig &conf) : startTs_(std::chrono::system_clock::now()), conf_(conf), state_(Init) {}

Error RPCServerFake::Ping(cproto::Context &) {
	//
	return {};
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
		return {};
	}

	if (!clientData) {
		return Error(errForbidden, "You should login");
	}

	return {};
}

Error RPCServerFake::OpenNamespace(cproto::Context &, p_string) {
	std::this_thread::sleep_for(conf_.openNsDelay);
	return {};
}

Error RPCServerFake::DropNamespace(cproto::Context &, p_string) { return Error(errOK); }

Error RPCServerFake::Stop() {
	listener_->Stop();
	state_ = Stopped;
	if (int const openedQR = OpenedQRCount(); openedQR == 0) {
		return errOK;
	} else {
		return Error{errLogic, "There are %d opened QueryResults", openedQR};
	}
}

Error RPCServerFake::Select(cproto::Context &ctx, p_string /*query*/, int /*flags*/, int /*limit*/, p_string /*ptVersions*/) {
	static constexpr size_t kQueryResultsPoolSize = 1024;
	std::this_thread::sleep_for(conf_.selectDelay);
	int qrId;
	{
		std::lock_guard lock{qrMutex_};
		if (usedQrIds_.size() >= kQueryResultsPoolSize) {
			return Error{errLogic, "Too many parallel queries"};
		}
		if (unusedQrIds_.empty()) {
			qrId = usedQrIds_.size();
		} else {
			qrId = *unusedQrIds_.begin();
			unusedQrIds_.erase(qrId);
		}
		auto [it, inserted] = usedQrIds_.insert(qrId);
		assertrx(inserted);
		(void)it;
		(void)inserted;
		(void)it;
	}
	ctx.Return({cproto::Arg{p_string("")}, cproto::Arg{qrId}});
	return errOK;
}

Error RPCServerFake::CloseResults(cproto::Context &ctx, int reqId, std::optional<int64_t> /*qrUID*/, std::optional<bool> doNotReply) {
	if (doNotReply && *doNotReply) {
		ctx.respSent = true;
	}
	{
		std::lock_guard lock{qrMutex_};
		const auto it = usedQrIds_.find(reqId);
		if (it == usedQrIds_.end()) {
			return Error(errLogic, "ReqId %d not found", reqId);
		}
		unusedQrIds_.insert(*it);
		usedQrIds_.erase(it);
	}
	closeQRRequestsCounter_.fetch_add(1, std::memory_order_relaxed);
	return errOK;
}

size_t RPCServerFake::OpenedQRCount() {
	std::lock_guard lock{qrMutex_};
	return usedQrIds_.size();
}

bool RPCServerFake::Start(const std::string &addr, ev::dynamic_loop &loop, Error loginError) {
#ifndef _WIN32
	signal(SIGPIPE, SIG_IGN);
#endif

	dsn_ = addr;
	loginError_ = std::move(loginError);
	dispatcher_.Register(cproto::kCmdPing, this, &RPCServerFake::Ping);
	dispatcher_.Register(cproto::kCmdLogin, this, &RPCServerFake::Login);
	dispatcher_.Register(cproto::kCmdOpenNamespace, this, &RPCServerFake::OpenNamespace);
	dispatcher_.Register(cproto::kCmdDropNamespace, this, &RPCServerFake::DropNamespace);
	dispatcher_.Register(cproto::kCmdSelect, this, &RPCServerFake::Select);
	dispatcher_.Register(cproto::kCmdCloseResults, this, &RPCServerFake::CloseResults);

	dispatcher_.Middleware(this, &RPCServerFake::CheckAuth);

	listener_.reset(new Listener<ListenerType::Mixed>(loop, cproto::ServerConnection::NewFactory(dispatcher_, false, 1024 * 1024 * 1024)));
	return listener_->Bind(addr);
}

RPCServerStatus RPCServerFake::Status() const { return state_; }
