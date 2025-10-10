#pragma once

#include <memory>
#include <set>
#include "core/reindexer.h"
#include "net/cproto/dispatcher.h"
#include "net/listener.h"
#include "server/dbmanager.h"

using namespace reindexer_server;
using namespace reindexer::net;
using namespace reindexer;

struct [[nodiscard]] RPCServerConfig {
	std::chrono::milliseconds loginDelay = std::chrono::milliseconds(2000);
	std::chrono::milliseconds openNsDelay = std::chrono::milliseconds(2000);
	std::chrono::milliseconds selectDelay = std::chrono::milliseconds(2000);
};

enum RPCServerStatus { Init, Connected, Stopped };

struct [[nodiscard]] RPCClientData final : public cproto::ClientData {
	AuthContext auth;
	int connID;
};

class [[nodiscard]] RPCServerFake {
public:
	RPCServerFake(const RPCServerConfig& conf);

	void Start(const std::string& addr, ev::dynamic_loop& loop, Error loginError);
	Error Stop();

	Error Ping(cproto::Context& ctx);
	Error Login(cproto::Context& ctx, p_string login, p_string password, p_string db);
	Error Select(cproto::Context& ctx, p_string query, int flags, int limit, p_string ptVersions);
	Error OpenNamespace(cproto::Context& ctx, p_string ns);
	Error DropNamespace(cproto::Context& ctx, p_string ns);
	Error CloseResults(cproto::Context& ctx, int reqId, std::optional<int64_t> qrUID, std::optional<bool> doNotReply);
	RPCServerStatus Status() const;

	Error CheckAuth(cproto::Context& ctx);
	size_t OpenedQRCount();
	size_t CloseQRRequestsCount() const { return closeQRRequestsCounter_.load(std::memory_order_relaxed); }

protected:
	cproto::Dispatcher dispatcher_;
	std::unique_ptr<IListener> listener_;

	system_clock_w::time_point startTs_;
	RPCServerConfig conf_;
	std::string dsn_;
	std::atomic<RPCServerStatus> state_;
	Error loginError_;
	reindexer::mutex qrMutex_;
	std::set<int> usedQrIds_;
	std::set<int> unusedQrIds_;
	std::atomic_size_t closeQRRequestsCounter_{0};
};
