#pragma once

#include <string.h>
#include "dispatcher.h"
#include "net/connection.h"
#include "net/iserverconnection.h"
#include "replicator/updatesobserver.h"

namespace reindexer {
namespace net {
namespace cproto {

using reindexer::h_vector;

class ServerConnection : public ConnectionST, public IServerConnection, public Writer {
public:
	ServerConnection(int fd, ev::dynamic_loop &loop, Dispatcher &dispatcher);

	// IServerConnection interface implementation
	static ConnectionFactory NewFactory(Dispatcher &dispatcher) {
		return [&dispatcher](ev::dynamic_loop &loop, int fd) { return new ServerConnection(fd, loop, dispatcher); };
	}

	bool IsFinished() override final { return !sock_.valid(); }
	bool Restart(int fd) override final;
	void Detach() override final;
	void Attach(ev::dynamic_loop &loop) override final;

	// Writer iterface implementation
	void WriteRPCReturn(Context &ctx, const Args &args) override final { responceRPC(ctx, errOK, args); }
	void CallRPC(CmdCode cmd, const Args &args) override final;
	void SetClientData(ClientData::Ptr data) override final { clientData_ = data; }
	ClientData::Ptr GetClientData() override final { return clientData_; }

protected:
	void onRead() override;
	void onClose() override;
	void handleRPC(Context &ctx);
	chunk packRPC(chunk chunk, Context &ctx, const Error &status, const Args &args);
	void responceRPC(Context &ctx, const Error &error, const Args &args);
	void async_cb(ev::async &) { sendUpdates(); }
	void timeout_cb(ev::periodic &, int) { sendUpdates(); }
	void sendUpdates();

	Dispatcher &dispatcher_;
	ClientData::Ptr clientData_;
	// keep here to prevent allocs
	RPCCall call_;
	std::vector<chunk> updates_;
	std::mutex updates_mtx_;
	ev::periodic updates_timeout_;
	ev::async updates_async_;
};
}  // namespace cproto
}  // namespace net
}  // namespace reindexer
