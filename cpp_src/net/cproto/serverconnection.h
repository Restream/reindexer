#pragma once

#include <string.h>
#include "dispatcher.h"
#include "estl/atomic_unique_ptr.h"
#include "net/connection.h"
#include "net/iserverconnection.h"

namespace reindexer {
namespace net {
namespace cproto {

using reindexer::h_vector;

class ServerConnection : public ConnectionST, public IServerConnection, public Writer {
public:
	ServerConnection(int fd, ev::dynamic_loop &loop, Dispatcher &dispatcher, bool enableStat);
	~ServerConnection();

	// IServerConnection interface implementation
	static ConnectionFactory NewFactory(Dispatcher &dispatcher, bool enableStat) {
		return [&dispatcher, enableStat](ev::dynamic_loop &loop, int fd) { return new ServerConnection(fd, loop, dispatcher, enableStat); };
	}

	bool IsFinished() override final { return !sock_.valid(); }
	bool Restart(int fd) override final;
	void Detach() override final;
	void Attach(ev::dynamic_loop &loop) override final;

	// Writer iterface implementation
	void WriteRPCReturn(Context &ctx, const Args &args, const Error &status) override final { responceRPC(ctx, status, args); }
	void CallRPC(const IRPCCall & /*call*/) override final {}
	void SetClientData(std::unique_ptr<ClientData> data) override final { clientData_ = std::move(data); }
	ClientData *GetClientData() override final { return clientData_.get(); }
	std::shared_ptr<connection_stat> GetConnectionStat() override final {
		return ConnectionST::stats_ ? ConnectionST::stats_->get_stat() : std::shared_ptr<connection_stat>();
	}

protected:
	void onRead() override;
	void onClose() override;
	void handleRPC(Context &ctx);
	void responceRPC(Context &ctx, const Error &error, const Args &args);

	Dispatcher &dispatcher_;
	std::unique_ptr<ClientData> clientData_;
	// keep here to prevent allocs
	RPCCall call_ = {kCmdPing, 0, {}, std::chrono::milliseconds(0), lsn_t(), -1, IndexValueType::NotSet, false};

	bool enableSnappy_ = false;
};
}  // namespace cproto
}  // namespace net
}  // namespace reindexer
