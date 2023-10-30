#pragma once

#include <string.h>
#include "dispatcher.h"
#include "estl/atomic_unique_ptr.h"
#include "net/connection.h"
#include "net/iserverconnection.h"
#include "replicator/updatesobserver.h"
#include "tools/assertrx.h"

namespace reindexer {
namespace net {
namespace cproto {

using reindexer::h_vector;

class ServerConnection final : public ConnectionST, public IServerConnection, public Writer {
public:
	using BaseConnT = ConnectionST;

	ServerConnection(socket &&s, ev::dynamic_loop &loop, Dispatcher &dispatcher, bool enableStat, size_t maxUpdatesSize,
					 bool enableCustomBalancing);
	~ServerConnection() override;

	// IServerConnection interface implementation
	static ConnectionFactory NewFactory(Dispatcher &dispatcher, bool enableStat, size_t maxUpdatesSize) {
		return [&dispatcher, enableStat, maxUpdatesSize](ev::dynamic_loop &loop, socket &&s, bool allowCustomBalancing) {
			return new ServerConnection(std::move(s), loop, dispatcher, enableStat, maxUpdatesSize, allowCustomBalancing);
		};
	}

	bool IsFinished() const noexcept override final { return !BaseConnT::sock_.valid(); }
	BalancingType GetBalancingType() const noexcept override final { return balancingType_; }
	void SetRebalanceCallback(std::function<void(IServerConnection *, BalancingType)> cb) override final {
		assertrx(!rebalance_);
		rebalance_ = std::move(cb);
	}
	bool HasPendingData() const noexcept override final { return hasPendingData_; }
	void HandlePendingData() override final {
		if (hasPendingData_) {
			hasPendingData_ = false;
			onRead();
		}
		BaseConnT::callback(BaseConnT::io_, ev::READ);
	}
	bool Restart(socket &&s) override final;
	void Detach() override final;
	void Attach(ev::dynamic_loop &loop) override final;

	// Writer iterface implementation
	void WriteRPCReturn(Context &ctx, const Args &args, const Error &status) override final { responceRPC(ctx, status, args); }
	void CallRPC(const IRPCCall &call) override final;
	void SetClientData(std::unique_ptr<ClientData> &&data) noexcept override final { clientData_ = std::move(data); }
	ClientData *GetClientData() noexcept override final { return clientData_.get(); }
	std::shared_ptr<connection_stat> GetConnectionStat() noexcept override final {
		return BaseConnT::stats_ ? BaseConnT::stats_->get_stat() : std::shared_ptr<connection_stat>();
	}

protected:
	typename BaseConnT::ReadResT onRead() override;
	void onClose() override;
	void handleRPC(Context &ctx);
	void responceRPC(Context &ctx, const Error &error, const Args &args);
	void async_cb(ev::async &) { sendUpdates(); }
	void timeout_cb(ev::periodic &, int) { sendUpdates(); }
	void sendUpdates();
	void handleException(Context &ctx, const Error &err) noexcept;

	Dispatcher &dispatcher_;
	std::unique_ptr<ClientData> clientData_;
	// keep here to prevent allocs
	RPCCall call_;

	std::vector<IRPCCall> updates_;
	size_t updatesSize_;
	bool updateLostFlag_;
	const size_t maxUpdatesSize_;
	std::mutex updates_mtx_;

	ev::periodic updates_timeout_;
	ev::async updates_async_;
	bool enableSnappy_;
	bool hasPendingData_ = false;
	BalancingType balancingType_ = BalancingType::NotSet;
	std::function<void(IServerConnection *, BalancingType)> rebalance_;
};

}  // namespace cproto
}  // namespace net
}  // namespace reindexer
