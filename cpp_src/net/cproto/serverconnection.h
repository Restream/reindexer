#pragma once

#include <string.h>
#include "dispatcher.h"
#include "net/connection.h"
#include "net/iserverconnection.h"

namespace reindexer {
namespace net {
namespace cproto {

using reindexer::h_vector;

class ServerConnection final : public ConnectionST, public IServerConnection, public Writer {
public:
	using BaseConnT = ConnectionST;

#ifdef REINDEX_WITH_V3_FOLLOWERS
	ServerConnection(socket &&s, ev::dynamic_loop &loop, Dispatcher &dispatcher, bool enableStat, size_t maxUpdatesSize,
					 bool enableCustomBalancing);
#else	// REINDEX_WITH_V3_FOLLOWERS
	ServerConnection(socket &&s, ev::dynamic_loop &loop, Dispatcher &dispatcher, bool enableStat, bool enableCustomBalancing);
#endif	// REINDEX_WITH_V3_FOLLOWERS
	~ServerConnection() override;

// IServerConnection interface implementation
#ifdef REINDEX_WITH_V3_FOLLOWERS
	static ConnectionFactory NewFactory(Dispatcher &dispatcher, bool enableStat, size_t maxUpdatesSize) {
		return [&dispatcher, enableStat, maxUpdatesSize](ev::dynamic_loop &loop, socket &&s, bool allowCustomBalancing) {
			return new ServerConnection(std::move(s), loop, dispatcher, enableStat, maxUpdatesSize, allowCustomBalancing);
		};
	}
#else	// REINDEX_WITH_V3_FOLLOWERS
	static ConnectionFactory NewFactory(Dispatcher &dispatcher, bool enableStat) {
		return [&dispatcher, enableStat](ev::dynamic_loop &loop, socket &&s, bool allowCustomBalancing) {
			return new ServerConnection(std::move(s), loop, dispatcher, enableStat, allowCustomBalancing);
		};
	}
#endif	// REINDEX_WITH_V3_FOLLOWERS

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
	void CallRPC(const IRPCCall & /*call*/) override final;
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
	void handleException(Context &ctx, const Error &err) noexcept;

#ifdef REINDEX_WITH_V3_FOLLOWERS
	void async_cb(ev::async &) { sendUpdates(); }
	void timeout_cb(ev::periodic &, int) { sendUpdates(); }
	void sendUpdates();
	std::vector<IRPCCall> updates_;
	size_t updatesSize_ = 0;
	bool updateLostFlag_ = false;
	const size_t maxUpdatesSize_;
	std::mutex updates_mtx_;
	ev::periodic updates_timeout_;
	ev::async updates_async_;
#endif	// REINDEX_WITH_V3_FOLLOWERS

	Dispatcher &dispatcher_;
	std::unique_ptr<ClientData> clientData_;
	// keep here to prevent allocs
	RPCCall call_ = {kCmdPing, 0, {}, std::chrono::milliseconds(0), lsn_t(), -1, ShardingKeyType::NotSetShard, false};

	bool enableSnappy_ = false;
	bool hasPendingData_ = false;
	BalancingType balancingType_ = BalancingType::NotSet;
	std::function<void(IServerConnection *, BalancingType)> rebalance_;
};

}  // namespace cproto
}  // namespace net
}  // namespace reindexer
