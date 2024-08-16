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
	ServerConnection(socket&& s, ev::dynamic_loop& loop, Dispatcher& dispatcher, bool enableStat, size_t maxUpdatesSize,
					 bool enableCustomBalancing);
#else	// REINDEX_WITH_V3_FOLLOWERS
	ServerConnection(socket&& s, ev::dynamic_loop& loop, Dispatcher& dispatcher, bool enableStat, bool enableCustomBalancing);
#endif	// REINDEX_WITH_V3_FOLLOWERS
	~ServerConnection() override;

// IServerConnection interface implementation
#ifdef REINDEX_WITH_V3_FOLLOWERS
	static ConnectionFactory NewFactory(Dispatcher& dispatcher, bool enableStat, size_t maxUpdatesSize) {
		return [&dispatcher, enableStat, maxUpdatesSize](ev::dynamic_loop& loop, socket&& s, bool allowCustomBalancing) {
			return new ServerConnection(std::move(s), loop, dispatcher, enableStat, maxUpdatesSize, allowCustomBalancing);
		};
	}
#else	// REINDEX_WITH_V3_FOLLOWERS
	static ConnectionFactory NewFactory(Dispatcher& dispatcher, bool enableStat) {
		return [&dispatcher, enableStat](ev::dynamic_loop& loop, socket&& s, bool allowCustomBalancing) {
			return new ServerConnection(std::move(s), loop, dispatcher, enableStat, allowCustomBalancing);
		};
	}
#endif	// REINDEX_WITH_V3_FOLLOWERS

	bool IsFinished() const noexcept override { return !BaseConnT::sock_.valid(); }
	BalancingType GetBalancingType() const noexcept override { return balancingType_; }
	void SetRebalanceCallback(std::function<void(IServerConnection*, BalancingType)> cb) override {
		assertrx(!rebalance_);
		rebalance_ = std::move(cb);
	}
	bool HasPendingData() const noexcept override { return hasPendingData_; }
	void HandlePendingData() override {
		if (hasPendingData_) {
			hasPendingData_ = false;
			onRead();
		}
		callback(BaseConnT::io_, ev::READ);
	}
	bool Restart(socket&& s) override;
	void Detach() override;
	void Attach(ev::dynamic_loop& loop) override;

	// Writer iterface implementation
	void WriteRPCReturn(Context& ctx, const Args& args, const Error& status) override { responceRPC(ctx, status, args); }
	void CallRPC(const IRPCCall& /*call*/) override;
	void SetClientData(std::unique_ptr<ClientData>&& data) noexcept override { clientData_ = std::move(data); }
	ClientData* GetClientData() noexcept override final { return clientData_.get(); }
	std::shared_ptr<connection_stat> GetConnectionStat() noexcept override {
		return BaseConnT::stats_ ? BaseConnT::stats_->get_stat() : std::shared_ptr<connection_stat>();
	}
	size_t AvailableEventsSpace() noexcept override {
		auto available = int64_t(maxPendingUpdates_) - BaseConnT::wrBuf_.size_atomic() - pendingUpdates();
		return available > 0 ? size_t(available) : 0;
	}
	void SendEvent(chunk&& ch) override;

protected:
	typename BaseConnT::ReadResT onRead() override;
	void onClose() override;
	void handleRPC(Context& ctx);
	void responceRPC(Context& ctx, const Error& error, const Args& args);
	void handleException(Context& ctx, const Error& err) noexcept;
	void sendUpdates();
	void async_cb(ev::async&) { sendUpdates(); }
	size_t pendingUpdates() const noexcept { return currentUpdatesCnt_.load(std::memory_order_acquire); }
	void callback(ev::io& watcher, int revents) {
		BaseConnT::callback(watcher, revents);
		while (pendingUpdates() > 0 && canWrite_) {
			sendUpdates();
			write_cb();
		}
	}

#ifdef REINDEX_WITH_V3_FOLLOWERS
	void timeout_cb(ev::periodic&, int) { sendUpdatesV3(); }
	void sendUpdatesV3();
	std::vector<IRPCCall> updatesV3_;
	size_t updatesSize_ = 0;
	bool updateLostFlag_ = false;
	const size_t maxUpdatesSize_;
	ev::periodic updates_timeout_;
#endif	// REINDEX_WITH_V3_FOLLOWERS
	std::mutex updatesMtx_;
	ev::async updatesAsync_;
	const size_t maxPendingUpdates_;
	std::vector<chunk> updates_;
	std::atomic<int64_t> currentUpdatesCnt_ = {0};

	Dispatcher& dispatcher_;
	std::unique_ptr<ClientData> clientData_;
	// keep here to prevent allocs
	RPCCall call_ = {kCmdPing, 0, {}, std::chrono::milliseconds(0), lsn_t(), -1, ShardingKeyType::NotSetShard, false};

	bool enableSnappy_ = false;
	bool hasPendingData_ = false;
	BalancingType balancingType_ = BalancingType::NotSet;
	std::function<void(IServerConnection*, BalancingType)> rebalance_;
};

}  // namespace cproto
}  // namespace net
}  // namespace reindexer
