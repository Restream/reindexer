#pragma once

#include "dispatcher.h"
#include "estl/mutex.h"
#include "net/connection.h"
#include "net/iserverconnection.h"

namespace reindexer::net::cproto {

using reindexer::h_vector;

class [[nodiscard]] ServerConnection final : public ConnectionST, public IServerConnection, public Writer {
public:
	using BaseConnT = ConnectionST;

	ServerConnection(socket&& s, ev::dynamic_loop& loop, Dispatcher& dispatcher, bool enableStat, bool enableCustomBalancing);
	~ServerConnection() override;

	static ConnectionFactory NewFactory(Dispatcher& dispatcher, bool enableStat) {
		return [&dispatcher, enableStat](ev::dynamic_loop& loop, socket&& s, bool allowCustomBalancing) {
			return new ServerConnection(std::move(s), loop, dispatcher, enableStat, allowCustomBalancing);
		};
	}

	// IServerConnection interface implementation
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
			std::ignore = onRead();
		}
		callback(BaseConnT::io_, ev::READ);
	}
	bool Restart(socket&& s) override;
	void Detach() override;
	void Attach(ev::dynamic_loop& loop) override;

	// Writer iterface implementation
	void WriteRPCReturn(Context& ctx, const Args& args, const Error& status) override { responseRPC(ctx, status, args); }

	void SetClientData(std::unique_ptr<ClientData>&& data) noexcept override { clientData_ = std::move(data); }
	ClientData* GetClientData() noexcept override final { return clientData_.get(); }
	std::shared_ptr<connection_stat> GetConnectionStat() noexcept override {
		return BaseConnT::stats_ ? BaseConnT::stats_->get_stat() : std::shared_ptr<connection_stat>();
	}
	size_t AvailableEventsSpace() noexcept override {
		int64_t available = int64_t(maxPendingUpdates_) - int64_t(BaseConnT::wrBuf_.size_atomic()) - int64_t(pendingUpdates());
		return available > 0 ? size_t(available) : 0;
	}
	void SendEvent(chunk&& ch) override;

protected:
	typename BaseConnT::ReadResT onRead() override;
	void onClose() override;
	void handleRPC(Context& ctx);
	void responseRPC(Context& ctx, const Error& error, const Args& args);
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

	reindexer::mutex updatesMtx_;
	ev::async updatesAsync_;
	const size_t maxPendingUpdates_;
	std::vector<chunk> updates_;
	std::atomic<int64_t> currentUpdatesCnt_ = {0};

	Dispatcher& dispatcher_;
	std::unique_ptr<ClientData> clientData_;
	// leave here to prevent memory allocation
	RPCCall call_ = {kCmdPing, 0, {}, std::chrono::milliseconds(0), lsn_t(), -1, ShardingKeyType::NotSetShard, false};

	bool enableSnappy_ = false;
	bool hasPendingData_ = false;
	BalancingType balancingType_ = BalancingType::NotSet;
	std::function<void(IServerConnection*, BalancingType)> rebalance_;
};

}  // namespace reindexer::net::cproto
