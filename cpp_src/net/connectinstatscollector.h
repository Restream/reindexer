#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include "net/ev/ev.h"
#include "tools/clock.h"
#include "tools/ssize_t.h"

namespace reindexer {
namespace net {

struct [[nodiscard]] connection_stat {
	connection_stat() noexcept {
		start_time = std::chrono::duration_cast<std::chrono::seconds>(system_clock_w::now_coarse().time_since_epoch()).count();
	}
	std::atomic_int_fast64_t recv_bytes{0};
	std::atomic_int_fast64_t last_recv_ts{0};
	std::atomic_int_fast64_t sent_bytes{0};
	std::atomic_int_fast64_t last_send_ts{0};
	std::atomic_int_fast64_t send_buf_bytes{0};
	std::atomic<uint32_t> send_rate{0};
	std::atomic<uint32_t> recv_rate{0};
	int64_t start_time{0};
};

class [[nodiscard]] connection_stats_collector {
public:
	connection_stats_collector() : stat_(std::make_shared<connection_stat>()) {}

	std::shared_ptr<connection_stat> get_stat() const noexcept { return stat_; }

	void attach(ev::dynamic_loop& loop) noexcept;
	void detach() noexcept;
	void restart();
	void stop() noexcept;
	void update_read_stats(ssize_t nread) noexcept {
		stat_->recv_bytes.fetch_add(nread, std::memory_order_relaxed);
		auto now = std::chrono::duration_cast<std::chrono::milliseconds>(system_clock_w::now_coarse().time_since_epoch());
		stat_->last_recv_ts.store(now.count(), std::memory_order_relaxed);
	}
	void update_write_stats(ssize_t written, size_t send_buf_size) noexcept {
		stat_->sent_bytes.fetch_add(written, std::memory_order_relaxed);
		auto now = std::chrono::duration_cast<std::chrono::milliseconds>(system_clock_w::now_coarse().time_since_epoch());
		stat_->last_send_ts.store(now.count(), std::memory_order_relaxed);
		stat_->send_buf_bytes.store(send_buf_size, std::memory_order_relaxed);
	}
	void update_send_buf_size(size_t size) noexcept { stat_->send_buf_bytes.store(size, std::memory_order_relaxed); }

protected:
	void stats_check_cb(ev::periodic& watcher, int) noexcept;

private:
	std::shared_ptr<connection_stat> stat_;
	ev::timer stats_update_timer_;
	int64_t prev_sec_sent_bytes_ = 0;
	int64_t prev_sec_recv_bytes_ = 0;
};

}  // namespace net
}  // namespace reindexer
