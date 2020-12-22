#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include "net/ev/ev.h"
#include "tools/ssize_t.h"

namespace reindexer {
namespace net {

struct connection_stat {
	connection_stat() {
		start_time = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
	}
	std::atomic_int_fast64_t recv_bytes{0};
	std::atomic_int_fast64_t last_recv_ts{0};
	std::atomic_int_fast64_t sent_bytes{0};
	std::atomic_int_fast64_t last_send_ts{0};
	std::atomic_int_fast64_t send_buf_bytes{0};
	std::atomic_int_fast64_t pended_updates{0};
	std::atomic<uint32_t> send_rate{0};
	std::atomic<uint32_t> recv_rate{0};
	int64_t start_time{0};
};

class connection_stats_collector {
public:
	connection_stats_collector();

	std::shared_ptr<connection_stat> get_stat() noexcept { return stat_; }

	void attach(ev::dynamic_loop &loop) noexcept;
	void detach() noexcept;
	void restart();
	void stop() noexcept;
	void update_read_stats(ssize_t nread) noexcept;
	void update_write_stats(ssize_t written, size_t send_buf_size) noexcept;
	void update_pended_updates(size_t) noexcept;
	void update_send_buf_size(size_t) noexcept;

protected:
	void stats_check_cb(ev::periodic &watcher, int) noexcept;

private:
	std::shared_ptr<connection_stat> stat_;
	ev::timer stats_update_timer_;
	int64_t prev_sec_sent_bytes_ = 0;
	int64_t prev_sec_recv_bytes_ = 0;
};

}  // namespace net
}  // namespace reindexer
