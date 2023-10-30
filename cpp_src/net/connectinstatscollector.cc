#include "connectinstatscollector.h"
#include <assert.h>

namespace reindexer {
namespace net {

constexpr double k_stats_update_period = 1.0;

void connection_stats_collector::attach(ev::dynamic_loop& loop) noexcept {
	stats_update_timer_.set<connection_stats_collector, &connection_stats_collector::stats_check_cb>(this);
	stats_update_timer_.set(loop);
	stats_update_timer_.start(k_stats_update_period, k_stats_update_period);
}

void connection_stats_collector::detach() noexcept {
	stats_update_timer_.stop();
	stats_update_timer_.reset();
}

void connection_stats_collector::restart() {
	stat_.reset(new connection_stat());
	prev_sec_sent_bytes_ = 0;
	prev_sec_recv_bytes_ = 0;
	stats_update_timer_.start(k_stats_update_period, k_stats_update_period);
}

void connection_stats_collector::stop() noexcept { stats_update_timer_.stop(); }

void connection_stats_collector::stats_check_cb(ev::periodic&, int) noexcept {
	assertrx(stat_);
	const uint64_t kAvgPeriod = 10;

	auto curRecvBytes = stat_->recv_bytes.load(std::memory_order_relaxed);
	auto recvRate = prev_sec_recv_bytes_ == 0 ? uint32_t(curRecvBytes)
											  : (stat_->recv_rate.load(std::memory_order_relaxed) / kAvgPeriod) * (kAvgPeriod - 1);
	stat_->recv_rate.store(recvRate + (curRecvBytes - prev_sec_recv_bytes_) / kAvgPeriod, std::memory_order_relaxed);
	prev_sec_recv_bytes_ = curRecvBytes;

	auto curSentBytes = stat_->sent_bytes.load(std::memory_order_relaxed);
	auto sendRate = prev_sec_sent_bytes_ == 0 ? uint32_t(curSentBytes)
											  : (stat_->send_rate.load(std::memory_order_relaxed) / kAvgPeriod) * (kAvgPeriod - 1);
	stat_->send_rate.store(sendRate + (curSentBytes - prev_sec_sent_bytes_) / kAvgPeriod, std::memory_order_relaxed);
	prev_sec_sent_bytes_ = curSentBytes;
}

}  // namespace net
}  // namespace reindexer
