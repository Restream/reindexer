

#include "connection.h"
#include <errno.h>

namespace reindexer {
namespace net {

constexpr double kStatsUpdatePeriod = 1.0;

template <typename Mutex>
Connection<Mutex>::Connection(int fd, ev::dynamic_loop &loop, bool enableStat, size_t readBufSize, size_t writeBufSize)
	: sock_(fd), curEvents_(0), wrBuf_(writeBufSize), rdBuf_(readBufSize) {
	attach(loop);
	if (enableStat) {
		stat_ = std::make_shared<ConnectionStat>();
		statsUpdater_.start(kStatsUpdatePeriod, kStatsUpdatePeriod);
	}
}

template <typename Mutex>
Connection<Mutex>::~Connection() {
	if (sock_.valid()) {
		io_.stop();
		sock_.close();
	}
}

template <typename Mutex>
void Connection<Mutex>::restart(int fd) {
	assert(!sock_.valid());
	sock_ = fd;
	wrBuf_.clear();
	rdBuf_.clear();
	curEvents_ = 0;
	closeConn_ = false;
	if (stat_) {
		stat_.reset(new ConnectionStat());
		prevSecSentBytes_ = 0;
		prevSecRecvBytes_ = 0;
		statsUpdater_.start(kStatsUpdatePeriod, kStatsUpdatePeriod);
	}
}

template <typename Mutex>
void Connection<Mutex>::attach(ev::dynamic_loop &loop) {
	assert(!attached_);
	io_.set<Connection, &Connection::callback>(this);
	io_.set(loop);
	if (sock_.valid()) {
		if (curEvents_) io_.start(sock_.fd(), curEvents_);
		clientAddr_ = sock_.addr();
	}
	timeout_.set<Connection, &Connection::timeout_cb>(this);
	timeout_.set(loop);
	async_.set<Connection, &Connection::async_cb>(this);
	async_.set(loop);
	statsUpdater_.set<Connection, &Connection::stats_check_cb>(this);
	statsUpdater_.set(loop);
	attached_ = true;
}

template <typename Mutex>
void Connection<Mutex>::detach() {
	assert(attached_);
	io_.stop();
	io_.reset();
	timeout_.stop();
	timeout_.reset();
	async_.stop();
	async_.reset();
	statsUpdater_.stop();
	statsUpdater_.reset();
	attached_ = false;
}

template <typename Mutex>
void Connection<Mutex>::closeConn() {
	io_.loop.break_loop();
	if (sock_.valid()) {
		io_.stop();
		sock_.close();
	}
	timeout_.stop();
	async_.stop();
	statsUpdater_.stop();
	onClose();
	closeConn_ = false;
}

// Generic callback
template <typename Mutex>
void Connection<Mutex>::callback(ev::io & /*watcher*/, int revents) {
	if (ev::ERROR & revents) return;

	if (revents & ev::READ) {
		read_cb();
		revents |= ev::WRITE;
	}
	if (revents & ev::WRITE) {
		canWrite_ = true;
		write_cb();
	}

	int nevents = ev::READ | (wrBuf_.size() ? ev::WRITE : 0);

	if (curEvents_ != nevents && sock_.valid()) {
		(curEvents_) ? io_.set(nevents) : io_.start(sock_.fd(), nevents);
		curEvents_ = nevents;
	}
}

// Socket is writable
template <typename Mutex>
void Connection<Mutex>::write_cb() {
	while (wrBuf_.size()) {
		auto chunks = wrBuf_.tail();
		constexpr size_t kMaxWriteChuncks = 1024;
		if (chunks.size() > kMaxWriteChuncks) {
			chunks = chunks.subspan(0, kMaxWriteChuncks);
		}
		ssize_t written = sock_.send(chunks);
		int err = sock_.last_error();

		if (written < 0 && err == EINTR) continue;

		if (written < 0) {
			if (!socket::would_block(err)) {
				closeConn();
			}
			canWrite_ = false;
			return;
		}

		ssize_t toWrite = 0;
		for (auto &chunk : chunks) toWrite += chunk.size();
		wrBuf_.erase(written);

		if (stat_) {
			stat_->sentBytes.fetch_add(written, std::memory_order_relaxed);
			auto now = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch());
			stat_->lastSendTs.store(now.count(), std::memory_order_relaxed);
			stat_->sendBufBytes.store(wrBuf_.data_size(), std::memory_order_relaxed);
		}

		if (written < toWrite) return;
	}
	if (closeConn_) {
		closeConn();
	}
}

// Receive message from client socket
template <typename Mutex>
void Connection<Mutex>::read_cb() {
	while (!closeConn_) {
		auto it = rdBuf_.head();
		ssize_t nread = sock_.recv(it);
		int err = sock_.last_error();

		if (nread < 0 && err == EINTR) continue;

		if ((nread < 0 && !socket::would_block(err)) || nread == 0) {
			closeConn();
			return;
		} else if (nread > 0) {
			if (stat_) {
				stat_->recvBytes.fetch_add(nread, std::memory_order_relaxed);
				auto now = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch());
				stat_->lastRecvTs.store(now.count(), std::memory_order_relaxed);
			}
			rdBuf_.advance_head(nread);
			if (!closeConn_) onRead();
		}
		if (nread < ssize_t(it.size()) || !rdBuf_.available()) return;
	}
}
template <typename Mutex>
void Connection<Mutex>::timeout_cb(ev::periodic & /*watcher*/, int /*time*/) {
	closeConn();
}
template <typename Mutex>
void Connection<Mutex>::async_cb(ev::async &) {
	callback(io_, ev::WRITE);
}
template <typename Mutex>
void Connection<Mutex>::stats_check_cb(ev::periodic & /*watcher*/, int /*time*/) {
	assert(stat_);
	const uint64_t kAvgPeriod = 10;

	auto curRecvBytes = stat_->recvBytes.load(std::memory_order_relaxed);
	auto recvRate =
		prevSecRecvBytes_ == 0 ? uint32_t(curRecvBytes) : (stat_->recvRate.load(std::memory_order_relaxed) / kAvgPeriod) * (kAvgPeriod - 1);
	stat_->recvRate.store(recvRate + (curRecvBytes - prevSecRecvBytes_) / kAvgPeriod, std::memory_order_relaxed);
	prevSecRecvBytes_ = curRecvBytes;

	auto curSentBytes = stat_->sentBytes.load(std::memory_order_relaxed);
	auto sendRate =
		prevSecSentBytes_ == 0 ? uint32_t(curSentBytes) : (stat_->sendRate.load(std::memory_order_relaxed) / kAvgPeriod) * (kAvgPeriod - 1);
	stat_->sendRate.store(sendRate + (curSentBytes - prevSecSentBytes_) / kAvgPeriod, std::memory_order_relaxed);
	prevSecSentBytes_ = curSentBytes;
}

template class Connection<std::mutex>;
template class Connection<reindexer::dummy_mutex>;
}  // namespace net
}  // namespace reindexer
