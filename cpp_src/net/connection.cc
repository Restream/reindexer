#include "connection.h"
#include <errno.h>

namespace reindexer {
namespace net {

template <typename Mutex>
Connection<Mutex>::Connection(int fd, ev::dynamic_loop &loop, bool enableStat, size_t readBufSize, size_t writeBufSize)
	: sock_(fd), curEvents_(0), wrBuf_(writeBufSize), rdBuf_(readBufSize), stats_(enableStat ? new connection_stats_collector : nullptr) {
	attach(loop);
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
	assertrx(!sock_.valid());
	sock_ = fd;
	wrBuf_.clear();
	rdBuf_.clear();
	curEvents_ = 0;
	closeConn_ = false;
	if (stats_) stats_->restart();
}

template <typename Mutex>
void Connection<Mutex>::attach(ev::dynamic_loop &loop) {
	assertrx(!attached_);
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
	if (stats_) stats_->attach(loop);
	attached_ = true;
}

template <typename Mutex>
void Connection<Mutex>::detach() {
	assertrx(attached_);
	io_.stop();
	io_.reset();
	timeout_.stop();
	timeout_.reset();
	async_.stop();
	async_.reset();
	if (stats_) stats_->detach();
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
	if (stats_) stats_->stop();
	onClose();
	closeConn_ = false;
}

// Generic callback
template <typename Mutex>
void Connection<Mutex>::callback(ev::io & /*watcher*/, int revents) {
	if (ev::ERROR & revents) return;

	if (revents & ev::READ) {
		const auto res = read_cb();
		if (res == ReadResT::Rebalanced) {
			return;
		}
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

		if (stats_) stats_->update_write_stats(written, wrBuf_.data_size());

		if (written < toWrite) return;
	}
	if (closeConn_) {
		closeConn();
	}
}

// Receive message from client socket
template <typename Mutex>
typename Connection<Mutex>::ReadResT Connection<Mutex>::read_cb() {
	while (!closeConn_) {
		auto it = rdBuf_.head();
		ssize_t nread = sock_.recv(it);
		int err = sock_.last_error();

		if (nread < 0 && err == EINTR) continue;

		if ((nread < 0 && !socket::would_block(err)) || nread == 0) {
			closeConn();
			return ReadResT::Default;
		} else if (nread > 0) {
			if (stats_) stats_->update_read_stats(nread);
			rdBuf_.advance_head(nread);
			if (!closeConn_) {
				if (onRead() == ReadResT::Rebalanced) {
					return ReadResT::Rebalanced;
				}
			}
		}
		if (nread < ssize_t(it.size()) || !rdBuf_.available()) return ReadResT::Default;
	}
	return ReadResT::Default;
}
template <typename Mutex>
void Connection<Mutex>::timeout_cb(ev::periodic &watcher, int /*time*/) {
	if (sock_.has_pending_data()) {
		fprintf(stdout,
				"Connection got idle timeout, but socket has pending data. Do not dropping the connection\nThis probably means, that "
				"there are some very long queries in some of the connections, which may affect the other connections. Consider to use "
				"dedicated threads for them\n");
		if (!watcher.has_period()) {
			watcher.start(watcher.last_delay());
		}
		return;
	}

	fprintf(stderr, "Dropping RPC-connection on the idle timeout\n");
	closeConn();
}
template <typename Mutex>
void Connection<Mutex>::async_cb(ev::async &) {
	callback(io_, ev::WRITE);
}

template class Connection<std::mutex>;
template class Connection<reindexer::dummy_mutex>;
}  // namespace net
}  // namespace reindexer
