

#include "connection.h"
#include <errno.h>

namespace reindexer {
namespace net {

template <typename Mutex>
Connection<Mutex>::Connection(int fd, ev::dynamic_loop &loop, size_t readBufSize, size_t writeBufSize)
	: sock_(fd), curEvents_(0), wrBuf_(writeBufSize), rdBuf_(readBufSize) {
	io_.set<Connection, &Connection::callback>(this);
	io_.set(loop);
	timeout_.set<Connection, &Connection::timeout_cb>(this);
	timeout_.set(loop);
	async_.set<Connection, &Connection::async_cb>(this);
	async_.set(loop);
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
}

template <typename Mutex>
void Connection<Mutex>::reatach(ev::dynamic_loop &loop) {
	io_.stop();
	io_.set<Connection, &Connection::callback>(this);
	io_.set(loop);
	io_.start(sock_.fd(), curEvents_);
	timeout_.stop();
	timeout_.set<Connection, &Connection::timeout_cb>(this);
	timeout_.set(loop);
}

template <typename Mutex>
void Connection<Mutex>::closeConn() {
	io_.loop.break_loop();

	if (sock_.valid()) {
		io_.stop();
		sock_.close();
	}
	timeout_.stop();
	onClose();
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
		write_cb();
	}

	wrBufLock_.lock();

	if (!wrBuf_.size()) wrBuf_.clear();
	int nevents = ev::READ | (wrBuf_.size() ? ev::WRITE : 0);

	wrBufLock_.unlock();

	if (curEvents_ != nevents && sock_.valid()) {
		(curEvents_) ? io_.set(nevents) : io_.start(sock_.fd(), nevents);
		curEvents_ = nevents;
	}
}

// Socket is writable
template <typename Mutex>
void Connection<Mutex>::write_cb() {
	while (wrBuf_.size()) {
		wrBufLock_.lock();

		auto it = wrBuf_.tail();

		wrBufLock_.unlock();

		ssize_t written = sock_.send(it.data, it.len);
		int err = sock_.last_error();

		if (written < 0 && err == EINTR) continue;

		if (written < 0) {
			if (!socket::would_block(err)) {
				closeConn();
			}
			return;
		}

		wrBufLock_.lock();
		wrBuf_.erase(written);
		wrBufLock_.unlock();

		if (written < ssize_t(it.len)) return;
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
		ssize_t nread = sock_.recv(it.data, it.len);
		int err = sock_.last_error();

		if (nread < 0 && err == EINTR) continue;

		if ((nread < 0 && !socket::would_block(err)) || nread == 0) {
			closeConn();
			return;
		} else if (nread > 0) {
			rdBuf_.advance_head(nread);
			if (!closeConn_) onRead();
		}
		if (nread < ssize_t(it.len) || !rdBuf_.available()) return;
	}
}

template <typename Mutex>
void Connection<Mutex>::timeout_cb(ev::periodic & /*watcher*/, int /*time*/) {
	closeConn();
}

template <typename Mutex>
void Connection<Mutex>::async_cb(ev::async &) {
	write_cb();
}

template class Connection<std::mutex>;
template class Connection<reindexer::dummy_mutex>;

}  // namespace net
}  // namespace reindexer
