

#include "connection.h"
#include <errno.h>

namespace reindexer {
namespace net {

template <typename Mutex>
Connection<Mutex>::Connection(int fd, ev::dynamic_loop &loop, size_t readBufSize, size_t writeBufSize)
	: sock_(fd), curEvents_(0), wrBuf_(writeBufSize), rdBuf_(readBufSize) {
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
	assert(!sock_.valid());
	sock_ = fd;
	wrBuf_.clear();
	rdBuf_.clear();
	curEvents_ = 0;
	closeConn_ = false;
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

template class Connection<std::mutex>;
template class Connection<reindexer::dummy_mutex>;
}  // namespace net
}  // namespace reindexer
