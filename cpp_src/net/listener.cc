#include "listener.h"
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <unistd.h>
#include <thread>
#include "core/type_consts.h"
#include "net/http/connection.h"
#include "tools/logger.h"

#ifndef SOL_TCP
#define SOL_TCP IPPROTO_TCP
#endif

namespace reindexer {

namespace net {

static atomic<int> counter_;

Listener::Listener(ev::dynamic_loop &loop, std::shared_ptr<Shared> shared) : loop_(loop), shared_(shared), idleConns_(0), id_(counter_++) {
	io_.set<Listener, &Listener::io_accept>(this);
	io_.set(loop);
	timer_.set<Listener, &Listener::timeout_cb>(this);
	timer_.set(loop);
	timer_.start(15., 15.);
	std::lock_guard<std::mutex> lck(shared_->lck_);
	shared_->listeners_.push_back(this);
}

Listener::Listener(ev::dynamic_loop &loop, ConnectionFactory connFactory, int maxListeners)
	: Listener(loop, std::make_shared<Shared>(connFactory, maxListeners ? maxListeners : std::thread::hardware_concurrency())) {}

Listener::~Listener() {
	io_.stop();
	std::lock_guard<std::mutex> lck(shared_->lck_);
	auto it = std::find(shared_->listeners_.begin(), shared_->listeners_.end(), this);
	assert(it != shared_->listeners_.end());
	shared_->listeners_.erase(it);
	shared_->count_--;
}

bool Listener::Bind(int port) {
	if (shared_->fd_ >= 0) {
		return false;
	}

	if ((shared_->fd_ = socket(PF_INET, SOCK_STREAM, 0)) < 0) {
		perror("socket error");
		return false;
	}

	int enable = 1;
	if (setsockopt(shared_->fd_, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(int)) < 0) {
		perror("setsockopt(SO_REUSEADDR) failed");
	}
	if (setsockopt(shared_->fd_, SOL_SOCKET, SO_REUSEPORT, &enable, sizeof(int)) < 0) {
		perror("setsockopt(SO_REUSEPORT) failed");
	}
	if (setsockopt(shared_->fd_, SOL_TCP, TCP_NODELAY, &enable, sizeof(int)) < 0) {
		perror("setsockopt(TCP_NODELAY) failed");
	}
#ifndef __APPLE__
	if (setsockopt(shared_->fd_, SOL_TCP, TCP_DEFER_ACCEPT, &enable, sizeof(int)) < 0) {
		perror("setsockopt(TCP_DEFER_ACCEPT) failed");
	}
	if (setsockopt(shared_->fd_, SOL_TCP, TCP_QUICKACK, &enable, sizeof(int)) < 0) {
		perror("setsockopt(TCP_QUICKACK) failed");
	}
#endif
	struct sockaddr_in addr;
	bzero(&addr, sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_port = htons(port);
	addr.sin_addr.s_addr = INADDR_ANY;

	fcntl(shared_->fd_, F_SETFL, fcntl(shared_->fd_, F_GETFL, 0) | O_NONBLOCK);

	if (bind(shared_->fd_, reinterpret_cast<struct sockaddr *>(&addr), sizeof(addr)) != 0) {
		perror("bind error");
		return false;
	}

	if (listen(shared_->fd_, 500) < 0) {
		perror("listen error");
		return false;
	}

	shared_->port_ = port;
	io_.start(shared_->fd_, ev::READ);
	reserveStack();
	return true;
}

void Listener::io_accept(ev::io &watcher, int revents) {
	if (ev::ERROR & revents) {
		perror("got invalid event");
		return;
	}

	struct sockaddr_in client_addr;
	socklen_t client_len = sizeof(client_addr);
#ifndef __APPLE__
	int client_fd = accept4(watcher.fd, reinterpret_cast<struct sockaddr *>(&client_addr), &client_len, SOCK_NONBLOCK);
#else
	int client_fd = accept(watcher.fd, reinterpret_cast<struct sockaddr *>(&client_addr), &client_len);
	if (client_fd >= 0) {
		fcntl(client_fd, F_SETFL, O_NONBLOCK);
	}
#endif
	if (client_fd < 0) return;

	if (idleConns_) {
		connectons_[--idleConns_]->Restart(client_fd);
	} else {
		connectons_.push_back(std::unique_ptr<IConnection>(shared_->connFactory_(loop_, client_fd)));
	}
	connCount_++;
	shared_->lck_.lock();
	if (shared_->count_ < shared_->maxListeners_) {
		shared_->count_++;
		std::thread th(&Listener::clone, this);
		th.detach();
	}
	shared_->lck_.unlock();
}

void Listener::timeout_cb(ev::periodic &, int) {
	reindexer::logPrintf(LogInfo, "Listener(%d) %d stats: %d connections, %d idle", shared_->port_, id_, connectons_.size() - idleConns_,
						 idleConns_);
	loop_.break_loop();
}

void Listener::Fork(int clones) {
	for (int i = 0; i < clones; i++) {
		std::thread th(&Listener::clone, this);
		th.detach();
		shared_->count_++;
	}
}

void Listener::Run() {
	for (;;) {
		loop_.run();

		assert(idleConns_ <= int(connectons_.size()));
		for (auto it = connectons_.begin() + idleConns_; it != connectons_.end(); it++) {
			if ((*it)->IsFinished()) {
				std::swap(*it, connectons_[idleConns_]);
				++idleConns_;
				connCount_--;
			}
		}
		//		if (root_==this && connectons_.size() - idleConns_ == 0) break;
	}
}

void Listener::clone() {
	ev::dynamic_loop loop;
	Listener listener(loop, shared_);

	listener.io_.start(shared_->fd_, ev::READ);
	//	listener.Bind(port_);
	listener.Run();
}

void Listener::reserveStack() {
	char placeholder[0x40000];
	for (size_t i = 0; i < sizeof(placeholder); i += 4096) placeholder[i] = i & 0xFF;
}

Listener::Shared::Shared(ConnectionFactory connFactory, int maxListeners)
	: fd_(-1), maxListeners_(maxListeners), count_(1), connFactory_(connFactory) {}

Listener::Shared::~Shared() { close(fd_); }

}  // namespace net
}  // namespace reindexer
