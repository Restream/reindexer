#include "listener.h"
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <unistd.h>
#include <thread>
#include "connection.h"
#include "core/type_consts.h"
#include "tools/logger.h"

#ifndef SOL_TCP
#define SOL_TCP IPPROTO_TCP
#endif

namespace reindexer_server {

namespace http {

atomic<int> Listener::listeners_;
atomic<int> Listener::counter_;

Listener::Listener(ev::dynamic_loop &loop, Router &router, int maxListeners)
	: loop_(loop),
	  router_(router),
	  fd_(-1),
	  maxListeners_(maxListeners ? maxListeners : std::thread::hardware_concurrency()),
	  idleConns_(0),
	  id_(counter_++) {
	io_.set<Listener, &Listener::io_accept>(this);
	io_.set(loop);
	// timer_.set<Listener, &Listener::timeout_cb>(this);
	// timer_.set(loop);
	// timer_.start(15., 15.);
	if (!id_) listeners_++;
}

Listener::~Listener() {
	close(fd_);
	io_.stop();

	listeners_--;
}

bool Listener::Bind(int port) {
	if (!root_) listeners_++;

	if (fd_ >= 0) {
		return false;
	}

	if ((fd_ = socket(PF_INET, SOCK_STREAM, 0)) < 0) {
		perror("socket error");
		return false;
	}

	int enable = 1;
	if (setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(int)) < 0) {
		perror("setsockopt(SO_REUSEADDR) failed");
	}
	if (setsockopt(fd_, SOL_SOCKET, SO_REUSEPORT, &enable, sizeof(int)) < 0) {
		perror("setsockopt(SO_REUSEPORT) failed");
	}
	if (setsockopt(fd_, SOL_TCP, TCP_NODELAY, &enable, sizeof(int)) < 0) {
		perror("setsockopt(TCP_NODELAY) failed");
	}
#ifndef __APPLE__
	if (setsockopt(fd_, SOL_TCP, TCP_DEFER_ACCEPT, &enable, sizeof(int)) < 0) {
		perror("setsockopt(TCP_DEFER_ACCEPT) failed");
	}
	if (setsockopt(fd_, SOL_TCP, TCP_QUICKACK, &enable, sizeof(int)) < 0) {
		perror("setsockopt(TCP_QUICKACK) failed");
	}
#endif
	struct sockaddr_in addr;
	bzero(&addr, sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_port = htons(port);
	addr.sin_addr.s_addr = INADDR_ANY;

	fcntl(fd_, F_SETFL, fcntl(fd_, F_GETFL, 0) | O_NONBLOCK);

	if (bind(fd_, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
		perror("bind error");
		return false;
	}

	if (listen(fd_, 500) < 0) {
		perror("listen error");
		return false;
	}

	port_ = port;
	io_.start(fd_, ev::READ);
	reserveStack();
	return true;
}

void Listener::io_accept(ev::io &watcher, int revents) {
	if (ev::ERROR & revents) {
		perror("got invalid event");
		return;
	}

	for (;;) {
		struct sockaddr_in client_addr;
		socklen_t client_len = sizeof(client_addr);
#ifndef __APPLE__
		int client_fd = accept4(watcher.fd, (struct sockaddr *)&client_addr, &client_len, SOCK_NONBLOCK);
#else
		int client_fd = accept(watcher.fd, (struct sockaddr *)&client_addr, &client_len);
		if (client_fd >= 0) {
			fcntl(client_fd, F_SETFL, O_NONBLOCK);
		}
#endif
		if (client_fd < 0) return;

		if (idleConns_) {
			connectons_[--idleConns_]->Restart(client_fd);
		} else {
			connectons_.push_back(new Connection(client_fd, loop_, router_));
		}
		if (++listeners_ <= maxListeners_) {
			std::thread th(&Listener::clone, this->root_ ? this->root_ : this);
			th.detach();
		} else {
			listeners_--;
		}
	}
}

void Listener::timeout_cb(ev::periodic &, int) {
	reindexer::logPrintf(LogInfo, "Listener %d stats: %d connections, %d idle", id_, connectons_.size() - idleConns_, idleConns_);
}

void Listener::Fork(int clones) {
	for (int i = 0; i < clones; i++) {
		std::thread th(&Listener::clone, this);
		th.detach();
		listeners_++;
	}
}

void Listener::Run() {
	for (;;) {
		loop_.run();

		assert(idleConns_ <= (int)connectons_.size());
		for (auto it = connectons_.begin() + idleConns_; it != connectons_.end(); it++) {
			if ((*it)->IsFinished()) {
				std::swap(*it, connectons_[idleConns_]);
				++idleConns_;
			}
		}
		//	if (root_ && connectons_.size() == 0) break;
	}
}

void Listener::clone() {
	ev::dynamic_loop loop;
	Listener listener(loop, router_, maxListeners_);

	listener.root_ = this;
	listener.fd_ = fd_;
	listener.port_ = port_;
	listener.io_.start(fd_, ev::READ);
	//	listener.Bind(port_);
	listener.Run();
}

void Listener::reserveStack() {
	char placeholder[0x40000];
	for (size_t i = 0; i < sizeof(placeholder); i += 4096) placeholder[i] = (char)i;
}

}  // namespace http
}  // namespace reindexer_server
