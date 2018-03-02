#pragma once

#include <assert.h>

#ifndef __APPLE__
#include <sys/epoll.h>
#else
#include <sys/select.h>
#include <sys/types.h>
#endif

#include <atomic>
#include <chrono>
#include <functional>
#include <vector>

namespace reindexer {
namespace net {
namespace ev {
const int READ = 0x01;
const int WRITE = 0x02;
const int HUP = 0x4;
const int ERROR = 0x08;

class dynamic_loop;

class loop_select_backend {
public:
	void init(dynamic_loop *owner);
	void set(int fd, int events, int oldevents);
	void stop(int fd);
	int runonce(int64_t tv);

protected:
	dynamic_loop *owner_;
	fd_set rfds_, wfds_;
	int maxfd_;
};

#ifndef __APPLE__
class loop_epoll_backend {
public:
	void init(dynamic_loop *owner);
	void set(int fd, int events, int oldevents);
	void stop(int fd);
	int runonce(int64_t tv);

protected:
	dynamic_loop *owner_;
	int ctlfd_ = -1;
	std::vector<epoll_event> events_;
};
using loop_backend = loop_epoll_backend;
#else
using loop_backend = loop_select_backend;

#endif

class io;
class timer;
class async;
class sig;
class dynamic_loop {
	friend class loop_ref;
	friend class loop_epoll_backend;
	friend class loop_select_backend;

public:
	dynamic_loop();
	~dynamic_loop();
	void run();
	void break_loop();

protected:
	void set(int fd, io *watcher, int events);
	void set(timer *watcher, double t);
	void set(async *watcher);
	void set(sig *watcher);
	void stop(int fd);
	void stop(timer *watcher);
	void stop(async *watcher);
	void stop(sig *watcher);
	void send(async *watcher);

	void callback(int fd, int events);

	struct fd_handler {
		int emask_ = 0;
		io *watcher_ = nullptr;
	};

	std::vector<fd_handler> fds_;
	std::vector<timer *> timers_;
	std::vector<async *> asyncs_;
	std::vector<sig *> sigs_;
	bool break_ = false;
	loop_backend backend_;
	int async_fds_[2] = {-1, -1};
};

class loop_ref {
	friend class io;
	friend class timer;
	friend class sig;
	friend class async;

public:
	void break_loop() {
		if (loop_) loop_->break_loop();
	}
	void run() {
		if (loop_) loop_->run();
	}

protected:
	template <typename... Args>
	void set(Args... args) {
		if (loop_) loop_->set(args...);
	}
	template <typename... Args>
	void stop(Args... args) {
		if (loop_) loop_->stop(args...);
	}
	template <typename... Args>
	void send(Args... args) {
		if (loop_) loop_->send(args...);
	}
	dynamic_loop *loop_ = nullptr;
};

class io {
	friend class dynamic_loop;

public:
	io(){};
	io(const io &) = delete;
	~io() { stop(); }

	void set(dynamic_loop &loop_) { loop.loop_ = &loop_; }
	void set(int events) { loop.set(fd, this, events); }
	void start(int _fd, int events) {
		fd = _fd;
		loop.set(fd, this, events);
	}
	void stop() { loop.stop(fd); }

	template <typename K, void (K::*func)(io &, int events)>
	void set(K *object) {
		func_ = [object](io &watcher, int events) { (static_cast<K *>(object)->*func)(watcher, events); };
	}
	void set(std::function<void(io &watcher, int events)> func) { func_ = func; }

	int fd = -1;
	loop_ref loop;

protected:
	void callback(int events) {
		assert(func_ != nullptr);
		func_(*this, events);
	}
	std::function<void(io &watcher, int events)> func_ = nullptr;
};
class timer {
	friend class dynamic_loop;

public:
	timer(){};
	timer(const timer &) = delete;
	~timer() { stop(); }

	void set(dynamic_loop &loop_) { loop.loop_ = &loop_; }
	void start(double t, double p = 0) {
		period_ = p;
		loop.set(this, t);
	}
	void stop() { loop.stop(this); }

	template <typename K, void (K::*func)(timer &, int t)>
	void set(K *object) {
		func_ = [object](timer &watcher, int t) { (static_cast<K *>(object)->*func)(watcher, t); };
	}
	void set(std::function<void(timer &, int)> func) { func_ = func; }

	loop_ref loop;
	std::chrono::time_point<std::chrono::steady_clock> deadline_;

protected:
	void callback(int tv) {
		assert(func_ != nullptr);
		func_(*this, tv);
		if (period_ > 0.00000001) {
			loop.set(this, period_);
		}
	}

	std::function<void(timer &watcher, int t)> func_ = nullptr;
	double period_ = 0;
};

using periodic = timer;

class sig {
	friend class dynamic_loop;

public:
	sig(){};
	sig(const sig &) = delete;
	~sig() { stop(); }

	void set(dynamic_loop &loop_) { loop.loop_ = &loop_; }
	void start(int signum) {
		signum_ = signum;
		loop.set(this);
	}
	void stop() { loop.stop(this); }

	template <typename K, void (K::*func)(sig &)>
	void set(K *object) {
		func_ = [object](sig &watcher) { (static_cast<K *>(object)->*func)(watcher); };
	}
	void set(std::function<void(sig &)> func) { func_ = func; }

	loop_ref loop;

protected:
	void callback() {
		assert(func_ != nullptr);
		func_(*this);
	}

	std::function<void(sig &watcher)> func_ = nullptr;
	void (*old_handler_)(int) = nullptr;
	int signum_;
};

class async {
	friend class dynamic_loop;

public:
	async() : sent_(false){};
	async(const sig &) = delete;
	~async() { stop(); }

	void set(dynamic_loop &loop_) { loop.loop_ = &loop_; }
	void start() { loop.set(this); }
	void stop() { loop.stop(this); }
	void send() {
		sent_ = true;
		loop.send(this);
	}

	template <typename K, void (K::*func)(async &)>
	void set(K *object) {
		func_ = [object](async &watcher) { (static_cast<K *>(object)->*func)(watcher); };
	}
	void set(std::function<void(async &)> func) { func_ = func; }

	loop_ref loop;

protected:
	void callback() {
		assert(func_ != nullptr);
		func_(*this);
	}

	std::function<void(async &watcher)> func_ = nullptr;
	std::atomic<bool> sent_;
};

extern bool gEnableBusyLoop;

}  // namespace ev
}  // namespace net
}  // namespace reindexer
