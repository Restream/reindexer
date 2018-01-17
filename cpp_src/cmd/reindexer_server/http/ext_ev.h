#pragma once

#ifdef USE_LIBEV
#include "ev++.h"

#else

#include <assert.h>

#ifndef __APPLE__
#include <sys/epoll.h>
#else
#include <sys/select.h>
#include <sys/types.h>
#endif

#include <chrono>
#include <functional>
#include <vector>

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
class dynamic_loop {
	friend class loop_ref;
	friend class loop_epoll_backend;
	friend class loop_select_backend;

public:
	dynamic_loop();
	void run();
	void break_loop();

protected:
	void set(int fd, io *watcher, int events);
	void set(timer *watcher, double t);
	void stop(int fd);
	void stop(timer *watcher);
	void callback(int fd, int events);

	struct fd_handler {
		int emask_ = 0;
		io *watcher_ = nullptr;
	};

	std::vector<fd_handler> fds_;
	std::vector<timer *> timers_;
	bool break_ = false;
	loop_backend backend_;
};

class loop_ref {
	friend class io;
	friend class timer;

public:
	void break_loop() {
		if (loop_) loop_->break_loop();
	}
	void run() {
		if (loop_) loop_->run();
	}

protected:
	void set(int fd, io *watcher, int events) {
		if (loop_) loop_->set(fd, watcher, events);
	}
	void set(timer *watcher, double t) {
		if (loop_) loop_->set(watcher, t);
	}
	void stop(int fd) {
		if (loop_) loop_->stop(fd);
	}
	void stop(timer *watcher) {
		if (loop_) loop_->stop(watcher);
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
		func_ = func_wrapper<K, func>;
		object_ = object;
	}

	int fd = -1;
	loop_ref loop;

protected:
	template <class K, void (K::*func)(io &, int events)>
	static void func_wrapper(void *obj, io &watcher, int events) {
		return (static_cast<K *>(obj)->*func)(watcher, events);
	}
	void callback(int events) {
		assert(func_ != nullptr);
		func_(object_, *this, events);
	}

	std::function<void(void *obj, io &watcher, int events)> func_ = nullptr;
	void *object_;
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
		func_ = func_wrapper<K, func>;
		object_ = object;
	}

	loop_ref loop;
	std::chrono::time_point<std::chrono::steady_clock> deadline_;

protected:
	template <class K, void (K::*func)(timer &, int t)>
	static void func_wrapper(void *obj, timer &watcher, int t) {
		return (static_cast<K *>(obj)->*func)(watcher, t);
	}
	void callback(int tv) {
		assert(func_ != nullptr);
		func_(object_, *this, tv);
		if (period_ != 0) {
			loop.set(this, period_);
		}
	}

	std::function<void(void *obj, timer &watcher, int t)> func_ = nullptr;
	void *object_;
	double period_ = 0;
};

using periodic = timer;

}  // namespace ev
#endif

namespace ev {
extern bool gEnableBusyLoop;
}